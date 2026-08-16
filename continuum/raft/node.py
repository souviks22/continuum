"""Raft leader election and log replication.

Step 2.1 (node states, randomized election timeouts, RequestVote,
durable term/vote persistence), Step 2.2 (real AppendEntries, log
matching/conflict resolution via RaftLog, commit index advancement with
Raft's current-term restriction, committed entries applied via an
injected `apply_fn`), and Step 2.3: InstallSnapshot for followers who've
fallen behind whatever the leader has already compacted away, plus real
wiring to Phase 1's StateMachine/SnapshotStore so this is an actual
replicated KV store rather than an inert log. `take_snapshot()`
snapshots the state machine and compacts the log to match; a lagging
follower's `_replicate_to_peer` automatically falls back to
InstallSnapshot instead of AppendEntries once its nextIndex has fallen
at or before the leader's compaction boundary.
"""

from __future__ import annotations

import random
from enum import Enum, auto
from typing import Any, Callable, Optional

from continuum.raft.log import LogCompactedError, LogEntry, RaftLog
from continuum.raft.persistent_state import RaftPersistentState
from continuum.rpc.endpoint import RpcEndpoint
from continuum.sim.scheduler import EventHandle, EventScheduler
from continuum.storage.snapshot import SnapshotStore
from continuum.storage.statemachine import Operation, StateMachine
from continuum.storage.wal import WriteAheadLog


class NodeState(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        rpc: RpcEndpoint,
        scheduler: EventScheduler,
        persistent_state: RaftPersistentState,
        log_wal: WriteAheadLog,
        rng: Optional[random.Random] = None,
        election_timeout_range: tuple[int, int] = (150, 300),
        heartbeat_interval: int = 50,
        rpc_timeout: int = 100,
        apply_fn: Optional[Callable[[dict[str, Any]], None]] = None,
        state_machine: Optional[StateMachine] = None,
        snapshot_store: Optional[SnapshotStore] = None,
        snapshot_threshold: Optional[int] = None,
    ) -> None:
        self.node_id = node_id
        self.peers = list(peers)
        self._rpc = rpc
        self._scheduler = scheduler
        self._persistent = persistent_state
        self._rng = rng if rng is not None else random.Random()
        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval
        self._rpc_timeout = rpc_timeout
        self._apply_fn = apply_fn
        self._state_machine = state_machine
        self._snapshot_store = snapshot_store
        self._snapshot_threshold = snapshot_threshold

        self.state = NodeState.FOLLOWER
        self._votes_received: set[str] = set()
        self._election_timer: Optional[EventHandle] = None
        self._heartbeat_timer: Optional[EventHandle] = None

        self.log = RaftLog(log_wal)
        self.commit_index = -1
        self.last_applied = -1
        # Leader-only volatile state (Raft ยง5.3); reinitialized every
        # time this node becomes leader, meaningless otherwise.
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}

        # If a snapshot already exists on disk (from a prior run), load
        # it before anything else -- last_applied jumps straight to what
        # the snapshot covers, and only entries after that get replayed
        # by the normal commit/apply path below.
        if self._state_machine is not None and self._snapshot_store is not None:
            snapshot = self._snapshot_store.read()
            if snapshot is not None:
                state, last_index = snapshot
                self._state_machine.load_state(state)
                self.last_applied = last_index
                self.commit_index = last_index

        rpc.register_method("RequestVote", self._on_request_vote)
        rpc.register_method("AppendEntries", self._on_append_entries)
        rpc.register_method("InstallSnapshot", self._on_install_snapshot)

    @property
    def last_log_index(self) -> int:
        return self.log.last_index

    @property
    def last_log_term(self) -> int:
        return self.log.last_term

    # -- persisted state, exposed read-only -------------------------------

    @property
    def current_term(self) -> int:
        return self._persistent.current_term

    @property
    def voted_for(self) -> Optional[str]:
        return self._persistent.voted_for

    # -- lifecycle -----------------------------------------------------

    def start(self) -> None:
        self._reset_election_timer()

    def stop(self) -> None:
        self._cancel_election_timer()
        self._cancel_heartbeat_timer()

    # -- timers -----------------------------------------------------------

    def _reset_election_timer(self) -> None:
        self._cancel_election_timer()
        timeout = self._rng.randint(*self._election_timeout_range)
        self._election_timer = self._scheduler.schedule_after(
            timeout, self._on_election_timeout
        )

    def _cancel_election_timer(self) -> None:
        if self._election_timer is not None:
            self._election_timer.cancel()
            self._election_timer = None

    def _cancel_heartbeat_timer(self) -> None:
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

    # -- term transitions ---------------------------------------------------

    def _become_follower_for_new_term(self, new_term: int) -> None:
        """Called whenever we observe a term higher than ours, from any
        RPC or reply. Raft requires resetting votedFor when the term
        advances, since a vote is only meaningful within the term it was
        cast for."""
        self._persistent.set_term_and_vote(new_term, None)
        self.state = NodeState.FOLLOWER
        self._cancel_heartbeat_timer()
        self._reset_election_timer()

    def _recognize_current_leader(self) -> None:
        """Called on a valid heartbeat/AppendEntries from a leader in
        our current term (not a higher one). If we were a candidate,
        stop competing -- but don't touch votedFor; our vote this term
        (if any) is still valid history, just no longer relevant since
        we've stopped contending ourselves."""
        if self.state != NodeState.FOLLOWER:
            self.state = NodeState.FOLLOWER
            self._cancel_heartbeat_timer()
        self._reset_election_timer()

    # -- becoming a candidate / running an election ------------------------

    def _on_election_timeout(self) -> None:
        if self.state == NodeState.LEADER:
            return  # leaders don't run election timers; stay safe if this ever fires anyway
        self._start_election()

    def _start_election(self) -> None:
        self.state = NodeState.CANDIDATE
        self._persistent.set_term_and_vote(self.current_term + 1, self.node_id)
        self._votes_received = {self.node_id}
        self._reset_election_timer()

        if self._has_majority():
            # Covers the single-node-cluster case (no peers to wait on)
            # and, in general, any case where the self-vote alone is
            # already enough -- don't wait on RPC replies that will
            # never arrive to grant a majority we already have.
            self._become_leader()
            return

        election_term = self.current_term  # snapshot: guards stale replies below
        for peer in self.peers:
            body = {
                "term": election_term,
                "candidate_id": self.node_id,
                "last_log_index": self.last_log_index,
                "last_log_term": self.last_log_term,
            }
            self._rpc.call(
                peer,
                "RequestVote",
                body,
                timeout=self._rpc_timeout,
                on_reply=lambda reply, timed_out, term=election_term, p=peer: (
                    self._on_vote_reply(term, p, reply, timed_out)
                ),
            )

    def _on_vote_reply(
        self,
        election_term: int,
        peer: str,
        reply: Optional[dict[str, Any]],
        timed_out: bool,
    ) -> None:
        if self.state != NodeState.CANDIDATE or self.current_term != election_term:
            return  # stale: we've moved to a new term or aren't a candidate anymore
        if timed_out or reply is None:
            return  # unreachable peer this round -- simply doesn't count
        if reply["term"] > self.current_term:
            self._become_follower_for_new_term(reply["term"])
            return
        if reply.get("vote_granted"):
            self._votes_received.add(peer)
            if self._has_majority() and self.state == NodeState.CANDIDATE:
                self._become_leader()

    def _has_majority(self) -> bool:
        total_nodes = len(self.peers) + 1
        majority = total_nodes // 2 + 1
        return len(self._votes_received) >= majority

    # -- becoming leader / replication ---------------------------------------

    def _become_leader(self) -> None:
        self.state = NodeState.LEADER
        self._cancel_election_timer()
        for peer in self.peers:
            self._next_index[peer] = self.log.last_index + 1
            self._match_index[peer] = -1
        self._replicate_to_all()

    def propose(self, command: dict[str, Any]) -> Optional[int]:
        """Client-facing entry point: if we're the leader, append
        `command` to our log and kick off replication, returning the
        assigned log index immediately. That index is NOT yet
        committed/durable-across-the-cluster at this point -- the caller
        finds out an entry actually committed via the `apply_fn`
        callback (or by polling commit_index/last_applied). Returns None
        if we're not the leader; callers must handle that by finding the
        real leader (client-routing logic, not this class's job)."""
        if self.state != NodeState.LEADER:
            return None
        entry = self.log.append(self.current_term, command)
        self._match_index[self.node_id] = entry.index  # not read anywhere, kept for symmetry
        self._replicate_to_all()
        return entry.index

    def _replicate_to_all(self) -> None:
        if self.state != NodeState.LEADER:
            return
        for peer in self.peers:
            self._replicate_to_peer(peer)
        self._heartbeat_timer = self._scheduler.schedule_after(
            self._heartbeat_interval, self._replicate_to_all
        )

    def _replicate_to_peer(self, peer: str) -> None:
        term = self.current_term
        next_idx = self._next_index.get(peer, self.log.last_index + 1)
        if next_idx <= self.log.last_included_index:
            self._send_install_snapshot(peer, term)
            return
        prev_log_index = next_idx - 1
        prev_log_term = self.log.term_at(prev_log_index)
        try:
            entries = self.log.entries_from(next_idx)
        except LogCompactedError:
            # Can only happen if compaction raced with this call; not
            # possible in this single-threaded, event-driven model today
            # (take_snapshot() is never called mid-replication), but
            # falling back rather than crashing keeps this correct even
            # if that invariant changes later.
            self._send_install_snapshot(peer, term)
            return
        body = {
            "term": term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": [e.__dict__ for e in entries],
            "leader_commit": self.commit_index,
        }
        sent_up_to = prev_log_index + len(entries)
        self._rpc.call(
            peer,
            "AppendEntries",
            body,
            timeout=self._rpc_timeout,
            on_reply=lambda reply, timed_out, term=term, peer=peer, sent_up_to=sent_up_to: (
                self._on_replicate_reply(term, peer, sent_up_to, reply, timed_out)
            ),
        )

    def _on_replicate_reply(
        self,
        term: int,
        peer: str,
        sent_up_to: int,
        reply: Optional[dict[str, Any]],
        timed_out: bool,
    ) -> None:
        if self.state != NodeState.LEADER or self.current_term != term:
            return  # stale reply from a round we're no longer running
        if timed_out or reply is None:
            return
        if reply["term"] > self.current_term:
            self._become_follower_for_new_term(reply["term"])
            return
        if reply["success"]:
            self._match_index[peer] = sent_up_to
            self._next_index[peer] = sent_up_to + 1
            self._try_advance_commit_index()
        else:
            conflict_index = reply.get("conflict_index")
            if conflict_index is not None:
                self._next_index[peer] = conflict_index
            else:
                self._next_index[peer] = max(0, self._next_index.get(peer, 1) - 1)
            self._replicate_to_peer(peer)  # retry immediately with the backed-off index

    def _try_advance_commit_index(self) -> None:
        for candidate in range(self.commit_index + 1, self.log.last_index + 1):
            if self.log.term_at(candidate) != self.current_term:
                # Raft safety: never commit a previous-term entry purely
                # by counting replicas -- only a current-term entry can
                # be committed this way. It gets pulled in indirectly
                # once a later current-term entry at a higher index
                # commits (Raft's Figure 8 hazard is exactly what this
                # guards against).
                continue
            replica_count = 1 + sum(
                1 for peer in self.peers if self._match_index.get(peer, -1) >= candidate
            )
            if replica_count >= self._majority_count():
                self.commit_index = candidate
        self._apply_committed()

    def _majority_count(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry is None:
                continue
            if self._apply_fn is not None:
                self._apply_fn(entry.command)
            if self._state_machine is not None:
                self._apply_to_state_machine(entry.command)
        if (
            self._snapshot_threshold is not None
            and self._snapshot_store is not None
            and self._state_machine is not None
            and self.last_applied - self.log.last_included_index >= self._snapshot_threshold
        ):
            self.take_snapshot()

    def _apply_to_state_machine(self, command: dict[str, Any]) -> None:
        operation = Operation(
            index=self.last_applied,
            op=command["op"],
            key=command["key"],
            value=command.get("value"),
        )
        self._state_machine.apply(operation)

    def take_snapshot(self) -> None:
        """Snapshot the state machine as of last_applied and compact the
        log to match. Safe to call any time (a no-op if there's nothing
        new since the last snapshot); also invoked automatically once
        `snapshot_threshold` newly-applied entries have accumulated, if
        one was configured."""
        if self._state_machine is None or self._snapshot_store is None:
            return
        last_included_index = self.last_applied
        if last_included_index <= self.log.last_included_index:
            return
        state = self._state_machine.snapshot_state()
        self._snapshot_store.write(state, last_included_index)
        self.log.compact(last_included_index)

    # -- InstallSnapshot: leader side -----------------------------------

    def _send_install_snapshot(self, peer: str, term: int) -> None:
        if self._state_machine is None:
            return  # nothing to send without a state machine to snapshot
        last_included_index = self.log.last_included_index
        body = {
            "term": term,
            "leader_id": self.node_id,
            "last_included_index": last_included_index,
            "last_included_term": self.log.last_included_term,
            "state": self._state_machine.snapshot_state(),
        }
        self._rpc.call(
            peer,
            "InstallSnapshot",
            body,
            timeout=self._rpc_timeout,
            on_reply=lambda reply, timed_out, term=term, peer=peer, lii=last_included_index: (
                self._on_install_snapshot_reply(term, peer, lii, reply, timed_out)
            ),
        )

    def _on_install_snapshot_reply(
        self,
        term: int,
        peer: str,
        last_included_index: int,
        reply: Optional[dict[str, Any]],
        timed_out: bool,
    ) -> None:
        if self.state != NodeState.LEADER or self.current_term != term:
            return
        if timed_out or reply is None:
            return
        if reply["term"] > self.current_term:
            self._become_follower_for_new_term(reply["term"])
            return
        self._match_index[peer] = max(self._match_index.get(peer, -1), last_included_index)
        self._next_index[peer] = last_included_index + 1
        self._try_advance_commit_index()

    # -- RPC handlers -----------------------------------------------------

    def _on_request_vote(self, from_node: str, body: dict[str, Any]) -> dict[str, Any]:
        term = body["term"]
        candidate_id = body["candidate_id"]
        candidate_last_log_term = body["last_log_term"]
        candidate_last_log_index = body["last_log_index"]

        if term > self.current_term:
            self._become_follower_for_new_term(term)

        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        log_ok = candidate_last_log_term > self.last_log_term or (
            candidate_last_log_term == self.last_log_term
            and candidate_last_log_index >= self.last_log_index
        )
        can_vote = self.voted_for is None or self.voted_for == candidate_id
        if can_vote and log_ok:
            self._persistent.set_term_and_vote(self.current_term, candidate_id)
            self._reset_election_timer()  # trusting this candidate; don't compete immediately
            return {"term": self.current_term, "vote_granted": True}
        return {"term": self.current_term, "vote_granted": False}

    def _on_append_entries(self, from_node: str, body: dict[str, Any]) -> dict[str, Any]:
        term = body["term"]
        if term > self.current_term:
            self._become_follower_for_new_term(term)
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
        self._recognize_current_leader()

        prev_log_index = body["prev_log_index"]
        prev_log_term = body["prev_log_term"]
        entries = [LogEntry(**e) for e in body["entries"]]
        leader_commit = body["leader_commit"]

        ok = self.log.append_entries_from_leader(prev_log_index, prev_log_term, entries)
        if not ok:
            conflict_index = self._compute_conflict_index(prev_log_index, prev_log_term)
            return {
                "term": self.current_term,
                "success": False,
                "conflict_index": conflict_index,
            }

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.last_index)
            self._apply_committed()

        return {"term": self.current_term, "success": True}

    def _compute_conflict_index(self, prev_log_index: int, prev_log_term: int) -> int:
        """Raft's nextIndex backtracking optimization: instead of the
        leader decrementing nextIndex by one and retrying on every single
        conflict (an O(log length) round trip in the worst case), tell it
        exactly where to jump to."""
        if prev_log_index > self.log.last_index:
            return self.log.last_index + 1  # our log is simply too short
        conflicting_term = self.log.term_at(prev_log_index)
        idx = prev_log_index
        while idx > 0 and self.log.term_at(idx - 1) == conflicting_term:
            idx -= 1
        return idx

    def _on_install_snapshot(self, from_node: str, body: dict[str, Any]) -> dict[str, Any]:
        term = body["term"]
        if term > self.current_term:
            self._become_follower_for_new_term(term)
        if term < self.current_term:
            return {"term": self.current_term}
        self._recognize_current_leader()

        last_included_index = body["last_included_index"]
        last_included_term = body["last_included_term"]
        if last_included_index <= self.log.last_included_index:
            return {"term": self.current_term}  # stale/duplicate: we're already this current

        if self._state_machine is not None:
            self._state_machine.load_state(body["state"])
        if self._snapshot_store is not None:
            self._snapshot_store.write(body["state"], last_included_index)
        self.log.install_snapshot(last_included_index, last_included_term)
        self.commit_index = max(self.commit_index, last_included_index)
        self.last_applied = last_included_index

        return {"term": self.current_term}
     