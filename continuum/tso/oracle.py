"""Timestamp Oracle: leader-side batch allocation on top of a Raft group
running TSOStateMachine.

The core safety property: never hand out a repeated or non-monotonic
timestamp, even across leader failure. The mechanism: reserve a batch
[current high-water mark + 1, current high-water mark + batch_size] with
a single Raft write, wait for that write to actually COMMIT (a majority
has durably persisted it -- not just that we proposed it locally) before
handing out anything from it, then serve individual requests from local
memory until the batch runs out. If this leader crashes with unused
timestamps still left in its batch, those are simply never handed out
again -- lost, not reused -- because the next leader starts fresh from
the durable high-water mark (which already accounts for the whole
batch, unused portion included), never from wherever the old leader's
local counter happened to be. Some timestamps get "burned" this way on
every leader change; that's the deliberate, standard trade for not
needing a Raft round-trip per timestamp request.

Also handles regaining leadership correctly: `_synced_term` tracks which
term we last reset our local batch-tracking state for, so becoming
leader again (possibly in a much later term, after this node was a
follower for a while) always re-derives `_next_to_hand_out` from the
current durable high-water mark rather than trusting whatever stale
local counter this object happened to have left over from a previous
stint as leader.
"""

from __future__ import annotations

from typing import Callable, Optional

from continuum.raft.node import NodeState, RaftNode
from continuum.tso.statemachine import TSOStateMachine

ResultCallback = Callable[[Optional[int], Optional[str]], None]  # (timestamp, error) -> None


class TimestampOracle:
    def __init__(self, node: RaftNode, state_machine: TSOStateMachine, batch_size: int = 1000) -> None:
        self._node = node
        self._sm = state_machine
        self._batch_size = batch_size
        self._next_to_hand_out: int = 0
        self._synced_term: Optional[int] = None
        self._allocation_in_flight = False
        self._pending: list[ResultCallback] = []

    def request_timestamp(self, on_result: ResultCallback) -> None:
        if self._node.state != NodeState.LEADER:
            on_result(None, "not leader")
            return
        self._ensure_synced_to_current_term()

        if self._next_to_hand_out <= self._sm.get_max_allocated():
            ts = self._next_to_hand_out
            self._next_to_hand_out += 1
            on_result(ts, None)
            return

        self._pending.append(on_result)
        if not self._allocation_in_flight:
            self._allocate_batch()

    def _ensure_synced_to_current_term(self) -> None:
        if self._synced_term == self._node.current_term:
            return
        # First request since (re)becoming leader in this term: any
        # locally-tracked "next to hand out" from a previous stint is
        # stale and must not be trusted -- re-derive strictly from the
        # durable high-water mark, and any requests that were left
        # pending from that previous stint can't be safely resumed
        # (we no longer know whether they'd land in a batch that's still
        # valid), so they're failed rather than silently carried over.
        self._next_to_hand_out = self._sm.get_max_allocated() + 1
        self._synced_term = self._node.current_term
        self._allocation_in_flight = False
        self._fail_pending("leadership changed before a batch allocation completed")

    def _allocate_batch(self) -> None:
        self._allocation_in_flight = True
        new_ceiling = self._sm.get_max_allocated() + self._batch_size
        term_at_proposal = self._node.current_term
        index = self._node.propose({"op": "allocate_batch", "up_to": new_ceiling})
        if index is None:
            self._allocation_in_flight = False
            self._fail_pending("lost leadership while allocating a new timestamp batch")
            return

        def on_committed() -> None:
            self._allocation_in_flight = False
            if self._node.state != NodeState.LEADER or self._node.current_term != term_at_proposal:
                # Stepped down (or term moved on) before/while this
                # committed. The batch is still durably valid -- a
                # future leader will see it via the state machine -- but
                # we can no longer be sure it's safe for *us* to hand out
                # timestamps from it, so fail these through instead of
                # risking a hand-out that races a new leader's own view.
                self._fail_pending("stepped down while allocating a new timestamp batch")
                return
            self._drain_pending()

        self._node.wait_for_commit(index, on_committed)

    def _drain_pending(self) -> None:
        requests, self._pending = self._pending, []
        for on_result in requests:
            if self._next_to_hand_out <= self._sm.get_max_allocated():
                ts = self._next_to_hand_out
                self._next_to_hand_out += 1
                on_result(ts, None)
            else:
                # Exhausted mid-drain (only possible if batch_size is
                # smaller than a single burst of requests) -- re-enter
                # through request_timestamp rather than duplicating the
                # allocate-a-new-batch logic here.
                self.request_timestamp(on_result)

    def _fail_pending(self, reason: str) -> None:
        requests, self._pending = self._pending, []
        for on_result in requests:
            on_result(None, reason)
            