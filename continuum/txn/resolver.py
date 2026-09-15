"""Lock resolver: what a reader does when it hits a blocking lock,
instead of just failing.

Percolator's actual algorithm: check the lock's primary key. If the
primary shows a committed write record for this start_ts, the whole
transaction committed -- roll the secondary forward by writing the same
commit_ts here too. If the primary shows no trace of this transaction
(no lock, no write) -- or still shows a lock old enough to presume the
coordinator crashed -- the transaction never completed; roll the
secondary back by releasing its lock.

This is inherently cross-shard: the primary key can live on a
completely different shard than the key being read, so resolution needs
its own shard lookup (`shard_for_key`), exactly like the Transaction
coordinator's.

TTL, simplified for this scope: real Percolator gates staleness on wall-
clock time (a lock older than N seconds is presumed abandoned). This
codebase has no wall clock at the state-machine layer by design (a
replicated state machine reading its own clock during apply would be
non-deterministic across replicas applying at different virtual times --
see mvcc.py's docstring for the same principle). Instead, staleness is
judged by how many TSO timestamps have been allocated since the lock's
start_ts: `current_ts - lock.start_ts >= min_ts_age`. Timestamps
advance roughly in proportion to real system activity, so this is a
reasonable proxy, not a literal wall-clock TTL -- worth naming
explicitly rather than pretending it's the same thing.
"""

from __future__ import annotations

from typing import Callable, Optional

from continuum.raft.node import RaftNode
from continuum.txn.store import Lock
from continuum.txn.statemachine import TransactionStateMachine

ResultCallback = Callable[[Optional[str], Optional[str]], None]  # (value, error) -> None


class TransactionNode(RaftNode):
    def __init__(self, state_machine: Optional[TransactionStateMachine], *args, **kwargs) -> None:
        super().__init__(state_machine, *args, **kwargs)
        self._state_machine = state_machine

    @property
    def state_machine(self) -> Optional[TransactionStateMachine]:
        return self._state_machine


class LockResolver:
    def __init__(self, shard_for_key: Callable[[str], TransactionNode], min_ts_age: int = 100) -> None:
        self._shard_for_key = shard_for_key
        self._min_ts_age = min_ts_age

    def resolve(
        self, key: str, lock: Lock, read_ts: int, current_ts: int, on_result: ResultCallback
    ) -> None:
        """Attempt to resolve a lock blocking a read of `key` at
        `read_ts`. `current_ts` (a fresh timestamp, not the read's own
        read_ts) is what staleness is judged against -- using read_ts
        itself would let an old snapshot read masquerade as a reason to
        force-resolve a lock that's actually still perfectly active."""
        if current_ts - lock.start_ts < self._min_ts_age:
            on_result(
                None,
                f"key {key!r} is locked by a transaction that started recently "
                f"(start_ts={lock.start_ts}); not stale enough to resolve, retry shortly",
            )
            return

        primary_node = self._shard_for_key(lock.primary_key)
        commit_ts = primary_node.state_machine.find_commit_ts(lock.primary_key, lock.start_ts)
        if commit_ts is not None:
            self._roll_forward(key, lock, commit_ts, read_ts, on_result)
            return

        # No committed write on the primary. Whether the primary is
        # still locked (transaction genuinely never finished) or shows
        # no trace at all (already rolled back by someone else, or never
        # got that far), the right move for *this* secondary is the
        # same: it can't possibly be part of a committed transaction, so
        # release its lock.
        self._roll_back(key, lock, read_ts, on_result)

    def _roll_forward(
        self, key: str, lock: Lock, commit_ts: int, read_ts: int, on_result: ResultCallback
    ) -> None:
        node = self._shard_for_key(key)
        index = node.propose({"op": "commit", "key": key, "start_ts": lock.start_ts, "commit_ts": commit_ts})
        if index is None:
            on_result(None, f"not leader for shard owning {key!r}; retry")
            return

        def on_committed() -> None:
            result = node.state_machine.get_result(index)
            if not result["success"]:
                # Someone else (another resolver, or the original
                # coordinator finally catching up) already resolved this
                # lock first -- idempotent from the caller's perspective,
                # just re-read now that *some* resolution has happened.
                pass
            value, still_blocked = node.state_machine.get_with_lock(key, read_ts)
            if still_blocked is not None:
                on_result(None, f"key {key!r} still locked after roll-forward attempt; retry")
            else:
                on_result(value, None)

        node.wait_for_commit(index, on_committed)

    def _roll_back(self, key: str, lock: Lock, read_ts: int, on_result: ResultCallback) -> None:
        node = self._shard_for_key(key)
        index = node.propose({"op": "rollback", "key": key, "start_ts": lock.start_ts})
        if index is None:
            on_result(None, f"not leader for shard owning {key!r}; retry")
            return

        def on_committed() -> None:
            value, still_blocked = node.state_machine.get_with_lock(key, read_ts)
            if still_blocked is not None:
                on_result(None, f"key {key!r} still locked after rollback attempt; retry")
            else:
                on_result(value, None)

        node.wait_for_commit(index, on_committed)
