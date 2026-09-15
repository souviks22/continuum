"""Percolator-style transaction coordinator.

Built directly against RaftNode objects (propose/wait_for_commit), the
same architectural choice Step 5.1's TimestampOracle made -- not
through the RPC-level ClientPropose path, since that currently acks on
propose rather than commit (a gap flagged when wait_for_commit was
added; building the coordinator on the correct commit-aware path here
rather than inheriting that gap). Wiring this through RPC for a real
out-of-process client is future work.

Protocol, matching Percolator:
  1. Get start_ts from the TSO.
  2. Prewrite every mutated key (one designated as primary, arbitrarily
     the first). Each prewrite stages its value in the data column and
     takes a lock, or fails on conflict.
  3. If every prewrite succeeded: get commit_ts from the TSO, commit the
     primary first (the atomicity point -- once the primary is
     committed, the transaction as a whole is considered committed even
     if secondaries haven't caught up yet), then commit secondaries
     best-effort.
  4. If any prewrite failed: roll back everything that succeeded and
     report the conflict. Nothing was ever visible to a reader in this
     case, since visibility only happens at commit (a write record),
     never at prewrite (a lock).

A secondary that fails to commit after the primary succeeds is left
locked -- exactly the scenario Step 6.2's lock-cleanup is for. This
coordinator doesn't retry or resolve that itself.
"""

from __future__ import annotations

from typing import Callable, Optional

from continuum.tso.oracle import TimestampOracle
from continuum.txn.resolver import LockResolver, TransactionNode
from continuum.txn.safe_point import SafePointCalculator

ResultCallback = Callable[[Optional[str], Optional[str]], None]  # (value, error) -> None
CommitCallback = Callable[[bool, Optional[str]], None]  # (success, error) -> None


class Transaction:
    def __init__(
        self,
        tso: TimestampOracle,
        shard_for_key: Callable[[str], TransactionNode],
        resolver: Optional[LockResolver] = None,
        safe_point_tracker: Optional[SafePointCalculator] = None,
    ) -> None:
        self._tso = tso
        self._shard_for_key = shard_for_key
        self._resolver = resolver
        self.safe_point_tracker = safe_point_tracker
        self._registered_ts: Optional[int] = None
        self._mutations: dict[str, Optional[str]] = {}
        self.start_ts: Optional[int] = None
        self.commit_ts: Optional[int] = None

    def close(self) -> None:
        """Release this transaction's hold on the GC safe point. Callers
        doing a read-only transaction (never calling commit()) must call
        this explicitly when done -- commit() calls it automatically on
        every path (success, conflict, or error), but there's no
        equivalent automatic signal for "this read-only transaction is
        finished" the way there is for a write transaction's commit.
        Worth being honest about: this is exactly the kind of thing a
        `with transaction(...) as txn:` context-manager pattern exists
        to make hard to forget, which this class doesn't yet provide."""
        if self.safe_point_tracker is not None and self._registered_ts is not None:
            self.safe_point_tracker.end(self._registered_ts)
            self._registered_ts = None

    def _register_start_ts(self) -> None:
        if self.safe_point_tracker is not None and self._registered_ts is None:
            self.safe_point_tracker.begin(self.start_ts)
            self._registered_ts = self.start_ts

    def set(self, key: str, value: str) -> None:
        self._mutations[key] = value

    def delete(self, key: str) -> None:
        self._mutations[key] = None

    # -- reads: buffered mutations first, then a real snapshot read -------

    def get(self, key: str, on_result: ResultCallback) -> None:
        if key in self._mutations:
            on_result(self._mutations[key], None)
            return
        if self.start_ts is not None:
            self._do_read(key, on_result)
            return
        self._tso.request_timestamp(lambda ts, err: self._on_read_ts(ts, err, key, on_result))

    def _on_read_ts(self, ts: Optional[int], err: Optional[str], key: str, on_result: ResultCallback) -> None:
        if err:
            on_result(None, err)
            return
        self.start_ts = ts
        self._register_start_ts()
        self._do_read(key, on_result)

    def _do_read(self, key: str, on_result: ResultCallback) -> None:
        node = self._shard_for_key(key)
        value, lock = node.state_machine.get_with_lock(key, self.start_ts)
        if lock is None:
            on_result(value, None)
            return
        if self._resolver is None:
            on_result(None, f"key {key!r} is locked by a transaction starting at {lock.start_ts}; retry")
            return
        self._tso.request_timestamp(
            lambda current_ts, err: self._on_resolve_ts(current_ts, err, key, lock, on_result)
        )

    def _on_resolve_ts(
        self, current_ts: Optional[int], err: Optional[str], key: str, lock, on_result: ResultCallback
    ) -> None:
        if err:
            on_result(None, f"failed to acquire a timestamp for lock resolution: {err}")
            return
        self._resolver.resolve(key, lock, self.start_ts, current_ts, on_result)

    # -- commit -----------------------------------------------------------

    def commit(self, on_result: CommitCallback) -> None:
        if not self._mutations:
            on_result(True, None)
            return
        wrapped_result = self._closing(on_result)
        if self.start_ts is not None:
            self._prewrite_phase(wrapped_result)
        else:
            self._tso.request_timestamp(lambda ts, err: self._on_start_ts(ts, err, wrapped_result))

    def _closing(self, on_result: CommitCallback) -> CommitCallback:
        """Wrap a commit result callback so close() always runs on the
        way out, on every path (success, conflict, or any error) --
        commit() is the one place this codebase can guarantee "this
        transaction is finished" without relying on the caller to
        remember anything."""

        def wrapped(success: bool, error: Optional[str]) -> None:
            self.close()
            on_result(success, error)

        return wrapped
    
    def _on_start_ts(self, ts: Optional[int], err: Optional[str], on_result: CommitCallback) -> None:
        if err:
            on_result(False, f"failed to acquire start_ts: {err}")
            return
        self.start_ts = ts
        self._register_start_ts()
        self._prewrite_phase(on_result)

    def _prewrite_phase(self, on_result: CommitCallback) -> None:
        keys = list(self._mutations.keys())
        primary_key = keys[0]
        results: dict[str, tuple[bool, Optional[str]]] = {}
        remaining = len(keys)

        def after_one(key: str, success: bool, reason: Optional[str]) -> None:
            nonlocal remaining
            results[key] = (success, reason)
            remaining -= 1
            if remaining == 0:
                self._after_prewrites(primary_key, keys, results, on_result)

        for key in keys:
            self._prewrite_one(key, primary_key, after_one)

    def _prewrite_one(
        self, key: str, primary_key: str, callback: Callable[[str, bool, Optional[str]], None]
    ) -> None:
        node = self._shard_for_key(key)
        command = {
            "op": "prewrite",
            "key": key,
            "value": self._mutations[key],
            "start_ts": self.start_ts,
            "primary_key": primary_key,
        }
        index = node.propose(command)
        if index is None:
            callback(key, False, f"not leader for shard owning {key!r}")
            return

        def on_committed() -> None:
            result = node.state_machine.get_result(index)
            callback(key, result["success"], result["reason"])

        node.wait_for_commit(index, on_committed)

    def _after_prewrites(
        self,
        primary_key: str,
        keys: list[str],
        results: dict[str, tuple[bool, Optional[str]]],
        on_result: CommitCallback,
    ) -> None:
        if all(success for success, _ in results.values()):
            self._tso.request_timestamp(
                lambda ts, err: self._on_commit_ts(ts, err, primary_key, keys, on_result)
            )
            return
        # At least one prewrite conflicted: nothing was ever visible to a
        # reader (visibility only happens at commit), so aborting just
        # means releasing the locks the successful prewrites took.
        first_failure = next(reason for success, reason in results.values() if not success)
        self._rollback_all(keys, lambda: on_result(False, f"prewrite conflict: {first_failure}"))

    def _on_commit_ts(
        self, ts: Optional[int], err: Optional[str], primary_key: str, keys: list[str], on_result: CommitCallback
    ) -> None:
        if err:
            self._rollback_all(keys, lambda: on_result(False, f"failed to acquire commit_ts: {err}"))
            return
        self.commit_ts = ts
        self._commit_one(
            primary_key,
            lambda success, reason: self._on_primary_committed(success, reason, primary_key, keys, on_result),
        )

    def _on_primary_committed(
        self, success: bool, reason: Optional[str], primary_key: str, keys: list[str], on_result: CommitCallback
    ) -> None:
        if not success:
            on_result(False, f"primary commit failed: {reason}")
            return
        # The transaction is durably committed from here on -- this is
        # the atomicity point, full stop. Report success now, rather
        # than waiting on the secondaries below: a partitioned or slow
        # secondary shard must never delay (or hang) the caller's
        # already-true commit result. A reader that hits a stale
        # secondary lock can resolve it via the primary (once Step 6.2
        # exists); secondaries are committed strictly best-effort, fully
        # decoupled from what this method reports.
        on_result(True, None)
        secondaries = [k for k in keys if k != primary_key]
        for key in secondaries:
            self._commit_one(key, lambda success, reason: None)

    def _commit_one(self, key: str, callback: CommitCallback) -> None:
        node = self._shard_for_key(key)
        command = {"op": "commit", "key": key, "start_ts": self.start_ts, "commit_ts": self.commit_ts}
        index = node.propose(command)
        if index is None:
            callback(False, f"not leader for shard owning {key!r}")
            return

        def on_committed() -> None:
            result = node.state_machine.get_result(index)
            callback(result["success"], result["reason"])

        node.wait_for_commit(index, on_committed)

    def _rollback_all(self, keys: list[str], on_done: Callable[[], None]) -> None:
        remaining = len(keys)
        if remaining == 0:
            on_done()
            return

        def after_one() -> None:
            nonlocal remaining
            remaining -= 1
            if remaining == 0:
                on_done()

        for key in keys:
            node = self._shard_for_key(key)
            command = {"op": "rollback", "key": key, "start_ts": self.start_ts}
            index = node.propose(command)
            if index is None:
                after_one()  # nothing was ever locked on our behalf here if we're not leader
                continue
            node.wait_for_commit(index, after_one)
