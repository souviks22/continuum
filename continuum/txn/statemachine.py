"""Percolator state machine: adapts PercolatorStore to RaftNode's
generic ReplicatedStateMachine protocol -- the same pattern the KV,
metadata, MVCC, and TSO state machines all follow. Zero new replication
code needed to make transactional storage durable and consistent across
a cluster.

One genuine addition beyond the plain apply_command/snapshot_state/
load_state protocol: prewrite/commit/rollback can *fail* (a lock
conflict, a write-write conflict, a missing lock), and the caller
proposing that command needs to learn the outcome -- unlike a plain KV
set/delete, which always succeeds once applied. apply_command's
signature returns nothing, so results are recorded per log-index in
`_results` and fetched afterward via get_result(index), once the
coordinator's wait_for_commit callback fires. Known simplification, not
an oversight: `_results` grows unboundedly for now -- pruning it (once
a coordinator has read a result, or tying it to log/snapshot
compaction) is deferred along with lock-cleanup to a later step.
"""

from __future__ import annotations

from typing import Any, Optional, Protocol

from continuum.txn.store import Lock, PercolatorStore
from continuum.raft.node import ReplicatedStateMachine


class TransactionStateMachine(ReplicatedStateMachine, Protocol):
    """A ReplicatedStateMachine that supports the Percolator transaction
    operations prewrite/commit/rollback, plus the read operations get,
    get_with_lock, get_lock, and find_commit_ts. The latter are used by
    the Transaction coordinator to implement reads and lock resolution.
    """

    def get_result(self, index: int) -> Optional[dict[str, Any]]: ...

    def get(self, key: str, read_ts: int) -> tuple[Optional[str], Optional[str]]: ...

    def get_with_lock(self, key: str, read_ts: int) -> tuple[Optional[str], Optional[Lock]]: ...

    def get_lock(self, key: str) -> Optional[Lock]: ...

    def find_commit_ts(self, key: str, start_ts: int) -> Optional[int]: ...


class PercolatorStateMachine:
    def __init__(self) -> None:
        self._store = PercolatorStore()
        self._results: dict[int, dict[str, Any]] = {}

    # -- ReplicatedStateMachine protocol -----------------------------------

    def apply_command(self, index: int, command: dict[str, Any]) -> None:
        op = command["op"]
        if op == "prewrite":
            success, reason = self._store.prewrite(
                command["key"], command.get("value"), command["start_ts"], command["primary_key"]
            )
        elif op == "commit":
            success, reason = self._store.commit(
                command["key"], command["start_ts"], command["commit_ts"]
            )
        elif op == "rollback":
            success, reason = self._store.rollback(command["key"], command["start_ts"])
        elif op == "gc":
            self._store.gc(command["safe_point"])
            self._results[index] = {"success": True, "reason": None}
            return
        else:
            raise ValueError(f"unknown percolator operation {op!r}")
        self._results[index] = {"success": success, "reason": reason}

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "data": {k: self._store.data.versions(k) for k in self._store.data.all_keys()},
            "writes": {k: self._store.writes.versions(k) for k in self._store.writes.all_keys()},
            "locks": {
                k: {"primary_key": lock.primary_key, "start_ts": lock.start_ts}
                for k, lock in self._store.locks.items()
            },
        }

    def load_state(self, state: dict[str, Any]) -> None:
        self._store = PercolatorStore()
        for key, versions in state["data"].items():
            for ts, value in versions:
                self._store.data.write(key, ts, value)
        for key, versions in state["writes"].items():
            for ts, pointer in versions:
                self._store.writes.write(key, ts, pointer)
        for key, lock_state in state["locks"].items():
            self._store.locks[key] = Lock(
                primary_key=lock_state["primary_key"], start_ts=lock_state["start_ts"]
            )

    # -- result feedback for the proposing coordinator -----------------------

    def get_result(self, index: int) -> Optional[dict[str, Any]]:
        return self._results.get(index)

    # -- reads -----------------------------------------------------------

    def get(self, key: str, read_ts: int) -> tuple[Optional[str], Optional[str]]:
        return self._store.get(key, read_ts)

    def get_with_lock(self, key: str, read_ts: int) -> tuple[Optional[str], Optional[Lock]]:
        return self._store.get_with_lock(key, read_ts)

    def get_lock(self, key: str) -> Optional[Lock]:
        return self._store.get_lock(key)

    def find_commit_ts(self, key: str, start_ts: int) -> Optional[int]:
        return self._store.find_commit_ts(key, start_ts)


def percolator_query_fn(state_machine: PercolatorStateMachine, query: dict[str, Any]) -> dict[str, Any]:
    """Query adapter for RaftNode's client-facing ClientQuery RPC:
    {"op": "get", "key": ..., "read_ts": ...}."""
    if query["op"] != "get":
        raise ValueError(f"unsupported percolator query op {query['op']!r}")
    value, error = state_machine.get(query["key"], query["read_ts"])
    return {"value": value, "error": error}
