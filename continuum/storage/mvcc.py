"""MVCC storage: multi-version key-value store, keyed by (key,
timestamp) rather than a single current value per key.

This is the substrate Phase 6's Percolator-style transactions need:
prewrite/commit stage values by timestamp rather than overwriting in
place, and reads need "the value as of timestamp T" (snapshot reads),
not just "the current value." Building this now, before the transaction
layer exists, keeps MVCC correctness (version ordering, tombstones,
garbage collection) provably right in isolation first -- the same
sequencing principle used for the WAL before Raft, and Raft before
sharding.

Timestamps are supplied by the caller, never invented here. A version's
timestamp has to be decided *before* the write is proposed and
replicated -- if the state machine picked its own timestamp (e.g. off a
wall clock) during apply_command, different replicas could disagree
about what timestamp a "concurrent" write got, breaking the determinism
a replicated state machine depends on. Phase 5's Timestamp Oracle is
what will actually hand out these timestamps to real callers; for now,
callers (including the query_fn/tests below) pass them explicitly.
"""

from __future__ import annotations

import bisect
from typing import Any, Optional


class MVCCStore:
    def __init__(self) -> None:
        # Parallel sorted-by-timestamp arrays per key, rather than a
        # single list of tuples, so bisect can search the timestamps
        # directly without a custom key function.
        self._timestamps: dict[str, list[int]] = {}
        self._values: dict[str, list[Optional[str]]] = {}

    def write(self, key: str, timestamp: int, value: Optional[str]) -> None:
        """Write a new version. `value=None` records a tombstone
        (delete) at this timestamp -- reads at or after it see the key
        as absent, but the tombstone itself is a real version, so a read
        at an earlier timestamp still sees whatever was there before."""
        ts_list = self._timestamps.setdefault(key, [])
        if ts_list and timestamp <= ts_list[-1]:
            raise ValueError(
                f"write timestamp {timestamp} for key {key!r} is not greater than "
                f"the latest existing version ({ts_list[-1]}) -- MVCC requires "
                "strictly increasing timestamps per key"
            )
        ts_list.append(timestamp)
        self._values.setdefault(key, []).append(value)

    def read(self, key: str, read_ts: int) -> Optional[str]:
        """The value visible to a read at `read_ts`: the newest version
        with timestamp <= read_ts, or None if the key doesn't exist yet
        (or was already deleted) as of that timestamp."""
        ts_list = self._timestamps.get(key)
        if not ts_list:
            return None
        idx = bisect.bisect_right(ts_list, read_ts) - 1
        if idx < 0:
            return None
        return self._values[key][idx]

    def read_latest(self, key: str) -> Optional[str]:
        ts_list = self._timestamps.get(key)
        if not ts_list:
            return None
        return self._values[key][-1]

    def versions(self, key: str) -> list[tuple[int, Optional[str]]]:
        """All versions of a key, oldest first. Mostly for
        tests/inspection and snapshotting, not a hot-path read."""
        return list(zip(self._timestamps.get(key, []), self._values.get(key, [])))

    def all_keys(self) -> list[str]:
        return list(self._timestamps.keys())

    def gc(self, safe_point: int) -> None:
        """Discard versions that can never be read again: for each key,
        keep the newest version with timestamp <= safe_point (a read at
        exactly safe_point, or anywhere below the next-oldest surviving
        version, still needs it) plus every version above safe_point;
        drop everything strictly older than that. Safe-point *policy*
        (how high it's safe to set this, given any transactions that
        might still need an older snapshot) is a Phase 6 concern tied to
        active transaction tracking -- this method just performs the
        mechanical discard once a safe point is given."""
        for key, ts_list in list(self._timestamps.items()):
            idx = bisect.bisect_right(ts_list, safe_point) - 1
            if idx <= 0:
                continue  # nothing strictly older than the kept version
            self._timestamps[key] = ts_list[idx:]
            self._values[key] = self._values[key][idx:]


class MVCCStateMachine:
    """Adapts MVCCStore to RaftNode's generic ReplicatedStateMachine
    protocol (apply_command / snapshot_state / load_state) -- the same
    pattern storage.statemachine.StateMachine and cluster.metadata.
    MetadataStateMachine already follow. Proves the point that
    generalization was built for in Step 3.2: MVCC is just another state
    machine sitting on identical Raft plumbing, with zero new
    replication code required to get it durable and consistent across a
    cluster."""

    def __init__(self) -> None:
        self._store = MVCCStore()

    def apply_command(self, index: int, command: dict[str, Any]) -> None:
        op = command["op"]
        if op == "set":
            self._store.write(command["key"], command["timestamp"], command["value"])
        elif op == "delete":
            self._store.write(command["key"], command["timestamp"], None)
        else:
            raise ValueError(f"unknown MVCC operation {op!r}")

    def get(self, key: str, read_ts: Optional[int] = None) -> Optional[str]:
        if read_ts is None:
            return self._store.read_latest(key)
        return self._store.read(key, read_ts)

    def snapshot_state(self) -> dict[str, Any]:
        return {key: self._store.versions(key) for key in self._store.all_keys()}

    def load_state(self, state: dict[str, Any]) -> None:
        self._store = MVCCStore()
        for key, versions in state.items():
            for ts, value in versions:
                self._store.write(key, ts, value)


def mvcc_query_fn(state_machine: MVCCStateMachine, query: dict[str, Any]) -> dict[str, Any]:
    """Query adapter for RaftNode's client-facing ClientQuery RPC (see
    client/client.py), for MVCC shards: {"op": "get", "key": ...,
    "read_ts": <optional>}. Omitting read_ts reads the latest version --
    NOT a snapshot read, just "whatever's newest right now," same
    non-linearizability caveat as the plain KV ClientQuery path."""
    if query["op"] != "get":
        raise ValueError(f"unsupported MVCC query op {query['op']!r}")
    return {"value": state_machine.get(query["key"], query.get("read_ts"))}
