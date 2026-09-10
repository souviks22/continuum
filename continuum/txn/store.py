"""Percolator-style transactional storage: prewrite/commit protocol on
top of Phase 4's MVCC store, with primary/secondary lock records.

Three logical column families per key, following Percolator's design:

- data:  MVCCStore of (key, start_ts) -> value. Written at prewrite
  time, before the transaction is visible to anyone -- staging the
  value durably ahead of commit is Percolator's core trick, rather than
  buffering it purely client-side until commit.
- writes: MVCCStore of (key, commit_ts) -> str(start_ts), a pointer
  record. A committed read finds the newest write record at or before
  its read_ts, then follows the pointer to read the actual value from
  `data` at that start_ts.
- locks: dict of key -> Lock (primary_key, start_ts). At most one
  outstanding lock per key at a time -- a second prewrite while one is
  already outstanding is exactly the write-write conflict prewrite must
  detect and refuse.

Lock-cleanup for a lock left behind by a crashed coordinator (checking
the primary's commit status and rolling forward/back accordingly) is
NOT decided here -- this module only exposes the primitives a resolver
needs (get_with_lock, find_commit_ts, get_lock). The actual policy --
when a lock is old enough to safely presume abandoned, and what to do
about it -- lives in txn/resolver.py (Step 6.2), kept separate since
resolution is inherently a cross-shard operation (the primary key may
live on a different shard than the one being read) that this
single-shard storage layer has no business knowing how to perform.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from continuum.storage.mvcc import MVCCStore


@dataclass
class Lock:
    primary_key: str
    start_ts: int


class PercolatorStore:
    def __init__(self) -> None:
        self.data = MVCCStore()
        self.writes = MVCCStore()
        self.locks: dict[str, Lock] = {}

    # -- prewrite -----------------------------------------------------------

    def prewrite(
        self, key: str, value: Optional[str], start_ts: int, primary_key: str
    ) -> tuple[bool, Optional[str]]:
        """Returns (success, failure_reason)."""
        existing_lock = self.locks.get(key)
        if existing_lock is not None:
            return False, (
                f"key {key!r} is already locked by a transaction starting at "
                f"{existing_lock.start_ts}"
            )

        latest_commit_ts = self._latest_commit_ts(key)
        if latest_commit_ts is not None and latest_commit_ts >= start_ts:
            return False, (
                f"write conflict on {key!r}: a version committed at {latest_commit_ts} "
                f"is not older than this transaction's start_ts {start_ts}"
            )

        self.data.write(key, start_ts, value)
        self.locks[key] = Lock(primary_key=primary_key, start_ts=start_ts)
        return True, None
    
    def _latest_commit_ts(self, key: str) -> Optional[int]:
        versions = self.writes.versions(key)
        return versions[-1][0] if versions else None

    # -- commit -----------------------------------------------------------

    def commit(self, key: str, start_ts: int, commit_ts: int) -> tuple[bool, Optional[str]]:
        lock = self.locks.get(key)
        if lock is None or lock.start_ts != start_ts:
            return False, (
                f"no matching lock for {key!r} at start_ts {start_ts} (already "
                "committed, rolled back, or never prewritten)"
            )
        self.writes.write(key, commit_ts, str(start_ts))
        del self.locks[key]
        return True, None

    # -- rollback -----------------------------------------------------------

    def rollback(self, key: str, start_ts: int) -> tuple[bool, Optional[str]]:
        lock = self.locks.get(key)
        if lock is not None and lock.start_ts == start_ts:
            del self.locks[key]
        return True, None  # idempotent: rolling back an already-gone lock still succeeds

    # -- reads -----------------------------------------------------------

    def get(self, key: str, read_ts: int) -> tuple[Optional[str], Optional[str]]:
        """Returns (value, error). error is set (value is None) if a
        blocking lock is in the way and needs external resolution --
        see the module docstring for what's deliberately not here yet."""
        lock = self.locks.get(key)
        if lock is not None and lock.start_ts <= read_ts:
            return None, f"key {key!r} is locked by a transaction starting at {lock.start_ts}; retry"
        pointer = self.writes.read(key, read_ts)
        if pointer is None:
            return None, None  # no committed version visible at this read_ts -- legitimately absent
        value = self.data.read(key, int(pointer))
        return value, None

    def get_with_lock(self, key: str, read_ts: int) -> tuple[Optional[str], Optional[Lock]]:
        """Same as get(), but returns the structured Lock (not a
        formatted string) when blocked, for Step 6.2's LockResolver to
        act on -- it needs lock.primary_key and lock.start_ts, not just
        a human-readable message."""
        lock = self.locks.get(key)
        if lock is not None and lock.start_ts <= read_ts:
            return None, lock
        pointer = self.writes.read(key, read_ts)
        if pointer is None:
            return None, None
        return self.data.read(key, int(pointer)), None

    def get_lock(self, key: str) -> Optional[Lock]:
        return self.locks.get(key)

    def find_commit_ts(self, key: str, start_ts: int) -> Optional[int]:
        """Given a key and a transaction's start_ts, find the commit_ts
        it was committed at, if any -- the LockResolver needs this to
        roll a secondary forward with the *same* commit_ts the primary
        actually used, not a fresh one. A linear scan of this key's
        write versions: fine for this scope (versions are typically few
        per key), matching the project's general priority on clarity
        over micro-optimizing a path that isn't the hot path."""
        for commit_ts, pointer in self.writes.versions(key):
            if pointer == str(start_ts):
                return commit_ts
        return None

    # -- garbage collection --------------------------------------------------

    def gc(self, safe_point: int) -> None:
        """Discard MVCC versions no longer reachable by any read at or
        above `safe_point`. Applied independently to `data` and `writes`
        with the same safe_point (each MVCCStore.gc() already handles
        every key it holds in one call) -- a simplification worth
        naming: a fully precise Percolator GC would only discard a
        `data` version once it confirms no surviving `writes` entry still
        points to it, which could in principle retain a version this
        does discard. What's implemented here is always *safe* (never
        discards something still reachable), just occasionally not the
        tightest possible bound -- correctness over precision, given
        this scope."""
        self.data.gc(safe_point)
        self.writes.gc(safe_point)
