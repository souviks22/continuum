"""Safe-point calculation for MVCC garbage collection.

Two complementary mechanisms, matching two different real systems'
actual approaches (both legitimate, worth knowing the tradeoff between):

- Precise active-transaction tracking (what TiDB's PD does): register a
  transaction's start_ts when it begins, deregister when it finishes.
  The safe point is bounded by the oldest still-active start_ts, so a
  long-running transaction correctly keeps the versions it needs from
  being collected. Kept in-memory here, NOT Raft-replicated -- adding a
  round-trip per transaction begin/end just to make this bookkeeping
  durable would cost real throughput for a purely advisory safety net,
  and losing track of an active transaction on a crash is the safe
  failure direction (GC becomes more conservative, not less).
- A TTL floor (what CockroachDB's GC TTL does): never consider anything
  younger than `gc_ttl` timestamp-units safe to collect, full stop,
  independent of tracking. This is what actually protects correctness
  if the in-memory registry above is ever wrong or incomplete (e.g.
  after whatever was tracking it restarts) -- a transaction that runs
  longer than the TTL is assumed abandoned or broken, exactly the same
  assumption Step 6.2's lock TTL already makes. The two floors are
  combined by taking the minimum (more conservative) of the two.
"""

from __future__ import annotations


class SafePointCalculator:
    def __init__(self, gc_ttl: int = 1000) -> None:
        self._gc_ttl = gc_ttl
        self._active_start_ts: set[int] = set()

    def begin(self, start_ts: int) -> None:
        self._active_start_ts.add(start_ts)

    def end(self, start_ts: int) -> None:
        self._active_start_ts.discard(start_ts)

    def compute_safe_point(self, current_ts: int) -> int:
        ttl_floor = current_ts - self._gc_ttl
        if not self._active_start_ts:
            return ttl_floor
        oldest_active = min(self._active_start_ts)
        # -1: a version committed exactly at the oldest active
        # transaction's start_ts must remain -- that's precisely what
        # makes its snapshot read at start_ts correct.
        return min(oldest_active - 1, ttl_floor)
