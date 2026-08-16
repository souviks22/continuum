"""Static range partitioning: maps keys to the shard (Raft group) that
owns them.

Scope for this step: the routing table itself, plus split() as a
bookkeeping operation on that table. What split() deliberately does NOT
yet do: actually drive a real, replicated range-split across every
replica of the original shard's Raft group. A correct split has to
happen as a proposed, committed log entry on the *original* shard so
every replica splits at exactly the same point -- otherwise replicas
could disagree about which shard now owns which keys, silently
corrupting routing. That's real distributed-systems complexity for a
later step; here, split() only updates the routing table, and is meant
to be paired with orchestration code that separately calls
ShardManager.start_shard() for the new range's Raft group.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Optional


@dataclass
class Range:
    start: Optional[str]  # None = unbounded below (-infinity)
    end: Optional[str]  # None = unbounded above (+infinity)
    shard_id: str

    def contains(self, key: str) -> bool:
        if self.start is not None and key < self.start:
            return False
        if self.end is not None and key >= self.end:
            return False
        return True


class RangePartitioner:
    def __init__(self, initial_shard_id: str = "shard-0") -> None:
        # Starts as a single range covering the entire keyspace -- the
        # ranges list is always kept exhaustive and non-overlapping, so
        # route() can never legitimately fail to find a match.
        self._ranges: list[Range] = [Range(start=None, end=None, shard_id=initial_shard_id)]
        self._split_counter = itertools.count(1)

    def route(self, key: str) -> str:
        for r in self._ranges:
            if r.contains(key):
                return r.shard_id
        raise AssertionError(
            f"no range covers key {key!r}; ranges must always be exhaustive -- this "
            "indicates a bug in split(), not a legitimate routing miss"
        )

    def range_for_shard(self, shard_id: str) -> Range:
        for r in self._ranges:
            if r.shard_id == shard_id:
                return r
        raise KeyError(f"no range for shard {shard_id!r}")

    def all_shard_ids(self) -> list[str]:
        return [r.shard_id for r in self._ranges]

    def split(self, shard_id: str, split_key: str) -> tuple[str, str]:
        """Split shard_id's range at split_key: [start, split_key) stays
        shard_id, [split_key, end) becomes a newly-minted shard id.
        Returns (left_shard_id, right_shard_id)."""
        r = self.range_for_shard(shard_id)
        if r.start is not None and split_key <= r.start:
            raise ValueError(
                f"split_key {split_key!r} is not inside shard {shard_id!r}'s range "
                f"[{r.start!r}, {r.end!r})"
            )
        if r.end is not None and split_key >= r.end:
            raise ValueError(
                f"split_key {split_key!r} is not inside shard {shard_id!r}'s range "
                f"[{r.start!r}, {r.end!r})"
            )

        new_shard_id = f"{shard_id}-split-{next(self._split_counter)}"
        idx = self._ranges.index(r)
        self._ranges[idx] = Range(start=r.start, end=split_key, shard_id=shard_id)
        self._ranges.insert(idx + 1, Range(start=split_key, end=r.end, shard_id=new_shard_id))
        return shard_id, new_shard_id
