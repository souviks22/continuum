"""Raft's replicated log: the ordered (term, command) entries that
AppendEntries replicates and majority-commit rules make durable across
the cluster. Backed by Phase 1's WriteAheadLog (via its Step 2.2
extension, truncate_to()) so log entries are crash-durable the same way
regular WAL records are, and so conflicting suffixes can be discarded
when a follower's log diverges from the leader's -- the WAL's
append-only design from Phase 1 was correct for that phase in isolation,
but Raft's log-matching property genuinely requires truncation, which is
why truncate_to() exists rather than working around its absence here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from continuum.storage.wal import WriteAheadLog


@dataclass(frozen=True)
class LogEntry:
    index: int
    term: int
    command: dict[str, Any]

    def encode(self) -> bytes:
        return json.dumps(
            {"index": self.index, "term": self.term, "command": self.command}
        ).encode()

    @staticmethod
    def decode(payload: bytes) -> "LogEntry":
        data = json.loads(payload.decode())
        return LogEntry(index=data["index"], term=data["term"], command=data["command"])


class RaftLog:
    def __init__(self, wal: WriteAheadLog) -> None:
        self._wal = wal
        # Log indices here are contiguous from 0, so list position ==
        # log index always holds -- truncate_to() and append() both
        # preserve that invariant, which is what keeps _entries[index]
        # a valid O(1) lookup instead of needing a dict.
        self._entries: list[LogEntry] = [
            LogEntry.decode(payload) for payload in wal.read_all()
        ]

    @property
    def last_index(self) -> int:
        return self._entries[-1].index if self._entries else -1

    @property
    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def term_at(self, index: int) -> int:
        """Term of the entry at `index`. index == -1 (the conventional
        "before the log starts" sentinel) is always term 0. Raises if
        asked about an index we don't have -- callers must check
        `last_index` first; silently returning 0 for a missing index
        would be indistinguishable from an actual term-0 entry."""
        if index == -1:
            return 0
        if index < -1 or index > self.last_index:
            raise IndexError(f"no entry at index {index}; last_index={self.last_index}")
        return self._entries[index].term

    def get(self, index: int) -> Optional[LogEntry]:
        if index < 0 or index > self.last_index:
            return None
        return self._entries[index]

    def entries_from(self, start_index: int) -> list[LogEntry]:
        """Every entry with index >= start_index, in order. Used by the
        leader to figure out what to send a peer that's behind."""
        if start_index > self.last_index:
            return []
        return self._entries[max(start_index, 0):]

    def append(self, term: int, command: dict[str, Any], fsync: bool = True) -> LogEntry:
        """Leader-side: append a new entry at the end of the log."""
        entry = LogEntry(index=self.last_index + 1, term=term, command=command)
        self._append_raw(entry)
        if fsync:
            self._wal.fsync()
        return entry

    def _append_raw(self, entry: LogEntry) -> None:
        self._wal.append(entry.encode())
        self._entries.append(entry)

    def append_entries_from_leader(
        self, prev_log_index: int, prev_log_term: int, entries: list[LogEntry]
    ) -> bool:
        """Follower-side: the log-matching / conflict-resolution step of
        AppendEntries. Returns False (reject) if our log doesn't have a
        matching entry at prev_log_index/prev_log_term -- the caller
        (node.py) is responsible for computing a conflict_index hint from
        that rejection. Returns True and applies `entries` (truncating
        any conflicting local suffix first) otherwise. Idempotent:
        re-delivering the same AppendEntries (e.g. a retried RPC) is safe
        -- entries whose index/term already match are left alone rather
        than being re-truncated and re-appended."""
        if prev_log_index > self.last_index:
            return False
        if prev_log_index >= 0 and self.term_at(prev_log_index) != prev_log_term:
            return False

        insert_at = prev_log_index + 1
        truncated = False
        for offset, new_entry in enumerate(entries):
            idx = insert_at + offset
            if idx <= self.last_index:
                if self._entries[idx].term == new_entry.term:
                    continue  # already have this exact entry; skip (idempotency)
                self._truncate_from(idx)
                truncated = True
            self._append_raw(new_entry)
        if entries or truncated:
            self._wal.fsync()  # persist the whole batch before acking success
        return True

    def _truncate_from(self, index: int) -> None:
        self._wal.truncate_to(index)
        del self._entries[index:]
