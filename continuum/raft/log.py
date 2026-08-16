"""Raft's replicated log: the ordered (term, command) entries that
AppendEntries replicates and majority-commit rules make durable across
the cluster. Backed by Phase 1's WriteAheadLog (via its Step 2.2
extension, truncate_to()) so log entries are crash-durable, and
conflicting suffixes can be discarded when a follower's log diverges
from the leader's.

Step 2.3 adds a compaction boundary: compact() discards the entries a
snapshot has made redundant (a *prefix* discard -- the opposite of
truncate_to()'s suffix discard used for conflict resolution). Everything
at or before last_included_index is gone; entries are addressed via
`self._start_index` (the logical index stored at physical list position
0) instead of assuming list position == logical index, which held
trivially before compaction existed but no longer does once the log's
first surviving entry isn't index 0. The compaction boundary itself
(last_included_index/last_included_term) is persisted separately from
the WAL, so recovery can filter out any stale pre-boundary entries a
not-yet-replaced WAL file might still physically contain -- the same
index-comparison-based crash-safety approach StorageEngine.compact()
uses in Phase 1, applied here to the log instead of the state machine.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from continuum.storage.atomic_json import AtomicJsonFile
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


class LogCompactedError(Exception):
    """Requested entries at or before the compaction boundary -- they no
    longer exist locally. The caller (RaftNode, replicating to a peer)
    must fall back to InstallSnapshot instead of AppendEntries."""


class RaftLog:
    def __init__(self, wal: WriteAheadLog) -> None:
        self._wal = wal
        self._metadata = AtomicJsonFile(wal.path.parent / "raft_log_meta.json")
        meta = self._metadata.read() or {"last_included_index": -1, "last_included_term": 0}
        self._last_included_index: int = meta["last_included_index"]
        self._last_included_term: int = meta["last_included_term"]
        self._start_index = self._last_included_index + 1

        # Filter out any entries at or before the boundary: if a prior
        # compact() persisted its metadata but crashed before replacing
        # the WAL, the WAL may still physically hold now-redundant
        # entries. This filter makes recovery correct regardless of
        # whether that replacement actually completed.
        self._entries: list[LogEntry] = [
            e
            for e in (LogEntry.decode(p) for p in wal.read_all())
            if e.index > self._last_included_index
        ]

    @property
    def last_index(self) -> int:
        if self._entries:
            return self._start_index + len(self._entries) - 1
        return self._start_index - 1

    @property
    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else self._last_included_term

    @property
    def last_included_index(self) -> int:
        return self._last_included_index

    @property
    def last_included_term(self) -> int:
        return self._last_included_term

    def term_at(self, index: int) -> int:
        if index == -1:
            return 0
        if index == self._last_included_index:
            return self._last_included_term
        if index < self._start_index or index > self.last_index:
            raise IndexError(f"no entry at index {index}; last_index={self.last_index}")
        return self._entries[index - self._start_index].term

    def get(self, index: int) -> Optional[LogEntry]:
        if index < self._start_index or index > self.last_index:
            return None  # includes index == last_included_index: compacted away, term-only
        return self._entries[index - self._start_index]

    def entries_from(self, start_index: int) -> list[LogEntry]:
        """Every entry with index >= start_index, in order. Raises
        LogCompactedError if start_index falls at or before the
        compaction boundary -- those entries no longer exist here, and
        the caller needs InstallSnapshot instead."""
        if start_index <= self._last_included_index:
            raise LogCompactedError(
                f"index {start_index} is at or before the compaction boundary "
                f"({self._last_included_index}); use InstallSnapshot"
            )
        if start_index > self.last_index:
            return []
        return self._entries[start_index - self._start_index :]

    def append(self, term: int, command: dict[str, Any], fsync: bool = True) -> LogEntry:
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
        if prev_log_index < self._last_included_index:
            return False  # too far back to verify; leader should use InstallSnapshot
        if prev_log_index == self._last_included_index:
            if prev_log_term != self._last_included_term:
                return False
        elif prev_log_index > self.last_index:
            return False
        elif self.term_at(prev_log_index) != prev_log_term:
            return False

        insert_at = prev_log_index + 1
        truncated = False
        for offset, new_entry in enumerate(entries):
            idx = insert_at + offset
            if idx <= self.last_index:
                existing = self._entries[idx - self._start_index]
                if existing.term == new_entry.term:
                    continue  # already have this exact entry; skip (idempotency)
                self._truncate_from(idx)
                truncated = True
            self._append_raw(new_entry)
        if entries or truncated:
            self._wal.fsync()  # persist the whole batch before acking success
        return True

    def _truncate_from(self, index: int) -> None:
        self._wal.truncate_to(index - self._start_index)
        del self._entries[index - self._start_index :]

    # -- compaction ---------------------------------------------------------

    def compact(self, last_included_index: int) -> None:
        """Discard entries at or before `last_included_index` -- the
        caller (RaftNode) has already durably snapshotted the state
        machine up to that point, so replaying these entries again would
        be redundant. Crash-safe the same way StorageEngine.compact() is
        in Phase 1: metadata is persisted first, then the WAL is
        replaced; if a crash lands in between, __init__'s index filter
        on the next open makes the result correct either way, without
        needing to know whether the WAL replacement actually finished.
        """
        if last_included_index <= self._last_included_index:
            return  # nothing new to compact
        if last_included_index > self.last_index:
            raise ValueError(
                f"cannot compact past the end of the log "
                f"(last_included_index={last_included_index}, last_index={self.last_index})"
            )
        new_last_included_term = self.term_at(last_included_index)
        surviving = self._entries[last_included_index - self._start_index + 1 :]

        self._metadata.write(
            {"last_included_index": last_included_index, "last_included_term": new_last_included_term}
        )

        self._wal.close()
        wal_path = self._wal.path
        if wal_path.exists():
            wal_path.unlink()
        self._wal = WriteAheadLog(wal_path)
        for entry in surviving:
            self._wal.append(entry.encode())
        self._wal.fsync()

        self._entries = surviving
        self._last_included_index = last_included_index
        self._last_included_term = new_last_included_term
        self._start_index = last_included_index + 1

    def install_snapshot(self, last_included_index: int, last_included_term: int) -> None:
        """Follower-side: adopt a leader-sent snapshot as authoritative,
        discarding the entire local log and starting fresh right after
        the snapshot boundary. Simpler than the optimization real Raft
        implementations sometimes do (retaining any local entries that
        happen to already match the leader's beyond the snapshot point)
        -- always safe, just occasionally re-fetches entries the
        follower technically already had. A deliberate simplification
        for this scope, not an oversight."""
        if last_included_index <= self._last_included_index:
            return  # stale/duplicate InstallSnapshot; we're already at least this current

        self._metadata.write(
            {"last_included_index": last_included_index, "last_included_term": last_included_term}
        )

        self._wal.close()
        wal_path = self._wal.path
        if wal_path.exists():
            wal_path.unlink()
        self._wal = WriteAheadLog(wal_path)

        self._entries = []
        self._last_included_index = last_included_index
        self._last_included_term = last_included_term
        self._start_index = last_included_index + 1
