"""Write-ahead log: the durable-write path everything above it builds on.

This is deliberately the first thing built in Phase 1, before there's any
replication, because Raft (Phase 2) needs a correct WAL underneath it,
and debugging WAL correctness and Raft correctness at the same time is a
bad idea -- get the single-node durability story right and provable in
isolation first.

Format: a sequence of length-prefixed, checksummed records --

    [4 bytes: payload length (big-endian uint32)]
    [4 bytes: CRC32 of payload (big-endian uint32)]
    [payload length bytes: payload]

The checksum is what turns "the process crashed mid-write" from a
silent-corruption problem into a detectable one: a torn write (the
process died after writing some but not all of a record's bytes) leaves
either an incomplete header, an incomplete payload, or -- pathologically
-- a full-length payload whose CRC doesn't match. All three are
distinguishable from a genuinely valid record, but they are NOT all
handled the same way on recovery: an incomplete header/payload is
treated as a torn write and silently truncated away (that tail was never
durably completed, so it isn't lost data, it's debris from an
interrupted write, and the caller never got a durability acknowledgment
for it). A full-length record with a bad checksum is a different failure
mode -- corruption of data that *was* durably completed -- and is raised
rather than silently discarded, since papering over that would hide a
real problem (disk bit rot, a filesystem bug) that recovery has no
business unilaterally deciding is fine to lose.

Durability boundary, kept explicit rather than implicit:
  - `append()` writes the record and flushes it out of the Python-level
    buffer into the OS page cache. At this point another process opening
    the same file would see it, but a power loss or kernel panic can
    still lose it -- the OS is still free to not have handed it to the
    disk controller yet.
  - `fsync()` forces the OS to hand the data to the storage device and
    wait for the device to acknowledge it. This is what survives power
    loss, and it's also the expensive part (a real device round-trip,
    not just a memory copy) -- which is why it's a separate call rather
    than baked into every `append()`. Batching several appends behind
    one `fsync()` (group commit) is the standard way to trade a little
    added latency-until-durable for much higher throughput; keeping the
    two operations separate is what lets a caller make that tradeoff
    instead of having it made for them.
"""

from __future__ import annotations

import os
import struct
import zlib
from pathlib import Path

_HEADER = struct.Struct(">II")  # (payload length, crc32), big-endian, 4 bytes each


class CorruptRecordError(Exception):
    """A fully-length-matched record failed its checksum. Distinct from a
    torn-write tail (which recovery handles silently by truncating) --
    this means previously-durable bytes were corrupted after the fact,
    which is not something recovery should quietly discard."""


class WriteAheadLog:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        is_new = not self.path.exists()
        # "a+b": writes always land at end of file (O_APPEND) regardless
        # of current seek position, while still being readable from the
        # start for recovery scans and read_all().
        self._file = open(self.path, "a+b")
        if is_new:
            self._next_index = 0
            self._valid_end_offset = 0
        else:
            self._next_index, self._valid_end_offset = self._recover()

    # -- recovery -------------------------------------------------------

    def _recover(self) -> tuple[int, int]:
        """Scan from the start, replaying every well-formed, checksum-
        valid record. On the first torn write (not enough bytes left for
        a header, or for the declared payload length), truncate the file
        at that point and stop."""
        self._file.seek(0)
        offset = 0
        index = 0
        while True:
            header = self._file.read(_HEADER.size)
            if len(header) < _HEADER.size:
                break  # torn write: incomplete header
            length, crc = _HEADER.unpack(header)
            payload = self._file.read(length)
            if len(payload) < length:
                break  # torn write: incomplete payload
            if zlib.crc32(payload) != crc:
                raise CorruptRecordError(
                    f"record {index} at offset {offset} in {self.path} failed "
                    "its checksum -- this is a fully-length-matched record "
                    "with corrupted bytes, not a torn write, and needs "
                    "operator attention rather than silent truncation"
                )
            offset += _HEADER.size + length
            index += 1
        self._file.truncate(offset)
        self._file.seek(offset)
        return index, offset

    # -- writes -----------------------------------------------------------

    def append(self, payload: bytes) -> int:
        """Append a record and flush it to the OS. Returns the record's
        index. Does NOT fsync -- call fsync() explicitly (or use
        append_durable()) when the caller needs a guarantee that survives
        power loss, not just a process crash."""
        record = _HEADER.pack(len(payload), zlib.crc32(payload)) + payload
        self._file.write(record)
        self._file.flush()
        index = self._next_index
        self._next_index += 1
        self._valid_end_offset += len(record)
        return index

    def append_durable(self, payload: bytes) -> int:
        """append() + fsync() in one call, for callers that want a
        synchronous per-write durability guarantee and are consciously
        accepting the fsync latency cost per record rather than
        batching several appends behind one fsync (group commit)."""
        index = self.append(payload)
        self.fsync()
        return index

    def fsync(self) -> None:
        os.fsync(self._file.fileno())

    # -- reads -----------------------------------------------------------

    def read_all(self) -> list[bytes]:
        """Return every recovered record's payload, in order."""
        self._file.seek(0)
        records: list[bytes] = []
        for _ in range(self._next_index):
            header = self._file.read(_HEADER.size)
            length, _crc = _HEADER.unpack(header)
            records.append(self._file.read(length))
        return records

    def __len__(self) -> int:
        return self._next_index

    def close(self) -> None:
        self._file.close()
