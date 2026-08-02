import os
from unittest.mock import patch

import pytest

from continuum.storage.wal import CorruptRecordError, WriteAheadLog


def test_append_returns_sequential_indices(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    assert wal.append(b"a") == 0
    assert wal.append(b"b") == 1
    assert wal.append(b"c") == 2
    wal.close()


def test_read_all_returns_payloads_in_order(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    wal.append(b"first")
    wal.append(b"second")
    wal.append(b"third")
    assert wal.read_all() == [b"first", b"second", b"third"]
    wal.close()


def test_len_reflects_record_count(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    assert len(wal) == 0
    wal.append(b"x")
    wal.append(b"y")
    assert len(wal) == 2
    wal.close()


def test_empty_payload_is_valid(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    wal.append(b"")
    assert wal.read_all() == [b""]
    wal.close()


def test_reopening_recovers_all_previously_appended_records(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"one")
    wal.append(b"two")
    wal.close()

    reopened = WriteAheadLog(path)
    assert reopened.read_all() == [b"one", b"two"]
    assert len(reopened) == 2
    reopened.close()


def test_reopening_continues_indices_after_existing_records(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"one")
    wal.append(b"two")
    wal.close()

    reopened = WriteAheadLog(path)
    assert reopened.append(b"three") == 2
    assert reopened.read_all() == [b"one", b"two", b"three"]
    reopened.close()


def test_append_flushes_to_os_visible_from_a_separate_handle(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"visible-without-fsync")
    # A second, independent file handle onto the same path -- if flush()
    # actually moved the bytes out of the Python-level buffer into the OS,
    # this sees them even though fsync() was never called.
    with open(path, "rb") as f:
        raw = f.read()
    assert b"visible-without-fsync" in raw
    wal.close()


def test_append_durable_calls_fsync(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    with patch("continuum.storage.wal.os.fsync") as mock_fsync:
        wal.append_durable(b"payload")
    mock_fsync.assert_called_once()
    wal.close()


def test_plain_append_does_not_call_fsync(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    with patch("continuum.storage.wal.os.fsync") as mock_fsync:
        wal.append(b"payload")
    mock_fsync.assert_not_called()
    wal.close()


def test_recovery_truncates_torn_tail_from_incomplete_payload(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"durable-one")
    wal.append(b"durable-two")
    wal.close()
    size_before_crash = os.path.getsize(path)

    # Simulate a crash mid-write: a well-formed header claiming a payload
    # longer than what actually got written before the process died.
    with open(path, "r+b") as f:
        f.seek(0, os.SEEK_END)
        f.write(struct_header(100) + b"only_a_few_bytes")

    reopened = WriteAheadLog(path)
    assert reopened.read_all() == [b"durable-one", b"durable-two"]
    assert len(reopened) == 2
    assert os.path.getsize(path) == size_before_crash  # torn tail discarded on disk too
    reopened.close()


def test_recovery_truncates_torn_tail_from_incomplete_header(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"durable-one")
    wal.close()
    size_before_crash = os.path.getsize(path)

    with open(path, "r+b") as f:
        f.seek(0, os.SEEK_END)
        f.write(b"\x00\x01")  # fewer than 8 header bytes

    reopened = WriteAheadLog(path)
    assert reopened.read_all() == [b"durable-one"]
    assert os.path.getsize(path) == size_before_crash
    reopened.close()


def test_appending_after_recovered_torn_tail_continues_cleanly(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"durable-one")
    wal.close()

    with open(path, "r+b") as f:
        f.seek(0, os.SEEK_END)
        f.write(struct_header(50) + b"partial")

    reopened = WriteAheadLog(path)
    assert reopened.append(b"durable-two") == 1
    reopened.close()

    final = WriteAheadLog(path)
    assert final.read_all() == [b"durable-one", b"durable-two"]
    final.close()


def test_corrupted_full_length_record_raises_not_silently_discarded(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"payload_with_matching_length")
    wal.close()

    # Flip a byte within the payload region without touching the length
    # header -- this is corruption of already-complete data, not a torn
    # write, so recovery must not silently truncate it away.
    with open(path, "r+b") as f:
        f.seek(8)  # past the 8-byte header, into the payload
        f.write(b"X")

    with pytest.raises(CorruptRecordError):
        WriteAheadLog(path)


def struct_header(length: int) -> bytes:
    import struct
    import zlib

    return struct.pack(">II", length, zlib.crc32(b"x" * min(length, 1)))
