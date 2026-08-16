import pytest

from continuum.raft.log import LogCompactedError, LogEntry, RaftLog
from continuum.storage.wal import WriteAheadLog


def make_log(tmp_path, name="log.wal"):
    wal = WriteAheadLog(tmp_path / name)
    return RaftLog(wal), wal


def test_compact_discards_prefix_keeps_suffix(tmp_path):
    log, wal = make_log(tmp_path)
    for i in range(5):
        log.append(term=1, command={"n": i})

    log.compact(last_included_index=2)

    assert log.last_included_index == 2
    assert log.last_included_term == 1
    assert log.get(2) is None  # compacted away
    assert log.get(3).command == {"n": 3}
    assert log.get(4).command == {"n": 4}
    assert log.last_index == 4
    wal.close()


def test_compact_is_a_noop_if_index_not_beyond_current_boundary(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    log.append(1, {"n": 1})
    log.compact(0)
    log.compact(0)  # second call: nothing new
    assert log.last_included_index == 0
    assert log.get(1).command == {"n": 1}
    wal.close()


def test_compact_beyond_end_of_log_raises(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    with pytest.raises(ValueError):
        log.compact(5)
    wal.close()


def test_term_at_compaction_boundary_returns_remembered_term(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(term=3, command={"n": 0})
    log.append(term=4, command={"n": 1})
    log.compact(0)
    assert log.term_at(0) == 3
    wal.close()


def test_entries_from_at_boundary_raises_log_compacted(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    log.append(1, {"n": 1})
    log.compact(0)
    with pytest.raises(LogCompactedError):
        log.entries_from(0)
    wal.close()


def test_entries_from_after_boundary_works_normally(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    log.append(1, {"n": 1})
    log.append(1, {"n": 2})
    log.compact(0)
    result = log.entries_from(1)
    assert [e.command["n"] for e in result] == [1, 2]
    wal.close()


def test_append_entries_from_leader_matches_at_compaction_boundary(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(term=1, command={"n": 0})
    log.append(term=1, command={"n": 1})
    log.compact(0)  # boundary is (index=0, term=1)

    ok = log.append_entries_from_leader(0, 1, [LogEntry(1, 1, {"n": "already-have-this"})])
    assert ok is True
    assert log.get(1).command == {"n": 1}  # idempotent -- unchanged since term matched
    wal.close()


def test_append_entries_from_leader_rejects_when_boundary_term_mismatches(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(term=1, command={"n": 0})
    log.compact(0)

    ok = log.append_entries_from_leader(0, 99, [LogEntry(1, 1, {"n": 1})])
    assert ok is False
    wal.close()


def test_append_entries_from_leader_rejects_prev_before_boundary(tmp_path):
    log, wal = make_log(tmp_path)
    for i in range(5):
        log.append(1, {"n": i})
    log.compact(3)

    ok = log.append_entries_from_leader(1, 1, [LogEntry(2, 1, {"n": "x"})])
    assert ok is False  # prev_log_index=1 is before the boundary at 3
    wal.close()


def test_compaction_boundary_persists_across_reopen(tmp_path):
    path = tmp_path / "log.wal"
    wal = WriteAheadLog(path)
    log = RaftLog(wal)
    for i in range(4):
        log.append(1, {"n": i})
    log.compact(1)
    wal.close()

    wal2 = WriteAheadLog(path)
    log2 = RaftLog(wal2)
    assert log2.last_included_index == 1
    assert log2.last_included_term == 1
    assert log2.get(1) is None
    assert log2.get(2).command == {"n": 2}
    assert log2.get(3).command == {"n": 3}
    wal2.close()


def test_crash_between_metadata_persist_and_wal_replace_still_recovers_correctly(tmp_path):
    """Reproduces the compact() crash scenario directly: metadata lands
    durably, but the WAL file is never actually replaced. A fresh
    RaftLog opened against the same files must still filter out the
    now-redundant entries via the metadata boundary, independent of
    whether the WAL was physically pruned."""
    path = tmp_path / "log.wal"
    wal = WriteAheadLog(path)
    log = RaftLog(wal)
    for i in range(4):
        log.append(1, {"n": i})

    # Manually perform only the metadata half of compact().
    log._metadata.write({"last_included_index": 1, "last_included_term": 1})
    wal.close()  # WAL file on disk still has all four original entries

    recovered_wal = WriteAheadLog(path)
    recovered_log = RaftLog(recovered_wal)
    assert recovered_log.last_included_index == 1
    assert recovered_log.get(0) is None
    assert recovered_log.get(1) is None
    assert recovered_log.get(2).command == {"n": 2}
    assert recovered_log.get(3).command == {"n": 3}
    recovered_wal.close()


# -- install_snapshot -------------------------------------------------------


def test_install_snapshot_discards_local_log_and_sets_boundary(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": "will-be-discarded"})
    log.append(1, {"n": "also-discarded"})

    log.install_snapshot(last_included_index=5, last_included_term=3)

    assert log.last_included_index == 5
    assert log.last_included_term == 3
    assert log.last_index == 5
    assert log.get(0) is None
    assert log.get(5) is None
    wal.close()


def test_install_snapshot_ignores_stale_or_duplicate_calls(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    log.install_snapshot(last_included_index=10, last_included_term=2)
    log.install_snapshot(last_included_index=3, last_included_term=1)  # stale: ignored
    assert log.last_included_index == 10
    assert log.last_included_term == 2
    wal.close()


def test_append_after_install_snapshot_continues_from_boundary(tmp_path):
    log, wal = make_log(tmp_path)
    log.install_snapshot(last_included_index=9, last_included_term=4)
    entry = log.append(term=4, command={"n": "first-after-snapshot"})
    assert entry.index == 10
    assert log.get(10).command == {"n": "first-after-snapshot"}
    wal.close()


def test_install_snapshot_persists_across_reopen(tmp_path):
    path = tmp_path / "log.wal"
    wal = WriteAheadLog(path)
    log = RaftLog(wal)
    log.install_snapshot(last_included_index=7, last_included_term=2)
    log.append(term=2, command={"n": "x"})
    wal.close()

    wal2 = WriteAheadLog(path)
    log2 = RaftLog(wal2)
    assert log2.last_included_index == 7
    assert log2.last_included_term == 2
    assert log2.get(8).command == {"n": "x"}
    wal2.close()
