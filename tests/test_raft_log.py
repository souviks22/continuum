from continuum.raft.log import LogEntry, RaftLog
from continuum.storage.wal import WriteAheadLog


def make_log(tmp_path, name="log.wal"):
    wal = WriteAheadLog(tmp_path / name)
    return RaftLog(wal), wal


def test_empty_log_has_sentinel_last_index_and_term(tmp_path):
    log, wal = make_log(tmp_path)
    assert log.last_index == -1
    assert log.last_term == 0
    wal.close()


def test_append_assigns_sequential_indices(tmp_path):
    log, wal = make_log(tmp_path)
    e0 = log.append(term=1, command={"op": "set", "key": "a"})
    e1 = log.append(term=1, command={"op": "set", "key": "b"})
    assert e0.index == 0
    assert e1.index == 1
    assert log.last_index == 1
    assert log.last_term == 1
    wal.close()


def test_term_at_negative_one_is_zero(tmp_path):
    log, wal = make_log(tmp_path)
    assert log.term_at(-1) == 0
    wal.close()


def test_term_at_missing_index_raises(tmp_path):
    log, wal = make_log(tmp_path)
    try:
        log.term_at(5)
        assert False, "expected IndexError"
    except IndexError:
        pass
    wal.close()


def test_entries_from_returns_suffix(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    log.append(1, {"n": 1})
    log.append(2, {"n": 2})
    result = log.entries_from(1)
    assert [e.command["n"] for e in result] == [1, 2]
    wal.close()


def test_entries_from_beyond_end_returns_empty(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    assert log.entries_from(5) == []
    wal.close()


# -- append_entries_from_leader: matching / acceptance ----------------------


def test_append_from_empty_base_with_matching_prev_minus_one(tmp_path):
    log, wal = make_log(tmp_path)
    ok = log.append_entries_from_leader(
        -1, 0, [LogEntry(0, 1, {"n": 0}), LogEntry(1, 1, {"n": 1})]
    )
    assert ok is True
    assert log.last_index == 1
    assert [log.get(i).command["n"] for i in (0, 1)] == [0, 1]
    wal.close()


def test_reject_when_prev_log_index_beyond_our_log(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    ok = log.append_entries_from_leader(5, 1, [LogEntry(6, 1, {"n": 6})])
    assert ok is False
    assert log.last_index == 0  # unchanged
    wal.close()


def test_reject_when_prev_log_term_mismatches(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(term=1, command={"n": 0})
    ok = log.append_entries_from_leader(0, 99, [LogEntry(1, 1, {"n": 1})])
    assert ok is False
    assert log.last_index == 0  # unchanged
    wal.close()


def test_conflicting_entry_truncates_and_replaces_suffix(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    log.append(1, {"n": "stale-1"})
    log.append(1, {"n": "stale-2"})

    ok = log.append_entries_from_leader(0, 1, [LogEntry(1, 2, {"n": "new-1"})])
    assert ok is True
    assert log.last_index == 1
    assert log.get(1).command["n"] == "new-1"
    assert log.get(2) is None  # stale-2 discarded along with the conflict
    wal.close()


def test_idempotent_redelivery_of_already_present_entries_is_a_noop(tmp_path):
    log, wal = make_log(tmp_path)
    log.append_entries_from_leader(-1, 0, [LogEntry(0, 1, {"n": 0}), LogEntry(1, 1, {"n": 1})])
    ok = log.append_entries_from_leader(-1, 0, [LogEntry(0, 1, {"n": 0}), LogEntry(1, 1, {"n": 1})])
    assert ok is True
    assert log.last_index == 1
    assert log.get(0).command["n"] == 0
    wal.close()


def test_heartbeat_with_no_entries_and_matching_prev_succeeds(tmp_path):
    log, wal = make_log(tmp_path)
    log.append(1, {"n": 0})
    ok = log.append_entries_from_leader(0, 1, [])
    assert ok is True
    assert log.last_index == 0
    wal.close()


# -- persistence across reopen --------------------------------------------


def test_log_entries_survive_reopen(tmp_path):
    path = tmp_path / "log.wal"
    wal = WriteAheadLog(path)
    log = RaftLog(wal)
    log.append(1, {"n": 0})
    log.append(1, {"n": 1})
    wal.close()

    wal2 = WriteAheadLog(path)
    log2 = RaftLog(wal2)
    assert log2.last_index == 1
    assert [log2.get(i).command["n"] for i in (0, 1)] == [0, 1]
    wal2.close()


def test_truncation_persists_across_reopen(tmp_path):
    path = tmp_path / "log.wal"
    wal = WriteAheadLog(path)
    log = RaftLog(wal)
    log.append(1, {"n": 0})
    log.append(1, {"n": "will-be-truncated"})
    log.append_entries_from_leader(0, 1, [LogEntry(1, 2, {"n": "replacement"})])
    wal.close()

    wal2 = WriteAheadLog(path)
    log2 = RaftLog(wal2)
    assert log2.last_index == 1
    assert log2.get(1).command["n"] == "replacement"
    wal2.close()
