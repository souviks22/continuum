import os

from continuum.storage.wal import WriteAheadLog


def test_truncate_to_removes_trailing_records(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    wal.append(b"a")
    wal.append(b"b")
    wal.append(b"c")
    wal.truncate_to(1)
    assert wal.read_all() == [b"a"]
    assert len(wal) == 1
    wal.close()


def test_truncate_to_full_length_is_a_noop(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    wal.append(b"a")
    wal.append(b"b")
    wal.truncate_to(2)
    assert wal.read_all() == [b"a", b"b"]
    wal.close()


def test_truncate_to_zero_empties_the_log(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    wal.append(b"a")
    wal.append(b"b")
    wal.truncate_to(0)
    assert wal.read_all() == []
    assert len(wal) == 0
    wal.close()


def test_truncate_actually_shrinks_the_file_on_disk(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"a")
    size_with_one = os.path.getsize(path)
    wal.append(b"b" * 100)
    size_with_two = os.path.getsize(path)
    assert size_with_two > size_with_one

    wal.truncate_to(1)
    assert os.path.getsize(path) == size_with_one
    wal.close()


def test_append_after_truncate_continues_from_the_truncated_point(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    wal.append(b"a")
    wal.append(b"b")
    wal.append(b"c")
    wal.truncate_to(1)
    idx = wal.append(b"new-b")
    assert idx == 1
    assert wal.read_all() == [b"a", b"new-b"]
    wal.close()


def test_truncate_persists_across_reopen(tmp_path):
    path = tmp_path / "wal.log"
    wal = WriteAheadLog(path)
    wal.append(b"a")
    wal.append(b"b")
    wal.append(b"c")
    wal.truncate_to(1)
    wal.close()

    reopened = WriteAheadLog(path)
    assert reopened.read_all() == [b"a"]
    reopened.close()


def test_truncate_to_negative_count_raises(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    wal.append(b"a")
    try:
        wal.truncate_to(-1)
        assert False, "expected ValueError"
    except ValueError:
        pass
    wal.close()


def test_truncate_to_count_beyond_length_raises(tmp_path):
    wal = WriteAheadLog(tmp_path / "wal.log")
    wal.append(b"a")
    try:
        wal.truncate_to(5)
        assert False, "expected ValueError"
    except ValueError:
        pass
    wal.close()
