from continuum.storage.snapshot import SnapshotStore


def test_read_returns_none_when_no_snapshot_exists(tmp_path):
    store = SnapshotStore(tmp_path)
    assert store.read() is None


def test_write_then_read_roundtrip(tmp_path):
    store = SnapshotStore(tmp_path)
    store.write({"a": "1", "b": "2"}, last_index=7)
    state, last_index = store.read()
    assert state == {"a": "1", "b": "2"}
    assert last_index == 7


def test_second_write_overwrites_the_first(tmp_path):
    store = SnapshotStore(tmp_path)
    store.write({"a": "1"}, last_index=1)
    store.write({"a": "2"}, last_index=2)
    state, last_index = store.read()
    assert state == {"a": "2"}
    assert last_index == 2


def test_leftover_tmp_file_from_a_simulated_crash_does_not_affect_read(tmp_path):
    store = SnapshotStore(tmp_path)
    store.write({"a": "1"}, last_index=1)

    # Simulate a crash partway through a *second* write: the tmp file
    # exists with new (or garbage) content, but os.replace() never ran,
    # so the final path must still reflect the first, fully-committed
    # write.
    with open(store._tmp_path, "w") as f:
        f.write("not-even-valid-json{{{")

    state, last_index = store.read()
    assert state == {"a": "1"}
    assert last_index == 1


def test_new_store_instance_reads_previously_written_snapshot(tmp_path):
    SnapshotStore(tmp_path).write({"x": "y"}, last_index=3)
    reopened = SnapshotStore(tmp_path)
    state, last_index = reopened.read()
    assert state == {"x": "y"}
    assert last_index == 3
