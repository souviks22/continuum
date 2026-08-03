import os

from continuum.storage.engine import StorageEngine


def test_set_and_get(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    assert engine.get("a") == "1"
    engine.close()


def test_delete_removes_key(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.delete("a")
    assert engine.get("a") is None
    engine.close()


def test_last_index_tracks_appends(tmp_path):
    engine = StorageEngine(tmp_path)
    assert engine.last_index == -1  # nothing appended yet
    engine.set("a", "1")
    assert engine.last_index == 0
    engine.set("b", "2")
    assert engine.last_index == 1
    engine.close()


def test_reopening_without_compaction_replays_full_wal(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.set("b", "2")
    engine.delete("a")
    engine.close()

    reopened = StorageEngine(tmp_path)
    assert reopened.get("a") is None
    assert reopened.get("b") == "2"
    assert reopened.last_index == 2
    reopened.close()


def test_compact_creates_snapshot_and_empties_the_wal_file(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.set("b", "2")
    engine.compact()

    assert len(engine.wal) == 0
    assert os.path.getsize(tmp_path / "wal.log") == 0
    assert (tmp_path / "snapshot.json").exists()
    engine.close()


def test_state_and_indices_correct_after_compaction(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.set("b", "2")
    engine.compact()
    assert engine.get("a") == "1"
    assert engine.get("b") == "2"
    assert engine.last_index == 1
    engine.close()


def test_appends_after_compaction_continue_logical_indices(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.set("b", "2")
    engine.compact()
    idx = engine.set("c", "3")
    assert idx == 2  # continues from where compaction left off, not reset to 0
    engine.close()


def test_reopen_after_compaction_recovers_from_snapshot_plus_new_entries(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.set("b", "2")
    engine.compact()
    engine.set("c", "3")  # only in the WAL, not yet snapshotted
    engine.close()

    reopened = StorageEngine(tmp_path)
    assert reopened.get("a") == "1"
    assert reopened.get("b") == "2"
    assert reopened.get("c") == "3"
    assert reopened.last_index == 2
    reopened.close()


def test_multiple_compactions_in_sequence(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.compact()
    engine.set("b", "2")
    engine.compact()
    engine.set("c", "3")
    engine.compact()

    assert engine.get("a") == "1"
    assert engine.get("b") == "2"
    assert engine.get("c") == "3"
    assert engine.last_index == 2
    engine.close()


def test_compact_with_no_new_entries_since_last_snapshot_is_a_noop(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.compact()
    snapshot_mtime_before = os.path.getmtime(tmp_path / "snapshot.json")

    engine.compact()  # nothing new appended since the first compact()

    snapshot_mtime_after = os.path.getmtime(tmp_path / "snapshot.json")
    assert snapshot_mtime_before == snapshot_mtime_after
    engine.close()


def test_crash_between_snapshot_write_and_wal_replacement_still_recovers_correctly(tmp_path):
    """Reproduces crash scenario 2 from StorageEngine.compact()'s
    docstring directly: the new snapshot is fully written, but the WAL
    file is never replaced (simulating a crash in between). A fresh
    engine instance opened against the same directory must still recover
    the correct state -- via the index check in _replay_wal, not by
    relying on the WAL having actually been compacted."""
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    engine.set("b", "2")

    # Manually perform only the snapshot half of compact(), deliberately
    # skipping the WAL replacement step to simulate the crash window.
    state = engine.state_machine.snapshot_state()
    engine._snapshots.write(state, last_index=engine.last_index)
    engine.close()  # WAL file on disk still has both original entries

    recovered = StorageEngine(tmp_path)
    assert recovered.get("a") == "1"
    assert recovered.get("b") == "2"
    assert recovered.last_index == 1  # not double-counted from replaying stale WAL entries
    recovered.close()


def test_append_after_simulated_partial_compaction_uses_correct_next_index(tmp_path):
    engine = StorageEngine(tmp_path)
    engine.set("a", "1")
    state = engine.state_machine.snapshot_state()
    engine._snapshots.write(state, last_index=engine.last_index)
    engine.close()

    recovered = StorageEngine(tmp_path)
    idx = recovered.set("b", "2")
    assert idx == 1  # continues correctly despite the stale WAL still on disk
    recovered.close()
