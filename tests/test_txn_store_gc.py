from continuum.txn.store import PercolatorStore


def test_gc_discards_superseded_data_and_write_versions():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=11)
    store.prewrite("x", "v2", start_ts=20, primary_key="x")
    store.commit("x", start_ts=20, commit_ts=21)
    store.prewrite("x", "v3", start_ts=30, primary_key="x")
    store.commit("x", start_ts=30, commit_ts=31)

    store.gc(safe_point=25)

    # Version at commit_ts=21 (covering reads from 21 up to just before
    # 31) survives since it's the newest at-or-below the safe point;
    # everything strictly older is gone.
    assert store.writes.versions("x") == [(21, "20"), (31, "30")]


def test_gc_preserves_correct_read_results_above_safe_point():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=11)
    store.prewrite("x", "v2", start_ts=20, primary_key="x")
    store.commit("x", start_ts=20, commit_ts=21)

    store.gc(safe_point=15)

    value, error = store.get("x", read_ts=15)
    assert value == "v1"
    assert error is None
    value, error = store.get("x", read_ts=999)
    assert value == "v2"


def test_gc_does_not_touch_locks():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.gc(safe_point=10**9)
    assert store.get_lock("x") is not None  # a lock is not versioned data; GC must not remove it
