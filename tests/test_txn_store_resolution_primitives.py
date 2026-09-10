from continuum.txn.store import PercolatorStore


def test_get_with_lock_returns_lock_object_when_blocked():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    value, lock = store.get_with_lock("x", read_ts=999)
    assert value is None
    assert lock is not None
    assert lock.primary_key == "x"
    assert lock.start_ts == 10


def test_get_with_lock_returns_none_lock_when_not_blocked():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=11)
    value, lock = store.get_with_lock("x", read_ts=11)
    assert value == "v1"
    assert lock is None


def test_get_lock_returns_current_lock():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="p")
    lock = store.get_lock("x")
    assert lock.primary_key == "p"
    assert lock.start_ts == 10


def test_get_lock_none_when_unlocked():
    store = PercolatorStore()
    assert store.get_lock("x") is None


def test_find_commit_ts_locates_matching_commit():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=15)
    assert store.find_commit_ts("x", start_ts=10) == 15


def test_find_commit_ts_none_when_never_committed():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    assert store.find_commit_ts("x", start_ts=10) is None


def test_find_commit_ts_distinguishes_between_multiple_commits():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=15)
    store.prewrite("x", "v2", start_ts=20, primary_key="x")
    store.commit("x", start_ts=20, commit_ts=25)
    assert store.find_commit_ts("x", start_ts=10) == 15
    assert store.find_commit_ts("x", start_ts=20) == 25
