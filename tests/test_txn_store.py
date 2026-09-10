from continuum.txn.store import PercolatorStore


def test_prewrite_then_commit_makes_value_visible():
    store = PercolatorStore()
    ok, reason = store.prewrite("x", "v1", start_ts=10, primary_key="x")
    assert ok is True
    ok, reason = store.commit("x", start_ts=10, commit_ts=11)
    assert ok is True
    value, error = store.get("x", read_ts=11)
    assert value == "v1"
    assert error is None


def test_prewritten_but_uncommitted_value_is_not_visible():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    value, error = store.get("x", read_ts=999)
    assert value is None
    assert error is not None  # blocked by the outstanding lock


def test_read_before_commit_ts_does_not_see_the_value():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=20)
    value, error = store.get("x", read_ts=15)
    assert value is None
    assert error is None  # legitimately absent at this snapshot, not blocked


def test_read_at_or_after_commit_ts_sees_the_value():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=20)
    assert store.get("x", read_ts=20)[0] == "v1"
    assert store.get("x", read_ts=999)[0] == "v1"


def test_second_prewrite_while_locked_fails():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    ok, reason = store.prewrite("x", "v2", start_ts=20, primary_key="x")
    assert ok is False
    assert reason is not None


def test_prewrite_after_newer_commit_fails_as_write_conflict():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=15)
    # A transaction that started before ts=15 committed must be rejected
    ok, reason = store.prewrite("x", "v2", start_ts=12, primary_key="x")
    assert ok is False
    assert "conflict" in reason


def test_prewrite_after_older_commit_succeeds():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=15)
    ok, reason = store.prewrite("x", "v2", start_ts=20, primary_key="x")
    assert ok is True


def test_commit_without_matching_lock_fails():
    store = PercolatorStore()
    ok, reason = store.commit("x", start_ts=10, commit_ts=11)
    assert ok is False


def test_commit_with_wrong_start_ts_fails():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    ok, reason = store.commit("x", start_ts=999, commit_ts=11)
    assert ok is False


def test_rollback_releases_the_lock():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    ok, reason = store.rollback("x", start_ts=10)
    assert ok is True
    assert "x" not in store.locks


def test_rollback_of_already_absent_lock_is_still_success():
    store = PercolatorStore()
    ok, reason = store.rollback("x", start_ts=10)
    assert ok is True


def test_prewrite_succeeds_again_after_rollback():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.rollback("x", start_ts=10)
    ok, reason = store.prewrite("x", "v2", start_ts=20, primary_key="x")
    assert ok is True


def test_rolled_back_value_never_becomes_visible():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.rollback("x", start_ts=10)
    value, error = store.get("x", read_ts=999)
    assert value is None
    assert error is None  # not locked (rolled back), and never committed


def test_delete_via_none_value_creates_a_visible_tombstone():
    store = PercolatorStore()
    store.prewrite("x", "v1", start_ts=10, primary_key="x")
    store.commit("x", start_ts=10, commit_ts=11)
    store.prewrite("x", None, start_ts=20, primary_key="x")
    store.commit("x", start_ts=20, commit_ts=21)
    assert store.get("x", read_ts=15)[0] == "v1"
    assert store.get("x", read_ts=21)[0] is None
