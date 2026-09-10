from continuum.storage.mvcc import MVCCStateMachine, mvcc_query_fn


def test_apply_set_then_get_latest():
    sm = MVCCStateMachine()
    sm.apply_command(0, {"op": "set", "key": "x", "value": "v1", "timestamp": 10})
    assert sm.get("x") == "v1"


def test_get_at_specific_read_ts_sees_correct_version():
    sm = MVCCStateMachine()
    sm.apply_command(0, {"op": "set", "key": "x", "value": "v1", "timestamp": 10})
    sm.apply_command(1, {"op": "set", "key": "x", "value": "v2", "timestamp": 20})
    assert sm.get("x", read_ts=15) == "v1"
    assert sm.get("x", read_ts=25) == "v2"


def test_apply_delete_removes_key_going_forward():
    sm = MVCCStateMachine()
    sm.apply_command(0, {"op": "set", "key": "x", "value": "v1", "timestamp": 10})
    sm.apply_command(1, {"op": "delete", "key": "x", "timestamp": 20})
    assert sm.get("x") is None
    assert sm.get("x", read_ts=15) == "v1"  # still visible before the delete's timestamp


def test_unknown_op_raises():
    sm = MVCCStateMachine()
    try:
        sm.apply_command(0, {"op": "bogus", "key": "x", "timestamp": 1})
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_snapshot_and_load_state_roundtrip():
    sm = MVCCStateMachine()
    sm.apply_command(0, {"op": "set", "key": "x", "value": "v1", "timestamp": 10})
    sm.apply_command(1, {"op": "set", "key": "x", "value": "v2", "timestamp": 20})
    sm.apply_command(2, {"op": "set", "key": "y", "value": "w1", "timestamp": 5})
    state = sm.snapshot_state()

    reloaded = MVCCStateMachine()
    reloaded.load_state(state)
    assert reloaded.get("x", read_ts=15) == "v1"
    assert reloaded.get("x", read_ts=25) == "v2"
    assert reloaded.get("y") == "w1"


# -- query_fn --------------------------------------------------------------


def test_query_fn_get_without_read_ts_returns_latest():
    sm = MVCCStateMachine()
    sm.apply_command(0, {"op": "set", "key": "x", "value": "v1", "timestamp": 10})
    result = mvcc_query_fn(sm, {"op": "get", "key": "x"})
    assert result == {"value": "v1"}


def test_query_fn_get_with_read_ts_returns_snapshot_value():
    sm = MVCCStateMachine()
    sm.apply_command(0, {"op": "set", "key": "x", "value": "v1", "timestamp": 10})
    sm.apply_command(1, {"op": "set", "key": "x", "value": "v2", "timestamp": 20})
    result = mvcc_query_fn(sm, {"op": "get", "key": "x", "read_ts": 15})
    assert result == {"value": "v1"}


def test_query_fn_unsupported_op_raises():
    sm = MVCCStateMachine()
    try:
        mvcc_query_fn(sm, {"op": "route", "key": "x"})
        assert False, "expected ValueError"
    except ValueError:
        pass
