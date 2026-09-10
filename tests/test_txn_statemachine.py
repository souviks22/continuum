from continuum.txn.statemachine import PercolatorStateMachine, percolator_query_fn


def test_apply_prewrite_records_success_result():
    sm = PercolatorStateMachine()
    sm.apply_command(0, {"op": "prewrite", "key": "x", "value": "v1", "start_ts": 10, "primary_key": "x"})
    assert sm.get_result(0) == {"success": True, "reason": None}


def test_apply_conflicting_prewrite_records_failure_result():
    sm = PercolatorStateMachine()
    sm.apply_command(0, {"op": "prewrite", "key": "x", "value": "v1", "start_ts": 10, "primary_key": "x"})
    sm.apply_command(1, {"op": "prewrite", "key": "x", "value": "v2", "start_ts": 20, "primary_key": "x"})
    result = sm.get_result(1)
    assert result["success"] is False
    assert result["reason"] is not None


def test_apply_commit_then_read_sees_value():
    sm = PercolatorStateMachine()
    sm.apply_command(0, {"op": "prewrite", "key": "x", "value": "v1", "start_ts": 10, "primary_key": "x"})
    sm.apply_command(1, {"op": "commit", "key": "x", "start_ts": 10, "commit_ts": 11})
    assert sm.get_result(1) == {"success": True, "reason": None}
    assert sm.get("x", 11) == ("v1", None)


def test_unknown_op_raises():
    sm = PercolatorStateMachine()
    try:
        sm.apply_command(0, {"op": "bogus"})
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_get_result_for_unknown_index_is_none():
    sm = PercolatorStateMachine()
    assert sm.get_result(999) is None


def test_snapshot_and_load_state_preserves_committed_data():
    sm = PercolatorStateMachine()
    sm.apply_command(0, {"op": "prewrite", "key": "x", "value": "v1", "start_ts": 10, "primary_key": "x"})
    sm.apply_command(1, {"op": "commit", "key": "x", "start_ts": 10, "commit_ts": 11})
    state = sm.snapshot_state()

    reloaded = PercolatorStateMachine()
    reloaded.load_state(state)
    assert reloaded.get("x", 11) == ("v1", None)


def test_snapshot_and_load_state_preserves_outstanding_locks():
    sm = PercolatorStateMachine()
    sm.apply_command(0, {"op": "prewrite", "key": "x", "value": "v1", "start_ts": 10, "primary_key": "x"})
    state = sm.snapshot_state()

    reloaded = PercolatorStateMachine()
    reloaded.load_state(state)
    value, error = reloaded.get("x", 999)
    assert value is None
    assert error is not None  # lock survived the snapshot roundtrip


# -- query_fn --------------------------------------------------------------


def test_query_fn_returns_value_and_error_fields():
    sm = PercolatorStateMachine()
    sm.apply_command(0, {"op": "prewrite", "key": "x", "value": "v1", "start_ts": 10, "primary_key": "x"})
    sm.apply_command(1, {"op": "commit", "key": "x", "start_ts": 10, "commit_ts": 11})
    result = percolator_query_fn(sm, {"op": "get", "key": "x", "read_ts": 11})
    assert result == {"value": "v1", "error": None}


def test_query_fn_unsupported_op_raises():
    sm = PercolatorStateMachine()
    try:
        percolator_query_fn(sm, {"op": "route"})
        assert False, "expected ValueError"
    except ValueError:
        pass
