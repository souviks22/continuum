from continuum.storage.statemachine import Operation, StateMachine


def test_apply_set_then_get():
    sm = StateMachine()
    sm.apply(Operation(index=0, op="set", key="a", value="1"))
    assert sm.get("a") == "1"


def test_apply_set_overwrites_existing_value():
    sm = StateMachine()
    sm.apply(Operation(index=0, op="set", key="a", value="1"))
    sm.apply(Operation(index=1, op="set", key="a", value="2"))
    assert sm.get("a") == "2"


def test_apply_delete_removes_key():
    sm = StateMachine()
    sm.apply(Operation(index=0, op="set", key="a", value="1"))
    sm.apply(Operation(index=1, op="delete", key="a"))
    assert sm.get("a") is None


def test_delete_nonexistent_key_is_a_noop():
    sm = StateMachine()
    sm.apply(Operation(index=0, op="delete", key="ghost"))
    assert sm.get("ghost") is None


def test_get_missing_key_returns_none():
    sm = StateMachine()
    assert sm.get("missing") is None


def test_snapshot_state_returns_an_independent_copy():
    sm = StateMachine()
    sm.apply(Operation(index=0, op="set", key="a", value="1"))
    snap = sm.snapshot_state()
    snap["a"] = "mutated"
    assert sm.get("a") == "1"  # internal state unaffected by mutating the copy


def test_load_state_replaces_all_data():
    sm = StateMachine()
    sm.apply(Operation(index=0, op="set", key="old", value="x"))
    sm.load_state({"new": "y"})
    assert sm.get("old") is None
    assert sm.get("new") == "y"


def test_operation_encode_decode_roundtrip_for_set():
    op = Operation(index=5, op="set", key="k", value="v")
    assert Operation.decode(op.encode()) == op


def test_operation_encode_decode_roundtrip_for_delete_with_none_value():
    op = Operation(index=5, op="delete", key="k", value=None)
    assert Operation.decode(op.encode()) == op
