import pytest

from continuum.tso.statemachine import TSOStateMachine


def test_fresh_state_machine_has_no_allocation_yet():
    sm = TSOStateMachine()
    assert sm.get_max_allocated() == -1


def test_allocate_batch_advances_high_water_mark():
    sm = TSOStateMachine()
    sm.apply_command(0, {"op": "allocate_batch", "up_to": 999})
    assert sm.get_max_allocated() == 999


def test_allocate_batch_must_strictly_increase():
    sm = TSOStateMachine()
    sm.apply_command(0, {"op": "allocate_batch", "up_to": 999})
    with pytest.raises(ValueError):
        sm.apply_command(1, {"op": "allocate_batch", "up_to": 999})
    with pytest.raises(ValueError):
        sm.apply_command(1, {"op": "allocate_batch", "up_to": 500})


def test_unknown_op_raises():
    sm = TSOStateMachine()
    with pytest.raises(ValueError):
        sm.apply_command(0, {"op": "bogus"})


def test_snapshot_and_load_state_roundtrip():
    sm = TSOStateMachine()
    sm.apply_command(0, {"op": "allocate_batch", "up_to": 1999})
    state = sm.snapshot_state()

    reloaded = TSOStateMachine()
    reloaded.load_state(state)
    assert reloaded.get_max_allocated() == 1999
