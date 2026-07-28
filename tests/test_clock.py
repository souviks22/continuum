import pytest

from continuum.sim.clock import VirtualClock


def test_starts_at_zero_by_default():
    clock = VirtualClock()
    assert clock.now() == 0


def test_starts_at_given_time():
    clock = VirtualClock(start=100)
    assert clock.now() == 100


def test_advance_to_moves_forward():
    clock = VirtualClock()
    clock.advance_to(50)
    assert clock.now() == 50


def test_advance_to_same_time_is_noop():
    clock = VirtualClock(start=10)
    clock.advance_to(10)
    assert clock.now() == 10


def test_cannot_move_backward():
    clock = VirtualClock(start=10)
    with pytest.raises(ValueError):
        clock.advance_to(5)


def test_negative_start_rejected():
    with pytest.raises(ValueError):
        VirtualClock(start=-1)
