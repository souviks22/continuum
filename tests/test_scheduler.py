import pytest

from continuum.sim.scheduler import EventScheduler


def test_events_fire_in_time_order():
    sched = EventScheduler()
    order: list[str] = []
    sched.schedule_after(30, lambda: order.append("c"))
    sched.schedule_after(10, lambda: order.append("a"))
    sched.schedule_after(20, lambda: order.append("b"))
    sched.run_until_idle()
    assert order == ["a", "b", "c"]


def test_ties_broken_by_insertion_order_deterministically():
    sched = EventScheduler()
    order: list[str] = []
    sched.schedule_at(10, lambda: order.append("first"))
    sched.schedule_at(10, lambda: order.append("second"))
    sched.schedule_at(10, lambda: order.append("third"))
    sched.run_until_idle()
    assert order == ["first", "second", "third"]


def test_clock_advances_to_event_time():
    sched = EventScheduler()
    seen_times: list[int] = []
    sched.schedule_after(15, lambda: seen_times.append(sched.clock.now()))
    sched.run_until_idle()
    assert seen_times == [15]
    assert sched.clock.now() == 15


def test_run_until_stops_at_boundary_and_still_advances_clock():
    sched = EventScheduler()
    fired: list[int] = []
    sched.schedule_after(5, lambda: fired.append(5))
    sched.schedule_after(50, lambda: fired.append(50))
    sched.run_until(10)
    assert fired == [5]
    assert sched.clock.now() == 10  # clock still advances even with nothing to fire
    assert sched.pending_count() == 1


def test_cancelled_event_does_not_fire():
    sched = EventScheduler()
    fired: list[int] = []
    handle = sched.schedule_after(10, lambda: fired.append(1))
    handle.cancel()
    sched.run_until_idle()
    assert fired == []


def test_cannot_schedule_in_the_past():
    sched = EventScheduler()
    sched.run_until(100)
    with pytest.raises(ValueError):
        sched.schedule_at(50, lambda: None)


def test_negative_delay_rejected():
    sched = EventScheduler()
    with pytest.raises(ValueError):
        sched.schedule_after(-1, lambda: None)


def test_run_until_idle_guards_against_infinite_retry_loop():
    sched = EventScheduler()

    def retry_forever():
        sched.schedule_after(1, retry_forever)

    sched.schedule_after(1, retry_forever)
    with pytest.raises(RuntimeError):
        sched.run_until_idle(max_steps=1000)


def test_a_callback_scheduling_a_new_event_is_processed_in_order():
    sched = EventScheduler()
    order: list[str] = []

    def step_two():
        order.append("two")

    def step_one():
        order.append("one")
        sched.schedule_after(5, step_two)

    sched.schedule_after(1, step_one)
    sched.run_until_idle()
    assert order == ["one", "two"]
