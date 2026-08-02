from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler


def make_net(seed: int = 0) -> tuple[EventScheduler, SimulatedNetwork]:
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    return sched, net


def test_node_starts_up_by_default():
    _, net = make_net()
    net.register_node("a", lambda src, payload: None)
    assert net.is_up("a")


def test_crash_now_marks_node_down():
    _, net = make_net()
    net.register_node("a", lambda src, payload: None)
    net.crash_now("a")
    assert not net.is_up("a")


def test_restart_now_marks_node_up_again():
    _, net = make_net()
    net.register_node("a", lambda src, payload: None)
    net.crash_now("a")
    net.restart_now("a")
    assert net.is_up("a")


def test_message_sent_by_crashed_node_never_delivered():
    sched, net = make_net()
    received = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append(payload))
    net.crash_now("a")

    net.send("a", "b", b"x")
    sched.run_until_idle()

    assert received == []


def test_message_to_node_that_crashes_before_delivery_is_lost():
    sched, net = make_net()
    received = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append(payload))
    net.set_default_link(LinkConfig(min_delay=10, max_delay=10))

    net.send("a", "b", b"in_flight_when_crash_happens")
    net.crash_at(3, "b")  # crashes well before the message would arrive at t=10
    sched.run_until_idle()

    assert received == []


def test_message_arrives_after_receiver_restarts():
    sched, net = make_net()
    received = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append(payload))
    net.set_default_link(LinkConfig(min_delay=10, max_delay=10))

    net.crash_now("b")
    net.send("a", "b", b"sent_while_down")  # b is down; a is up, so send succeeds
    net.restart_at(5, "b")
    sched.run_until_idle()

    assert received == [b"sent_while_down"]


def test_on_restart_callback_runs_before_node_marked_up():
    sched, net = make_net()
    events = []
    net.register_node("a", lambda src, payload: None)

    def recover():
        events.append(("recovering", net.is_up("a")))

    net.crash_now("a")
    net.restart_at(5, "a", on_restart=recover)
    sched.run_until_idle()

    assert events == [("recovering", False)]  # still down mid-recovery
    assert net.is_up("a")


def test_crash_at_schedules_crash_in_the_future():
    sched, net = make_net()
    received = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append(sched.clock.now()))
    net.set_default_link(LinkConfig(min_delay=1, max_delay=1))

    net.crash_at(100, "b")
    net.send("a", "b", b"before_crash")  # delivered at t=1, well before crash at t=100
    sched.run_until_idle()

    assert received == [1]
    assert not net.is_up("b")


def test_repeated_crash_restart_cycle():
    sched, net = make_net()
    net.register_node("a", lambda src, payload: None)

    net.crash_at(10, "a")
    net.restart_at(20, "a")
    net.crash_at(30, "a")
    net.restart_at(40, "a")

    sched.run_until(15)
    assert not net.is_up("a")
    sched.run_until(25)
    assert net.is_up("a")
    sched.run_until(35)
    assert not net.is_up("a")
    sched.run_until(45)
    assert net.is_up("a")
