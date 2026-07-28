import pytest

from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler


def make_net(seed: int = 0) -> tuple[EventScheduler, SimulatedNetwork]:
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    return sched, net


def test_message_delivered_with_configured_delay():
    sched, net = make_net()
    received: list[tuple[int, bytes]] = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append((sched.clock.now(), payload)))
    net.set_default_link(LinkConfig(min_delay=7, max_delay=7))

    net.send("a", "b", b"hello")
    sched.run_until_idle()

    assert received == [(7, b"hello")]


def test_send_to_unknown_node_raises():
    _, net = make_net()
    net.register_node("a", lambda src, payload: None)
    with pytest.raises(ValueError):
        net.send("a", "ghost", b"x")


def test_full_partition_blocks_both_directions():
    sched, net = make_net()
    received: list[bytes] = []
    net.register_node("a", lambda src, payload: received.append(payload))
    net.register_node("b", lambda src, payload: received.append(payload))
    net.partition({"a"}, {"b"})

    net.send("a", "b", b"x")
    net.send("b", "a", b"y")
    sched.run_until_idle()

    assert received == []


def test_healed_partition_allows_delivery_again():
    sched, net = make_net()
    received: list[bytes] = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append(payload))
    net.partition({"a"}, {"b"})
    net.heal({"a"}, {"b"})

    net.send("a", "b", b"x")
    sched.run_until_idle()

    assert received == [b"x"]


def test_asymmetric_partition_blocks_only_one_direction():
    sched, net = make_net()
    received_by_b: list[bytes] = []
    received_by_a: list[bytes] = []
    net.register_node("a", lambda src, payload: received_by_a.append(payload))
    net.register_node("b", lambda src, payload: received_by_b.append(payload))

    # a can reach b, but b cannot reach a
    net.block("b", "a")

    net.send("a", "b", b"from_a")
    net.send("b", "a", b"from_b")
    sched.run_until_idle()

    assert received_by_b == [b"from_a"]
    assert received_by_a == []


def test_loss_rate_one_drops_every_message():
    sched, net = make_net()
    received: list[bytes] = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append(payload))
    net.set_default_link(LinkConfig(loss_rate=1.0))

    for i in range(20):
        net.send("a", "b", str(i).encode())
    sched.run_until_idle()

    assert received == []


def test_duplicate_rate_one_delivers_every_message_twice():
    sched, net = make_net(seed=1)
    received: list[bytes] = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append(payload))
    net.set_default_link(LinkConfig(min_delay=1, max_delay=1, duplicate_rate=1.0))

    net.send("a", "b", b"x")
    sched.run_until_idle()

    assert received == [b"x", b"x"]


def test_variable_delay_can_reorder_messages():
    # First message sent draws a long delay, second sent draws a short one:
    # the transport has no notion of "send order" preservation, only
    # "delivery scheduled at time = send_time + delay". This test picks a
    # seed known (empirically, for this RNG stream) to produce that
    # ordering, to demonstrate the mechanism explicitly rather than assert
    # on probabilistic behaviour across arbitrary seeds.
    for seed in range(50):
        sched, net = make_net(seed=seed)
        received: list[bytes] = []
        net.register_node("a", lambda src, payload: None)
        net.register_node(
            "b", lambda src, payload: received.append(payload)
        )
        net.set_default_link(LinkConfig(min_delay=1, max_delay=20))

        net.send("a", "b", b"first")
        net.send("a", "b", b"second")
        sched.run_until_idle()

        if received == [b"second", b"first"]:
            return  # found a seed that reorders -- mechanism confirmed
    pytest.fail("no seed in range produced reordering; delay-based reordering may be broken")


def test_same_seed_is_fully_deterministic():
    def run(seed: int) -> list[tuple[int, bytes]]:
        sched, net = make_net(seed=seed)
        received: list[tuple[int, bytes]] = []
        net.register_node("a", lambda src, payload: None)
        net.register_node(
            "b", lambda src, payload: received.append((sched.clock.now(), payload))
        )
        net.set_default_link(LinkConfig(min_delay=1, max_delay=10, loss_rate=0.3))
        for i in range(30):
            net.send("a", "b", str(i).encode())
        sched.run_until_idle()
        return received

    assert run(42) == run(42)


def test_per_link_override_takes_precedence_over_default():
    sched, net = make_net()
    received: list[int] = []
    net.register_node("a", lambda src, payload: None)
    net.register_node("b", lambda src, payload: received.append(sched.clock.now()))
    net.set_default_link(LinkConfig(min_delay=100, max_delay=100))
    net.set_link("a", "b", LinkConfig(min_delay=3, max_delay=3))

    net.send("a", "b", b"x")
    sched.run_until_idle()

    assert received == [3]
