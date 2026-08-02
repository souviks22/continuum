from continuum.rpc.endpoint import RpcEndpoint
from continuum.rpc.message import Message
from continuum.rpc.transport import SimulatedTransport
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler


def make_cluster(node_ids, seed: int = 0):
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    net.set_default_link(LinkConfig(min_delay=1, max_delay=1))
    endpoints: dict[str, RpcEndpoint] = {}
    for node_id in node_ids:
        transport = SimulatedTransport(node_id, net)
        endpoints[node_id] = RpcEndpoint(node_id, transport, sched)
    return sched, net, endpoints


# -- message envelope ---------------------------------------------------


def test_message_round_trips_through_encode_decode():
    msg = Message(kind="request", msg_id="7", method="Ping", body={"n": 1})
    decoded = Message.decode(msg.encode())
    assert decoded == msg


# -- transport ------------------------------------------------------------


def test_transport_delivers_raw_payload_to_receive_handler():
    sched = EventScheduler()
    net = SimulatedNetwork(sched)
    received = []
    a = SimulatedTransport("a", net)
    b = SimulatedTransport("b", net)
    b.set_receive_handler(lambda src, payload: received.append((src, payload)))

    a.send("b", b"hello")
    sched.run_until_idle()

    assert received == [("a", b"hello")]


# -- request/reply endpoint -----------------------------------------------


def test_successful_call_delivers_reply_body():
    sched, net, ep = make_cluster(["a", "b"])
    ep["b"].register_method("Echo", lambda src, body: {"echo": body["msg"]})

    results = []
    ep["a"].call(
        "b", "Echo", {"msg": "hi"}, timeout=100,
        on_reply=lambda body, timed_out: results.append((body, timed_out)),
    )
    sched.run_until_idle()

    assert results == [({"echo": "hi"}, False)]


def test_call_to_unknown_method_times_out():
    sched, net, ep = make_cluster(["a", "b"])
    # b registers nothing

    results = []
    ep["a"].call(
        "b", "DoesNotExist", {}, timeout=10,
        on_reply=lambda body, timed_out: results.append((body, timed_out)),
    )
    sched.run_until_idle()

    assert results == [(None, True)]


def test_call_across_partition_times_out():
    sched, net, ep = make_cluster(["a", "b"])
    ep["b"].register_method("Echo", lambda src, body: {"echo": body})
    net.partition({"a"}, {"b"})

    results = []
    ep["a"].call(
        "b", "Echo", {"msg": "hi"}, timeout=10,
        on_reply=lambda body, timed_out: results.append((body, timed_out)),
    )
    sched.run_until_idle()

    assert results == [(None, True)]


def test_late_reply_after_timeout_is_ignored_not_double_delivered():
    sched, net, ep = make_cluster(["a", "b"])
    # b's reply link back to a is slow (delay 20); a's timeout is 5, so
    # the timeout fires first and the reply arrives after.
    net.set_link("b", "a", LinkConfig(min_delay=20, max_delay=20))
    ep["b"].register_method("Echo", lambda src, body: {"echo": body})

    results = []
    ep["a"].call(
        "b", "Echo", {"msg": "hi"}, timeout=5,
        on_reply=lambda body, timed_out: results.append((body, timed_out)),
    )
    sched.run_until_idle()

    # exactly one callback invocation: the timeout. The late reply must
    # not trigger a second call to on_reply.
    assert results == [(None, True)]


def test_concurrent_pending_requests_correlate_independently():
    sched, net, ep = make_cluster(["a", "b"])
    ep["b"].register_method("Double", lambda src, body: {"n": body["n"] * 2})

    results = {}
    for n in [1, 2, 3]:
        ep["a"].call(
            "b", "Double", {"n": n}, timeout=100,
            on_reply=lambda body, timed_out, n=n: results.__setitem__(n, body),
        )
    sched.run_until_idle()

    assert results == {1: {"n": 2}, 2: {"n": 4}, 3: {"n": 6}}


def test_timeout_still_fires_if_destination_crashes_mid_flight():
    sched, net, ep = make_cluster(["a", "b"])
    ep["b"].register_method("Echo", lambda src, body: {"echo": body})
    net.set_default_link(LinkConfig(min_delay=5, max_delay=5))

    results = []
    ep["a"].call(
        "b", "Echo", {"msg": "hi"}, timeout=20,
        on_reply=lambda body, timed_out: results.append((body, timed_out)),
    )
    net.crash_at(1, "b")  # crashes before the request (arriving at t=5) lands
    sched.run_until_idle()

    assert results == [(None, True)]


def test_reply_handler_receives_the_correct_requesting_node():
    sched, net, ep = make_cluster(["a", "b"])
    seen_from = []

    def handler(from_node, body):
        seen_from.append(from_node)
        return {}

    ep["b"].register_method("Ping", handler)
    ep["a"].call("b", "Ping", {}, timeout=100, on_reply=lambda body, timed_out: None)
    sched.run_until_idle()

    assert seen_from == ["a"]
