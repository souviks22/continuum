import random

from continuum.raft.node import NodeState, RaftNode
from continuum.raft.persistent_state import RaftPersistentState
from continuum.rpc.endpoint import RpcEndpoint
from continuum.rpc.transport import SimulatedTransport
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.storage.wal import WriteAheadLog


def build_cluster(node_ids, tmp_path, seed=0, link_delay=(1, 5), **node_kwargs):
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    net.set_default_link(LinkConfig(min_delay=link_delay[0], max_delay=link_delay[1]))
    nodes: dict[str, RaftNode] = {}
    for i, node_id in enumerate(node_ids):
        transport = SimulatedTransport(node_id, net)
        rpc = RpcEndpoint(node_id, transport, sched)
        persistent = RaftPersistentState(tmp_path / node_id)
        log_wal = WriteAheadLog(tmp_path / node_id / "log.wal")
        peers = [n for n in node_ids if n != node_id]
        node = RaftNode(
            node_id,
            peers,
            rpc,
            sched,
            persistent,
            log_wal,
            rng=random.Random(seed * 1000 + i),
            **node_kwargs,
        )
        nodes[node_id] = node
    return sched, net, nodes


def leaders(nodes):
    return [n for n in nodes.values() if n.state == NodeState.LEADER]


# -- basic election ------------------------------------------------------


def test_single_node_cluster_elects_itself_leader(tmp_path):
    sched, net, nodes = build_cluster(["a"], tmp_path)
    nodes["a"].start()
    sched.run_until(400)
    assert nodes["a"].state == NodeState.LEADER


def test_three_node_cluster_elects_exactly_one_leader(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c"], tmp_path)
    for n in nodes.values():
        n.start()
    sched.run_until(2000)
    assert len(leaders(nodes)) == 1


def test_five_node_cluster_elects_exactly_one_leader(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c", "d", "e"], tmp_path)
    for n in nodes.values():
        n.start()
    sched.run_until(3000)
    assert len(leaders(nodes)) == 1


def test_non_leaders_remain_followers(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c"], tmp_path)
    for n in nodes.values():
        n.start()
    sched.run_until(2000)
    leader = leaders(nodes)[0]
    others = [n for n in nodes.values() if n is not leader]
    assert all(n.state == NodeState.FOLLOWER for n in others)


def test_term_persisted_durably_after_election(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c"], tmp_path)
    for n in nodes.values():
        n.start()
    sched.run_until(2000)
    leader = leaders(nodes)[0]
    reopened = RaftPersistentState(tmp_path / leader.node_id)
    assert reopened.current_term == leader.current_term
    assert reopened.voted_for == leader.node_id


# -- election safety -----------------------------------------------------


def test_node_does_not_grant_two_votes_in_the_same_term(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c"], tmp_path)
    voter = nodes["c"]

    results = []

    def make_request(term, candidate):
        return {
            "term": term,
            "candidate_id": candidate,
            "last_log_index": -1,
            "last_log_term": 0,
        }

    r1 = voter._on_request_vote("a", make_request(1, "a"))
    r2 = voter._on_request_vote("b", make_request(1, "b"))

    assert r1["vote_granted"] is True
    assert r2["vote_granted"] is False  # already voted for "a" in term 1
    assert voter.voted_for == "a"


def test_stale_term_request_vote_is_rejected(tmp_path):
    sched, net, nodes = build_cluster(["a", "b"], tmp_path)
    node = nodes["a"]
    node._persistent.set_term_and_vote(5, None)

    reply = node._on_request_vote(
        "b", {"term": 3, "candidate_id": "b", "last_log_index": -1, "last_log_term": 0}
    )
    assert reply["vote_granted"] is False
    assert reply["term"] == 5


def test_higher_term_request_vote_advances_our_term(tmp_path):
    sched, net, nodes = build_cluster(["a", "b"], tmp_path)
    node = nodes["a"]
    node._persistent.set_term_and_vote(2, None)

    reply = node._on_request_vote(
        "b", {"term": 9, "candidate_id": "b", "last_log_index": -1, "last_log_term": 0}
    )
    assert reply["vote_granted"] is True
    assert node.current_term == 9
    assert node.voted_for == "b"


# -- partition behavior ----------------------------------------------------


def test_partitioned_minority_cannot_elect_a_leader(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c", "d", "e"], tmp_path)
    for n in nodes.values():
        n.start()
    net.partition({"a"}, {"b", "c", "d", "e"})
    sched.run_until(3000)

    assert nodes["a"].state != NodeState.LEADER
    assert len(leaders(nodes)) == 1
    assert leaders(nodes)[0].node_id in {"b", "c", "d", "e"}


def test_majority_partition_still_elects_a_leader(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c", "d", "e"], tmp_path)
    for n in nodes.values():
        n.start()
    net.partition({"a", "b"}, {"c", "d", "e"})
    sched.run_until(3000)

    majority_leaders = [n for n in leaders(nodes) if n.node_id in {"c", "d", "e"}]
    assert len(majority_leaders) == 1


def test_healed_partition_converges_to_a_single_leader_via_term_comparison(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c", "d", "e"], tmp_path)
    for n in nodes.values():
        n.start()
    net.partition({"a"}, {"b", "c", "d", "e"})
    # isolated node "a" repeatedly times out and bumps its term while cut off
    sched.run_until(3000)
    isolated_term_before_heal = nodes["a"].current_term
    assert isolated_term_before_heal > 0

    net.heal({"a"}, {"b", "c", "d", "e"})
    sched.run_until(6000)

    assert len(leaders(nodes)) == 1  # exactly one leader after convergence, never two


# -- leader stability via heartbeats ---------------------------------------


def test_leader_remains_stable_across_many_election_timeout_windows(tmp_path):
    sched, net, nodes = build_cluster(
        ["a", "b", "c"], tmp_path, election_timeout_range=(150, 300), heartbeat_interval=50
    )
    for n in nodes.values():
        n.start()
    sched.run_until(2000)
    first_leader = leaders(nodes)[0]
    first_term = first_leader.current_term

    # Run for a long stretch -- many multiples of the election timeout
    # window -- and confirm heartbeats suppress any re-election.
    sched.run_until(12000)
    assert len(leaders(nodes)) == 1
    assert leaders(nodes)[0].node_id == first_leader.node_id
    assert leaders(nodes)[0].current_term == first_term


# -- randomized timeouts ---------------------------------------------------


def test_election_timeouts_are_randomized_not_identical_across_nodes(tmp_path):
    sched, net, nodes = build_cluster(["a", "b", "c"], tmp_path, seed=7)
    for n in nodes.values():
        n.start()
    fire_times = sorted(n._election_timer._event.time for n in nodes.values())
    assert len(set(fire_times)) > 1  # not all nodes chose the same timeout
