import random

from continuum.raft.node import NodeState, RaftNode
from continuum.raft.persistent_state import RaftPersistentState
from continuum.rpc.endpoint import RpcEndpoint
from continuum.rpc.transport import SimulatedTransport
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.storage.wal import WriteAheadLog


def build_cluster(node_ids, tmp_path, seed=0, link_delay=(1, 5)):
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    net.set_default_link(LinkConfig(min_delay=link_delay[0], max_delay=link_delay[1]))
    nodes: dict[str, RaftNode] = {}
    applied: dict[str, list] = {nid: [] for nid in node_ids}
    for i, node_id in enumerate(node_ids):
        transport = SimulatedTransport(node_id, net)
        rpc = RpcEndpoint(node_id, transport, sched)
        persistent = RaftPersistentState(tmp_path / node_id)
        log_wal = WriteAheadLog(tmp_path / node_id / "log.wal")
        peers = [n for n in node_ids if n != node_id]
        node = RaftNode(
            node_id, peers, rpc, sched, persistent, log_wal,
            rng=random.Random(seed * 1000 + i),
            apply_fn=lambda cmd, nid=node_id: applied[nid].append(cmd),
        )
        nodes[node_id] = node
    return sched, net, nodes, applied


def test_single_node_cluster_commits_proposed_entries(tmp_path):
    """Regression test: propose() on a single-node (zero-peer) cluster
    used to never advance commit_index at all, since majority was only
    ever checked inside the AppendEntries-reply handler, which a
    zero-peer cluster never triggers."""
    sched, net, nodes, applied = build_cluster(["a"], tmp_path)
    nodes["a"].start()
    sched.run_until(400)
    assert nodes["a"].state == NodeState.LEADER

    idx = nodes["a"].propose({"op": "set", "key": "x", "value": "1"})
    assert idx == 0
    sched.run_until(sched.clock.now() + 10)

    assert nodes["a"].commit_index == 0
    assert applied["a"] == [{"op": "set", "key": "x", "value": "1"}]


def test_wait_for_commit_fires_immediately_if_already_committed(tmp_path):
    sched, net, nodes, applied = build_cluster(["a"], tmp_path)
    nodes["a"].start()
    sched.run_until(400)
    idx = nodes["a"].propose({"op": "set", "key": "x", "value": "1"})
    sched.run_until(sched.clock.now() + 10)
    assert nodes["a"].commit_index >= idx

    fired = []
    nodes["a"].wait_for_commit(idx, lambda: fired.append(True))
    assert fired == [True]  # synchronous, immediate


def test_wait_for_commit_fires_once_entry_actually_commits(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c"], tmp_path)
    for n in nodes.values():
        n.start()
    sched.run_until(2000)
    leader = next(n for n in nodes.values() if n.state == NodeState.LEADER)

    idx = leader.propose({"op": "set", "key": "x", "value": "1"})
    fired = []
    leader.wait_for_commit(idx, lambda: fired.append(True))
    assert fired == []  # not yet -- replication hasn't happened

    sched.run_until(sched.clock.now() + 500)
    assert fired == [True]


def test_wait_for_commit_multiple_waiters_on_same_index(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c"], tmp_path)
    for n in nodes.values():
        n.start()
    sched.run_until(2000)
    leader = next(n for n in nodes.values() if n.state == NodeState.LEADER)

    idx = leader.propose({"op": "set", "key": "x", "value": "1"})
    fired = []
    leader.wait_for_commit(idx, lambda: fired.append("first"))
    leader.wait_for_commit(idx, lambda: fired.append("second"))
    sched.run_until(sched.clock.now() + 500)
    assert set(fired) == {"first", "second"}


def test_wait_for_commit_on_follower_fires_when_follower_learns_of_commit(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c"], tmp_path)
    for n in nodes.values():
        n.start()
    sched.run_until(2000)
    leader = next(n for n in nodes.values() if n.state == NodeState.LEADER)
    follower = next(n for n in nodes.values() if n is not leader)

    idx = leader.propose({"op": "set", "key": "x", "value": "1"})
    fired = []
    follower.wait_for_commit(idx, lambda: fired.append(True))
    sched.run_until(sched.clock.now() + 500)
    assert fired == [True]
