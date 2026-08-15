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
    applied: dict[str, list] = {node_id: [] for node_id in node_ids}
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
            apply_fn=lambda cmd, nid=node_id: applied[nid].append(cmd),
            **node_kwargs,
        )
        nodes[node_id] = node
    return sched, net, nodes, applied


def leader_of(nodes):
    ls = [n for n in nodes.values() if n.state == NodeState.LEADER]
    assert len(ls) == 1
    return ls[0]


def elect(sched, nodes, run_until=2000):
    for n in nodes.values():
        n.start()
    sched.run_until(run_until)
    return leader_of(nodes)


# -- basic replication and commit ------------------------------------------


def test_proposed_entry_gets_committed_and_applied_on_all_nodes(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c"], tmp_path)
    leader = elect(sched, nodes)

    idx = leader.propose({"op": "set", "key": "x", "value": "1"})
    assert idx == 0
    sched.run_until(sched.clock.now() + 500)

    assert leader.commit_index == 0
    for n in nodes.values():
        assert n.commit_index == 0
        assert applied[n.node_id] == [{"op": "set", "key": "x", "value": "1"}]


def test_propose_on_a_follower_returns_none(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c"], tmp_path)
    leader = elect(sched, nodes)
    follower = next(n for n in nodes.values() if n is not leader)
    assert follower.propose({"op": "set", "key": "x", "value": "1"}) is None


def test_multiple_entries_committed_and_applied_in_order(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c"], tmp_path)
    leader = elect(sched, nodes)

    for i in range(5):
        leader.propose({"op": "set", "key": f"k{i}", "value": str(i)})
    sched.run_until(sched.clock.now() + 1000)

    for n in nodes.values():
        assert applied[n.node_id] == [
            {"op": "set", "key": f"k{i}", "value": str(i)} for i in range(5)
        ]


def test_follower_logs_match_leader_log_after_replication(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c"], tmp_path)
    leader = elect(sched, nodes)
    leader.propose({"op": "set", "key": "x", "value": "1"})
    leader.propose({"op": "set", "key": "y", "value": "2"})
    sched.run_until(sched.clock.now() + 500)

    for n in nodes.values():
        assert n.log.last_index == leader.log.last_index
        for i in range(leader.log.last_index + 1):
            assert n.log.get(i).command == leader.log.get(i).command
            assert n.log.get(i).term == leader.log.get(i).term


# -- catching up a lagging / conflicting follower ---------------------------


def test_partitioned_follower_catches_up_after_healing(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c"], tmp_path)
    leader = elect(sched, nodes)
    lagging = next(n for n in nodes.values() if n is not leader)

    net.partition({lagging.node_id}, {n for n in nodes if n != lagging.node_id})
    for i in range(3):
        leader.propose({"op": "set", "key": f"k{i}", "value": str(i)})
    sched.run_until(sched.clock.now() + 1000)
    assert applied[lagging.node_id] == []  # cut off, nothing applied yet

    net.heal({lagging.node_id}, {n for n in nodes if n != lagging.node_id})
    sched.run_until(sched.clock.now() + 1000)

    assert applied[lagging.node_id] == [
        {"op": "set", "key": f"k{i}", "value": str(i)} for i in range(3)
    ]


def test_follower_overwrites_conflicting_entry_from_a_stale_term(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b"], tmp_path)
    follower = nodes["b"]

    # Follower has a stale, never-committed entry at index 0 from term 1
    # -- e.g. written under an old leader that never reached a majority
    # before a new election happened.
    follower.log.append(term=1, command={"op": "set", "key": "bogus", "value": "z"})
    follower._persistent.set_term_and_vote(2, None)  # follower has since observed term 2

    # The real (term 2) leader's AppendEntries disagrees at that same
    # index -- a genuine conflict (same index, different term), which is
    # exactly what append_entries_from_leader's truncate-and-replace path
    # exists for.
    reply = follower._on_append_entries(
        "a",
        {
            "term": 2,
            "leader_id": "a",
            "prev_log_index": -1,
            "prev_log_term": 0,
            "entries": [
                {"index": 0, "term": 2, "command": {"op": "set", "key": "real", "value": "1"}}
            ],
            "leader_commit": 0,
        },
    )

    assert reply["success"] is True
    assert follower.log.get(0).command == {"op": "set", "key": "real", "value": "1"}
    assert applied["b"] == [{"op": "set", "key": "real", "value": "1"}]


# -- safety across leader change -------------------------------------------


def test_committed_entries_survive_a_leader_change(tmp_path):
    sched, net, nodes, applied = build_cluster(["a", "b", "c", "d", "e"], tmp_path)
    first_leader = elect(sched, nodes)
    first_leader.propose({"op": "set", "key": "durable", "value": "1"})
    sched.run_until(sched.clock.now() + 500)
    assert first_leader.commit_index == 0

    # Force the current leader out so a new election has to happen.
    first_leader.stop()
    remaining = {nid: n for nid, n in nodes.items() if n is not first_leader}
    net.crash_now(first_leader.node_id)
    sched.run_until(sched.clock.now() + 2000)

    new_leader = leader_of(remaining)
    assert new_leader.node_id != first_leader.node_id
    # The committed entry must still be present and still committed on
    # whichever node became the new leader -- Raft's leader completeness
    # property (a candidate can't win without holding every committed entry).
    assert new_leader.log.get(0).command == {"op": "set", "key": "durable", "value": "1"}
    assert new_leader.commit_index >= 0
