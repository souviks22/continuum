from continuum.cluster.shard_manager import ShardManager
from continuum.raft.node import NodeState
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.storage.mvcc import MVCCStateMachine, mvcc_query_fn


def build_mvcc_cluster(physical_node_ids, tmp_path, seed=0):
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    net.set_default_link(LinkConfig(min_delay=1, max_delay=5))
    managers = {
        pid: ShardManager(pid, net, sched, tmp_path / pid, rng_seed=seed)
        for pid in physical_node_ids
    }
    for pid in physical_node_ids:
        peers = [p for p in physical_node_ids if p != pid]
        managers[pid].start_shard(
            "mvcc-shard-0", peers,
            state_machine_factory=MVCCStateMachine, query_fn=mvcc_query_fn,
        )
    sched.run_until(2000)
    return sched, net, managers


def leader_of(managers, shard_id="mvcc-shard-0"):
    ls = [mgr.get(shard_id) for mgr in managers.values() if mgr.get(shard_id).state == NodeState.LEADER]
    assert len(ls) == 1
    return ls[0]


def test_mvcc_writes_replicate_with_correct_versions_on_all_replicas(tmp_path):
    sched, net, managers = build_mvcc_cluster(["n1", "n2", "n3"], tmp_path)
    leader = leader_of(managers)

    leader.propose({"op": "set", "key": "x", "value": "v1", "timestamp": 10})
    leader.propose({"op": "set", "key": "x", "value": "v2", "timestamp": 20})
    sched.run_until(sched.clock.now() + 500)

    for mgr in managers.values():
        sm = mgr.state_machine_for("mvcc-shard-0")
        assert sm.get("x", read_ts=15) == "v1"
        assert sm.get("x", read_ts=25) == "v2"
        assert sm.get("x") == "v2"


def test_mvcc_delete_replicates_as_a_tombstone(tmp_path):
    sched, net, managers = build_mvcc_cluster(["n1", "n2", "n3"], tmp_path)
    leader = leader_of(managers)

    leader.propose({"op": "set", "key": "x", "value": "v1", "timestamp": 10})
    leader.propose({"op": "delete", "key": "x", "timestamp": 20})
    sched.run_until(sched.clock.now() + 500)

    for mgr in managers.values():
        sm = mgr.state_machine_for("mvcc-shard-0")
        assert sm.get("x", read_ts=15) == "v1"
        assert sm.get("x") is None


def test_mvcc_snapshot_and_recovery_preserve_version_history(tmp_path):
    sched, net, managers = build_mvcc_cluster(
        ["n1", "n2", "n3"], tmp_path, seed=1
    )
    leader = leader_of(managers)
    for i in range(5):
        leader.propose({"op": "set", "key": f"k{i}", "value": str(i), "timestamp": 10 + i})
    sched.run_until(sched.clock.now() + 1000)
    leader.take_snapshot()

    for mgr in managers.values():
        sm = mgr.state_machine_for("mvcc-shard-0")
        for i in range(5):
            assert sm.get(f"k{i}") == str(i)
