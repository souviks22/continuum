from continuum.cluster.shard_manager import ShardManager, shard_address
from continuum.raft.node import NodeState
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler


def build_physical_cluster(physical_node_ids, tmp_path, seed=0, link_delay=(1, 5)):
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    net.set_default_link(LinkConfig(min_delay=link_delay[0], max_delay=link_delay[1]))
    managers = {
        pid: ShardManager(pid, net, sched, tmp_path / pid, rng_seed=seed)
        for pid in physical_node_ids
    }
    return sched, net, managers


def start_shard_everywhere(managers, shard_id, physical_node_ids, **kwargs):
    for pid in physical_node_ids:
        peers = [p for p in physical_node_ids if p != pid]
        managers[pid].start_shard(shard_id, peers, **kwargs)


def leader_of_shard(managers, shard_id, physical_node_ids):
    ls = [
        managers[pid].get(shard_id)
        for pid in physical_node_ids
        if managers[pid].get(shard_id).state == NodeState.LEADER
    ]
    assert len(ls) == 1
    return ls[0]


# -- addressing ---------------------------------------------------------


def test_shard_address_composes_physical_id_and_shard_id():
    assert shard_address("node1", "shard-A") == "node1:shard-A"


# -- single-shard lifecycle -----------------------------------------------


def test_start_shard_creates_a_running_raft_node(tmp_path):
    physical = ["n1", "n2", "n3"]
    sched, net, managers = build_physical_cluster(physical, tmp_path)
    start_shard_everywhere(managers, "shard-A", physical)
    sched.run_until(2000)

    leader = leader_of_shard(managers, "shard-A", physical)
    assert leader.node_id == shard_address(
        next(pid for pid in physical if managers[pid].get("shard-A") is leader), "shard-A"
    )


def test_starting_duplicate_shard_id_on_same_node_raises(tmp_path):
    physical = ["n1", "n2", "n3"]
    sched, net, managers = build_physical_cluster(physical, tmp_path)
    start_shard_everywhere(managers, "shard-A", physical)
    try:
        managers["n1"].start_shard("shard-A", ["n2", "n3"])
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_stop_shard_removes_it_and_halts_activity(tmp_path):
    physical = ["n1", "n2", "n3"]
    sched, net, managers = build_physical_cluster(physical, tmp_path)
    start_shard_everywhere(managers, "shard-A", physical)
    sched.run_until(2000)

    managers["n1"].stop_shard("shard-A")
    assert managers["n1"].get("shard-A") is None
    assert managers["n1"].state_machine_for("shard-A") is None


# -- true multi-Raft: independent shards on the same physical nodes ---------


def test_two_shards_on_same_physical_nodes_each_elect_independent_leaders(tmp_path):
    physical = ["n1", "n2", "n3"]
    sched, net, managers = build_physical_cluster(physical, tmp_path)
    start_shard_everywhere(managers, "shard-A", physical)
    start_shard_everywhere(managers, "shard-B", physical)
    sched.run_until(3000)

    leader_a = leader_of_shard(managers, "shard-A", physical)
    leader_b = leader_of_shard(managers, "shard-B", physical)
    # Leadership for each shard is independent -- no requirement that the
    # same physical node leads both, and no interference between the two
    # groups' elections despite sharing a network and scheduler.
    assert leader_a.node_id.endswith(":shard-A")
    assert leader_b.node_id.endswith(":shard-B")


def test_shard_state_machines_are_independent(tmp_path):
    physical = ["n1", "n2", "n3"]
    sched, net, managers = build_physical_cluster(physical, tmp_path)
    start_shard_everywhere(managers, "shard-A", physical)
    start_shard_everywhere(managers, "shard-B", physical)
    sched.run_until(2000)

    leader_a = leader_of_shard(managers, "shard-A", physical)
    leader_b = leader_of_shard(managers, "shard-B", physical)
    leader_a.propose({"op": "set", "key": "x", "value": "shard-A-value"})
    leader_b.propose({"op": "set", "key": "x", "value": "shard-B-value"})
    sched.run_until(sched.clock.now() + 500)

    for pid in physical:
        sm_a = managers[pid].state_machine_for("shard-A")
        sm_b = managers[pid].state_machine_for("shard-B")
        assert sm_a.get("x") == "shard-A-value"
        assert sm_b.get("x") == "shard-B-value"


def test_partitioning_shard_a_does_not_affect_shard_b(tmp_path):
    physical = ["n1", "n2", "n3"]
    sched, net, managers = build_physical_cluster(physical, tmp_path)
    start_shard_everywhere(managers, "shard-A", physical)
    start_shard_everywhere(managers, "shard-B", physical)
    sched.run_until(2000)
    leader_b_before = leader_of_shard(managers, "shard-B", physical)

    # Partition n1 away from the others, but only on shard-A's addresses.
    net.partition({shard_address("n1", "shard-A")}, {shard_address("n2", "shard-A"), shard_address("n3", "shard-A")})
    sched.run_until(sched.clock.now() + 3000)

    # shard-B's network paths were never touched, so its leader should
    # remain untouched by shard-A's disruption.
    leader_b_after = leader_of_shard(managers, "shard-B", physical)
    assert leader_b_after.node_id == leader_b_before.node_id


def test_many_shards_across_many_nodes_all_converge(tmp_path):
    physical = ["n1", "n2", "n3", "n4", "n5"]
    sched, net, managers = build_physical_cluster(physical, tmp_path)
    shard_ids = [f"shard-{i}" for i in range(4)]
    for shard_id in shard_ids:
        start_shard_everywhere(managers, shard_id, physical)
    sched.run_until(5000)

    for shard_id in shard_ids:
        leader_of_shard(managers, shard_id, physical)  # raises if not exactly one leader
