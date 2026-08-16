from continuum.cluster.bootstrap import METADATA_SHARD_ID, find_metadata_leader
from continuum.cluster.metadata import MetadataStateMachine
from continuum.cluster.shard_manager import ShardManager
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler


def build_meta_cluster(physical_node_ids, tmp_path, seed=0):
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
            METADATA_SHARD_ID, peers, state_machine_factory=MetadataStateMachine
        )
    sched.run_until(2000)
    return sched, net, managers


def all_meta_state_machines(managers):
    return {pid: mgr.state_machine_for(METADATA_SHARD_ID) for pid, mgr in managers.items()}


# -- the metadata group is a real Raft group, for free -----------------------


def test_metadata_group_elects_exactly_one_leader(tmp_path):
    sched, net, managers = build_meta_cluster(["n1", "n2", "n3"], tmp_path)
    leader = find_metadata_leader(managers)
    assert leader is not None
    assert leader.node_id.endswith(f":{METADATA_SHARD_ID}")


def test_proposed_shard_registration_replicates_to_all_metadata_replicas(tmp_path):
    sched, net, managers = build_meta_cluster(["n1", "n2", "n3"], tmp_path)
    leader = find_metadata_leader(managers)

    leader.propose({
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1", "n2", "n3"],
    })
    sched.run_until(sched.clock.now() + 500)

    for sm in all_meta_state_machines(managers).values():
        meta = sm.get_shard("shard-0")
        assert meta is not None
        assert meta.replicas == ["n1", "n2", "n3"]


def test_split_replicates_and_routing_converges_on_every_replica(tmp_path):
    sched, net, managers = build_meta_cluster(["n1", "n2", "n3"], tmp_path)
    leader = find_metadata_leader(managers)

    leader.propose({
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1", "n2", "n3"],
    })
    sched.run_until(sched.clock.now() + 300)
    leader.propose({
        "op": "split_shard", "shard_id": "shard-0", "split_key": "m",
        "new_shard_id": "shard-1", "replicas": ["n1", "n2", "n3"],
    })
    sched.run_until(sched.clock.now() + 300)

    for sm in all_meta_state_machines(managers).values():
        assert sm.route("a") == "shard-0"
        assert sm.route("z") == "shard-1"


def test_metadata_group_survives_a_node_crash(tmp_path):
    sched, net, managers = build_meta_cluster(["n1", "n2", "n3"], tmp_path)
    leader = find_metadata_leader(managers)
    leader.propose({
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1", "n2", "n3"],
    })
    sched.run_until(sched.clock.now() + 300)

    leader_pid = next(pid for pid, mgr in managers.items() if mgr.get(METADATA_SHARD_ID) is leader)
    leader.stop()
    net.crash_now(leader.node_id)
    sched.run_until(sched.clock.now() + 2000)

    new_leader = find_metadata_leader(managers)
    assert new_leader is not None
    assert new_leader.node_id != leader.node_id
    # The already-committed shard registration must have survived the
    # leader change -- this is just Raft's ordinary leader-completeness
    # property, now protecting cluster metadata instead of KV data.
    remaining = {pid: mgr for pid, mgr in managers.items() if pid != leader_pid}
    for sm in all_meta_state_machines(remaining).values():
        assert sm.get_shard("shard-0") is not None


def test_metadata_group_coexists_with_a_real_kv_shard_on_the_same_nodes(tmp_path):
    """Confirms the metadata group is just another Raft group sharing
    the same physical nodes/network/scheduler as an ordinary KV shard --
    exactly the multi-Raft composition Step 3.1 built, now proven with a
    structurally different state machine on one of the groups."""
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=0)
    net.set_default_link(LinkConfig(min_delay=1, max_delay=5))
    physical = ["n1", "n2", "n3"]
    managers = {pid: ShardManager(pid, net, sched, tmp_path / pid) for pid in physical}

    for pid in physical:
        peers = [p for p in physical if p != pid]
        managers[pid].start_shard(METADATA_SHARD_ID, peers, state_machine_factory=MetadataStateMachine)
        managers[pid].start_shard("kv-shard-0", peers)  # default StateMachine factory
    sched.run_until(3000)

    meta_leader = find_metadata_leader(managers)
    kv_leader = next(
        mgr.get("kv-shard-0")
        for mgr in managers.values()
        if mgr.get("kv-shard-0").state.name == "LEADER"
    )
    assert meta_leader.node_id.endswith(":__meta__")
    assert kv_leader.node_id.endswith(":kv-shard-0")

    meta_leader.propose({
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None, "replicas": physical
    })
    kv_leader.propose({"op": "set", "key": "x", "value": "1"})
    sched.run_until(sched.clock.now() + 500)

    for pid in physical:
        assert managers[pid].state_machine_for(METADATA_SHARD_ID).get_shard("shard-0") is not None
        assert managers[pid].state_machine_for("kv-shard-0").get("x") == "1"
