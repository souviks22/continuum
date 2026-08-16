from continuum.client.client import ContinuumClient
from continuum.cluster.bootstrap import METADATA_SHARD_ID, find_metadata_leader
from continuum.cluster.metadata import MetadataStateMachine, metadata_query_fn
from continuum.cluster.shard_manager import ShardManager
from continuum.raft.node import NodeState
from continuum.rpc.endpoint import RpcEndpoint
from continuum.rpc.transport import SimulatedTransport
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler


def build_cluster(physical_node_ids, tmp_path, seed=0):
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
            METADATA_SHARD_ID, peers, state_machine_factory=MetadataStateMachine,
            query_fn=metadata_query_fn,
        )
        managers[pid].start_shard("shard-0", peers)
    sched.run_until(2000)

    meta_leader = find_metadata_leader(managers)
    meta_leader.propose({
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": physical_node_ids,
    })
    sched.run_until(sched.clock.now() + 300)

    client_transport = SimulatedTransport("client", net)
    client_rpc = RpcEndpoint("client", client_transport, sched)
    client = ContinuumClient(client_rpc, [f"{pid}:{METADATA_SHARD_ID}" for pid in physical_node_ids])
    return sched, net, managers, client


def run_and_collect(sched, fn, run_for=1000):
    results = []
    fn(lambda value, error: results.append((value, error)))
    sched.run_until(sched.clock.now() + run_for)
    return results


# -- basic roundtrip ------------------------------------------------------


def test_set_then_get_roundtrip(tmp_path):
    sched, net, managers, client = build_cluster(["n1", "n2", "n3"], tmp_path)

    set_results = run_and_collect(sched, lambda cb: client.set("x", "1", cb))
    assert set_results[0][1] is None  # no error
    assert isinstance(set_results[0][0], int)  # returned log index

    get_results = run_and_collect(sched, lambda cb: client.get("x", cb))
    assert get_results == [("1", None)]


def test_get_missing_key_returns_none_no_error(tmp_path):
    sched, net, managers, client = build_cluster(["n1", "n2", "n3"], tmp_path)
    results = run_and_collect(sched, lambda cb: client.get("missing", cb))
    assert results == [(None, None)]


def test_delete_removes_key(tmp_path):
    sched, net, managers, client = build_cluster(["n1", "n2", "n3"], tmp_path)
    run_and_collect(sched, lambda cb: client.set("x", "1", cb))
    run_and_collect(sched, lambda cb: client.delete("x", cb))
    results = run_and_collect(sched, lambda cb: client.get("x", cb))
    assert results == [(None, None)]


# -- leader caching and redirect-following -----------------------------------


def test_leader_is_cached_after_first_successful_call(tmp_path):
    sched, net, managers, client = build_cluster(["n1", "n2", "n3"], tmp_path)
    run_and_collect(sched, lambda cb: client.set("x", "1", cb))
    assert "shard-0" in client._leader_cache
    assert METADATA_SHARD_ID in client._leader_cache


def test_client_recovers_after_shard_leader_changes(tmp_path):
    sched, net, managers, client = build_cluster(["n1", "n2", "n3"], tmp_path)
    run_and_collect(sched, lambda cb: client.set("x", "1", cb))

    old_leader_addr = client._leader_cache["shard-0"]
    old_leader_pid = old_leader_addr.split(":")[0]
    old_leader = managers[old_leader_pid].get("shard-0")
    old_leader.stop()
    net.crash_now(old_leader_addr)
    sched.run_until(sched.clock.now() + 2000)  # let a new leader get elected

    results = run_and_collect(sched, lambda cb: client.set("y", "2", cb), run_for=2000)
    assert results[0][1] is None  # succeeded despite the stale cached leader
    assert client._leader_cache["shard-0"] != old_leader_addr

    get_results = run_and_collect(sched, lambda cb: client.get("y", cb))
    assert get_results == [("2", None)]


# -- error handling -----------------------------------------------------


def test_all_metadata_replicas_unreachable_returns_error(tmp_path):
    sched, net, managers, client = build_cluster(["n1", "n2", "n3"], tmp_path)
    for pid in ["n1", "n2", "n3"]:
        net.crash_now(f"{pid}:{METADATA_SHARD_ID}")

    results = run_and_collect(sched, lambda cb: client.get("x", cb), run_for=5000)
    assert results[0][0] is None
    assert results[0][1] is not None  # a non-None error string
