from continuum.cluster.shard_manager import ShardManager
from continuum.raft.node import NodeState
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.tso.oracle import TimestampOracle
from continuum.tso.statemachine import TSOStateMachine

TSO_SHARD_ID = "__tso__"


def build_tso_cluster(physical_node_ids, tmp_path, seed=0, batch_size=1000):
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    net.set_default_link(LinkConfig(min_delay=1, max_delay=5))
    managers = {
        pid: ShardManager(pid, net, sched, tmp_path / pid, rng_seed=seed)
        for pid in physical_node_ids
    }
    for pid in physical_node_ids:
        peers = [p for p in physical_node_ids if p != pid]
        managers[pid].start_shard(TSO_SHARD_ID, peers, state_machine_factory=TSOStateMachine)
    sched.run_until(2000)

    oracles = {
        pid: TimestampOracle(mgr.get(TSO_SHARD_ID), mgr.state_machine_for(TSO_SHARD_ID), batch_size=batch_size)
        for pid, mgr in managers.items()
    }
    return sched, net, managers, oracles


def leader_pid(managers):
    for pid, mgr in managers.items():
        if mgr.get(TSO_SHARD_ID).state == NodeState.LEADER:
            return pid
    raise AssertionError("no leader found")


def request_and_collect(sched, oracle, run_for=500):
    results = []
    oracle.request_timestamp(lambda ts, err: results.append((ts, err)))
    sched.run_until(sched.clock.now() + run_for)
    return results[0] if results else (None, "no reply within run window")


# -- basic monotonicity -----------------------------------------------------


def test_timestamps_are_strictly_increasing(tmp_path):
    sched, net, managers, oracles = build_tso_cluster(["n1", "n2", "n3"], tmp_path)
    oracle = oracles[leader_pid(managers)]

    seen = []
    for _ in range(10):
        ts, err = request_and_collect(sched, oracle)
        assert err is None
        seen.append(ts)
    assert seen == sorted(set(seen))  # strictly increasing, no duplicates
    assert len(seen) == len(set(seen))


def test_non_leader_oracle_returns_not_leader_error(tmp_path):
    sched, net, managers, oracles = build_tso_cluster(["n1", "n2", "n3"], tmp_path)
    lpid = leader_pid(managers)
    follower_pid = next(pid for pid in managers if pid != lpid)

    ts, err = request_and_collect(sched, oracles[follower_pid])
    assert ts is None
    assert err == "not leader"


# -- batch exhaustion --------------------------------------------------------


def test_batch_exhaustion_triggers_new_allocation_seamlessly(tmp_path):
    sched, net, managers, oracles = build_tso_cluster(["n1", "n2", "n3"], tmp_path, batch_size=3)
    oracle = oracles[leader_pid(managers)]

    seen = []
    for _ in range(10):  # more than one batch's worth (batch_size=3)
        ts, err = request_and_collect(sched, oracle)
        assert err is None
        seen.append(ts)
    assert seen == list(range(seen[0], seen[0] + 10))  # no gaps within one stable leader's lifetime


def test_concurrent_requests_during_in_flight_allocation_all_succeed(tmp_path):
    sched, net, managers, oracles = build_tso_cluster(["n1", "n2", "n3"], tmp_path, batch_size=2)
    oracle = oracles[leader_pid(managers)]
    # Exhaust the first tiny batch so the next round triggers allocation.
    request_and_collect(sched, oracle)
    request_and_collect(sched, oracle)

    results = []
    for _ in range(5):
        oracle.request_timestamp(lambda ts, err: results.append((ts, err)))
    sched.run_until(sched.clock.now() + 1000)

    assert len(results) == 5
    assert all(err is None for _, err in results)
    values = [ts for ts, _ in results]
    assert len(set(values)) == 5  # all distinct despite being queued behind one allocation


# -- the core safety property: monotonicity across a leader crash -----------


def test_new_leader_never_reissues_a_timestamp_the_old_leader_handed_out(tmp_path):
    sched, net, managers, oracles = build_tso_cluster(["n1", "n2", "n3"], tmp_path, batch_size=1000)
    old_leader_pid = leader_pid(managers)
    old_oracle = oracles[old_leader_pid]

    handed_out = []
    for _ in range(5):
        ts, err = request_and_collect(sched, old_oracle)
        assert err is None
        handed_out.append(ts)
    highest_handed_out = max(handed_out)

    # Crash the old leader with most of its 1000-timestamp batch still
    # unused -- those go permanently unburned/unused, which is exactly
    # the point: the next leader must never reuse any of them.
    old_node = managers[old_leader_pid].get(TSO_SHARD_ID)
    old_node.stop()
    net.crash_now(old_node.node_id)
    sched.run_until(sched.clock.now() + 3000)

    new_leader_pid = leader_pid({pid: mgr for pid, mgr in managers.items() if pid != old_leader_pid})
    new_oracle = oracles[new_leader_pid]
    ts, err = request_and_collect(sched, new_oracle, run_for=2000)

    assert err is None
    assert ts > highest_handed_out
    # And in fact well past it -- the new leader's batch starts above
    # the *entire* unused portion of the old leader's batch, not just
    # above what was actually handed out.
    assert ts > highest_handed_out + 900
