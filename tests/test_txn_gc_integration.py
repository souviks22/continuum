from continuum.cluster.shard_manager import ShardManager
from continuum.raft.node import NodeState
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.tso.oracle import TimestampOracle
from continuum.tso.statemachine import TSOStateMachine
from continuum.txn.coordinator import Transaction
from continuum.txn.safe_point import SafePointCalculator
from continuum.txn.statemachine import PercolatorStateMachine, percolator_query_fn

TSO_SHARD_ID = "__tso__"


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
        managers[pid].start_shard(TSO_SHARD_ID, peers, state_machine_factory=TSOStateMachine)
        managers[pid].start_shard(
            "shard-0", peers, state_machine_factory=PercolatorStateMachine, query_fn=percolator_query_fn
        )
    sched.run_until(3000)

    def leader_of(shard_id):
        for mgr in managers.values():
            node = mgr.get(shard_id)
            if node.state == NodeState.LEADER:
                return node
        raise AssertionError(f"no leader for shard {shard_id!r}")

    tso = TimestampOracle(leader_of(TSO_SHARD_ID), leader_of(TSO_SHARD_ID).state_machine)
    return sched, net, managers, tso, leader_of


def run(sched, amount=1000):
    sched.run_until(sched.clock.now() + amount)


def get_timestamp(sched, tso):
    holder = []
    tso.request_timestamp(lambda ts, err: holder.append(ts))
    run(sched)
    return holder[0]


def propose_gc_and_wait(sched, node, safe_point):
    outcome = []
    index = node.propose({"op": "gc", "safe_point": safe_point})
    node.wait_for_commit(index, lambda: outcome.append(node.state_machine.get_result(index)))
    run(sched)
    return outcome[0]


def test_gc_replicates_and_applies_identically_on_every_replica(tmp_path):
    sched, net, managers, tso, leader_of = build_cluster(["n1", "n2", "n3"], tmp_path)
    shard_for_key = lambda key: leader_of("shard-0")

    for i in range(3):
        txn = Transaction(tso, shard_for_key)
        txn.set("x", f"v{i}")
        r = []
        txn.commit(lambda s, e: r.append((s, e)))
        run(sched)
        assert r == [(True, None)]

    safe_point = get_timestamp(sched, tso)
    result = propose_gc_and_wait(sched, leader_of("shard-0"), safe_point)
    assert result["success"] is True

    versions_per_replica = {
        pid: mgr.state_machine_for("shard-0")._store.writes.versions("x")
        for pid, mgr in managers.items()
    }
    values = list(versions_per_replica.values())
    assert all(v == values[0] for v in values)  # every replica GC'd to the identical result
    assert len(values[0]) == 1  # only the newest committed version survives


def test_safe_point_calculator_protects_a_long_running_transaction_from_gc(tmp_path):
    sched, net, managers, tso, leader_of = build_cluster(["n1", "n2", "n3"], tmp_path)
    shard_for_key = lambda key: leader_of("shard-0")
    tracker = SafePointCalculator(gc_ttl=10**9)  # huge ttl: only active-txn tracking binds here

    # Commit an initial value, then begin a long-running read-only
    # transaction that snapshots it.
    setup = Transaction(tso, shard_for_key)
    setup.set("x", "v1")
    r = []
    setup.commit(lambda s, e: r.append((s, e)))
    run(sched)
    assert r == [(True, None)]

    reader = Transaction(tso, shard_for_key, safe_point_tracker=tracker)
    read_results = []
    reader.get("x", lambda v, e: read_results.append((v, e)))
    run(sched)
    assert read_results == [("v1", None)]
    reader_start_ts = reader.start_ts

    # A second transaction commits a newer value for the same key --
    # this is what a naive GC (ignoring the still-open reader) could
    # wrongly discard the reader's version for.
    updater = Transaction(tso, shard_for_key)
    updater.set("x", "v2")
    r2 = []
    updater.commit(lambda s, e: r2.append((s, e)))
    run(sched)
    assert r2 == [(True, None)]

    # Compute the safe point *while the reader is still open* and GC to it.
    current_ts = get_timestamp(sched, tso)
    safe_point = tracker.compute_safe_point(current_ts)
    assert safe_point < reader_start_ts  # correctly bounded below the open reader's snapshot
    propose_gc_and_wait(sched, leader_of("shard-0"), safe_point)

    # The reader's original snapshot value must still be readable.
    value, error = leader_of("shard-0").state_machine.get("x", reader_start_ts)
    assert value == "v1"
    assert error is None

    # Once the reader closes, a fresh safe-point computation is free to
    # advance past what it was protecting.
    reader.close()
    current_ts2 = get_timestamp(sched, tso)
    advanced_safe_point = tracker.compute_safe_point(current_ts2)
    assert advanced_safe_point > safe_point
