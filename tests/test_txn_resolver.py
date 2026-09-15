from continuum.cluster.shard_manager import ShardManager
from continuum.raft.node import NodeState
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.tso.oracle import TimestampOracle
from continuum.tso.statemachine import TSOStateMachine
from continuum.txn.coordinator import Transaction
from continuum.txn.resolver import LockResolver
from continuum.txn.statemachine import PercolatorStateMachine, percolator_query_fn

TSO_SHARD_ID = "__tso__"


def build_txn_cluster(shard_ids, physical_node_ids, tmp_path, seed=0):
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
        for shard_id in shard_ids:
            managers[pid].start_shard(
                shard_id, peers,
                state_machine_factory=PercolatorStateMachine, query_fn=percolator_query_fn,
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


def propose_and_wait(sched, node, command):
    outcome = []
    index = node.propose(command)
    node.wait_for_commit(index, lambda: outcome.append(node.state_machine.get_result(index)))
    run(sched)
    return outcome[0]


# -- roll-forward: primary committed, secondary abandoned mid-flight --------


def test_resolver_rolls_forward_when_primary_committed(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(
        ["shard-0", "shard-1"], ["n1", "n2", "n3"], tmp_path
    )
    start_ts = get_timestamp(sched, tso)
    node_a, node_b = leader_of("shard-0"), leader_of("shard-1")

    # Prewrite both (primary "a" on shard-0, secondary "b" on shard-1),
    # commit only the primary -- simulating a coordinator that crashed
    # after the atomicity point but before reaching the secondary.
    assert propose_and_wait(sched, node_a, {
        "op": "prewrite", "key": "a", "value": "1", "start_ts": start_ts, "primary_key": "a"
    })["success"]
    assert propose_and_wait(sched, node_b, {
        "op": "prewrite", "key": "b", "value": "2", "start_ts": start_ts, "primary_key": "a"
    })["success"]
    commit_ts = get_timestamp(sched, tso)
    assert propose_and_wait(sched, node_a, {
        "op": "commit", "key": "a", "start_ts": start_ts, "commit_ts": commit_ts
    })["success"]
    # "b" is still locked -- the coordinator never got to it.

    # Advance timestamps past the resolver's TTL window.
    for _ in range(5):
        get_timestamp(sched, tso)

    routing = {"a": "shard-0", "b": "shard-1"}
    resolver = LockResolver(lambda key: leader_of(routing[key]), min_ts_age=3)
    reader = Transaction(tso, lambda key: leader_of(routing[key]), resolver=resolver)
    results = []
    reader.get("b", lambda v, e: results.append((v, e)))
    run(sched, amount=2000)

    assert results == [("2", None)]
    # And the lock is actually gone now, not just bypassed.
    assert leader_of("shard-1").state_machine.get_lock("b") is None


# -- roll-back: primary never committed --------------------------------------


def test_resolver_rolls_back_when_primary_never_committed(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(
        ["shard-0", "shard-1"], ["n1", "n2", "n3"], tmp_path
    )
    start_ts = get_timestamp(sched, tso)
    node_b = leader_of("shard-1")

    # Only the secondary got prewritten -- the primary was never even
    # attempted (coordinator crashed immediately after starting).
    assert propose_and_wait(sched, node_b, {
        "op": "prewrite", "key": "b", "value": "2", "start_ts": start_ts, "primary_key": "a"
    })["success"]

    for _ in range(5):
        get_timestamp(sched, tso)

    routing = {"a": "shard-0", "b": "shard-1"}
    resolver = LockResolver(lambda key: leader_of(routing[key]), min_ts_age=3)
    reader = Transaction(tso, lambda key: leader_of(routing[key]), resolver=resolver)
    results = []
    reader.get("b", lambda v, e: results.append((v, e)))
    run(sched, amount=2000)

    assert results == [(None, None)]  # resolved to legitimately absent, not an error
    assert leader_of("shard-1").state_machine.get_lock("b") is None


# -- TTL gating: a fresh lock is not resolved -------------------------------


def test_resolver_refuses_to_resolve_a_fresh_lock(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(["shard-0"], ["n1", "n2", "n3"], tmp_path)
    start_ts = get_timestamp(sched, tso)
    node = leader_of("shard-0")
    assert propose_and_wait(sched, node, {
        "op": "prewrite", "key": "x", "value": "1", "start_ts": start_ts, "primary_key": "x"
    })["success"]

    resolver = LockResolver(lambda key: leader_of("shard-0"), min_ts_age=1000)
    reader = Transaction(tso, lambda key: leader_of("shard-0"), resolver=resolver)
    results = []
    reader.get("x", lambda v, e: results.append((v, e)))
    run(sched, amount=2000)

    assert results[0][0] is None
    assert "not stale enough" in results[0][1]
    # Refusing to resolve must not have touched the lock.
    assert leader_of("shard-0").state_machine.get_lock("x") is not None


# -- without a resolver, behavior is unchanged from Step 6.1 -----------------


def test_without_a_resolver_blocked_read_just_fails(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(["shard-0"], ["n1", "n2", "n3"], tmp_path)
    start_ts = get_timestamp(sched, tso)
    node = leader_of("shard-0")
    propose_and_wait(sched, node, {
        "op": "prewrite", "key": "x", "value": "1", "start_ts": start_ts, "primary_key": "x"
    })

    reader = Transaction(tso, lambda key: leader_of("shard-0"))  # no resolver
    results = []
    reader.get("x", lambda v, e: results.append((v, e)))
    run(sched, amount=2000)

    assert results[0][0] is None
    assert results[0][1] is not None
    assert leader_of("shard-0").state_machine.get_lock("x") is not None  # untouched
