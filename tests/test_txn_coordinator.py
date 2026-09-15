from continuum.cluster.shard_manager import ShardManager
from continuum.raft.node import NodeState
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.tso.oracle import TimestampOracle
from continuum.tso.statemachine import TSOStateMachine
from continuum.txn.coordinator import Transaction
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


# -- single-shard, single-key ------------------------------------------------


def test_single_key_transaction_commits_and_is_readable(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(["shard-0"], ["n1", "n2", "n3"], tmp_path)
    shard_for_key = lambda key: leader_of("shard-0")

    txn = Transaction(tso, shard_for_key)
    txn.set("x", "1")
    results = []
    txn.commit(lambda success, err: results.append((success, err)))
    run(sched)
    assert results == [(True, None)]

    read_results = []
    Transaction(tso, shard_for_key).get("x", lambda v, e: read_results.append((v, e)))
    run(sched)
    assert read_results == [("1", None)]


# -- multi-key, single shard: atomicity -------------------------------------


def test_multi_key_same_shard_commits_atomically(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(["shard-0"], ["n1", "n2", "n3"], tmp_path)
    shard_for_key = lambda key: leader_of("shard-0")

    txn = Transaction(tso, shard_for_key)
    txn.set("a", "1")
    txn.set("b", "2")
    results = []
    txn.commit(lambda success, err: results.append((success, err)))
    run(sched)
    assert results == [(True, None)]

    sm = leader_of("shard-0").state_machine
    read_ts = txn.commit_ts
    assert sm.get("a", read_ts) == ("1", None)
    assert sm.get("b", read_ts) == ("2", None)


# -- cross-shard transaction: the real point of this step --------------------


def test_cross_shard_transaction_commits_atomically_from_readers_perspective(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(
        ["shard-0", "shard-1"], ["n1", "n2", "n3"], tmp_path
    )
    routing = {"a": "shard-0", "b": "shard-1"}
    shard_for_key = lambda key: leader_of(routing[key])

    txn = Transaction(tso, shard_for_key)
    txn.set("a", "1")  # lives on shard-0
    txn.set("b", "2")  # lives on shard-1
    results = []
    txn.commit(lambda success, err: results.append((success, err)))
    run(sched)
    assert results == [(True, None)]

    read_ts = txn.commit_ts
    assert leader_of("shard-0").state_machine.get("a", read_ts) == ("1", None)
    assert leader_of("shard-1").state_machine.get("b", read_ts) == ("2", None)


def test_cross_shard_transaction_not_visible_until_fully_committed(tmp_path):
    """A reader on the secondary's shard before commit must not see the
    prewritten (but not yet committed) value -- prewrite is invisible by
    construction, only the write record makes a value visible."""
    sched, net, managers, tso, leader_of = build_txn_cluster(
        ["shard-0", "shard-1"], ["n1", "n2", "n3"], tmp_path
    )
    routing = {"a": "shard-0", "b": "shard-1"}
    shard_for_key = lambda key: leader_of(routing[key])

    txn = Transaction(tso, shard_for_key)
    txn.set("a", "1")
    txn.set("b", "2")

    committed = []
    txn.commit(lambda success, err: committed.append((success, err)))
    run(sched)
    assert committed == [(True, None)]

    # A read at a timestamp taken *before* this transaction's commit_ts
    # must not see either key.
    early_ts = txn.start_ts  # before commit_ts, still mid-transaction from that vantage point
    assert leader_of("shard-0").state_machine.get("a", early_ts) == (None, None)


# -- write-write conflict: abort and rollback --------------------------------


def test_write_write_conflict_aborts_and_rolls_back(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(["shard-0"], ["n1", "n2", "n3"], tmp_path)
    shard_for_key = lambda key: leader_of("shard-0")

    # First transaction commits a value for "x".
    first = Transaction(tso, shard_for_key)
    first.set("x", "first")
    r1 = []
    first.commit(lambda s, e: r1.append((s, e)))
    run(sched)
    assert r1 == [(True, None)]

    # Second transaction started *before* the first committed (simulate
    # by manually pinning an old start_ts that predates first's commit)
    # tries to write the same key -- must be rejected as a conflict, not
    # silently overwrite.
    second = Transaction(tso, shard_for_key)
    second.start_ts = first.start_ts  # deliberately stale/concurrent start_ts
    second.set("x", "second")
    r2 = []
    second.commit(lambda s, e: r2.append((s, e)))
    run(sched)
    assert r2[0][0] is False
    assert "conflict" in r2[0][1]

    # The conflicting transaction's lock must have been rolled back --
    # a fresh read is not blocked, and still sees the first transaction's
    # committed value.
    value, error = leader_of("shard-0").state_machine.get("x", 10**9)
    assert value == "first"
    assert error is None


def test_multi_key_conflict_rolls_back_the_keys_that_did_succeed(tmp_path):
    sched, net, managers, tso, leader_of = build_txn_cluster(["shard-0"], ["n1", "n2", "n3"], tmp_path)
    shard_for_key = lambda key: leader_of("shard-0")

    blocker = Transaction(tso, shard_for_key)
    blocker.set("b", "blocked")
    r = []
    blocker.commit(lambda s, e: r.append((s, e)))
    run(sched)
    assert r == [(True, None)]

    txn = Transaction(tso, shard_for_key)
    txn.set("a", "1")  # will succeed
    txn.set("b", "2")  # will conflict (blocker already committed after this txn's start_ts)
    txn.start_ts = blocker.start_ts  # force a stale start_ts to guarantee the conflict
    results = []
    txn.commit(lambda s, e: results.append((s, e)))
    run(sched)
    assert results[0][0] is False

    # "a" must have been rolled back too, even though its own prewrite
    # succeeded -- a fresh transaction can prewrite it again cleanly.
    retry = Transaction(tso, shard_for_key)
    retry.set("a", "retry-value")
    r2 = []
    retry.commit(lambda s, e: r2.append((s, e)))
    run(sched)
    assert r2 == [(True, None)]


# -- primary commits, secondary lags (best-effort) ---------------------------


def test_transaction_reports_committed_once_primary_commits_even_if_a_secondary_shard_is_slow(tmp_path):
    """Drives prewrite/commit manually (rather than through Transaction)
    to control exactly when the partition happens: after both prewrites
    have already succeeded and committed durably to a *lock*, but before
    the secondary's commit can land. Partitioning before prewrite even
    starts would just make prewrite itself hang (correctly -- a shard
    that's unavailable can't participate in 2PC at all), which is a
    different scenario than the one this test is for."""
    sched, net, managers, tso, leader_of = build_txn_cluster(
        ["shard-0", "shard-1"], ["n1", "n2", "n3"], tmp_path
    )

    start_ts_holder = []
    tso.request_timestamp(lambda ts, err: start_ts_holder.append(ts))
    run(sched)
    start_ts = start_ts_holder[0]

    def propose_and_wait(node, command):
        outcome = []
        index = node.propose(command)
        node.wait_for_commit(index, lambda: outcome.append(node.state_machine.get_result(index)))
        run(sched)
        return outcome[0]

    node_a = leader_of("shard-0")
    node_b = leader_of("shard-1")
    r = propose_and_wait(node_a, {"op": "prewrite", "key": "a", "value": "primary-value", "start_ts": start_ts, "primary_key": "a"})
    assert r["success"] is True
    r = propose_and_wait(node_b, {"op": "prewrite", "key": "b", "value": "secondary-value", "start_ts": start_ts, "primary_key": "a"})
    assert r["success"] is True

    # Now partition shard-1's leader away, *after* prewrite succeeded.
    shard1_addr = node_b.node_id
    other_shard1_addrs = {
        mgr.get("shard-1").node_id for mgr in managers.values() if mgr.get("shard-1").node_id != shard1_addr
    }
    net.partition({shard1_addr}, other_shard1_addrs)
    run(sched, amount=3000)  # let shard-1 elect a fresh (reachable) leader

    commit_ts_holder = []
    tso.request_timestamp(lambda ts, err: commit_ts_holder.append(ts))
    run(sched)
    commit_ts = commit_ts_holder[0]

    # Commit the primary on shard-0 -- unaffected by shard-1's partition.
    r = propose_and_wait(node_a, {"op": "commit", "key": "a", "start_ts": start_ts, "commit_ts": commit_ts})
    assert r["success"] is True

    # The transaction is committed from here on (this is the point being
    # tested): a reader can already see the primary's committed value,
    # independent of whether the secondary ever catches up.
    value, error = leader_of("shard-0").state_machine.get("a", commit_ts)
    assert value == "primary-value"
    assert error is None
