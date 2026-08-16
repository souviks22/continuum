import random

from continuum.raft.node import NodeState, RaftNode
from continuum.raft.persistent_state import RaftPersistentState
from continuum.rpc.endpoint import RpcEndpoint
from continuum.rpc.transport import SimulatedTransport
from continuum.sim.network import LinkConfig, SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.storage.snapshot import SnapshotStore
from continuum.storage.statemachine import StateMachine
from continuum.storage.wal import WriteAheadLog


def build_kv_cluster(node_ids, tmp_path, seed=0, link_delay=(1, 5), **node_kwargs):
    sched = EventScheduler()
    net = SimulatedNetwork(sched, seed=seed)
    net.set_default_link(LinkConfig(min_delay=link_delay[0], max_delay=link_delay[1]))
    nodes: dict[str, RaftNode] = {}
    state_machines: dict[str, StateMachine] = {}
    for i, node_id in enumerate(node_ids):
        transport = SimulatedTransport(node_id, net)
        rpc = RpcEndpoint(node_id, transport, sched)
        persistent = RaftPersistentState(tmp_path / node_id)
        log_wal = WriteAheadLog(tmp_path / node_id / "log.wal")
        sm = StateMachine()
        snap_store = SnapshotStore(tmp_path / node_id / "snapshots")
        state_machines[node_id] = sm
        peers = [n for n in node_ids if n != node_id]
        node = RaftNode(
            node_id,
            peers,
            rpc,
            sched,
            persistent,
            log_wal,
            rng=random.Random(seed * 1000 + i),
            state_machine=sm,
            snapshot_store=snap_store,
            **node_kwargs,
        )
        nodes[node_id] = node
    return sched, net, nodes, state_machines


def leader_of(nodes):
    ls = [n for n in nodes.values() if n.state == NodeState.LEADER]
    assert len(ls) == 1
    return ls[0]


def elect(sched, nodes, run_until=2000):
    for n in nodes.values():
        n.start()
    sched.run_until(run_until)
    return leader_of(nodes)


# -- state machine wiring ---------------------------------------------------


def test_committed_set_is_visible_in_state_machine_on_all_nodes(tmp_path):
    sched, net, nodes, sms = build_kv_cluster(["a", "b", "c"], tmp_path)
    leader = elect(sched, nodes)

    leader.propose({"op": "set", "key": "x", "value": "1"})
    sched.run_until(sched.clock.now() + 500)

    for node_id, sm in sms.items():
        assert sm.get("x") == "1"


def test_committed_delete_removes_key_on_all_nodes(tmp_path):
    sched, net, nodes, sms = build_kv_cluster(["a", "b", "c"], tmp_path)
    leader = elect(sched, nodes)

    leader.propose({"op": "set", "key": "x", "value": "1"})
    sched.run_until(sched.clock.now() + 300)
    leader.propose({"op": "delete", "key": "x"})
    sched.run_until(sched.clock.now() + 300)

    for sm in sms.values():
        assert sm.get("x") is None


# -- automatic snapshotting --------------------------------------------------


def test_snapshot_threshold_triggers_compaction(tmp_path):
    sched, net, nodes, sms = build_kv_cluster(
        ["a", "b", "c"], tmp_path, snapshot_threshold=3
    )
    leader = elect(sched, nodes)

    for i in range(10):
        leader.propose({"op": "set", "key": f"k{i}", "value": str(i)})
    sched.run_until(sched.clock.now() + 2000)

    assert leader.log.last_included_index >= 0  # compaction actually ran
    for sm in sms.values():
        for i in range(10):
            assert sm.get(f"k{i}") == str(i)


def test_snapshot_file_written_to_disk_after_threshold(tmp_path):
    sched, net, nodes, sms = build_kv_cluster(
        ["a", "b", "c"], tmp_path, snapshot_threshold=2
    )
    leader = elect(sched, nodes)
    for i in range(5):
        leader.propose({"op": "set", "key": f"k{i}", "value": str(i)})
    sched.run_until(sched.clock.now() + 2000)

    snap = SnapshotStore(tmp_path / leader.node_id / "snapshots").read()
    assert snap is not None
    state, last_index = snap
    assert last_index == leader.log.last_included_index


# -- InstallSnapshot catching up a far-behind follower -----------------------


def test_far_behind_follower_catches_up_via_install_snapshot(tmp_path):
    sched, net, nodes, sms = build_kv_cluster(
        ["a", "b", "c"], tmp_path, snapshot_threshold=2
    )
    leader = elect(sched, nodes)
    lagging = next(n for n in nodes.values() if n is not leader)

    # Cut the follower off entirely, then drive enough commits+compactions
    # on the majority side that the leader's log no longer contains the
    # entries this follower would need for plain AppendEntries -- the
    # only way it can possibly catch up is InstallSnapshot.
    net.partition({lagging.node_id}, {n for n in nodes if n != lagging.node_id})
    for i in range(8):
        leader.propose({"op": "set", "key": f"k{i}", "value": str(i)})
        sched.run_until(sched.clock.now() + 300)  # let each round commit+compact before the next

    assert leader.log.last_included_index >= 0
    # The follower's nextIndex, whenever it reconnects, will be far
    # earlier than what the leader still has in its log.
    assert leader._next_index[lagging.node_id] <= leader.log.last_included_index

    net.heal({lagging.node_id}, {n for n in nodes if n != lagging.node_id})
    sched.run_until(sched.clock.now() + 2000)

    assert sms[lagging.node_id].get("k7") == "7"
    for i in range(8):
        assert sms[lagging.node_id].get(f"k{i}") == str(i)
    assert lagging.log.last_included_index == leader.log.last_included_index


def test_install_snapshot_sets_follower_commit_and_applied_indices(tmp_path):
    sched, net, nodes, sms = build_kv_cluster(
        ["a", "b", "c"], tmp_path, snapshot_threshold=2
    )
    leader = elect(sched, nodes)
    lagging = next(n for n in nodes.values() if n is not leader)
    net.partition({lagging.node_id}, {n for n in nodes if n != lagging.node_id})
    for i in range(6):
        leader.propose({"op": "set", "key": f"k{i}", "value": str(i)})
        sched.run_until(sched.clock.now() + 300)

    net.heal({lagging.node_id}, {n for n in nodes if n != lagging.node_id})
    sched.run_until(sched.clock.now() + 2000)

    assert lagging.commit_index >= lagging.log.last_included_index
    assert lagging.last_applied >= lagging.log.last_included_index


# -- persistence across restart ----------------------------------------------


def test_state_recovers_from_snapshot_without_replaying_compacted_entries(tmp_path):
    sched, net, nodes, sms = build_kv_cluster(
        ["a", "b", "c"], tmp_path, snapshot_threshold=2
    )
    leader = elect(sched, nodes)
    for i in range(5):
        leader.propose({"op": "set", "key": f"k{i}", "value": str(i)})
    sched.run_until(sched.clock.now() + 2000)

    leader_id = leader.node_id
    boundary = leader.log.last_included_index
    assert boundary >= 0

    # Reopen just the persisted state for that node, independent of the
    # running cluster -- a fresh StateMachine/SnapshotStore/RaftLog over
    # the same on-disk files, simulating a process restart.
    sm2 = StateMachine()
    snap_store2 = SnapshotStore(tmp_path / leader_id / "snapshots")
    state, last_index = snap_store2.read()
    sm2.load_state(state)
    assert last_index == boundary
    for i in range(5):
        assert sm2.get(f"k{i}") == str(i)

    log_wal2 = WriteAheadLog(tmp_path / leader_id / "log.wal")
    from continuum.raft.log import RaftLog

    log2 = RaftLog(log_wal2)
    assert log2.last_included_index == boundary
    log_wal2.close()
