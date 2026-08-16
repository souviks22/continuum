"""Multi-Raft group lifecycle: spins up and tears down independent Raft
groups (shards) on a single physical node, sharing that node's network
transport and event scheduler.

Each shard's local replica gets its own logical network identity,
`{physical_node_id}:{shard_id}`, registered as an independent endpoint
on the shared SimulatedNetwork. This is what makes true multi-Raft work
with zero changes to the RPC/Raft layers built in Phase 2: shard A's
RequestVote/AppendEntries never collide with shard B's, because they're
addressed to entirely different network node ids -- conceptually the
same thing a real deployment does by scoping every Raft RPC with a
group/shard id, just implemented here as address composition instead of
an extra field threaded through every message.
"""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any, Callable, Optional

from continuum.raft.node import RaftNode
from continuum.raft.persistent_state import RaftPersistentState
from continuum.rpc.endpoint import RpcEndpoint
from continuum.rpc.transport import SimulatedTransport
from continuum.sim.network import SimulatedNetwork
from continuum.sim.scheduler import EventScheduler
from continuum.storage.snapshot import SnapshotStore
from continuum.storage.statemachine import StateMachine, kv_query_fn
from continuum.storage.wal import WriteAheadLog

def shard_address(physical_node_id: str, shard_id: str) -> str:
    return f"{physical_node_id}:{shard_id}"


class ShardManager:
    def __init__(
        self,
        physical_node_id: str,
        network: SimulatedNetwork,
        scheduler: EventScheduler,
        data_dir: str | Path,
        rng_seed: int = 0,
    ) -> None:
        self.physical_node_id = physical_node_id
        self._network = network
        self._scheduler = scheduler
        self._data_dir = Path(data_dir)
        self._rng_seed = rng_seed
        self._shards: dict[str, RaftNode] = {}
        self._state_machines: dict[str, Any] = {}

    def shards(self) -> dict[str, RaftNode]:
        return dict(self._shards)

    def get(self, shard_id: str) -> Optional[RaftNode]:
        return self._shards.get(shard_id)

    def state_machine_for(self, shard_id: str) -> Optional[Any]:
        return self._state_machines.get(shard_id)

    def start_shard(
        self,
        shard_id: str,
        peer_physical_node_ids: list[str],
        apply_fn: Optional[Callable[[dict[str, Any]], None]] = None,
        state_machine_factory: Callable[[], Any] = StateMachine,
        query_fn: Optional[Callable[[Any, dict[str, Any]], dict[str, Any]]] = kv_query_fn,
        **raft_node_kwargs: Any,
    ) -> RaftNode:
        """Spin up this node's replica of `shard_id`'s Raft group. Peers
        are given as *physical* node ids -- addressed here to their
        `{peer}:{shard_id}` identity, since every replica of the same
        shard shares that shard_id in its address. Starts the node's
        election timer immediately (matches how every other RaftNode in
        this codebase is brought up).

        `state_machine_factory` defaults to the KV `StateMachine`, but
        Step 3.2's metadata Raft group is started through this same
        method with `state_machine_factory=MetadataStateMachine` --
        RaftNode only depends on the generic ReplicatedStateMachine
        protocol (see raft/node.py), so a metadata group gets every bit
        of Phase 2's election/replication/snapshot machinery for free,
        with zero new Raft code."""
        if shard_id in self._shards:
            raise ValueError(
                f"shard {shard_id!r} is already running on {self.physical_node_id!r}"
            )
        my_address = shard_address(self.physical_node_id, shard_id)
        peer_addresses = [shard_address(p, shard_id) for p in peer_physical_node_ids]

        shard_dir = self._data_dir / shard_id
        transport = SimulatedTransport(my_address, self._network)
        rpc = RpcEndpoint(my_address, transport, self._scheduler)
        persistent = RaftPersistentState(shard_dir / "raft_state")
        log_wal = WriteAheadLog(shard_dir / "log.wal")
        state_machine = state_machine_factory()
        snapshot_store = SnapshotStore(shard_dir / "snapshots")
        rng = random.Random(f"{self._rng_seed}:{shard_id}:{self.physical_node_id}")

        node = RaftNode(
            my_address,
            peer_addresses,
            rpc,
            self._scheduler,
            persistent,
            log_wal,
            rng=rng,
            apply_fn=apply_fn,
            state_machine=state_machine,
            snapshot_store=snapshot_store,
            query_fn=query_fn,
            **raft_node_kwargs,
        )
        self._shards[shard_id] = node
        self._state_machines[shard_id] = state_machine
        node.start()
        return node

    def stop_shard(self, shard_id: str) -> None:
        """Tear down this node's replica of `shard_id`. The RaftNode
        stops acting (no more elections or replication) and is dropped
        from bookkeeping, but its network address stays registered --
        SimulatedNetwork has no unregister operation, so a "removed"
        shard is simulated as permanently inert rather than physically
        absent. Sufficient to demonstrate lifecycle; a real deployment
        would free the listening port instead."""
        node = self._shards.pop(shard_id, None)
        if node is not None:
            node.stop()
        self._state_machines.pop(shard_id, None)
