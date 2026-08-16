"""Metadata state machine: the replicated record of which shard owns
which key range, and which physical nodes host each shard's replicas.

This is the "who manages the manager" piece -- placement metadata can't
just live as process-local state (Step 3.1's RangePartitioner) once
there's more than one physical node that needs a consistent view of it,
so it's driven through its own Raft group instead, exactly like a KV
shard is. The critical design point: RaftNode doesn't need a single new
line of Raft logic to support this, because Step 3.2 generalized it to
depend only on the ReplicatedStateMachine protocol (apply_command /
snapshot_state / load_state). This class satisfies that protocol the
same way storage.statemachine.StateMachine does, just with a completely
different command vocabulary -- create_shard and split_shard instead of
set and delete.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ShardMeta:
    shard_id: str
    start: Optional[str]  # None = unbounded below
    end: Optional[str]  # None = unbounded above
    replicas: list[str]  # physical node ids hosting this shard's Raft group


def metadata_query_fn(state_machine: "MetadataStateMachine", query: dict) -> dict:
    """Query adapter for RaftNode's client-facing ClientQuery RPC (Step
    3.3), for the metadata group specifically. Two query ops: "route"
    (find which shard owns a key, plus that shard's replica set --
    everything a client needs in one round trip to then go talk to the
    right shard) and "get_shard" (look up a known shard_id's replicas
    directly, e.g. after a client's cached routing turns out stale)."""
    op = query["op"]
    if op == "route":
        shard_id = state_machine.route(query["key"])
        meta = state_machine.get_shard(shard_id)
        return {"shard_id": shard_id, "replicas": meta.replicas}
    if op == "get_shard":
        meta = state_machine.get_shard(query["shard_id"])
        return {"replicas": meta.replicas if meta is not None else None}
    raise ValueError(f"unsupported metadata query op {op!r}")


class MetadataStateMachine:
    def __init__(self) -> None:
        self._shards: dict[str, ShardMeta] = {}

    # -- ReplicatedStateMachine protocol -----------------------------------

    def apply_command(self, index: int, command: dict[str, Any]) -> None:
        op = command["op"]
        if op == "create_shard":
            self._shards[command["shard_id"]] = ShardMeta(
                shard_id=command["shard_id"],
                start=command.get("start"),
                end=command.get("end"),
                replicas=list(command["replicas"]),
            )
        elif op == "split_shard":
            self._apply_split(command)
        else:
            raise ValueError(f"unknown metadata operation {op!r}")

    def _apply_split(self, command: dict[str, Any]) -> None:
        shard_id = command["shard_id"]
        parent = self._shards.get(shard_id)
        if parent is None:
            raise KeyError(f"split_shard on unknown shard {shard_id!r}")
        split_key = command["split_key"]
        new_shard_id = command["new_shard_id"]
        new_replicas = list(command.get("replicas", parent.replicas))

        # Idempotency: a retried/duplicate split_shard command (e.g. a
        # client retry after an ambiguous timeout) must not re-split an
        # already-split parent -- if the new shard already exists with
        # matching boundaries, treat this as a no-op rather than
        # corrupting the range table with a second split.
        existing_new = self._shards.get(new_shard_id)
        if existing_new is not None and existing_new.start == split_key:
            return

        self._shards[shard_id] = ShardMeta(
            shard_id=shard_id, start=parent.start, end=split_key, replicas=parent.replicas
        )
        self._shards[new_shard_id] = ShardMeta(
            shard_id=new_shard_id, start=split_key, end=parent.end, replicas=new_replicas
        )

    def snapshot_state(self) -> dict[str, Any]:
        return {
            shard_id: {"start": m.start, "end": m.end, "replicas": list(m.replicas)}
            for shard_id, m in self._shards.items()
        }

    def load_state(self, state: dict[str, Any]) -> None:
        self._shards = {
            shard_id: ShardMeta(
                shard_id=shard_id, start=v["start"], end=v["end"], replicas=list(v["replicas"])
            )
            for shard_id, v in state.items()
        }

    # -- queries -----------------------------------------------------------

    def get_shard(self, shard_id: str) -> Optional[ShardMeta]:
        return self._shards.get(shard_id)

    def all_shards(self) -> list[ShardMeta]:
        return list(self._shards.values())

    def route(self, key: str) -> str:
        for m in self._shards.values():
            if (m.start is None or key >= m.start) and (m.end is None or key < m.end):
                return m.shard_id
        raise KeyError(
            f"no shard covers key {key!r} -- metadata not yet bootstrapped, or a real gap "
            "in range coverage (a bug, since ranges are meant to stay exhaustive)"
        )
