"""Bootstrap helpers for the metadata Raft group.

Bootstrapping has a genuine chicken-and-egg wrinkle worth naming rather
than hiding: nobody can propose the initial create_shard command until
the metadata group has elected a leader, and there's no way to know that
without observing the cluster. This module keeps that step explicit and
manual (find the leader, then propose) rather than pretending there's a
magic "just works" bootstrap -- real systems handle this with an
out-of-band bootstrap tool or a well-known first-node convention for
exactly this reason.
"""

from __future__ import annotations

from typing import Optional

from continuum.cluster.shard_manager import ShardManager
from continuum.raft.node import NodeState, RaftNode

METADATA_SHARD_ID = "__meta__"


def find_metadata_leader(
    managers: dict[str, ShardManager], meta_shard_id: str = METADATA_SHARD_ID
) -> Optional[RaftNode]:
    for manager in managers.values():
        node = manager.get(meta_shard_id)
        if node is not None and node.state == NodeState.LEADER:
            return node
    return None
