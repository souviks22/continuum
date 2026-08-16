"""Client-side routing.

A client needs to: (1) find which shard owns a key, by asking the
metadata group; (2) find and talk to that shard's *current leader*, not
just any replica; (3) recover gracefully when either guess is wrong --
a stale cached leader, a shard that's moved, a replica that's down.

Callback-based throughout (matching RpcEndpoint.call's style), since
everything in this codebase runs on the deterministic EventScheduler
rather than real blocking I/O -- there's no thread to block on here, so
a synchronous-looking `client.get(key)` would have nothing to actually
wait on.

Deliberately NOT linearizable reads: ClientQuery is served by whichever
replica currently believes it's leader, with no read-index or lease
check first. A stale "leader" that hasn't yet stepped down after a
partition heals could theoretically serve one election-timeout window's
worth of stale reads. That's a distinct, deliberately separate concern
from routing (see the read-semantics discussion from cluster planning),
scoped for a later step rather than bolted on here.

Also deliberately NOT caching key-to-shard-range mappings -- every call
re-queries the metadata group for routing. What IS cached is
shard-to-believed-leader, since leadership changes far more often than
range ownership but is also cheap to get wrong and recover from (one
extra round trip via leader_hint or the next candidate in line).
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from continuum.cluster.bootstrap import METADATA_SHARD_ID
from continuum.cluster.shard_manager import shard_address
from continuum.rpc.endpoint import RpcEndpoint

ResultCallback = Callable[[Optional[Any], Optional[str]], None]  # (value, error) -> None


class ContinuumClient:
    def __init__(
        self,
        rpc: RpcEndpoint,
        metadata_replicas: list[str],
        rpc_timeout: int = 200,
        max_retries: int = 10,
    ) -> None:
        self._rpc = rpc
        self._metadata_replicas = list(metadata_replicas)
        self._rpc_timeout = rpc_timeout
        self._max_retries = max_retries
        self._leader_cache: dict[str, str] = {}  # shard_id -> believed current leader address

    # -- public KV API --------------------------------------------------

    def get(self, key: str, on_result: ResultCallback) -> None:
        def after_route(shard_id: Optional[str], replicas: Optional[list[str]], error: Optional[str]) -> None:
            if error:
                on_result(None, error)
                return
            self._call_shard(
                shard_id,
                replicas,
                "ClientQuery",
                {"query": {"op": "get", "key": key}},
                lambda reply, err: on_result(reply["value"] if reply else None, err),
            )

        self._route(key, after_route)

    def set(self, key: str, value: str, on_result: ResultCallback) -> None:
        self._propose_for_key(key, {"op": "set", "key": key, "value": value}, on_result)

    def delete(self, key: str, on_result: ResultCallback) -> None:
        self._propose_for_key(key, {"op": "delete", "key": key}, on_result)

    def _propose_for_key(self, key: str, command: dict[str, Any], on_result: ResultCallback) -> None:
        def after_route(shard_id: Optional[str], replicas: Optional[list[str]], error: Optional[str]) -> None:
            if error:
                on_result(None, error)
                return
            self._call_shard(
                shard_id,
                replicas,
                "ClientPropose",
                {"command": command},
                lambda reply, err: on_result(reply["index"] if reply else None, err),
            )

        self._route(key, after_route)

    # -- routing --------------------------------------------------------

    def _route(
        self, key: str, on_done: Callable[[Optional[str], Optional[list[str]], Optional[str]], None]
    ) -> None:
        def handle(reply: Optional[dict[str, Any]], error: Optional[str]) -> None:
            if error:
                on_done(None, None, error)
                return
            shard_id = reply["shard_id"]
            addresses = [shard_address(pid, shard_id) for pid in reply["replicas"]]
            on_done(shard_id, addresses, None)

        self._call_shard(
            METADATA_SHARD_ID,
            self._metadata_replicas,
            "ClientQuery",
            {"query": {"op": "route", "key": key}},
            handle,
        )

    # -- leader-following RPC with retry/redirect ------------------------

    def _call_shard(
        self,
        shard_id: str,
        replicas: list[str],
        method: str,
        body: dict[str, Any],
        on_done: Callable[[Optional[dict[str, Any]], Optional[str]], None],
    ) -> None:
        candidates = self._ordered_candidates(shard_id, replicas)
        self._try_candidates(shard_id, candidates, method, body, on_done, attempt=0)

    def _ordered_candidates(self, shard_id: str, replicas: list[str]) -> list[str]:
        cached_leader = self._leader_cache.get(shard_id)
        if cached_leader is not None and cached_leader in replicas:
            return [cached_leader] + [r for r in replicas if r != cached_leader]
        return list(replicas)

    def _try_candidates(
        self,
        shard_id: str,
        candidates: list[str],
        method: str,
        body: dict[str, Any],
        on_done: Callable[[Optional[dict[str, Any]], Optional[str]], None],
        attempt: int,
    ) -> None:
        if attempt >= self._max_retries or not candidates:
            on_done(None, f"no leader found for shard {shard_id!r} within retry budget")
            return

        target, rest = candidates[0], candidates[1:]

        def on_reply(reply: Optional[dict[str, Any]], timed_out: bool) -> None:
            if timed_out or reply is None:
                self._try_candidates(shard_id, rest, method, body, on_done, attempt + 1)
                return
            if reply.get("success"):
                self._leader_cache[shard_id] = target
                on_done(reply, None)
                return
            # Not the leader: follow its hint if it gave one (moves
            # straight to the node it believes is leader instead of
            # blindly trying the next replica in an arbitrary order),
            # otherwise fall through to the remaining candidates.
            hint = reply.get("leader_hint")
            if hint:
                next_candidates = [hint] + [c for c in rest if c != hint]
            else:
                next_candidates = rest
            self._try_candidates(shard_id, next_candidates, method, body, on_done, attempt + 1)

        self._rpc.call(target, method, body, timeout=self._rpc_timeout, on_reply=on_reply)
