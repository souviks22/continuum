"""Transport abstraction: the seam between RPC logic and the underlying
delivery mechanism, so node logic (and later, Raft) is written once and
runs unchanged against the deterministic simulator now and a real
transport (gRPC) later -- swapping SimulatedTransport for a GrpcTransport
should not require touching RpcEndpoint or anything built on it.
"""

from __future__ import annotations

from typing import Callable, Optional, Protocol

from continuum.sim.network import SimulatedNetwork

ReceiveHandler = Callable[[str, bytes], None]  # (from_node_id, payload) -> None


class Transport(Protocol):
    """The entire surface RPC code needs from a transport."""

    def send(self, to: str, payload: bytes) -> None: ...

    def set_receive_handler(self, handler: ReceiveHandler) -> None: ...


class SimulatedTransport:
    """Transport backed by the deterministic SimulatedNetwork. Bound to a
    single node_id -- each node gets its own instance, mirroring how a
    real transport is bound to one local socket/identity rather than
    shared across nodes."""

    def __init__(self, node_id: str, network: SimulatedNetwork) -> None:
        self.node_id = node_id
        self._network = network
        self._handler: Optional[ReceiveHandler] = None
        network.register_node(node_id, self._on_receive)

    def send(self, to: str, payload: bytes) -> None:
        self._network.send(self.node_id, to, payload)

    def set_receive_handler(self, handler: ReceiveHandler) -> None:
        self._handler = handler

    def _on_receive(self, from_node: str, payload: bytes) -> None:
        if self._handler is None:
            raise RuntimeError(
                f"node {self.node_id!r} received a message before a receive "
                "handler was registered"
            )
        self._handler(from_node, payload)
