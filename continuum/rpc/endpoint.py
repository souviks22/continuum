"""Request/reply RPC on top of a Transport, with scheduler-driven timeouts.

This is what Raft's RequestVote/AppendEntries (Phase 2) and the placement
layer's control RPCs (Phase 3) get built on. Timeouts are expressed as
scheduled events rather than real sleeps/threads, consistent with
everything else in the simulator running off the same EventScheduler --
that's what keeps a whole run deterministic and reproducible from a seed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from continuum.rpc.message import Message, next_message_id
from continuum.rpc.transport import Transport
from continuum.sim.scheduler import EventHandle, EventScheduler

# (from_node, request_body) -> reply_body
MethodHandler = Callable[[str, dict[str, Any]], dict[str, Any]]
# (reply_body_or_None, timed_out) -> None
ReplyCallback = Callable[[Optional[dict[str, Any]], bool], None]


@dataclass
class _Pending:
    on_reply: ReplyCallback
    timeout_handle: EventHandle


class RpcEndpoint:
    """One per node. Wires a Transport to request/reply semantics with
    correlation ids and timeouts."""

    def __init__(
        self, node_id: str, transport: Transport, scheduler: EventScheduler
    ) -> None:
        self.node_id = node_id
        self._transport = transport
        self._scheduler = scheduler
        self._methods: dict[str, MethodHandler] = {}
        self._pending: dict[str, _Pending] = {}
        transport.set_receive_handler(self._on_receive)

    def register_method(self, name: str, handler: MethodHandler) -> None:
        if name in self._methods:
            raise ValueError(f"method {name!r} already registered")
        self._methods[name] = handler

    def call(
        self,
        to: str,
        method: str,
        body: dict[str, Any],
        timeout: int,
        on_reply: ReplyCallback,
    ) -> str:
        """Fire off a request. `on_reply(body, timed_out)` runs exactly
        once: either with the reply body when it arrives, or with
        (None, True) if `timeout` virtual-time-units pass first. A reply
        that arrives after the timeout has already fired is silently
        ignored -- the caller has moved on and must not be invoked twice.
        Returns the correlation id, mostly useful for tests/logging."""
        msg_id = next_message_id()
        request = Message(kind="request", msg_id=msg_id, method=method, body=body)
        self._transport.send(to, request.encode())

        handle = self._scheduler.schedule_after(timeout, self._on_timeout, msg_id)
        self._pending[msg_id] = _Pending(on_reply=on_reply, timeout_handle=handle)
        return msg_id

    def _on_timeout(self, msg_id: str) -> None:
        pending = self._pending.pop(msg_id, None)
        if pending is None:
            return  # reply already arrived and cancelled this timeout
        pending.on_reply(None, True)

    def _on_receive(self, from_node: str, payload: bytes) -> None:
        message = Message.decode(payload)
        if message.kind == "request":
            self._handle_request(from_node, message)
        else:
            self._handle_reply(message)

    def _handle_request(self, from_node: str, message: Message) -> None:
        handler = self._methods.get(message.method)
        if handler is None:
            # Unknown method: real RPC frameworks would error back; here we
            # simply don't reply, which surfaces as the caller's timeout --
            # matching how an unreachable/unimplemented peer looks on a
            # real network (no distinguishable error, just silence).
            return
        reply_body = handler(from_node, message.body)
        reply = Message(kind="reply", msg_id=message.msg_id, method="", body=reply_body)
        self._transport.send(from_node, reply.encode())

    def _handle_reply(self, message: Message) -> None:
        pending = self._pending.pop(message.msg_id, None)
        if pending is None:
            return  # timed out already, or a stray/duplicate reply
        pending.timeout_handle.cancel()
        pending.on_reply(message.body, False)
