"""RPC message envelope and (de)serialization.

Kept deliberately simple -- JSON over a dict body -- so both the
simulated transport and the eventual gRPC transport can share the same
envelope shape without either one caring how the underlying bytes get
delivered. Raft's RequestVote/AppendEntries RPCs (Phase 2) are built
directly on top of this.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass, field
from typing import Any, Literal

Kind = Literal["request", "reply"]

_id_counter = itertools.count()


def next_message_id() -> str:
    """Process-local, monotonically increasing id -- unique enough to
    correlate a request with its reply within one node's lifetime. Not a
    UUID deliberately: sequential ids make simulator traces and failing
    test output easy to read, and uniqueness only needs to hold
    per-sender, since a caller only ever matches replies against its own
    outstanding requests."""
    return str(next(_id_counter))


@dataclass(frozen=True)
class Message:
    kind: Kind
    msg_id: str  # id of the originating request; a reply carries that same id
    method: str  # RPC method name, e.g. "RequestVote"; empty for replies
    body: dict[str, Any] = field(default_factory=dict)

    def encode(self) -> bytes:
        return json.dumps(
            {
                "kind": self.kind,
                "msg_id": self.msg_id,
                "method": self.method,
                "body": self.body,
            }
        ).encode()

    @staticmethod
    def decode(payload: bytes) -> "Message":
        data = json.loads(payload.decode())
        return Message(
            kind=data["kind"],
            msg_id=data["msg_id"],
            method=data["method"],
            body=data["body"],
        )
