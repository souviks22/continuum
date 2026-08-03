"""In-memory key-value state machine plus the operation type that drives
it.

Kept deliberately simple -- a single dict, two operation types. Phase 4
replaces this dict with an MVCC-versioned store; that rework is scoped
separately and deliberately not anticipated here, so this step's surface
stays small and provably correct on its own.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal, Optional

OpType = Literal["set", "delete"]


@dataclass(frozen=True)
class Operation:
    """One state-machine mutation. `index` is the storage engine's own
    logical log index -- NOT the WAL file's internal physical record
    position. Embedding it in the payload itself, rather than letting it
    be implied by a record's position within whatever WAL file happens to
    exist, is what lets replay stay correct even when a previous
    compaction was interrupted partway and left a WAL file whose physical
    contents don't start where the current snapshot leaves off."""

    index: int
    op: OpType
    key: str
    value: Optional[str] = None

    def encode(self) -> bytes:
        return json.dumps(
            {"index": self.index, "op": self.op, "key": self.key, "value": self.value}
        ).encode()

    @staticmethod
    def decode(payload: bytes) -> "Operation":
        data = json.loads(payload.decode())
        return Operation(
            index=data["index"], op=data["op"], key=data["key"], value=data["value"]
        )


class StateMachine:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def apply(self, operation: Operation) -> None:
        if operation.op == "set":
            assert operation.value is not None
            self._data[operation.key] = operation.value
        elif operation.op == "delete":
            self._data.pop(operation.key, None)
        else:
            raise ValueError(f"unknown operation type {operation.op!r}")

    def get(self, key: str) -> Optional[str]:
        return self._data.get(key)

    def snapshot_state(self) -> dict[str, str]:
        return dict(self._data)  # copy: caller must not be able to mutate our state

    def load_state(self, state: dict[str, str]) -> None:
        self._data = dict(state)
