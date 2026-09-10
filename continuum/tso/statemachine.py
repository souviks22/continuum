"""Timestamp Oracle state machine: the durably-replicated high-water
mark for batch timestamp allocation.

Why batches, not one Raft write per timestamp: every read/write
transaction in Phase 6 needs a timestamp, so if issuing one meant a full
Raft round-trip every time, the TSO would be the throughput ceiling for
the entire cluster. Instead the leader periodically reserves a *batch*
of timestamps with a single replicated write (this state machine just
tracks "the highest timestamp any batch has ever reserved up to"), then
hands out individual timestamps from that batch out of local memory
without touching Raft again until the batch is exhausted. See
oracle.py for the leader-side allocation logic this state machine's
durability guarantee makes safe.
"""

from __future__ import annotations

from typing import Any


class TSOStateMachine:
    def __init__(self) -> None:
        self._max_allocated: int = -1

    # -- ReplicatedStateMachine protocol -----------------------------------

    def apply_command(self, index: int, command: dict[str, Any]) -> None:
        if command["op"] != "allocate_batch":
            raise ValueError(f"unknown TSO operation {command['op']!r}")
        up_to = command["up_to"]
        if up_to <= self._max_allocated:
            # A safety net, not the primary defense: the *leader-side*
            # oracle (oracle.py) is what should never propose a
            # non-increasing batch in the first place. This guards
            # against ever silently accepting one anyway (e.g. a bug
            # there, or a stale/duplicate proposal replayed) rather than
            # quietly corrupting the monotonicity guarantee the whole
            # point of this state machine is to provide.
            raise ValueError(
                f"batch allocation up_to={up_to} is not greater than the current "
                f"high-water mark ({self._max_allocated}) -- monotonicity violated"
            )
        self._max_allocated = up_to

    def snapshot_state(self) -> dict[str, Any]:
        return {"max_allocated": self._max_allocated}

    def load_state(self, state: dict[str, Any]) -> None:
        self._max_allocated = state["max_allocated"]

    # -- queries -----------------------------------------------------------

    def get_max_allocated(self) -> int:
        return self._max_allocated
