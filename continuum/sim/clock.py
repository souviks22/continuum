"""Virtual clock for the deterministic simulator.

Real wall-clock time must never leak into simulated runs -- every notion of
"now" that the simulator or the code under test relies on has to come from
here, so that a run is byte-for-byte reproducible given the same seed and
the same sequence of scheduled events.
"""

from __future__ import annotations


class VirtualClock:
    """Monotonic virtual time, advanced only by the scheduler.

    Time is an integer number of virtual microseconds. Integers (not floats)
    are used deliberately: float drift would make two runs of the "same"
    schedule diverge in ways that have nothing to do with the failure mode
    being tested.
    """

    def __init__(self, start: int = 0) -> None:
        if start < 0:
            raise ValueError("start must be >= 0")
        self._now = start

    def now(self) -> int:
        return self._now

    def advance_to(self, t: int) -> None:
        """Move the clock forward to `t`. Time never moves backward."""
        if t < self._now:
            raise ValueError(
                f"clock cannot move backward: now={self._now}, requested={t}"
            )
        self._now = t
