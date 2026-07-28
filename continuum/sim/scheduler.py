"""Deterministic discrete-event scheduler.

This is the heart of the simulator: every piece of simulated behaviour
(message delivery, timer firing, node crash/restart) is expressed as an
event scheduled here rather than as a real OS-level thread, sleep, or
socket. That is what makes a run reproducible -- given the same seed and
the same sequence of `schedule_*` calls, the callbacks fire in exactly the
same order every time, even though real network chaos-testing later
(Phase 0 also plans a real-cluster harness) will not be reproducible in
the same way.

Ties (two events scheduled for the same virtual time) are broken by
insertion order, not by an arbitrary heap comparison on the callback
object -- Python's heapq would otherwise raise on incomparable callables,
and even if it didn't, silently-nondeterministic tie-breaking would
defeat the entire point of this module.
"""

from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass, field
from typing import Any, Callable

from continuum.sim.clock import VirtualClock

Callback = Callable[..., None]


@dataclass(order=True)
class _Event:
    time: int
    seq: int
    callback: Callback = field(compare=False)
    args: tuple[Any, ...] = field(compare=False)
    kwargs: dict[str, Any] = field(compare=False)
    cancelled: bool = field(compare=False, default=False)


class EventHandle:
    """Returned by schedule_* calls; lets the caller cancel a pending event."""

    def __init__(self, event: _Event) -> None:
        self._event = event

    def cancel(self) -> None:
        self._event.cancelled = True


class EventScheduler:
    def __init__(self, clock: VirtualClock | None = None) -> None:
        self.clock = clock if clock is not None else VirtualClock()
        self._queue: list[_Event] = []
        self._seq_counter = itertools.count()

    def schedule_at(
        self,
        time: int,
        callback: Callback,
        *args: Any,
        **kwargs: Any,
    ) -> EventHandle:
        if time < self.clock.now():
            raise ValueError(
                f"cannot schedule an event in the past: now={self.clock.now()}, "
                f"requested={time}"
            )
        event = _Event(
            time=time,
            seq=next(self._seq_counter),
            callback=callback,
            args=args,
            kwargs=kwargs,
        )
        heapq.heappush(self._queue, event)
        return EventHandle(event)

    def schedule_after(
        self,
        delay: int,
        callback: Callback,
        *args: Any,
        **kwargs: Any,
    ) -> EventHandle:
        if delay < 0:
            raise ValueError("delay must be >= 0")
        return self.schedule_at(self.clock.now() + delay, callback, *args, **kwargs)

    def pending_count(self) -> int:
        return sum(1 for e in self._queue if not e.cancelled)

    def run_until(self, end_time: int) -> None:
        """Process every event with time <= end_time, then advance the
        clock to end_time even if no event landed exactly on it."""
        while self._queue and self._queue[0].time <= end_time:
            event = heapq.heappop(self._queue)
            if event.cancelled:
                continue
            self.clock.advance_to(event.time)
            event.callback(*event.args, **event.kwargs)
        if self.clock.now() < end_time:
            self.clock.advance_to(end_time)

    def run_until_idle(self, max_steps: int | None = None) -> int:
        """Process events until the queue is empty (or max_steps is hit,
        which guards against a bug that schedules an infinite chain of
        events -- e.g. a retry loop with no backoff limit). Returns the
        number of callbacks actually executed."""
        steps = 0
        while self._queue:
            if max_steps is not None and steps >= max_steps:
                raise RuntimeError(
                    f"run_until_idle exceeded max_steps={max_steps}; "
                    "likely an unbounded retry/resend loop in the code under test"
                )
            event = heapq.heappop(self._queue)
            if event.cancelled:
                continue
            self.clock.advance_to(event.time)
            event.callback(*event.args, **event.kwargs)
            steps += 1
        return steps
