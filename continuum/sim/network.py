"""Simulated network: the fault-injection surface for Phase 0.

Design goals, matching the failure taxonomy agreed on before implementation
started:

- Full and partial (asymmetric) partitions: A can reach B without B being
  able to reach A. Modelled as a set of *blocked directed edges*, not a
  simple "split into two groups" partition, because asymmetric partitions
  are a distinct and important failure mode (real routers/NICs fail this
  way) that a symmetric model would hide entirely.
- Variable per-link delay, which is what actually produces message
  reordering -- there is no separate "reorder" knob. If two messages are
  sent A->B close together and the second happens to draw a shorter delay,
  it arrives first. This is the honest way to get reordering: nothing
  about the transport magically preserves send order, that guarantee has
  to be re-established above this layer if it's needed.
- Message loss, independently per link.
- Message duplication, independently per link.

Everything randomized here is drawn from a single seeded RNG so a whole
run is reproducible from one seed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable, Optional

from continuum.sim.scheduler import EventScheduler

Handler = Callable[[str, bytes], None]  # (from_node_id, payload) -> None


@dataclass
class LinkConfig:
    """Per-directed-edge network conditions. Applies to messages sent
    along `from_node -> to_node` specifically -- the reverse direction has
    its own (independently configurable) LinkConfig."""

    min_delay: int = 1
    max_delay: int = 1
    loss_rate: float = 0.0
    duplicate_rate: float = 0.0

    def __post_init__(self) -> None:
        if self.min_delay < 0 or self.max_delay < self.min_delay:
            raise ValueError("require 0 <= min_delay <= max_delay")
        if not (0.0 <= self.loss_rate <= 1.0):
            raise ValueError("loss_rate must be in [0, 1]")
        if not (0.0 <= self.duplicate_rate <= 1.0):
            raise ValueError("duplicate_rate must be in [0, 1]")


class SimulatedNetwork:
    def __init__(self, scheduler: EventScheduler, seed: int = 0) -> None:
        self.scheduler = scheduler
        self._rng = random.Random(seed)
        self._handlers: dict[str, Handler] = {}
        self._default_link = LinkConfig()
        self._link_overrides: dict[tuple[str, str], LinkConfig] = {}
        # Directed edges that are currently blocked outright (partition).
        self._blocked_edges: set[tuple[str, str]] = set()
        # Nodes whose process is currently down (crashed / not yet restarted).
        self._down: set[str] = set()

    # -- topology -----------------------------------------------------

    def register_node(self, node_id: str, handler: Handler) -> None:
        if node_id in self._handlers:
            raise ValueError(f"node {node_id!r} already registered")
        self._handlers[node_id] = handler

    # -- node lifecycle ------------------------------------------------
    #
    # A crash is modelled purely as a network-delivery fact: a down node
    # is one whose messages are never delivered, in either direction --
    # not because the network dropped them, but because there is no
    # process there to send or receive. This is deliberately a network
    # concern rather than a separate subsystem: it's the simplest model
    # that produces the right observable behaviour (peers see a crashed
    # node exactly the way they'd see a partition, except a restart is
    # under the *crashed* node's control, not the network's).
    #
    # Two timing rules, chosen to match real socket behaviour:
    #  - `send` checks the *sender's* liveness immediately (a dead
    #    process cannot originate a send).
    #  - delivery checks the *receiver's* liveness at delivery time, not
    #    send time, so a message already "in flight" when the receiver
    #    crashes is correctly lost, and a message sent while the
    #    receiver was still down but delivered after it restarts is
    #    correctly delivered.

    def is_up(self, node_id: str) -> bool:
        return node_id not in self._down

    def crash_now(self, node_id: str) -> None:
        if node_id not in self._handlers:
            raise ValueError(f"unknown node {node_id!r}")
        self._down.add(node_id)

    def restart_now(self, node_id: str, on_restart: Optional[Callable[[], None]] = None) -> None:
        if node_id not in self._handlers:
            raise ValueError(f"unknown node {node_id!r}")
        if on_restart is not None:
            on_restart()
        self._down.discard(node_id)

    def crash_at(self, time: int, node_id: str):
        return self.scheduler.schedule_at(time, self.crash_now, node_id)

    def restart_at(
        self,
        time: int,
        node_id: str,
        on_restart: Optional[Callable[[], None]] = None,
    ):
        """Schedule a restart. `on_restart`, if given, runs at the restart
        instant *before* the node is marked up again -- this is where
        recovery logic (e.g. replaying a WAL in a later phase) belongs,
        so that any messages arriving at the same virtual time as the
        restart still see fully-recovered state."""
        return self.scheduler.schedule_at(time, self.restart_now, node_id, on_restart)

    # -- link configuration --------------------------------------------

    def set_link(self, from_node: str, to_node: str, config: LinkConfig) -> None:
        """Configure the directed edge from_node -> to_node."""
        self._link_overrides[(from_node, to_node)] = config

    def set_default_link(self, config: LinkConfig) -> None:
        self._default_link = config

    def _link_for(self, from_node: str, to_node: str) -> LinkConfig:
        return self._link_overrides.get((from_node, to_node), self._default_link)

    # -- partitions ----------------------------------------------

    def block(self, from_node: str, to_node: str) -> None:
        """Block the directed edge from_node -> to_node outright. Call
        twice (both directions) for a symmetric partition."""
        self._blocked_edges.add((from_node, to_node))

    def unblock(self, from_node: str, to_node: str) -> None:
        self._blocked_edges.discard((from_node, to_node))

    def partition(self, group_a: set[str], group_b: set[str]) -> None:
        """Symmetric full partition between two disjoint groups: every
        edge crossing the boundary, in both directions, is blocked."""
        for a in group_a:
            for b in group_b:
                self.block(a, b)
                self.block(b, a)

    def heal(self, group_a: set[str], group_b: set[str]) -> None:
        for a in group_a:
            for b in group_b:
                self.unblock(a, b)
                self.unblock(b, a)

    def is_blocked(self, from_node: str, to_node: str) -> bool:
        return (from_node, to_node) in self._blocked_edges

    # -- sending -----------------------------------------------------------

    def send(self, from_node: str, to_node: str, payload: bytes) -> None:
        if to_node not in self._handlers:
            raise ValueError(f"unknown destination node {to_node!r}")
        if not self.is_up(from_node):
            return  # a crashed process cannot originate a send
        if self.is_blocked(from_node, to_node):
            return  # partitioned: message silently vanishes, as on a real network

        link = self._link_for(from_node, to_node)

        if self._rng.random() < link.loss_rate:
            return  # dropped

        self._schedule_delivery(from_node, to_node, payload, link)
        if self._rng.random() < link.duplicate_rate:
            self._schedule_delivery(from_node, to_node, payload, link)

    def _schedule_delivery(
        self, from_node: str, to_node: str, payload: bytes, link: LinkConfig
    ) -> None:
        delay = self._rng.randint(link.min_delay, link.max_delay)
        self.scheduler.schedule_after(
            delay, self._deliver, from_node, to_node, payload
        )
    
    def _deliver(self, from_node: str, to_node: str, payload: bytes) -> None:
        if not self.is_up(to_node):
            return  # receiver has crashed since this message was sent
        self._handlers[to_node](from_node, payload)