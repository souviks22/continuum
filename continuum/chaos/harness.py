"""Real-cluster chaos harness (skeleton).

Everything through Step 0.3 exercises fault injection against the
deterministic in-process simulator. That's the right tool for finding
protocol bugs cheaply and reproducibly, but it can't represent failure
modes that only exist because a real OS, real disk, and real NIC are
involved -- actual fsync durability, real scheduling jitter and gray
failures, packet loss on a real link. This module is the seam for that:
once Phase 2 produces a real running node binary, this harness drives
fault injection against real processes and a real Linux network stack
via network namespaces and `tc netem`.

Stubbed for now -- there's no real binary to point it at yet -- but the
interface is fixed here, deliberately mirroring SimulatedNetwork's
(partition/heal/set_link, node crash/restart), so a Phase 7 failure
scenario can eventually be written once against ChaosHarness and run
against either backend. The Linux implementation's *command
construction* logic can be unit-tested today by injecting a fake runner,
without root or a real multi-namespace setup -- but process lifecycle
management (capturing a real pid, sending real signals) is left as an
explicit gap until Phase 2 gives it something real to manage.
"""

from __future__ import annotations

import itertools
import subprocess
from typing import Callable, Protocol

CommandRunner = Callable[[list[str]], "subprocess.CompletedProcess"]


def _default_runner(cmd: list[str]) -> "subprocess.CompletedProcess":
    return subprocess.run(cmd, capture_output=True, text=True, check=True)


class ChaosHarness(Protocol):
    """What Phase 7's failure-mode exploration needs to be able to do to
    a real running cluster."""

    def start_node(self, node_id: str, argv: list[str]) -> None: ...

    def kill_node(self, node_id: str, signal: str = "SIGKILL") -> None: ...

    def restart_node(self, node_id: str) -> None: ...

    def partition(self, group_a: set[str], group_b: set[str]) -> None: ...

    def heal(self, group_a: set[str], group_b: set[str]) -> None: ...

    def set_link(
        self, from_node: str, to_node: str, delay_ms: int = 0, loss_pct: float = 0.0
    ) -> None: ...

    def clear_link(self, from_node: str, to_node: str) -> None: ...


class NullChaosHarness:
    """No-op implementation. Lets a Phase 7 scenario runner be wired up
    and tested end-to-end before there's a real cluster or root access
    to run `ip netns`/`tc` against. Records every call it receives so a
    test can assert on what a scenario *intended* to do."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def start_node(self, node_id: str, argv: list[str]) -> None:
        self.calls.append(("start_node", (node_id, tuple(argv))))

    def kill_node(self, node_id: str, signal: str = "SIGKILL") -> None:
        self.calls.append(("kill_node", (node_id, signal)))

    def restart_node(self, node_id: str) -> None:
        self.calls.append(("restart_node", (node_id,)))

    def partition(self, group_a: set[str], group_b: set[str]) -> None:
        self.calls.append(("partition", (frozenset(group_a), frozenset(group_b))))

    def heal(self, group_a: set[str], group_b: set[str]) -> None:
        self.calls.append(("heal", (frozenset(group_a), frozenset(group_b))))

    def set_link(
        self, from_node: str, to_node: str, delay_ms: int = 0, loss_pct: float = 0.0
    ) -> None:
        self.calls.append(("set_link", (from_node, to_node, delay_ms, loss_pct)))

    def clear_link(self, from_node: str, to_node: str) -> None:
        self.calls.append(("clear_link", (from_node, to_node)))


class TcNetnsChaosHarness:
    """Linux implementation: one network namespace per node, `tc netem`
    for per-link delay/loss, an unreachable-route rule for hard
    partitions (kept per-directed-edge, not a shared bridge rule, so
    asymmetric partitions are representable the same way
    SimulatedNetwork represents them). Requires root and iproute2.

    KNOWN GAP, left explicit rather than papered over: real node process
    lifecycle (spawning detached, capturing a pid, sending real signals)
    isn't implemented yet -- `start_node` currently issues a *blocking*
    `ip netns exec`, which is wrong for a long-running server process.
    This needs Popen-based process management, deferred until Phase 2
    produces an actual node binary to manage; wiring it against nothing
    real would just be guessing at an interface that binary hasn't
    defined yet.
    """

    def __init__(self, run: CommandRunner = _default_runner) -> None:
        self._run = run
        self._argvs: dict[str, list[str]] = {}
        self._addrs: dict[str, str] = {}
        self._addr_counter = itertools.count(1)

    def _netns(self, node_id: str) -> str:
        return f"continuum-{node_id}"

    def _ensure_addr(self, node_id: str) -> str:
        if node_id not in self._addrs:
            self._addrs[node_id] = f"10.200.{next(self._addr_counter)}.1"
        return self._addrs[node_id]

    def start_node(self, node_id: str, argv: list[str]) -> None:
        self._argvs[node_id] = argv
        self._ensure_addr(node_id)
        ns = self._netns(node_id)
        self._run(["ip", "netns", "add", ns])
        self._run(["ip", "netns", "exec", ns, *argv])

    def kill_node(self, node_id: str, signal: str = "SIGKILL") -> None:
        # No pid tracking yet -- see class docstring. Left as a no-op
        # rather than a fake success, so a caller relying on this in a
        # real scenario fails loudly (nothing happens) rather than
        # believing a kill occurred that didn't.
        return

    def restart_node(self, node_id: str) -> None:
        argv = self._argvs.get(node_id)
        if argv is None:
            raise ValueError(f"node {node_id!r} was never started")
        self.kill_node(node_id)
        self.start_node(node_id, argv)

    def partition(self, group_a: set[str], group_b: set[str]) -> None:
        for a in group_a:
            for b in group_b:
                self._block(a, b)
                self._block(b, a)

    def heal(self, group_a: set[str], group_b: set[str]) -> None:
        for a in group_a:
            for b in group_b:
                self._unblock(a, b)
                self._unblock(b, a)

    def _block(self, from_node: str, to_node: str) -> None:
        to_addr = self._ensure_addr(to_node)
        self._run(
            [
                "ip", "netns", "exec", self._netns(from_node),
                "ip", "route", "add", "unreachable", to_addr,
            ]
        )

    def _unblock(self, from_node: str, to_node: str) -> None:
        to_addr = self._ensure_addr(to_node)
        self._run(
            [
                "ip", "netns", "exec", self._netns(from_node),
                "ip", "route", "del", "unreachable", to_addr,
            ]
        )

    def set_link(
        self, from_node: str, to_node: str, delay_ms: int = 0, loss_pct: float = 0.0
    ) -> None:
        self._run(
            [
                "ip", "netns", "exec", self._netns(from_node),
                "tc", "qdisc", "replace", "dev", "veth0", "root", "netem",
                "delay", f"{delay_ms}ms", "loss", f"{loss_pct}%",
            ]
        )

    def clear_link(self, from_node: str, to_node: str) -> None:
        self._run(
            [
                "ip", "netns", "exec", self._netns(from_node),
                "tc", "qdisc", "del", "dev", "veth0", "root",
            ]
        )
