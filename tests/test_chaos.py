import pytest

from continuum.chaos.harness import NullChaosHarness, TcNetnsChaosHarness


# -- NullChaosHarness ----------------------------------------------------


def test_null_harness_records_start_node():
    h = NullChaosHarness()
    h.start_node("n1", ["continuum-node", "--id", "n1"])
    assert h.calls == [("start_node", ("n1", ("continuum-node", "--id", "n1")))]


def test_null_harness_records_partition_as_frozensets():
    h = NullChaosHarness()
    h.partition({"a", "b"}, {"c"})
    assert h.calls == [("partition", (frozenset({"a", "b"}), frozenset({"c"})))]


def test_null_harness_records_full_call_sequence():
    h = NullChaosHarness()
    h.start_node("n1", ["bin"])
    h.set_link("n1", "n2", delay_ms=50, loss_pct=1.5)
    h.kill_node("n1")
    h.restart_node("n1")
    h.clear_link("n1", "n2")
    kinds = [call for call, _ in h.calls]
    assert kinds == ["start_node", "set_link", "kill_node", "restart_node", "clear_link"]


# -- TcNetnsChaosHarness: command construction, via a fake runner --------


def make_harness():
    commands: list[list[str]] = []

    def fake_run(cmd: list[str]):
        commands.append(cmd)
        return None

    return TcNetnsChaosHarness(run=fake_run), commands


def test_start_node_creates_namespace_and_execs_argv():
    h, commands = make_harness()
    h.start_node("n1", ["continuum-node", "--id", "n1"])

    assert ["ip", "netns", "add", "continuum-n1"] in commands
    assert ["ip", "netns", "exec", "continuum-n1", "continuum-node", "--id", "n1"] in commands


def test_restart_node_requires_prior_start():
    h, _ = make_harness()
    with pytest.raises(ValueError):
        h.restart_node("never-started")


def test_restart_node_reissues_start_commands():
    h, commands = make_harness()
    h.start_node("n1", ["bin"])
    commands.clear()
    h.restart_node("n1")
    assert ["ip", "netns", "exec", "continuum-n1", "bin"] in commands


def test_partition_blocks_both_directions_with_unreachable_routes():
    h, commands = make_harness()
    h.start_node("a", ["bin"])
    h.start_node("b", ["bin"])
    commands.clear()

    h.partition({"a"}, {"b"})

    addr_a = h._addrs["a"]
    addr_b = h._addrs["b"]
    assert ["ip", "netns", "exec", "continuum-a", "ip", "route", "add", "unreachable", addr_b] in commands
    assert ["ip", "netns", "exec", "continuum-b", "ip", "route", "add", "unreachable", addr_a] in commands


def test_heal_removes_unreachable_routes_both_directions():
    h, commands = make_harness()
    h.start_node("a", ["bin"])
    h.start_node("b", ["bin"])
    h.partition({"a"}, {"b"})
    commands.clear()

    h.heal({"a"}, {"b"})

    addr_a = h._addrs["a"]
    addr_b = h._addrs["b"]
    assert ["ip", "netns", "exec", "continuum-a", "ip", "route", "del", "unreachable", addr_b] in commands
    assert ["ip", "netns", "exec", "continuum-b", "ip", "route", "del", "unreachable", addr_a] in commands


def test_set_link_issues_netem_delay_and_loss():
    h, commands = make_harness()
    h.start_node("a", ["bin"])
    commands.clear()

    h.set_link("a", "b", delay_ms=100, loss_pct=5.0)

    assert [
        "ip", "netns", "exec", "continuum-a",
        "tc", "qdisc", "replace", "dev", "veth0", "root", "netem",
        "delay", "100ms", "loss", "5.0%",
    ] in commands


def test_clear_link_removes_qdisc():
    h, commands = make_harness()
    h.start_node("a", ["bin"])
    commands.clear()

    h.clear_link("a", "b")

    assert ["ip", "netns", "exec", "continuum-a", "tc", "qdisc", "del", "dev", "veth0", "root"] in commands


def test_kill_node_is_a_documented_noop_without_pid_tracking():
    h, commands = make_harness()
    h.start_node("a", ["bin"])
    commands.clear()

    h.kill_node("a")  # should not raise, and should not issue any command

    assert commands == []


def test_addresses_are_stable_and_assigned_once():
    h, _ = make_harness()
    h.start_node("a", ["bin"])
    first = h._addrs["a"]
    h.set_link("a", "b", delay_ms=1)
    second = h._addrs["a"]
    assert first == second
