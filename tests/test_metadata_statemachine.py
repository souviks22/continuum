import pytest

from continuum.cluster.metadata import MetadataStateMachine


def test_create_shard_registers_range_and_replicas():
    sm = MetadataStateMachine()
    sm.apply_command(0, {
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1", "n2", "n3"],
    })
    meta = sm.get_shard("shard-0")
    assert meta.start is None
    assert meta.end is None
    assert meta.replicas == ["n1", "n2", "n3"]


def test_route_before_bootstrap_raises():
    sm = MetadataStateMachine()
    with pytest.raises(KeyError):
        sm.route("anything")


def test_route_with_single_shard_covers_all_keys():
    sm = MetadataStateMachine()
    sm.apply_command(0, {
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1"],
    })
    assert sm.route("a") == "shard-0"
    assert sm.route("z") == "shard-0"


def test_split_shard_creates_two_ranges_with_correct_boundaries():
    sm = MetadataStateMachine()
    sm.apply_command(0, {
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1", "n2"],
    })
    sm.apply_command(1, {
        "op": "split_shard", "shard_id": "shard-0", "split_key": "m",
        "new_shard_id": "shard-1", "replicas": ["n1", "n2"],
    })

    left = sm.get_shard("shard-0")
    right = sm.get_shard("shard-1")
    assert left.start is None
    assert left.end == "m"
    assert right.start == "m"
    assert right.end is None


def test_route_after_split_directs_to_correct_shard():
    sm = MetadataStateMachine()
    sm.apply_command(0, {
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1"],
    })
    sm.apply_command(1, {
        "op": "split_shard", "shard_id": "shard-0", "split_key": "m",
        "new_shard_id": "shard-1", "replicas": ["n1"],
    })
    assert sm.route("a") == "shard-0"
    assert sm.route("m") == "shard-1"
    assert sm.route("z") == "shard-1"


def test_split_on_unknown_shard_raises():
    sm = MetadataStateMachine()
    with pytest.raises(KeyError):
        sm.apply_command(0, {
            "op": "split_shard", "shard_id": "ghost", "split_key": "m",
            "new_shard_id": "shard-1", "replicas": ["n1"],
        })


def test_duplicate_split_command_is_idempotent():
    sm = MetadataStateMachine()
    sm.apply_command(0, {
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1"],
    })
    split_cmd = {
        "op": "split_shard", "shard_id": "shard-0", "split_key": "m",
        "new_shard_id": "shard-1", "replicas": ["n1"],
    }
    sm.apply_command(1, split_cmd)
    sm.apply_command(2, split_cmd)  # retried/duplicate delivery

    assert len(sm.all_shards()) == 2
    assert sm.get_shard("shard-0").end == "m"
    assert sm.get_shard("shard-1").start == "m"


def test_unknown_op_raises():
    sm = MetadataStateMachine()
    with pytest.raises(ValueError):
        sm.apply_command(0, {"op": "delete_universe"})


def test_snapshot_and_load_state_roundtrip():
    sm = MetadataStateMachine()
    sm.apply_command(0, {
        "op": "create_shard", "shard_id": "shard-0", "start": None, "end": None,
        "replicas": ["n1", "n2"],
    })
    sm.apply_command(1, {
        "op": "split_shard", "shard_id": "shard-0", "split_key": "m",
        "new_shard_id": "shard-1", "replicas": ["n3"],
    })
    state = sm.snapshot_state()

    reloaded = MetadataStateMachine()
    reloaded.load_state(state)
    assert reloaded.route("a") == "shard-0"
    assert reloaded.route("z") == "shard-1"
    assert reloaded.get_shard("shard-1").replicas == ["n3"]
