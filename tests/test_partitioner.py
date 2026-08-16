import pytest

from continuum.cluster.partitioner import RangePartitioner


def test_fresh_partitioner_routes_everything_to_initial_shard():
    p = RangePartitioner("shard-0")
    assert p.route("apple") == "shard-0"
    assert p.route("zebra") == "shard-0"
    assert p.route("") == "shard-0"


def test_all_shard_ids_starts_with_one_shard():
    p = RangePartitioner("shard-0")
    assert p.all_shard_ids() == ["shard-0"]


def test_split_creates_two_ranges():
    p = RangePartitioner("shard-0")
    left, right = p.split("shard-0", "m")
    assert left == "shard-0"
    assert right != "shard-0"
    assert set(p.all_shard_ids()) == {left, right}


def test_split_routes_keys_to_correct_side():
    p = RangePartitioner("shard-0")
    left, right = p.split("shard-0", "m")
    assert p.route("a") == left
    assert p.route("l") == left
    assert p.route("m") == right  # split key itself belongs to the right (>=) side
    assert p.route("z") == right


def test_split_key_at_range_end_raises():
    p = RangePartitioner("shard-0")
    left, right = p.split("shard-0", "m")  # shard-0 (left) is now [None, "m")
    with pytest.raises(ValueError):
        p.split(left, "m")  # "m" is not < this range's end ("m"); invalid


def test_split_key_at_or_beyond_range_end_raises():
    p = RangePartitioner("shard-0")
    left, right = p.split("shard-0", "m")  # right is now ["m", None)
    with pytest.raises(ValueError):
        p.split(right, "a")  # "a" < "m", below this range's start


def test_multiple_splits_route_correctly(tmp_path=None):
    p = RangePartitioner("shard-0")
    left, right = p.split("shard-0", "m")
    right_left, right_right = p.split(right, "t")

    assert p.route("a") == left
    assert p.route("n") == right_left
    assert p.route("t") == right_right
    assert p.route("z") == right_right
    assert set(p.all_shard_ids()) == {left, right_left, right_right}


def test_range_for_shard_reflects_current_boundaries():
    p = RangePartitioner("shard-0")
    left, right = p.split("shard-0", "m")
    left_range = p.range_for_shard(left)
    right_range = p.range_for_shard(right)
    assert left_range.start is None
    assert left_range.end == "m"
    assert right_range.start == "m"
    assert right_range.end is None


def test_range_for_unknown_shard_raises_key_error():
    p = RangePartitioner("shard-0")
    with pytest.raises(KeyError):
        p.range_for_shard("does-not-exist")


def test_split_ids_are_unique_across_multiple_splits():
    p = RangePartitioner("shard-0")
    _, r1 = p.split("shard-0", "m")
    _, r2 = p.split("shard-0", "d")
    assert r1 != r2
