import pytest

from continuum.storage.mvcc import MVCCStore


def test_read_missing_key_returns_none():
    store = MVCCStore()
    assert store.read("x", read_ts=100) is None
    assert store.read_latest("x") is None


def test_write_then_read_at_exact_timestamp():
    store = MVCCStore()
    store.write("x", 10, "v1")
    assert store.read("x", 10) == "v1"


def test_read_at_timestamp_before_first_write_returns_none():
    store = MVCCStore()
    store.write("x", 10, "v1")
    assert store.read("x", 5) is None


def test_read_at_timestamp_after_write_returns_that_version():
    store = MVCCStore()
    store.write("x", 10, "v1")
    assert store.read("x", 999) == "v1"


def test_read_returns_newest_version_not_exceeding_read_ts():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, "v2")
    store.write("x", 30, "v3")
    assert store.read("x", 15) == "v1"
    assert store.read("x", 25) == "v2"
    assert store.read("x", 35) == "v3"
    assert store.read("x", 20) == "v2"  # exact match on a middle version


def test_read_latest_returns_newest_regardless_of_timestamp():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, "v2")
    assert store.read_latest("x") == "v2"


def test_write_requires_strictly_increasing_timestamp():
    store = MVCCStore()
    store.write("x", 10, "v1")
    with pytest.raises(ValueError):
        store.write("x", 10, "v2")
    with pytest.raises(ValueError):
        store.write("x", 5, "v2")


def test_different_keys_have_independent_timestamp_sequences():
    store = MVCCStore()
    store.write("x", 10, "vx")
    store.write("y", 3, "vy")  # fine -- independent key, no ordering conflict with x
    assert store.read_latest("x") == "vx"
    assert store.read_latest("y") == "vy"


# -- tombstones (deletes) -------------------------------------------------


def test_tombstone_makes_key_absent_at_and_after_its_timestamp():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, None)  # delete
    assert store.read("x", 20) is None
    assert store.read("x", 999) is None
    assert store.read_latest("x") is None


def test_read_before_tombstone_still_sees_old_value():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, None)
    assert store.read("x", 15) == "v1"


def test_write_after_tombstone_resurrects_key():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, None)
    store.write("x", 30, "v2")
    assert store.read("x", 25) is None
    assert store.read("x", 30) == "v2"


# -- versions / all_keys ---------------------------------------------------


def test_versions_returns_all_versions_oldest_first():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, "v2")
    assert store.versions("x") == [(10, "v1"), (20, "v2")]


def test_versions_of_missing_key_is_empty():
    store = MVCCStore()
    assert store.versions("ghost") == []


def test_all_keys_lists_every_written_key():
    store = MVCCStore()
    store.write("a", 1, "1")
    store.write("b", 1, "1")
    assert set(store.all_keys()) == {"a", "b"}


# -- garbage collection -------------------------------------------------


def test_gc_discards_versions_strictly_older_than_the_kept_version():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, "v2")
    store.write("x", 30, "v3")
    store.gc(safe_point=25)
    assert store.versions("x") == [(20, "v2"), (30, "v3")]


def test_gc_keeps_reads_at_or_above_safe_point_correct():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, "v2")
    store.write("x", 30, "v3")
    store.gc(safe_point=25)
    assert store.read("x", 25) == "v2"
    assert store.read("x", 30) == "v3"


def test_gc_with_no_versions_below_safe_point_is_a_noop():
    store = MVCCStore()
    store.write("x", 30, "v1")
    store.gc(safe_point=10)
    assert store.versions("x") == [(30, "v1")]


def test_gc_keeps_single_version_when_only_one_exists_below_safe_point():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.gc(safe_point=100)
    assert store.versions("x") == [(10, "v1")]


def test_gc_across_multiple_keys():
    store = MVCCStore()
    store.write("x", 10, "v1")
    store.write("x", 20, "v2")
    store.write("y", 5, "w1")
    store.write("y", 15, "w2")
    store.gc(safe_point=12)
    assert store.versions("x") == [(10, "v1"), (20, "v2")]  # nothing below 12 except the kept one
    assert store.versions("y") == [(5, "w1"), (15, "w2")]
