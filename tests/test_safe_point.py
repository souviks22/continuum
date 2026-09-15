from continuum.txn.safe_point import SafePointCalculator


def test_no_active_transactions_uses_ttl_floor():
    calc = SafePointCalculator(gc_ttl=100)
    assert calc.compute_safe_point(current_ts=1000) == 900


def test_active_transaction_bounds_the_safe_point_below_ttl_floor():
    # ttl_floor = 1000 - 100 = 900; an old-but-still-active transaction
    # (start_ts=50) must pin the safe point far below that.
    calc = SafePointCalculator(gc_ttl=100)
    calc.begin(start_ts=50)
    assert calc.compute_safe_point(current_ts=1000) == 49


def test_ttl_floor_wins_when_it_is_more_conservative_than_active_txns():
    calc = SafePointCalculator(gc_ttl=100)
    calc.begin(start_ts=200)  # oldest_active - 1 = 199, but ttl floor = 900 is more conservative
    assert calc.compute_safe_point(current_ts=1000) == 199


def test_end_removes_a_transaction_from_consideration():
    # A tiny ttl keeps ttl_floor (999) well above the active bound, so
    # this isolates the active-txn logic.
    calc = SafePointCalculator(gc_ttl=1)
    calc.begin(start_ts=100)
    calc.begin(start_ts=500)
    calc.end(start_ts=100)
    assert calc.compute_safe_point(current_ts=1000) == 499


def test_ending_an_unknown_transaction_is_a_noop():
    calc = SafePointCalculator(gc_ttl=1000)
    calc.end(start_ts=42)  # never began -- should not raise
    assert calc.compute_safe_point(current_ts=1000) == 0


def test_multiple_active_transactions_bound_by_the_oldest():
    calc = SafePointCalculator(gc_ttl=500)  # ttl_floor = 500, well above the active bound
    calc.begin(start_ts=300)
    calc.begin(start_ts=100)
    calc.begin(start_ts=200)
    assert calc.compute_safe_point(current_ts=1000) == 99
