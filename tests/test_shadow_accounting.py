from copy import deepcopy

from regime_intraday_ledger import empty_ledger, update_ledger, performance_views


def opened(side="buy"):
    signal = {"symbol": "SPY", "underlying_side": side, "entry_price": 100,
              "stop_price": 99 if side == "buy" else 101, "target_price": 102 if side == "buy" else 98}
    return update_ledger(empty_ledger(), {"signals": [signal], "features": {"SPY": {"price": 100, "last_ts": "2026-09-03T10:00:00-04:00"}}})


def close(ledger, **feature):
    return update_ledger(ledger, {"signals": [], "features": {"SPY": {"last_ts": "2026-09-03T10:01:00-04:00", **feature}}})


def test_both_touched_stop_first_and_closed_status():
    result = close(opened(), price=102.5, last_open=100, last_high=103, last_low=98)
    row = result["closed"][0]
    assert row["realized_r"] == -1
    assert row["status"] == "shadow_closed"
    assert row["exit_reason"] == "stop"


def test_gap_stop_not_capped_at_one_r():
    row = close(opened(), price=98, last_open=97, last_high=98, last_low=96)["closed"][0]
    assert row["realized_r"] == -3


def test_short_target_capped():
    row = close(opened("sell"), price=97, last_open=100, last_high=100, last_low=97)["closed"][0]
    assert row["realized_r"] == 2


def test_same_bar_not_reused_for_exit():
    result = close(opened(), price=98, last_ts="2026-09-03T10:00:00-04:00")
    assert not result["closed"]


def test_legacy_preserved_and_missing_broker_fills_not_estimated():
    ledger = empty_ledger()
    ledger["closed"] = [{"status": "shadow_open", "realized_r": 3.0192}]
    ledger["orders"] = {"a": {"status": "filled_closed", "actual_realized_dollars": 999}}
    original = deepcopy(ledger["closed"])
    views = performance_views(ledger)
    assert views["shadow"]["legacy_closed_count"] == 1
    assert views["shadow"]["total_r"] == 0
    assert views["broker_paper"]["missing_fill_roundtrips"] == 1
    assert views["broker_paper"]["gross_realized_dollars_from_fills"] == 0
    assert ledger["closed"] == original
