from datetime import datetime, timezone
import pytest
from regime_intraday_entry_guard import pending_entry_invalidation
from regime_intraday_ledger import empty_ledger, paper_submission_decision
from regime_intraday_readiness import readiness_snapshot


def test_shadow_losses_do_not_trip_broker_lock():
    ledger = empty_ledger()
    ledger["closed"] = [{"session": "2026-09-03", "realized_r": -3}, {"session": "2026-09-03", "realized_r": -2}]
    assert paper_submission_decision(ledger, "new", session="2026-09-03")["allowed"]


def test_missing_broker_pnl_blocks():
    ledger = empty_ledger()
    ledger["closed"] = [{"paper_signal_id": "x", "session": "2026-09-03"}]
    assert paper_submission_decision(ledger, "new", session="2026-09-03")["reason"] == "broker_realized_pnl_missing"


def test_readiness_includes_daily_attempt_limit():
    from zoneinfo import ZoneInfo
    now = datetime.now(timezone.utc)
    session = now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    ledger = empty_ledger()
    ledger["orders"] = {key: {"session": session, "status": "canceled"} for key in ("a", "b")}
    result = readiness_snapshot(config={"paper_submit_enabled": True, "max_trades_per_day": 2}, ledger=ledger, last_scan={"ts_utc": now.isoformat()}, paper_credentials_present=True)
    assert not result["paper_ready"]
    assert "daily_trade_limit" in result["paper_blockers"]


def test_zero_attempt_limit_is_unlimited_but_active_order_still_blocks():
    ledger = empty_ledger()
    ledger["orders"] = {str(i): {"session": "2026-09-03", "status": "canceled"} for i in range(20)}
    assert paper_submission_decision(ledger, "new", session="2026-09-03", max_trades_per_day=0)["allowed"]
    ledger["orders"]["active"] = {"session": "2026-09-03", "status": "new"}
    assert paper_submission_decision(ledger, "new", session="2026-09-03", max_trades_per_day=0)["reason"] == "active_paper_order_or_position"


@pytest.mark.parametrize("side,low,high,expected", [
    ("buy", 98, 101, "underlying_stop_breached"),
    ("buy", 100, 103, "underlying_target_already_reached"),
    ("sell", 99, 102, "underlying_stop_breached"),
    ("sell", 97, 100, "underlying_target_already_reached"),
])
def test_pending_invalidation(side, low, high, expected):
    signal = {"underlying_side": side, "stop_price": 99 if side == "buy" else 101, "target_price": 102 if side == "buy" else 98}
    feature = {"ready": True, "last_ts": "2026-09-03T14:01:00+00:00", "last_low": low, "last_high": high}
    now = datetime(2026, 9, 3, 14, 2, 20, tzinfo=timezone.utc)
    assert pending_entry_invalidation(signal, feature, "2026-09-03T14:00:30+00:00", now) == expected
    assert pending_entry_invalidation(signal, feature, "2026-09-03T14:01:30+00:00", now) is None
    assert pending_entry_invalidation(signal, feature, "2026-09-03T14:00:30+00:00", now.replace(minute=10)) is None
