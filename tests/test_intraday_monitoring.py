from copy import deepcopy
from datetime import datetime, timezone
import pytest
from intraday_monitoring import apply_live_freshness, candidate_views

NOW = datetime(2026, 9, 3, 15, 24, tzinfo=timezone.utc)


def test_queue_preserves_records_and_separates_history():
    ledger = {"pending_candidates": {
        "active": {"status": "awaiting_paper_approval", "expires_at": "2026-09-03T15:25:00Z"},
        "expired": {"status": "awaiting_paper_approval", "expires_at": "2026-09-03T15:24:00Z"},
        "canceled": {"status": "canceled"},
        "bad": {"status": "awaiting_paper_approval", "expires_at": "bad"},
    }}
    original = deepcopy(ledger)
    result = candidate_views(ledger, now=NOW)
    assert list(result["active"]) == ["active"]
    assert result["history"]["expired"]["display_status"] == "expired"
    assert ledger == original
    blocked = candidate_views(ledger, now=NOW, blocker="daily_trade_limit")
    assert not blocked["active"]
    assert blocked["history"]["active"]["display_status"] == "blocked: daily_trade_limit"


@pytest.mark.parametrize("stamp,expected", [
    ("2026-09-03T15:23:00Z", "fresh"),
    ("2026-09-03T15:21:00Z", "fresh"),
    ("2026-09-03T15:20:59Z", "stale_bar"),
    ("2026-09-03T15:24:00Z", "incomplete_or_future_bar"),
    ("invalid", "missing_bar_timestamp"),
])
def test_freshness_blocks_signals(stamp, expected):
    scan = {"features": {"SPY": {"last_ts": stamp, "ready": True}}, "signals": [{"symbol": "SPY"}], "regime": {"name": "range"}}
    apply_live_freshness(scan, now=NOW)
    assert scan["features"]["SPY"]["freshness"] == expected
    assert bool(scan["signals"]) == (expected == "fresh")


def test_stale_dia_does_not_mutate_independent_primary():
    primary = {"features": {s: {"last_ts": "2026-09-03T15:23:00Z", "ready": True} for s in ("SPY", "QQQ")}, "signals": [1]}
    dia = {"features": {"SPY": {"last_ts": "2026-09-03T15:23:00Z", "ready": True}, "DIA": {"last_ts": "2026-09-03T15:20:00Z", "ready": True}}, "signals": [2]}
    apply_live_freshness(primary, now=NOW)
    apply_live_freshness(dia, now=NOW)
    assert primary["signals"] == [1]
    assert not dia["signals"]
