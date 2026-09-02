import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


class _Clock:
    is_open = True
    timestamp = datetime(2026, 7, 3, 12, 0, tzinfo=app.NY_TZ)
    next_open = datetime(2026, 7, 6, 9, 30, tzinfo=app.NY_TZ)
    next_close = datetime(2026, 7, 6, 16, 0, tzinfo=app.NY_TZ)


def test_patch_231_configured_holiday_overrides_alpaca_open(monkeypatch):
    monkeypatch.setattr(app, "MARKET_CLOSED_DATES_NY", {"2026-07-03"})
    monkeypatch.setattr(app, "_MARKET_CLOCK_CACHE", {"ts": 0.0, "snapshot": {}})
    monkeypatch.setattr(app, "now_ny", lambda: datetime(2026, 7, 3, 12, 0, tzinfo=app.NY_TZ))
    monkeypatch.setattr(app.trading_client, "get_clock", lambda: _Clock())

    out = app._market_clock_snapshot(force=True)

    assert out["source"] == "alpaca_clock"
    assert out["is_open"] is False
    assert out["reason"] == "configured_market_closed_date"
    assert app.in_market_hours() is False


def test_patch_231_clock_error_fails_closed(monkeypatch):
    monkeypatch.setattr(app, "MARKET_CLOCK_FAIL_OPEN", False)
    monkeypatch.setattr(app, "MARKET_CLOSED_DATES_NY", set())
    monkeypatch.setattr(app, "_MARKET_CLOCK_CACHE", {"ts": 0.0, "snapshot": {}})
    monkeypatch.setattr(app, "now_ny", lambda: datetime(2026, 7, 6, 12, 0, tzinfo=app.NY_TZ))
    monkeypatch.setattr(app.trading_client, "get_clock", lambda: (_ for _ in ()).throw(RuntimeError("clock_down")))

    out = app._market_clock_snapshot(force=True)

    assert out["source"] == "alpaca_clock_error"
    assert out["is_open"] is False
    assert out["reason"] == "alpaca_clock_error_fail_closed"


def test_patch_231_pending_order_freeze_blocks_entries(monkeypatch):
    monkeypatch.setattr(app, "PENDING_ORDER_ENTRY_FREEZE_ENABLE", True)
    monkeypatch.setattr(app, "_market_clock_snapshot", lambda force=False: {"is_open": True, "source": "test"})
    monkeypatch.setattr(app, "daily_halt_active", lambda: False)
    monkeypatch.setattr(app, "daily_stop_hit", lambda: False)
    monkeypatch.setattr(app, "_p229_realized_closed_trade_loss_halt", lambda: {"active": False})
    monkeypatch.setattr(app, "list_open_orders_safe", lambda: [{"symbol": "SHOP", "status": "accepted"}])

    out = app.execute_entry_signal("SHOP", "buy", "daily_breakout", source="worker_scan")

    assert out["ignored"] is True
    assert out["reason"] == "pending_order_entry_freeze"


def test_patch_231_market_clock_diagnostic_shape(monkeypatch):
    monkeypatch.setattr(app, "_market_clock_snapshot", lambda force=False: {"is_open": False, "source": "test", "reason": "configured_market_closed_date"})
    monkeypatch.setattr(app, "_pending_order_entry_freeze_snapshot", lambda: {"active": False, "open_order_count": 0})

    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/market_clock"})
    out = app.diagnostics_market_clock(req)

    assert out["ok"] is True
    assert out["market_clock"]["is_open"] is False
    assert "pending_order_entry_freeze" in out