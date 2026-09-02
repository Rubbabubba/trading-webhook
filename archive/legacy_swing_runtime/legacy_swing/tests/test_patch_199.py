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


class _Order:
    def __init__(self, oid):
        self.id = oid


def _ny(hour, minute):
    return datetime(2026, 6, 9, hour, minute, tzinfo=app.NY_TZ)


def test_patch_199_eod_due_runs_after_market_hours_without_submitting_by_default(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "EOD_FLATTEN_TIME", "15:55")
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", True)
    monkeypatch.setattr(app, "EOD_FLATTEN_AFTER_MARKET_SUBMIT_ENABLE", False)
    monkeypatch.setattr(app, "now_ny", lambda: _ny(16, 5))
    monkeypatch.setattr(app, "in_market_hours", lambda: False)
    monkeypatch.setattr(app, "reconcile_trade_plans_from_alpaca", lambda: [])
    monkeypatch.setattr(app, "list_open_positions_allowed", lambda: [{"symbol": "IWM", "qty": 4.21}])
    monkeypatch.setattr(app, "list_open_orders_safe", lambda limit=None: [])
    called = []
    monkeypatch.setattr(app, "close_position", lambda *a, **k: called.append(a) or {"closed": True})
    monkeypatch.setattr(app, "persist_positions_snapshot", lambda *a, **k: {})
    monkeypatch.setattr(app, "WORKER_SECRET", "")

    out = app.worker_exit({})

    assert out["mode"] == "eod_flatten"
    assert out["can_submit_closes"] is False
    assert out["still_open_symbols"] == ["IWM"]
    assert out["recommended_action"] == "manual_close_or_wait_next_regular_session"
    assert called == []


def test_patch_199_eod_flatten_verifies_residuals_and_does_not_deactivate_plan_on_submit(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "EOD_FLATTEN_TIME", "15:55")
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", True)
    monkeypatch.setattr(app, "EOD_FLATTEN_AFTER_MARKET_SUBMIT_ENABLE", False)
    monkeypatch.setattr(app, "now_ny", lambda: _ny(15, 56))
    monkeypatch.setattr(app, "in_market_hours", lambda: True)
    monkeypatch.setattr(app, "reconcile_trade_plans_from_alpaca", lambda: [{"action": "noop"}])
    calls = {"positions": 0}

    def positions():
        calls["positions"] += 1
        return [{"symbol": "UBER", "qty": 17.07}]

    monkeypatch.setattr(app, "list_open_positions_allowed", positions)
    monkeypatch.setattr(app, "list_open_orders_safe", lambda limit=None: [])
    monkeypatch.setattr(app, "close_position", lambda sym, reason="", source="": {"closed": True, "symbol": sym, "order_id": "ord-1"})
    monkeypatch.setattr(app, "persist_positions_snapshot", lambda *a, **k: {})
    monkeypatch.setattr(app, "WORKER_SECRET", "")
    monkeypatch.setattr(app, "TRADE_PLAN", {"UBER": {"active": True}})

    out = app.worker_exit({})

    assert out["submitted_symbols"] == ["UBER"]
    assert out["still_open_symbols"] == ["UBER"]
    assert out["fully_flat"] is False
    assert app.TRADE_PLAN["UBER"]["active"] is True
    assert app.TRADE_PLAN["UBER"]["eod_flatten_requested"] is True


def test_patch_199_eod_status_endpoint_reports_residuals(monkeypatch):
    monkeypatch.setattr(app, "EOD_FLATTEN_TIME", "15:55")
    monkeypatch.setattr(app, "now_ny", lambda: _ny(16, 10))
    monkeypatch.setattr(app, "in_market_hours", lambda: False)
    monkeypatch.setattr(app, "list_open_positions_allowed", lambda: [{"symbol": "PLTR", "qty": 9.09}])
    monkeypatch.setattr(app, "list_open_orders_safe", lambda limit=None: [{"symbol": "PLTR", "side": "sell", "status": "new"}])
    monkeypatch.setattr(app, "TRADE_PLAN", {})

    out = app.diagnostics_eod_flatten_status()

    assert out["ok"] is True
    assert out["patch_version"].startswith("patch-199")
    assert out["residual_symbols"] == ["PLTR"]
    assert out["pending_close_order_symbols"] == ["PLTR"]
    assert out["recommended_action"] == "verify_pending_close_orders"


def test_patch_199_dashboard_renders_eod_flatten_panel(monkeypatch):
    monkeypatch.setattr(app, "LAST_EOD_FLATTEN_STATUS", {"fully_flat": False, "still_open_symbols": ["MU"], "residual_count": 1, "recommended_action": "manual_close_or_wait_next_regular_session"})
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/"})

    body = app.dashboard(req).body.decode("utf-8")

    assert "EOD Flatten Status" in body
    assert "residual_symbols" in body