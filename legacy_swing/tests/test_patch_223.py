import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_223_dashboard_days_fallback_uses_opened_at(monkeypatch, tmp_path):
    opened = (app.now_ny() - timedelta(days=3)).isoformat()
    snap_path = tmp_path / "positions.json"
    snap_path.write_text(app.json.dumps({
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "positions": [{"symbol": "AMD", "qty": 1, "avg_entry_price": 100, "current_price": 103, "unrealized_pl": 3}],
        "active_plans": {"AMD": {"active": True, "symbol": "AMD", "opened_at": opened, "signal": "daily_breakout", "order_status": "filled"}},
    }))
    for name in ["SCAN_STATE_PATH", "REGIME_STATE_PATH", "SCANNER_TELEMETRY_STATE_PATH", "PAPER_LIFECYCLE_STATE_PATH", "STRATEGY_PERFORMANCE_STATE_PATH"]:
        p = tmp_path / f"{name}.json"
        p.write_text("{}")
        monkeypatch.setattr(app, name, str(p), raising=False)
    monkeypatch.setattr(app, "POSITION_SNAPSHOT_PATH", str(snap_path))
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)

    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")

    assert "patch-223-dashboard-trust-reconciliation" in body
    assert "<td>3</td>" in body


def test_patch_223_recovered_reconcile_uses_broker_entry_fill_for_days(monkeypatch):
    opened = (app.now_ny() - timedelta(days=2)).isoformat()
    monkeypatch.setattr(app, "CANDIDATE_HISTORY", [{"selected": ["AMD"], "candidates": [{"symbol": "AMD", "strategy": "daily_breakout"}]}])
    monkeypatch.setattr(app, "SCAN_HISTORY", [])
    monkeypatch.setattr(app, "DECISIONS", [])
    monkeypatch.setattr(app, "PAPER_LIFECYCLE_HISTORY", [])
    monkeypatch.setattr(app, "LAST_PAPER_LIFECYCLE", {})
    monkeypatch.setattr(app, "_read_journal", lambda *args, **kwargs: [])
    monkeypatch.setattr(app, "list_open_positions_details_allowed", lambda: [{"symbol": "AMD", "qty": "2", "avg_entry_price": "500"}])
    monkeypatch.setattr(app, "list_open_orders_safe", lambda *args, **kwargs: [])
    monkeypatch.setattr(app, "_find_recent_entry_fill_for_symbol", lambda *args, **kwargs: {"filled_at": opened, "id": "entry-1"})
    monkeypatch.setattr(app, "persist_positions_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(app, "_ensure_exit_arm_for_symbol", lambda *args, **kwargs: None)
    monkeypatch.setattr(app, "record_decision", lambda *args, **kwargs: None)
    app.TRADE_PLAN.clear()

    app.reconcile_trade_plans_from_alpaca()
    plan = app.TRADE_PLAN["AMD"]

    assert plan["opened_at"] == opened
    assert plan["recovered_entry_fill_source"] == "broker_order_history"
    assert plan["days_held"] >= 2


def test_patch_223_broker_realized_sync_appends_missing_closed_trade(monkeypatch):
    monkeypatch.setattr(app, "persist_strategy_performance_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(app, "STRATEGY_PERFORMANCE_STATE", {"closed_trades": [], "by_strategy": {}, "kill_switch": {}})
    monkeypatch.setattr(app, "_infer_recovered_plan_attribution", lambda *args, **kwargs: {"strategy_name": "daily_breakout", "signal": "daily_breakout", "attribution_source": "unit"})
    broker = {"ok": True, "rows": [{
        "calc_ok": True,
        "symbol": "AMD",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "qty": 1,
        "entry_price": 100,
        "exit_price": 104,
        "gross_pnl": 4,
        "entry_order_id": "entry-1",
        "exit_order_id": "exit-1",
    }]}

    out = app._sync_broker_realized_rows_to_strategy_performance(broker)
    state = app.STRATEGY_PERFORMANCE_STATE

    assert out["synced"] == 1
    assert state["closed_trades"][0]["source"] == "alpaca_orders_reconciled"
    assert state["closed_trades"][0]["exit_order_id"] == "exit-1"
    assert state["by_strategy"]["daily_breakout"]["closed_trades"] == 1

    second = app._sync_broker_realized_rows_to_strategy_performance(broker)
    assert second["synced"] == 0
    assert len(app.STRATEGY_PERFORMANCE_STATE["closed_trades"]) == 1