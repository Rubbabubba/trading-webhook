import os
from datetime import datetime, timezone

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


class Req:
    def __init__(self, query=None):
        self.headers = {}
        self.query_params = query or {}


def _base_monkeypatch(monkeypatch):
    monkeypatch.setattr(app, "LIVE_DASHBOARD_CACHE", {"ts": 0.0, "payload": None})
    monkeypatch.setattr(app, "read_positions_snapshot", lambda: {"ts_utc": datetime.now(timezone.utc).isoformat(), "positions": [], "active_plans": {}})
    monkeypatch.setattr(app, "today_pnl_truth_snapshot", lambda: {"ok": True, "accounting_source": "unit", "today_realized_pnl": 5.0, "today_unrealized_pnl": 10.0, "today_net_pnl": 15.0, "closed_trades_today": 1})
    monkeypatch.setattr(app, "_recompute_strategy_performance_state", lambda: {"closed_trades": [{"gross_pnl": 5.0, "pnl_r": 0.2}], "by_strategy": {}, "kill_switch": {}})
    monkeypatch.setattr(app, "in_market_hours", lambda: True)


def test_live_positions_endpoint_aligns_broker_plan_and_pnl(monkeypatch):
    _base_monkeypatch(monkeypatch)
    app.TRADE_PLAN.clear()
    app.TRADE_PLAN["AMD"] = {"active": True, "symbol": "AMD", "qty": 2, "signal": "daily_breakout", "strategy_name": "daily_breakout", "stop_price": 95, "take_price": 115, "order_status": "filled"}
    monkeypatch.setattr(app, "list_open_positions_details_allowed", lambda: [{"symbol": "AMD", "qty": "2", "avg_entry_price": "100", "current_price": "105", "market_value": "210", "unrealized_pl": "10", "unrealized_plpc": "0.05"}])
    monkeypatch.setattr(app, "list_open_orders_safe", lambda *args, **kwargs: [])

    out = app.diagnostics_live_positions(Req({"refresh": "1"}))

    assert out["patch_version"] == "patch-225-live-pnl-color"
    assert out["summary"]["broker_positions_count"] == 1
    assert out["summary"]["bad_count"] == 0
    assert out["positions"][0]["symbol"] == "AMD"
    assert out["positions"][0]["trust"] == "ok"
    assert out["today_pnl"]["today_net_pnl"] == 15.0


def test_live_positions_flags_short_and_missing_internal_plan(monkeypatch):
    _base_monkeypatch(monkeypatch)
    app.TRADE_PLAN.clear()
    monkeypatch.setattr(app, "list_open_positions_details_allowed", lambda: [{"symbol": "PANW", "qty": "-1", "avg_entry_price": "320", "current_price": "315", "unrealized_pl": "5", "unrealized_plpc": "0.01"}])
    monkeypatch.setattr(app, "list_open_orders_safe", lambda *args, **kwargs: [])

    out = app._live_operator_snapshot(force=True)
    row = out["positions"][0]

    assert row["broker"]["side"] == "short"
    assert row["trust"] == "bad"
    assert "short_position" in row["warnings"]
    assert "missing_internal_plan" in row["warnings"]
    assert out["summary"]["short_count"] == 1


def test_live_dashboard_renders_compact_operator_view(monkeypatch):
    payload = {
        "ok": True,
        "patch_version": "patch-225-live-pnl-color",
        "generated_utc": "2026-06-30T12:00:00+00:00",
        "cached": False,
        "cache_ttl_sec": 10,
        "summary": {"broker_positions_count": 1, "active_plan_count": 1, "open_order_count": 0, "bad_count": 0, "warning_count": 0, "short_count": 0, "position_truth_status": "aligned", "position_truth_mismatch_count": 0},
        "today_pnl": {"today_net_pnl": 15, "today_realized_pnl": 5, "today_unrealized_pnl": 10, "closed_trades_today": 1, "accounting_source": "unit"},
        "performance": {"closed_trades": 125, "wins": 67, "losses": 49, "gross_pnl": 2000, "avg_r": 0.7, "win_rate": 0.53, "sample_maturity": "ESTABLISHED"},
        "positions": [{"symbol": "AMD", "trust": "ok", "warnings": [], "broker": {"side": "long", "qty": 2, "avg_entry_price": 100, "last_price": 105, "unrealized_pl": 10, "unrealized_plpc": 0.05}, "plan": {"stop_price": 95, "take_price": 115, "signal": "daily_breakout"}, "open_orders": []}],
        "open_orders": [],
    }
    monkeypatch.setattr(app, "_live_operator_snapshot", lambda force=False: payload)

    body = app.dashboard_live(Req({"auto": "0"})).body.decode("utf-8")

    assert "Live Operator Dashboard" in body
    assert "Active Positions Audit" in body
    assert "AMD" in body
    assert "class='good signed-value'" in body
    assert "Research dashboard" in body