import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _rows():
    rows = []
    for _ in range(80):
        rows.append({"symbol": "GOOD", "strategy_name": "daily_breakout", "entry_type": "standard", "exit_reason": "target", "gross_pnl": 30.0, "pnl_r": 0.9, "holding_days": 1.0, "entry_regime_mode": "trend", "rank_score": 105})
    for _ in range(3):
        rows.append({"symbol": "STAL", "strategy_name": "daily_breakout", "entry_type": "standard", "exit_reason": "stall_exit", "gross_pnl": -75.0, "pnl_r": -2.4, "holding_days": 4.0, "entry_regime_mode": "weak", "rank_score": 72})
    rows.append({"symbol": "STAL", "strategy_name": "daily_breakout", "entry_type": "standard", "exit_reason": "stall_exit", "gross_pnl": 10.0, "pnl_r": 0.2, "holding_days": 2.0, "entry_regime_mode": "trend", "rank_score": 91})
    return rows


def test_patch_211_stall_exit_drilldown_summarizes_and_recommends():
    payload = app._swing_stall_exit_drilldown(perf_state={"closed_trades": _rows(), "by_strategy": {}, "kill_switch": {}})

    assert payload["ok"] is True
    assert payload["mode"] == "read_only_drilldown"
    assert payload["stall_exit_summary"]["closed_trades"] == 4
    assert payload["stall_exit_summary"]["gross_pnl"] == -215.0
    assert payload["by_symbol"][0]["name"] == "STAL"
    assert payload["recommended_exit_tuning"]["name"] in {"stall_loss_guard_for_guard_leaks", "raise_stall_min_r_filter_negative_stalls", "earlier_break_even_for_deep_stalls", "reduce_stall_exit_days_for_slow_stalls", "avoid_weak_regime_stall_setups"}
    assert payload["recommended_exit_tuning"]["gross_pnl_delta"] > 0
    assert payload["config"]["read_only"] is True


def test_patch_211_diagnostics_endpoint(monkeypatch):
    monkeypatch.setattr(app, "_recompute_strategy_performance_state", lambda: {"closed_trades": _rows(), "by_strategy": {}, "kill_switch": {}})
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/swing_stall_exit_drilldown"})

    payload = app.diagnostics_swing_stall_exit_drilldown(req)

    assert payload["patch_version"].startswith("patch-")
    assert payload["stall_exit_summary"]["closed_trades"] == 4
    assert payload["exit_tightening_simulations"]


def test_patch_211_dashboard_renders_stall_exit_panel():
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")

    assert "Stall Exit Drilldown" in body
    assert "Exit Tightening Simulations" in body
    assert "/diagnostics/swing_stall_exit_drilldown" in body