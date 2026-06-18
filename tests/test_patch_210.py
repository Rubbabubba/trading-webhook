import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _sim_rows():
    rows = []
    for i in range(80):
        rows.append({
            "symbol": "GOOD",
            "strategy_name": "daily_breakout",
            "entry_type": "standard",
            "exit_reason": "target",
            "gross_pnl": 30.0,
            "pnl_r": 0.9,
            "holding_days": 1.2,
            "entry_regime_mode": "trend",
            "rank_score": 105,
        })
    for i in range(4):
        rows.append({
            "symbol": "BAD",
            "strategy_name": "daily_breakout",
            "entry_type": "standard",
            "exit_reason": "stall_exit",
            "gross_pnl": -75.0,
            "pnl_r": -2.5,
            "holding_days": 2.5,
            "entry_regime_mode": "weak",
            "rank_score": 72,
        })
    return rows


def test_patch_210_simulator_identifies_best_filter_candidate():
    payload = app._swing_profit_acceleration_simulator(perf_state={"closed_trades": _sim_rows(), "by_strategy": {}, "kill_switch": {}})
    best = payload["best_tuning_candidate"]

    assert payload["ok"] is True
    assert payload["mode"] == "read_only_simulation"
    assert payload["baseline"]["risk_scaling"]["ready_for_40_risk"] is False
    assert best["category"] == "symbols"
    assert best["name"] == "BAD"
    assert best["trades_removed"] == 4
    assert best["gross_pnl_delta"] == 300.0
    assert best["ready_for_40_risk_after"] is True
    assert best["recommended_next_step"] == "consider_quarantine_or_tighten_filter"


def test_patch_210_diagnostics_endpoint(monkeypatch):
    monkeypatch.setattr(app, "_recompute_strategy_performance_state", lambda: {"closed_trades": _sim_rows(), "by_strategy": {}, "kill_switch": {}})
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/swing_tuning_simulator"})

    payload = app.diagnostics_swing_tuning_simulator(req)

    assert payload["ok"] is True
    assert payload["patch_version"].startswith("patch-210")
    assert payload["best_tuning_candidate"]["name"] == "BAD"
    assert payload["config"]["read_only"] is True


def test_patch_210_dashboard_renders_simulator_panel():
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")

    assert "Swing Tuning Simulator" in body
    assert "/diagnostics/swing_tuning_simulator" in body
    assert "best_gross_pnl_delta" in body