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
    return [
        {"symbol": "AAPL", "strategy_name": "daily_breakout", "entry_type": "standard", "exit_reason": "target", "gross_pnl": 50, "pnl_r": 1.5, "holding_days": 2},
        {"symbol": "AAPL", "strategy_name": "daily_breakout", "entry_type": "adaptive", "exit_reason": "target", "gross_pnl": 40, "pnl_r": 1.2, "holding_days": 3},
        {"symbol": "AAPL", "strategy_name": "daily_breakout", "entry_type": "worker_scan_early_override", "exit_reason": "trail", "gross_pnl": 30, "pnl_r": 0.9, "holding_days": 4},
        {"symbol": "AAPL", "strategy_name": "daily_breakout", "entry_type": "standard", "exit_reason": "target", "gross_pnl": 35, "pnl_r": 1.0, "holding_days": 2},
        {"symbol": "AAPL", "strategy_name": "daily_breakout", "entry_type": "standard", "exit_reason": "target", "gross_pnl": 25, "pnl_r": 0.8, "holding_days": 2},
        {"symbol": "MSFT", "strategy_name": "daily_mean_reversion", "entry_type": "standard", "exit_reason": "stop", "gross_pnl": -20, "pnl_r": -0.5, "holding_days": 1},
        {"symbol": "MSFT", "strategy_name": "daily_mean_reversion", "entry_type": "standard", "exit_reason": "stop", "gross_pnl": -10, "pnl_r": -0.2, "holding_days": 1},
        {"symbol": "MSFT", "strategy_name": "daily_mean_reversion", "entry_type": "standard", "exit_reason": "stop", "gross_pnl": -15, "pnl_r": -0.3, "holding_days": 1},
    ]


def test_swing_performance_attribution_buckets_and_recommendations(monkeypatch):
    state = {"closed_trades": _rows(), "by_strategy": {}, "kill_switch": {}}
    payload = app._swing_performance_attribution(perf_state=state)
    assert payload["patch_version"].startswith("patch-")
    assert payload["totals"]["closed_trades"] == 8
    symbols = {row["name"]: row for row in payload["by_symbol"]}
    assert symbols["AAPL"]["gross_pnl"] == 180
    assert symbols["AAPL"]["avg_r"] == 1.08
    assert symbols["MSFT"]["avg_r"] < 0
    assert any(r["name"] == "AAPL" for r in payload["recommendations"]["promote"])
    assert any(r["name"] == "MSFT" for r in payload["recommendations"]["quarantine"])
    entry_names = {row["name"] for row in payload["by_entry_type"]}
    assert {"standard", "adaptive", "early_override"}.issubset(entry_names)


def test_risk_scaling_readiness_ready_for_40_not_50(monkeypatch):
    rows = [{"gross_pnl": 30, "pnl_r": 0.8} for _ in range(80)]
    totals = {"closed_trades": 80, "wins": 50, "win_rate": 0.625, "avg_r": 0.8}
    readiness = app._p207_risk_scaling_readiness(rows, totals)
    assert readiness["ready_for_40_risk"] is True
    assert readiness["ready_for_50_risk"] is False
    assert readiness["recommended_risk_dollars"] == 40


def test_dashboard_renders_swing_profit_acceleration():
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")
    assert "Swing Profit Acceleration" in body
    assert "risk_ready_for_40" in body
    assert "/diagnostics/swing_performance_attribution" in body