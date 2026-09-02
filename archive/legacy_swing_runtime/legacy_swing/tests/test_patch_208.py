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
    for i in range(80):
        rows.append({"symbol": "GOOD", "strategy_name": "daily_breakout", "entry_type": "standard", "exit_reason": "target", "gross_pnl": 25, "pnl_r": 0.8, "holding_days": 2, "entry_regime_mode": "trend", "rank_score": 101})
    rows.append({"symbol": "BAD", "strategy_name": "daily_mean_reversion", "entry_type": "standard", "exit_reason": "stop_loss", "gross_pnl": -75, "pnl_r": -2.7, "holding_days": 1, "entry_regime_mode": "weak", "rank_score": 75})
    return rows


def test_worst_trade_contributors_and_risk_blocker_explanation():
    payload = app._swing_performance_attribution(perf_state={"closed_trades": _rows(), "by_strategy": {}, "kill_switch": {}})
    worst = payload["worst_trade_contributors"][0]
    assert worst["symbol"] == "BAD"
    assert worst["exit_reason"] == "stop_loss"
    explanation = payload["risk_blocker_explanation"]
    assert explanation["worst_r_too_deep"] is True
    assert explanation["worst_r_symbol"] == "BAD"
    assert explanation["worst_r_strategy"] == "daily_mean_reversion"
    assert explanation["risk_scale_next_required_fix"] == "inspect_worst_r_trade_and_tighten_bucket_or_quarantine"
    assert "worst_r_too_deep" in payload["risk_scaling"]["blockers"]


def test_quarantine_decision_read_only_by_default(monkeypatch):
    monkeypatch.setattr(app, "SWING_QUARANTINE_SYMBOLS", {"BAD"})
    monkeypatch.setattr(app, "SWING_QUARANTINE_ENFORCE", False)
    decision = app._swing_quarantine_decision("BAD", "daily_breakout", "standard")
    assert decision["matched"] is True
    assert decision["blocked"] is False
    assert decision["enforced"] is False


def test_quarantine_decision_blocks_when_enforced(monkeypatch):
    monkeypatch.setattr(app, "SWING_QUARANTINE_SYMBOLS", {"BAD"})
    monkeypatch.setattr(app, "SWING_QUARANTINE_STRATEGIES", {"daily_mean_reversion"})
    monkeypatch.setattr(app, "SWING_QUARANTINE_ENTRY_TYPES", {"early_override"})
    monkeypatch.setattr(app, "SWING_QUARANTINE_ENFORCE", True)
    decision = app._swing_quarantine_decision("BAD", "daily_mean_reversion", "early_override")
    assert decision["blocked"] is True
    assert set(decision["reasons"]) == {"symbol_quarantined", "strategy_quarantined", "entry_type_quarantined"}


def test_dashboard_shows_patch_208_drilldown_rows():
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")
    assert "Worst Trade Contributors" in body
    assert "risk_scale_next_required_fix" in body
    assert "SWING_QUARANTINE_SYMBOLS" in body