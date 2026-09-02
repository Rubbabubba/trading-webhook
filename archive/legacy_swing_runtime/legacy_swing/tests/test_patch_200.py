import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _bars_for_shadow():
    session = datetime(2026, 6, 10, 10, 0, tzinfo=app.NY_TZ)
    bars = []
    for i in range(32):
        base = 100.0 + i * 0.045
        open_px = base - 0.02
        close = base + 0.03
        high = close + 0.05
        low = open_px - 0.03
        if i in {24, 25, 26}:
            low = close - 0.35
        bars.append({
            "ts_utc": (session + timedelta(minutes=i)).astimezone(app.timezone.utc),
            "ts_ny": session + timedelta(minutes=i),
            "open": round(open_px, 4),
            "high": round(high, 4),
            "low": round(low, 4),
            "close": round(close, 4),
            "volume": 1000 if i < 26 else 1600,
        })
    return bars


def test_patch_200_new_entries_gate_blocks_entries_but_not_exits(monkeypatch):
    monkeypatch.setattr(app, "NEW_ENTRIES_ENABLED", False)
    monkeypatch.setattr(app, "DRY_RUN", False)
    monkeypatch.setattr(app, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(app, "SCANNER_ALLOW_LIVE", True)
    monkeypatch.setattr(app, "RELEASE_GATE_ENFORCED", False)
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", False)
    monkeypatch.setattr(app, "KILL_SWITCH", False)
    monkeypatch.setattr(app, "daily_halt_active", lambda: False)
    monkeypatch.setattr(app, "daily_stop_hit", lambda: False)

    assert app.is_live_trading_permitted("worker_scan") is False
    assert app.effective_entry_dry_run("worker_scan") is True
    assert app.is_live_exit_permitted("worker_exit") is True

    out = app.execute_entry_signal("SPY", "buy", "daily_breakout", source="worker_scan", meta={})

    assert out["ignored"] is True
    assert out["reason"] == "new_entries_disabled"


def test_patch_200_intraday_shadow_runs_while_strategy_mode_is_swing(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "INTRADAY_SHADOW_EVALUATION_ENABLE", True)
    monkeypatch.setattr(app, "INTRADAY_SHADOW_FORCE_ENABLED_PATHS", True)
    monkeypatch.setattr(app, "INTRADAY_SHADOW_MAX_SYMBOLS", 3)
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", True)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)
    monkeypatch.setattr(app, "SCANNER_LOOKBACK_DAYS", 1)
    monkeypatch.setattr(app, "fetch_1m_bars_multi_guarded", lambda syms, lookback_days=1: {s: _bars_for_shadow() for s in syms})

    out = app._run_intraday_shadow_scan(["SPY", "QQQ", "IWM", "AAPL"])

    assert out["enabled"] is True
    assert out["status"] == "completed"
    assert out["strategy_mode"] == "swing"
    assert out["live_submission"] is False
    assert out["symbols_scanned"] == 3
    assert "hybrid_capacity_plan" in out


def test_patch_200_intraday_force_enable_does_not_require_intraday_mode(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_ENABLE", False)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_ENABLE", False)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_DAY_RANGE_PCT", 0.001)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_MIN_DAY_RANGE_PCT", 0.001)

    reclaim_sig, reclaim_diag = app.eval_intraday_vwap_reclaim_signal_with_diag(_bars_for_shadow(), force_enable=True)
    continuation_sig, continuation_diag = app.eval_intraday_vwap_continuation_signal_with_diag(_bars_for_shadow(), force_enable=True)

    assert reclaim_diag["enabled"] is True
    assert continuation_diag["enabled"] is True
    assert reclaim_sig in {"BUY", None}
    assert continuation_sig in {"BUY", None}


def test_patch_200_intraday_shadow_endpoint_reports_hybrid_plan(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "NEW_ENTRIES_ENABLED", False)
    monkeypatch.setattr(app, "SWING_SLEEVE_MAX_OPEN_POSITIONS", 7)
    monkeypatch.setattr(app, "INTRADAY_SLEEVE_MAX_OPEN_POSITIONS", 7)
    monkeypatch.setattr(app, "count_open_positions_allowed", lambda: 2)
    monkeypatch.setattr(app, "RELEASE_GATE_ENFORCED", False)

    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/intraday_shadow"})
    out = app.diagnostics_intraday_shadow(req)

    assert out["ok"] is True
    assert out["patch_version"].startswith("patch-200")
    assert out["entry_controls"]["new_entries_enabled"] is False
    assert out["hybrid_capacity_plan"]["combined_target_open_positions"] == 14
    assert out["proof_plan"]["live_intraday_submission"] is False


def test_patch_200_dashboard_renders_shadow_and_entry_controls():
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/"})

    body = app.dashboard(req).body.decode("utf-8")

    assert "Swing Rollback / Entry Controls" in body
    assert "Intraday Shadow Proof" in body