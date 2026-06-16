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


def _bars_for_paper():
    session = datetime.combine(app.now_ny().date(), datetime(2026, 6, 11, 10, 0).time(), tzinfo=app.NY_TZ)
    bars = []
    for i in range(36):
        base = 100.0 + i * 0.06
        low = base - (0.35 if i in {25, 26} else 0.04)
        dt = session + timedelta(minutes=i)
        bars.append({
            "ts_utc": dt.astimezone(app.timezone.utc),
            "ts_ny": dt,
            "open": round(base - 0.02, 4),
            "high": round(base + 0.09, 4),
            "low": round(low, 4),
            "close": round(base + 0.05, 4),
            "volume": 1000 if i < 28 else 1900,
        })
    return bars


def test_patch_206_sleeve_aware_eod_ignores_swing_residuals(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "POSITION_SNAPSHOT_PATH", str(tmp_path / "missing.json"))
    monkeypatch.setattr(app, "TRADE_PLAN", {})
    monkeypatch.setattr(app, "HYBRID_PROOF_EOD_GATE_MODE", "intraday_only")
    monkeypatch.setattr(app, "LAST_EOD_FLATTEN_STATUS", {"fully_flat": False, "residual_count": 2, "residual_symbols": ["AMAT", "QQQ"]})

    out = app._hybrid_eod_readiness_view()

    assert out["ok"] is True
    assert out["note"] == "swing_positions_ignored"
    assert out["raw_residual_count"] == 2
    assert out["unresolved_intraday_residual_symbols"] == []


def test_patch_206_sleeve_aware_eod_flags_intraday_residual(monkeypatch, tmp_path):
    path = tmp_path / "positions.json"
    path.write_text('{"positions":[{"symbol":"NVDA","signal":"INTRADAY_VWAP_RECLAIM"}]}')
    monkeypatch.setattr(app, "POSITION_SNAPSHOT_PATH", str(path))
    monkeypatch.setattr(app, "TRADE_PLAN", {})
    monkeypatch.setattr(app, "HYBRID_PROOF_EOD_GATE_MODE", "intraday_only")
    monkeypatch.setattr(app, "LAST_EOD_FLATTEN_STATUS", {"fully_flat": False, "residual_count": 1, "residual_symbols": ["NVDA"]})

    out = app._hybrid_eod_readiness_view()

    assert out["ok"] is False
    assert out["unresolved_intraday_residual_symbols"] == ["NVDA"]


def test_patch_206_proof_ready_for_paper_does_not_require_eod_by_default(monkeypatch):
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [
        {"latest_return_pct": 0.01, "latest_r": 0.4},
        {"latest_return_pct": 0.02, "latest_r": 0.2},
    ])
    monkeypatch.setattr(app, "HYBRID_PROOF_MIN_SHADOW_TRADES", 2)
    monkeypatch.setattr(app, "HYBRID_PROOF_MIN_AVG_R", 0.1)
    monkeypatch.setattr(app, "HYBRID_PROOF_REQUIRE_EOD_FOR_PAPER", False)
    monkeypatch.setattr(app, "HYBRID_PROOF_REQUIRE_EOD_FLAT_SESSIONS", 3)
    monkeypatch.setattr(app, "LAST_EOD_FLATTEN_STATUS", {})

    out = app._hybrid_proof_metrics(capacity_plan={"sleeves_configured": True})

    assert out["sample_ready"] is True
    assert out["avg_r_ready"] is True
    assert out["proof_ready_for_paper"] is True
    assert out["proof_ready_for_live"] is False


def test_patch_206_shadow_scan_builds_paper_pilot_when_enabled(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [
        {"latest_return_pct": 0.01, "latest_r": 0.5},
        {"latest_return_pct": 0.02, "latest_r": 0.3},
    ])
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER_STATE_PATH", str(tmp_path / "hybrid.json"))
    monkeypatch.setattr(app, "HYBRID_MODE", "paper")
    monkeypatch.setattr(app, "INTRADAY_PAPER_ENABLED", True)
    monkeypatch.setattr(app, "INTRADAY_PAPER_MAX_POSITIONS", 1)
    monkeypatch.setattr(app, "INTRADAY_PAPER_REQUIRE_PROOF_READY", False)
    monkeypatch.setattr(app, "INTRADAY_SHADOW_EVALUATION_ENABLE", True)
    monkeypatch.setattr(app, "INTRADAY_SHADOW_FORCE_ENABLED_PATHS", True)
    monkeypatch.setattr(app, "INTRADAY_SHADOW_MAX_SYMBOLS", 2)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_DAY_RANGE_PCT", 0.001)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_RANK_SCORE", 0.0)
    monkeypatch.setattr(app, "SWING_SLEEVE_MAX_OPEN_POSITIONS", 7)
    monkeypatch.setattr(app, "INTRADAY_SLEEVE_MAX_OPEN_POSITIONS", 7)
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", True)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)
    monkeypatch.setattr(app, "fetch_1m_bars_multi_guarded", lambda syms, lookback_days=1: {s: _bars_for_paper() for s in syms})

    out = app._run_intraday_shadow_scan(["SPY", "QQQ"])

    assert out["paper_pilot"]["enabled"] is True
    assert out["paper_pilot"]["eligible"] is True
    assert out["paper_pilot"]["planned_count"] <= 1
    assert out["paper_pilot"]["live_submission"] is False


def test_patch_206_dashboard_and_diagnostics_show_paper_fields(monkeypatch):
    monkeypatch.setattr(app, "HYBRID_MODE", "paper")
    monkeypatch.setattr(app, "INTRADAY_PAPER_ENABLED", True)
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [{"symbol": "SPY", "signal": "INTRADAY_VWAP_RECLAIM", "latest_r": 0.25, "latest_return_pct": 0.006}])

    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/hybrid_proof"})
    payload = app.diagnostics_hybrid_proof(req)
    body = app.dashboard(req).body.decode("utf-8")

    assert payload["hybrid_mode"] == "paper"
    assert payload["paper_pilot_config"]["intraday_paper_enabled"] is True
    assert "Intraday Shadow Proof / Paper Pilot" in body
    assert "paper_pilot_eligible" in body