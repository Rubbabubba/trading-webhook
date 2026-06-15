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


def _shadow_bars(start_price=100.0):
    session = datetime.combine(app.now_ny().date(), datetime(2026, 6, 11, 10, 0).time(), tzinfo=app.NY_TZ)
    bars = []
    for i in range(34):
        base = start_price + i * 0.05
        low = base - 0.04
        if i in {25, 26}:
            low = base - 0.35
        dt = session + timedelta(minutes=i)
        bars.append({
            "ts_utc": dt.astimezone(app.timezone.utc),
            "ts_ny": dt,
            "open": round(base - 0.02, 4),
            "high": round(base + 0.08, 4),
            "low": round(low, 4),
            "close": round(base + 0.04, 4),
            "volume": 1000 if i < 28 else 1800,
        })
    return bars


def test_patch_201_shadow_scan_appends_proof_ledger(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [])
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER_STATE_PATH", str(tmp_path / "hybrid.json"))
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "INTRADAY_SHADOW_EVALUATION_ENABLE", True)
    monkeypatch.setattr(app, "INTRADAY_SHADOW_FORCE_ENABLED_PATHS", True)
    monkeypatch.setattr(app, "INTRADAY_SHADOW_MAX_SYMBOLS", 2)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_DAY_RANGE_PCT", 0.001)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_RANK_SCORE", 0.0)
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", True)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)
    monkeypatch.setattr(app, "fetch_1m_bars_multi_guarded", lambda syms, lookback_days=1: {s: _shadow_bars(100 + idx) for idx, s in enumerate(syms)})

    out = app._run_intraday_shadow_scan(["SPY", "QQQ"])

    assert out["live_submission"] is False
    assert out["proof_ledger_update"]["appended"] >= 1
    assert out["proof_metrics"]["ledger_count"] == len(app.HYBRID_PROOF_LEDGER)
    assert app.HYBRID_PROOF_LEDGER[0]["source"] == "intraday_shadow"
    assert Path(app.HYBRID_PROOF_LEDGER_STATE_PATH).exists()


def test_patch_201_hybrid_proof_metrics_gate_on_sample_and_sleeves(monkeypatch):
    monkeypatch.setattr(app, "SWING_SLEEVE_MAX_OPEN_POSITIONS", 7)
    monkeypatch.setattr(app, "INTRADAY_SLEEVE_MAX_OPEN_POSITIONS", 7)
    monkeypatch.setattr(app, "HYBRID_PROOF_MIN_SHADOW_TRADES", 2)
    monkeypatch.setattr(app, "HYBRID_PROOF_MIN_AVG_R", 0.1)
    monkeypatch.setattr(app, "HYBRID_PROOF_REQUIRE_EOD_FLAT_SESSIONS", 1)
    monkeypatch.setattr(app, "LAST_EOD_FLATTEN_STATUS", {"fully_flat": True, "residual_count": 0})
    monkeypatch.setattr(app, "count_open_positions_allowed", lambda: 1)
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [
        {"status": "open_shadow", "latest_return_pct": 0.01, "latest_r": 0.4},
        {"status": "open_shadow", "latest_return_pct": 0.02, "latest_r": 0.2},
    ])

    out = app._hybrid_proof_metrics()

    assert out["ledger_count"] == 2
    assert out["positive_rate"] == 1.0
    assert out["avg_r"] == 0.3
    assert out["proof_ready_for_paper"] is True


def test_patch_201_diagnostics_and_dashboard_show_hybrid_proof(monkeypatch):
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [{"symbol": "SPY", "signal": "INTRADAY_VWAP_RECLAIM", "latest_r": 0.25, "latest_return_pct": 0.006}])
    monkeypatch.setattr(app, "LAST_EOD_FLATTEN_STATUS", {"fully_flat": True, "residual_count": 0})
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/hybrid_proof"})

    payload = app.diagnostics_hybrid_proof(req)
    body = app.dashboard(req).body.decode("utf-8")

    assert payload["ok"] is True
    assert payload["proof_metrics"]["ledger_count"] == 1
    assert "Hybrid Proof Ledger" in body
    assert "Hybrid Sleeve Readiness" in body