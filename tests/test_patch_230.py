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


def _orb_breakout_bars():
    session = datetime(2026, 7, 2, 9, 30, tzinfo=app.NY_TZ)
    bars = []
    for i in range(32):
        if i < 5:
            close = 100.0 + i * 0.04
        else:
            close = 100.45 + i * 0.07
        dt = session + timedelta(minutes=i)
        bars.append({
            "ts_utc": dt.astimezone(app.timezone.utc),
            "ts_ny": dt,
            "open": round(close - 0.03, 4),
            "high": round(close + 0.08, 4),
            "low": round(close - 0.08, 4),
            "close": round(close, 4),
            "volume": 1000 if i < 24 else 1600,
        })
    return bars


def test_patch_230_orb_vwap_quality_gate_passes_after_opening_range(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "INTRADAY_ORB_VWAP_QUALITY_GATE_ENABLE", True)
    monkeypatch.setattr(app, "INTRADAY_ORB_BARS", 5)
    monkeypatch.setattr(app, "INTRADAY_ORB_MIN_RECENT_VOL_RATIO", 0.8)
    monkeypatch.setattr(app, "INTRADAY_ORB_MIN_DAY_RANGE_PCT", 0.001)
    monkeypatch.setattr(app, "INTRADAY_ORB_MAX_EXTENSION_FROM_VWAP_PCT", 0.05)

    out = app._intraday_orb_vwap_quality_gate(_orb_breakout_bars(), path="test")

    assert out["enabled"] is True
    assert out["ok"] is True
    assert out["price_above_orb"] is True
    assert out["price_above_vwap"] is True


def test_patch_230_orb_vwap_quality_gate_blocks_before_orb_breakout(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "INTRADAY_ORB_VWAP_QUALITY_GATE_ENABLE", True)
    monkeypatch.setattr(app, "INTRADAY_ORB_BARS", 5)

    bars = _orb_breakout_bars()
    bars[-1]["close"] = 99.9
    bars[-1]["high"] = 100.0

    out = app._intraday_orb_vwap_quality_gate(bars, path="test")

    assert out["ok"] is False
    assert out["reason"] in {"orb_breakout_missing", "price_below_vwap"}


def test_patch_230_shadow_update_settles_stop_and_target(monkeypatch):
    monkeypatch.setattr(app, "in_market_hours", lambda: True)

    stop_row = {
        "status": "open_shadow",
        "entry_price": 100.0,
        "stop_price": 98.0,
        "target_price": 104.0,
    }
    target_row = {
        "status": "open_shadow",
        "entry_price": 100.0,
        "stop_price": 98.0,
        "target_price": 104.0,
    }

    app._hybrid_proof_update_entry(stop_row, 97.95, "2026-07-02T14:00:00+00:00")
    app._hybrid_proof_update_entry(target_row, 104.05, "2026-07-02T14:00:00+00:00")

    assert stop_row["status"] == "shadow_stop_hit"
    assert target_row["status"] == "shadow_target_hit"


def test_patch_230_live_preflight_blocks_loss_halt_and_open_shadow(monkeypatch):
    monkeypatch.setattr(app, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "HYBRID_MODE", "live")
    monkeypatch.setattr(app, "INTRADAY_LIVE_ENABLED", True)
    monkeypatch.setattr(app, "SCANNER_ALLOW_LIVE", True)
    monkeypatch.setattr(app, "DRY_RUN", False)
    monkeypatch.setattr(app, "INTRADAY_LIVE_PREFLIGHT_REQUIRE_SHADOW_SETTLED", True)
    monkeypatch.setattr(app, "INTRADAY_LIVE_PREFLIGHT_MAX_OPEN_SHADOW", 0)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)
    monkeypatch.setattr(app, "_p229_realized_closed_trade_loss_halt", lambda: {"active": True})

    out = app._intraday_live_preflight({
        "proof_ready_for_live": True,
        "open_shadow_count": 2,
    })

    assert out["allowed"] is False
    assert "realized_loss_halt_clear" in out["blockers"]
    assert "shadow_exits_settled" in out["blockers"]


def test_patch_230_hybrid_proof_reports_live_preflight(monkeypatch):
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [])
    monkeypatch.setattr(app, "in_market_hours", lambda: False)

    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/hybrid_proof"})
    out = app.diagnostics_hybrid_proof(req)

    assert "intraday_live_preflight" in out
    assert out["intraday_live_preflight"]["allowed"] is False