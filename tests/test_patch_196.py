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


def _intraday_reclaim_bars():
    session = datetime(2026, 6, 8, 10, 0, tzinfo=app.NY_TZ)
    closes = []
    for i in range(18):
        closes.append(100.0 + i * 0.025)
    closes.extend([100.18, 100.12, 100.08, 100.14, 100.22, 100.30, 100.38])
    closes.extend([100.44, 100.52, 100.62, 100.75, 100.88])
    bars = []
    prev = closes[0]
    for i, close in enumerate(closes):
        open_px = prev if i else close - 0.02
        high = max(open_px, close) + 0.04
        low = min(open_px, close) - (0.08 if 18 <= i <= 22 else 0.03)
        volume = 1000 if i < len(closes) - 5 else 1800
        dt = session + timedelta(minutes=i)
        bars.append({
            "ts_utc": dt.astimezone(app.timezone.utc),
            "ts_ny": dt,
            "open": round(open_px, 4),
            "high": round(high, 4),
            "low": round(low, 4),
            "close": round(close, 4),
            "volume": volume,
            "vwap": round(close, 4),
        })
        prev = close
    return bars


def test_patch_196_intraday_vwap_reclaim_triggers(monkeypatch):
    bars = _intraday_reclaim_bars()
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_ENABLE", True)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_1M_BARS", 20)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_5M_BARS", 4)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_SCORE_MIN", 55.0)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_DAY_RANGE_PCT", 0.001)
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_MIN_RECENT_VOL_RATIO", 0.75)
    monkeypatch.setattr(app, "now_ny", lambda: datetime(2026, 6, 8, 10, 35, tzinfo=app.NY_TZ))

    sig, diag = app.eval_intraday_vwap_reclaim_signal_with_diag(bars)

    assert sig == "BUY"
    assert diag["triggered"] is True
    assert diag["candidate_path"] == "intraday_vwap_reclaim"
    assert diag["reclaimed_vwap"] is True
    assert diag["score"] >= 55.0


def test_patch_196_intraday_vwap_reclaim_disabled_in_swing(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    out = app._intraday_vwap_reclaim_setup(_intraday_reclaim_bars())
    assert out["enabled"] is False
    assert out["eligible"] is False
    assert out["reason"] == "strategy_mode_not_intraday"


def test_patch_196_no_signal_prioritizes_intraday_path(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "SCANNER_PRIMARY_STRATEGY", "vwap_pullback")
    primary, details = app._derive_no_signal_details({
        "vwap_pullback": {"enabled": True, "eligible": True, "reason": "touch_fail"},
        "intraday_vwap_reclaim": {"enabled": True, "eligible": True, "reason": "vwap_reclaim_missing"},
    })
    assert primary == "vwap_reclaim_missing"
    assert details["intraday_vwap_reclaim"]["reason"] == "vwap_reclaim_missing"


def test_patch_196_diagnostics_scanner_reports_intraday_candidate_path(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "SCANNER_PRIMARY_STRATEGY", "vwap_pullback")
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_ENABLE", True)
    out = app.diagnostics_scanner()
    path = out["intraday_candidate_path"]
    assert path["strategy_mode"] == "intraday"
    assert path["scanner_primary_strategy"] == "vwap_pullback"
    assert path["intraday_vwap_reclaim_enabled"] is True