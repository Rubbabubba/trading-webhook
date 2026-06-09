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


def _continuation_bars():
    session = datetime(2026, 6, 9, 10, 0, tzinfo=app.NY_TZ)
    closes = [100.0 + i * 0.04 for i in range(30)]
    bars = []
    prev = closes[0]
    for i, close in enumerate(closes):
        open_px = prev if i else close - 0.03
        high = max(open_px, close) + 0.05
        low = min(open_px, close) - 0.03
        volume = 1200 if i < len(closes) - 5 else 1700
        dt = session + timedelta(minutes=i)
        bars.append({
            "ts_utc": dt.astimezone(app.timezone.utc),
            "ts_ny": dt,
            "open": round(open_px, 4),
            "high": round(high, 4),
            "low": round(low, 4),
            "close": round(close, 4),
            "volume": volume,
        })
        prev = close
    return bars


def test_patch_198_intraday_vwap_continuation_triggers(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_ENABLE", True)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_SCORE_MIN", 62.0)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_MIN_RECENT_VOL_RATIO", 0.75)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_MIN_DAY_RANGE_PCT", 0.001)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_MAX_EXTENSION_PCT", 0.02)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_REQUIRE_HIGHER_LOW", True)

    sig, diag = app.eval_intraday_vwap_continuation_signal_with_diag(_continuation_bars())

    assert sig == "BUY"
    assert diag["triggered"] is True
    assert diag["candidate_path"] == "intraday_vwap_continuation"
    assert diag["continuation"] is True
    assert diag["score"] >= 62.0


def test_patch_198_no_signal_can_prioritize_continuation(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "SCANNER_PRIMARY_STRATEGY", "vwap_pullback")

    primary, details = app._derive_no_signal_details({
        "intraday_vwap_reclaim": {"enabled": True, "eligible": True, "reason": "score_too_low"},
        "intraday_vwap_continuation": {"enabled": True, "eligible": True, "reason": "price_below_vwap"},
        "vwap_pullback": {"enabled": True, "eligible": True, "reason": "touch_fail"},
    })

    assert primary == "score_too_low"
    assert details["intraday_vwap_continuation"]["reason"] == "price_below_vwap"


def test_patch_198_intraday_rank_floor_is_path_specific(monkeypatch):
    monkeypatch.setattr(app, "SCANNER_RANK_MIN_SCORE", 115.0)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_MIN_RANK_SCORE", 80.0)

    assert app._scanner_min_rank_for_signal("INTRADAY_VWAP_CONTINUATION", "primary") == 80.0
    assert app._scanner_min_rank_for_signal("VWAP_PULLBACK", "primary") == 115.0


def test_patch_198_debug_payload_includes_continuation(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_ENABLE", True)
    monkeypatch.setattr(app, "INTRADAY_VWAP_CONTINUATION_SCORE_MIN", 62.0)
    scan = {
        "summary": {
            "strategy_breakdown": {
                "intraday_vwap_continuation": [{"reason": "price_below_vwap", "count": 3}],
            },
            "top_signals": [],
        },
        "results": [],
    }

    out = app._intraday_signal_debug_from_scan(scan)

    assert out["intraday_vwap_continuation"]["enabled"] is True
    assert out["intraday_vwap_continuation"]["score_min"] == 62.0
    assert out["intraday_vwap_continuation"]["breakdown"][0]["reason"] == "price_below_vwap"