import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_219_daily_halt_rolls_over_to_new_session(monkeypatch):
    monkeypatch.setattr(app, "now_ny", lambda: datetime(2026, 6, 24, 8, 55, tzinfo=app.NY_TZ))
    app.DAILY_HALT_STATE.clear()
    app.DAILY_HALT_STATE.update({"session": "2026-06-23", "active": True, "triggered_at": "2026-06-23T10:00:00-04:00", "reason": "daily_stop_hit"})

    assert app.daily_halt_active() is False
    assert app.DAILY_HALT_STATE["session"] == "2026-06-24"
    assert app.DAILY_HALT_STATE["reason"] == ""


def test_patch_219_daily_stop_does_not_rearm_outside_regular_market_hours(monkeypatch):
    monkeypatch.setattr(app, "now_ny", lambda: datetime(2026, 6, 24, 8, 55, tzinfo=app.NY_TZ))
    monkeypatch.setattr(app, "in_market_hours", lambda: False)
    monkeypatch.setattr(app, "_is_regular_market_day", lambda now=None: True)
    monkeypatch.setattr(app, "_configured_daily_stop_dollars_safe", lambda: 150.0)
    monkeypatch.setattr(app, "daily_pnl", lambda: -999.0)

    context = app._daily_stop_evaluation_context()

    assert context["allowed"] is False
    assert "outside_regular_market_hours" in context["reasons"]
    assert app.daily_stop_hit() is False


def test_patch_219_worker_exit_outside_market_persists_daily_stop_context(monkeypatch):
    monkeypatch.setattr(app, "cleanup_caches", lambda: None)
    heartbeats = []
    monkeypatch.setattr(app, "update_exit_heartbeat", lambda **kwargs: heartbeats.append(kwargs))
    monkeypatch.setattr(app, "reconcile_trade_plans_from_alpaca", lambda: [])
    monkeypatch.setattr(app, "_deactivate_plans_without_broker_positions", lambda source="", reason="": [])
    monkeypatch.setattr(app, "daily_stop_hit", lambda: False)
    monkeypatch.setattr(app, "_eod_flatten_due", lambda now=None: False)
    monkeypatch.setattr(app, "_eod_flatten_status_snapshot", lambda live=False: {"fully_flat": True, "residual_count": 0, "residual_symbols": []})
    monkeypatch.setattr(app, "in_market_hours", lambda: False)
    monkeypatch.setattr(app, "_is_regular_market_day", lambda now=None: True)
    monkeypatch.setattr(app, "KILL_SWITCH", False)
    monkeypatch.setattr(app, "WORKER_SECRET", "")
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", True)
    snapshots = []
    monkeypatch.setattr(app, "persist_positions_snapshot", lambda reason="", extra=None: snapshots.append({"reason": reason, "extra": extra or {}, "ts_utc": "2026-06-24T12:55:00+00:00"}) or snapshots[-1])

    out = app.worker_exit({})

    assert out["reason"] == "outside_market_hours"
    assert snapshots[-1]["extra"]["daily_stop_evaluation"]["allowed"] is False
    assert "outside_regular_market_hours" in snapshots[-1]["extra"]["daily_stop_evaluation"]["reasons"]
    assert heartbeats[-1]["daily_stop_evaluation_allowed"] is False