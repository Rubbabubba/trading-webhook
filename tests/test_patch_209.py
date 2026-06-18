import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_worker_exit_refreshes_position_snapshot_after_hours(monkeypatch):
    monkeypatch.setattr(app, "WORKER_SECRET", "")
    monkeypatch.setattr(app, "KILL_SWITCH", False)
    monkeypatch.setattr(app, "ONLY_MARKET_HOURS", True)
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "daily_stop_hit", lambda: False)
    monkeypatch.setattr(app, "in_market_hours", lambda: False)
    monkeypatch.setattr(app, "_eod_flatten_due", lambda now: False)
    monkeypatch.setattr(app, "reconcile_trade_plans_from_alpaca", lambda: [{"symbol": "AAPL", "action": "synced"}])
    monkeypatch.setattr(app, "_eod_flatten_status_snapshot", lambda live=True: {"fully_flat": False, "residual_count": 1, "residual_symbols": ["AAPL"]})

    persisted = {}

    def fake_persist(reason="", extra=None):
        persisted["reason"] = reason
        persisted["extra"] = extra or {}
        return {"reason": reason, "ts_utc": "2026-06-17T21:30:00+00:00", "extra": persisted["extra"]}

    heartbeat = {}
    monkeypatch.setattr(app, "persist_positions_snapshot", fake_persist)
    monkeypatch.setattr(app, "update_exit_heartbeat", lambda status="ok", **extra: heartbeat.update({"status": status, **extra}))

    out = app.worker_exit({})

    assert out["ok"] is True
    assert out["skipped"] is True
    assert out["reason"] == "outside_market_hours"
    assert out["snapshot_persisted"] is True
    assert out["snapshot_reason"] == "worker_exit_outside_market_hours"
    assert out["snapshot_ts_utc"] == "2026-06-17T21:30:00+00:00"
    assert out["reconcile"] == [{"symbol": "AAPL", "action": "synced"}]
    assert persisted["reason"] == "worker_exit_outside_market_hours"
    assert persisted["extra"]["worker_exit_status"] == "outside_market_hours"
    assert persisted["extra"]["market_open"] is False
    assert persisted["extra"]["strategy_mode"] == "swing"
    assert persisted["extra"]["reconcile_count"] == 1
    assert persisted["extra"]["eod_flatten"]["residual_symbols"] == ["AAPL"]
    assert heartbeat["status"] == "outside_market_hours"
    assert heartbeat["snapshot_reason"] == "worker_exit_outside_market_hours"
    assert heartbeat["snapshot_ts_utc"] == "2026-06-17T21:30:00+00:00"


def test_dashboard_snapshot_age_view_marks_recent_after_hours_snapshot_fresh():
    generated = datetime(2026, 6, 17, 21, 31, tzinfo=timezone.utc)
    view = app._dashboard_snapshot_age_view("2026-06-17T21:30:00+00:00", generated_utc=generated)

    assert view["snapshot_ts"] == "2026-06-17T21:30:00+00:00"
    assert view["age_sec"] == 60
    assert view["fresh"] is True
    assert view["status"] == "fresh"