import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _safe_worker_common(monkeypatch):
    monkeypatch.setattr(app, "cleanup_caches", lambda: None)
    monkeypatch.setattr(app, "update_exit_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(app, "activate_daily_halt", lambda reason="": app.DAILY_HALT_STATE.update({"active": True, "reason": reason}))
    monkeypatch.setattr(app, "reconcile_trade_plans_from_alpaca", lambda: [])
    monkeypatch.setattr(app, "_deactivate_plans_without_broker_positions", lambda source="", reason="": [])
    monkeypatch.setattr(app, "persist_positions_snapshot", lambda reason="", extra=None: {"reason": reason, "ts_utc": "2026-06-23T14:00:00+00:00", "extra": extra or {}})
    monkeypatch.setattr(app, "daily_stop_hit", lambda: True)
    monkeypatch.setattr(app, "KILL_SWITCH", False)
    monkeypatch.setattr(app, "WORKER_SECRET", "")


def test_patch_217_daily_stop_halts_without_bulk_flatten_by_default_for_swing(monkeypatch):
    _safe_worker_common(monkeypatch)
    app.DAILY_STOP_CONFIRMATION_STATE.clear()
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "DAILY_STOP_ACTION", "")
    monkeypatch.setattr(app, "ALLOW_DAILY_STOP_BULK_FLATTEN", False)

    calls = {"flatten": 0}
    monkeypatch.setattr(app, "flatten_all", lambda reason: calls.__setitem__("flatten", calls["flatten"] + 1) or [{"reason": reason}])

    out = app.worker_exit({})

    assert out["mode"] == "daily_stop_hit"
    assert out["daily_stop_flatten_decision"]["action"] == "halt_only"
    assert out["daily_stop_flatten_decision"]["can_flatten"] is False
    assert "bulk_flatten_not_explicitly_enabled" in out["daily_stop_flatten_decision"]["reasons"]
    assert out["results"] == []
    assert calls["flatten"] == 0
    assert out["snapshot_reason"] == "daily_stop_halt_only"


def test_patch_217_daily_stop_bulk_flatten_requires_explicit_action_and_confirmation(monkeypatch):
    _safe_worker_common(monkeypatch)
    app.DAILY_STOP_CONFIRMATION_STATE.clear()
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "DAILY_STOP_ACTION", "flatten_all")
    monkeypatch.setattr(app, "ALLOW_DAILY_STOP_BULK_FLATTEN", True)
    monkeypatch.setattr(app, "DAILY_STOP_CONFIRMATION_SEC", 0)
    monkeypatch.setattr(app, "SWING_DAILY_STOP_OPEN_GRACE_MIN", 0)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)

    calls = {"flatten": 0}
    monkeypatch.setattr(app, "flatten_all", lambda reason: calls.__setitem__("flatten", calls["flatten"] + 1) or [{"symbol": "AAPL", "reason": reason}])

    out = app.worker_exit({})

    assert out["daily_stop_flatten_decision"]["action"] == "flatten_all"
    assert out["daily_stop_flatten_decision"]["can_flatten"] is True
    assert out["daily_stop_flatten_decision"]["reasons"] == []
    assert calls["flatten"] == 1
    assert out["results"][0]["reason"] == "daily_stop_hit"
    assert out["snapshot_reason"] == "daily_stop_bulk_flatten"


def test_patch_217_opening_grace_blocks_explicit_swing_daily_stop_bulk_flatten(monkeypatch):
    app.DAILY_STOP_CONFIRMATION_STATE.clear()
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "DAILY_STOP_ACTION", "flatten_all")
    monkeypatch.setattr(app, "ALLOW_DAILY_STOP_BULK_FLATTEN", True)
    monkeypatch.setattr(app, "DAILY_STOP_CONFIRMATION_SEC", 0)
    monkeypatch.setattr(app, "SWING_DAILY_STOP_OPEN_GRACE_MIN", 15)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)
    monkeypatch.setattr(app, "_minutes_since_market_open", lambda ts_ny=None: 2.0)

    decision = app._daily_stop_flatten_decision()

    assert decision["can_flatten"] is False
    assert decision["in_open_grace"] is True
    assert "swing_open_grace_active" in decision["reasons"]


def test_patch_217_stale_plan_recovery_deactivates_plans_without_broker_positions(monkeypatch):
    monkeypatch.setattr(app, "list_open_positions_details_allowed", lambda: [])
    monkeypatch.setattr(app, "list_open_orders_safe", lambda: [])
    monkeypatch.setattr(app, "record_decision", lambda *args, **kwargs: None)
    monkeypatch.setattr(app, "_transition_execution_lifecycle", lambda *args, **kwargs: None)
    snapshots = []
    monkeypatch.setattr(app, "persist_positions_snapshot", lambda reason="", extra=None: snapshots.append({"reason": reason, "extra": extra or {}}) or snapshots[-1])

    app.TRADE_PLAN["AAPL"] = {"active": True, "side": "buy", "signal": "RECOVERED"}

    actions = app._deactivate_plans_without_broker_positions(source="worker_exit", reason="broker_flat_verified")

    assert actions == [{"symbol": "AAPL", "action": "deactivated_stale_plan", "reason": "broker_flat_verified"}]
    assert app.TRADE_PLAN["AAPL"]["active"] is False
    assert snapshots[-1]["reason"] == "broker_flat_verified"

    app.TRADE_PLAN.pop("AAPL", None)