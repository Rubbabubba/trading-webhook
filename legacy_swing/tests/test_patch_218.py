import os
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_218_account_daily_pnl_snapshot_uses_alpaca_equity_delta(monkeypatch):
    acct = SimpleNamespace(equity="7210.36", last_equity="7966.84", buying_power="28841.44", cash="7210.36")
    monkeypatch.setattr(app.trading_client, "get_account", lambda: acct)

    snap = app._account_daily_pnl_snapshot()

    assert snap["ok"] is True
    assert snap["source"] == "alpaca_account_equity"
    assert snap["account_daily_pnl"] == -756.48
    assert snap["equity"] == 7210.36
    assert snap["last_equity"] == 7966.84


def test_patch_218_loss_control_incident_prefers_account_pnl_and_halt_only_policy(monkeypatch):
    monkeypatch.setattr(app, "_configured_daily_stop_dollars_safe", lambda: 150.0)
    monkeypatch.setattr(app, "_configured_daily_loss_limit_safe", lambda: 200.0)
    monkeypatch.setattr(app, "daily_halt_active", lambda: True)
    monkeypatch.setattr(app, "list_open_orders_safe", lambda: [])
    monkeypatch.setattr(app, "DAILY_HALT_STATE", {"session": "2026-06-23", "reason": "daily_stop_hit", "triggered_at": "2026-06-23T09:30:00-04:00"})

    incident = app._loss_control_incident_summary(
        daily_halt_truth={"daily_halt_active": True, "daily_halt_reason": "daily_stop_dollars"},
        today_pnl_truth={"account_daily_pnl": -756.48, "today_net_pnl": 0.0, "broker_exit_fills_today": 9},
        account_pnl={"ok": True, "source": "alpaca_account_equity", "account_daily_pnl": -756.48, "equity": 7210.36, "last_equity": 7966.84},
        flatten_decision={"action": "halt_only", "can_flatten": False, "allow_bulk_flatten": False, "reasons": ["daily_stop_action_halt_only"]},
        snapshot={"reason": "daily_stop_halt_only", "positions": [], "active_plans": {}},
    )

    assert incident["active"] is True
    assert incident["breach_source"] == "alpaca_account_daily_pnl"
    assert incident["account_daily_pnl"] == -756.48
    assert incident["flatten_policy"] == "halt_only"
    assert incident["bulk_flatten_allowed"] is False
    assert incident["fully_flat"] is True
    assert "remain_halted_until_next_session" in incident["recommended_actions"]
    assert "do_not_bulk_flatten_without_explicit_policy" in incident["recommended_actions"]


def test_patch_218_worker_exit_persists_loss_control_incident(monkeypatch):
    monkeypatch.setattr(app, "cleanup_caches", lambda: None)
    monkeypatch.setattr(app, "update_exit_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(app, "activate_daily_halt", lambda reason="": app.DAILY_HALT_STATE.update({"active": True, "reason": reason, "session": "2026-06-23"}))
    monkeypatch.setattr(app, "reconcile_trade_plans_from_alpaca", lambda: [])
    monkeypatch.setattr(app, "_deactivate_plans_without_broker_positions", lambda source="", reason="": [])
    monkeypatch.setattr(app, "daily_stop_hit", lambda: True)
    monkeypatch.setattr(app, "daily_halt_truth_snapshot", lambda: {"daily_halt_active": True, "daily_halt_reason": "daily_stop_dollars", "today_pnl_truth": {"account_daily_pnl": -756.48, "today_net_pnl": 0.0}})
    monkeypatch.setattr(app, "list_open_orders_safe", lambda: [])
    monkeypatch.setattr(app, "flatten_all", lambda reason: [])
    monkeypatch.setattr(app, "KILL_SWITCH", False)
    monkeypatch.setattr(app, "WORKER_SECRET", "")
    monkeypatch.setattr(app, "JOURNAL_ENABLED", False)
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "DAILY_STOP_ACTION", "halt_only")
    monkeypatch.setattr(app, "ALLOW_DAILY_STOP_BULK_FLATTEN", False)
    snapshots = []

    def fake_persist(reason="", extra=None):
        snap = {"reason": reason, "ts_utc": "2026-06-23T18:54:48+00:00", "positions": [], "active_plans": {}, "extra": extra or {}}
        snapshots.append(snap)
        return snap

    monkeypatch.setattr(app, "persist_positions_snapshot", fake_persist)

    out = app.worker_exit({})

    incident = out["loss_control_incident"]
    assert out["snapshot_reason"] == "daily_stop_halt_only"
    assert incident["active"] is True
    assert incident["fully_flat"] is True
    assert incident["flatten_policy"] == "halt_only"
    assert snapshots[-1]["extra"]["loss_control_incident"]["recommended_action"] == "remain_halted_until_next_session"