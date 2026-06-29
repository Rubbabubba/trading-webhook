import os
from datetime import datetime, timedelta

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _plan(**overrides):
    base = {
        "symbol": "STAL",
        "active": True,
        "side": "buy",
        "entry_price": 100.0,
        "stop_price": 95.0,
        "initial_stop_price": 95.0,
        "take_price": 110.0,
        "risk_per_share": 5.0,
        "filled_qty": 1,
        "qty": 1,
        "opened_at": (app.now_ny() - timedelta(days=1)).isoformat(),
    }
    base.update(overrides)
    return base


def test_patch_221_stall_loss_guard_arms_before_classic_stall_window(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "SWING_STALL_LOSS_GUARD_ENABLED", True)
    monkeypatch.setattr(app, "SWING_STALL_LOSS_GUARD_DAYS", 1)
    monkeypatch.setattr(app, "SWING_STALL_MAX_LOSS_R", -0.60)
    monkeypatch.setattr(app, "SWING_STALL_EXIT_DAYS", 3)
    monkeypatch.setattr(app, "SWING_STALL_MIN_R", 0.50)
    monkeypatch.setattr(app, "plan_days_held", lambda plan: 1)

    out = app._calc_swing_dynamic_levels("STAL", _plan(), 96.50)  # -0.70R

    assert out["stall_exit"] is True
    assert out["stall_loss_guard"] is True
    assert "stall_loss_guard_ready" in out["flags"]
    assert "stall_exit_ready" not in out["flags"]


def test_patch_221_stall_loss_guard_respects_disabled_and_threshold(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "SWING_STALL_LOSS_GUARD_ENABLED", False)
    monkeypatch.setattr(app, "SWING_STALL_LOSS_GUARD_DAYS", 1)
    monkeypatch.setattr(app, "SWING_STALL_MAX_LOSS_R", -0.60)
    monkeypatch.setattr(app, "SWING_STALL_EXIT_DAYS", 3)
    monkeypatch.setattr(app, "SWING_STALL_MIN_R", 0.50)
    monkeypatch.setattr(app, "plan_days_held", lambda plan: 1)

    disabled = app._calc_swing_dynamic_levels("STAL", _plan(), 96.50)
    assert disabled["stall_exit"] is False

    monkeypatch.setattr(app, "SWING_STALL_LOSS_GUARD_ENABLED", True)
    not_deep_enough = app._calc_swing_dynamic_levels("STAL", _plan(), 97.50)  # -0.50R
    assert not_deep_enough["stall_exit"] is False


def test_patch_221_stall_drilldown_surfaces_guardrail_config(monkeypatch):
    monkeypatch.setattr(app, "SWING_STALL_MAX_LOSS_R", -0.60)
    rows = [
        {"symbol": "STAL", "exit_reason": "stall_exit", "gross_pnl": -75.0, "pnl_r": -1.2, "holding_days": 4.0},
        {"symbol": "OK", "exit_reason": "target", "gross_pnl": 40.0, "pnl_r": 1.0, "holding_days": 2.0},
    ]

    out = app._swing_stall_exit_drilldown(perf_state={"closed_trades": rows, "by_strategy": {}, "kill_switch": {}})

    assert out["patch_version"] == "patch-221-stall-exit-guardrails"
    assert out["config"]["SWING_STALL_MAX_LOSS_R"] == -0.60
    assert any(s["name"] == "stall_loss_guard_for_guard_leaks" for s in out["exit_tightening_simulations"])