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


def _plan():
    return {
        "active": True,
        "side": "buy",
        "entry_price": 100.0,
        "avg_fill_price": 100.0,
        "stop_price": 96.0,
        "initial_stop_price": 96.0,
        "take_price": 108.0,
        "risk_per_share": 4.0,
        "opened_at": datetime(2026, 7, 6, 9, 35, tzinfo=app.NY_TZ).isoformat(),
        "signal": "daily_breakout",
        "strategy_name": "daily_breakout",
    }


def test_patch_232_same_day_protective_exit_not_blocked(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "SWING_ALLOW_SAME_DAY_EXIT", False)
    monkeypatch.setattr(app, "now_ny", lambda: datetime(2026, 7, 6, 10, 0, tzinfo=app.NY_TZ))

    assert app.same_day_exit_blocked(_plan(), reason="time_exit") is True
    assert app.same_day_exit_blocked(_plan(), reason="stop") is False
    assert app.same_day_exit_blocked(_plan(), reason="opening_damage_exit") is False


def test_patch_232_opening_damage_triggers_on_deep_r(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_GUARD_ENABLED", True)
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_REQUIRE_MARKET_OPEN", False)
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_GRACE_MIN", 0)
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_MAX_LOSS_R", -1.10)
    monkeypatch.setattr(app, "_minutes_since_market_open", lambda ts_ny=None: 30.0)

    out = app._swing_opening_damage_guard("TEST", _plan(), 95.0)

    assert out["triggered"] is True
    assert out["reason"] in {"opening_damage_max_loss_r", "opening_damage_below_protective_stop"}
    assert out["unrealized_r"] <= -1.1


def test_patch_232_opening_damage_respects_grace(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "swing")
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_GUARD_ENABLED", True)
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_REQUIRE_MARKET_OPEN", False)
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_GRACE_MIN", 10)
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_MAX_LOSS_R", -1.10)
    monkeypatch.setattr(app, "_minutes_since_market_open", lambda ts_ny=None: 3.0)

    out = app._swing_opening_damage_guard("TEST", _plan(), 95.0)

    assert out["triggered"] is False
    assert out["reason"] == "opening_grace_active"


def test_patch_232_damage_guard_diagnostic_shape(monkeypatch):
    monkeypatch.setattr(app, "TRADE_PLAN", {"TEST": _plan()})
    monkeypatch.setattr(app, "get_position", lambda symbol: (1.0, "long"))
    monkeypatch.setattr(app, "get_latest_price", lambda symbol: 95.0)
    monkeypatch.setattr(app, "in_market_hours", lambda: True)
    monkeypatch.setattr(app, "_minutes_since_market_open", lambda ts_ny=None: 30.0)
    monkeypatch.setattr(app, "SWING_OPENING_DAMAGE_REQUIRE_MARKET_OPEN", False)

    out = app._swing_damage_guard_snapshot()

    assert out["ok"] is True
    assert out["positions_checked"] == 1
    assert out["positions"][0]["symbol"] == "TEST"
    assert "damage_guard" in out["positions"][0]