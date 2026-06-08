import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_194_action_plan_keeps_capacity_scaling_optional_when_slots_meet_minimum(monkeypatch):
    prev_mode = app.STRATEGY_MODE
    prev_max = app.MAX_OPEN_POSITIONS
    prev_count_open = app.count_open_positions_allowed
    try:
        app.STRATEGY_MODE = "swing"
        app.MAX_OPEN_POSITIONS = 7
        app.count_open_positions_allowed = lambda: 3
        monkeypatch.delenv("INTRADAY_MAX_OPEN_POSITIONS", raising=False)
        monkeypatch.setenv("INTRADAY_LAUNCH_MIN_OPEN_SLOTS", "3")

        projection = app._intraday_launch_projection()
        plan = projection["launch_action_plan"]

        assert projection["projected_intraday"]["open_slots_available"] == 4
        assert projection["projected_intraday"]["open_slots_gap_to_min"] == 0
        assert projection["blockers"] == []
        assert "set_INTRADAY_MAX_OPEN_POSITIONS_to_at_least_10" in plan["optional_scaling"]
        assert "set_INTRADAY_MAX_OPEN_POSITIONS_to_at_least_10" not in plan["required_now"]
        assert plan["recommended_env"]["INTRADAY_MAX_OPEN_POSITIONS"] == "10"
    finally:
        app.STRATEGY_MODE = prev_mode
        app.MAX_OPEN_POSITIONS = prev_max
        app.count_open_positions_allowed = prev_count_open


def test_patch_194_action_plan_requires_capacity_when_slots_below_minimum(monkeypatch):
    prev_mode = app.STRATEGY_MODE
    prev_max = app.MAX_OPEN_POSITIONS
    prev_count_open = app.count_open_positions_allowed
    try:
        app.STRATEGY_MODE = "swing"
        app.MAX_OPEN_POSITIONS = 7
        app.count_open_positions_allowed = lambda: 7
        monkeypatch.delenv("INTRADAY_MAX_OPEN_POSITIONS", raising=False)
        monkeypatch.setenv("INTRADAY_LAUNCH_MIN_OPEN_SLOTS", "3")

        projection = app._intraday_launch_projection()
        plan = projection["launch_action_plan"]

        assert "projected_intraday_open_slots_below_launch_min" in projection["blockers"]
        assert "set_INTRADAY_MAX_OPEN_POSITIONS_to_at_least_10" in plan["required_now"]
        assert "set_INTRADAY_MAX_OPEN_POSITIONS_above_current_base" in plan["required_now"]
    finally:
        app.STRATEGY_MODE = prev_mode
        app.MAX_OPEN_POSITIONS = prev_max
        app.count_open_positions_allowed = prev_count_open


def test_patch_194_intraday_overrides_apply_only_in_intraday_mode(monkeypatch):
    prev_mode = app.STRATEGY_MODE
    prev_max = app.MAX_OPEN_POSITIONS
    prev_stop = app.DAILY_STOP_DOLLARS
    try:
        app.MAX_OPEN_POSITIONS = 7
        app.DAILY_STOP_DOLLARS = 150.0
        monkeypatch.setenv("DAILY_LOSS_LIMIT", "200")
        monkeypatch.setenv("INTRADAY_MAX_OPEN_POSITIONS", "12")
        monkeypatch.setenv("INTRADAY_DAILY_STOP_DOLLARS", "300")
        monkeypatch.setenv("INTRADAY_DAILY_LOSS_LIMIT", "400")

        app.STRATEGY_MODE = "swing"
        assert app._effective_max_open_positions() == 7
        assert app._configured_daily_stop_dollars_safe() == 150.0
        assert app._configured_daily_loss_limit_safe() == 200.0

        app.STRATEGY_MODE = "intraday"
        assert app._effective_max_open_positions() == 12
        assert app._configured_daily_stop_dollars_safe() == 300.0
        assert app._configured_daily_loss_limit_safe() == 400.0
    finally:
        app.STRATEGY_MODE = prev_mode
        app.MAX_OPEN_POSITIONS = prev_max
        app.DAILY_STOP_DOLLARS = prev_stop


def test_patch_194_display_label_and_gate_action_mapping():
    assert app.patch_display_label("patch-194-clean-intraday-launch-action-plan") == "Patch 194 clean intraday launch action plan"
    assert app._intraday_launch_gate_actions(["market_not_tradable_now", "regime_not_favorable"]) == [
        "wait_for_market_open",
        "wait_for_regime_favorable",
    ]


def test_patch_194_readiness_endpoint_returns_launch_action_plan(monkeypatch):
    prev_mode = app.STRATEGY_MODE
    prev_max = app.MAX_OPEN_POSITIONS
    prev_count_open = app.count_open_positions_allowed
    prev_regime = app.LAST_REGIME_SNAPSHOT
    try:
        app.STRATEGY_MODE = "swing"
        app.MAX_OPEN_POSITIONS = 7
        app.count_open_positions_allowed = lambda: 3
        app.LAST_REGIME_SNAPSHOT = {"favorable": False, "score": 0.0}
        monkeypatch.setattr(app, "in_market_hours", lambda: False)
        monkeypatch.delenv("INTRADAY_MAX_OPEN_POSITIONS", raising=False)

        req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/intraday_launch_readiness"})
        out = app.diagnostics_intraday_launch_readiness(req)
        plan = out["launch_action_plan"]

        assert out["patch_version"].startswith("patch-")
        assert out["projection"]["blockers"] == []
        assert "wait_for_market_open" in plan["required_now"]
        assert "wait_for_regime_favorable" in plan["required_now"]
        assert "set_STRATEGY_MODE_intraday_for_launch" in plan["required_now"]
        assert "set_INTRADAY_MAX_OPEN_POSITIONS_to_at_least_10" in plan["optional_scaling"]
        assert out["recommended_followups"]
    finally:
        app.STRATEGY_MODE = prev_mode
        app.MAX_OPEN_POSITIONS = prev_max
        app.count_open_positions_allowed = prev_count_open
        app.LAST_REGIME_SNAPSHOT = prev_regime