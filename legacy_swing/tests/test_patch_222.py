import os
from datetime import timedelta

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_222_candidate_history_backfills_recovered_identity(monkeypatch):
    monkeypatch.setattr(app, "CANDIDATE_HISTORY", [{
        "ts_utc": "2026-06-26T14:00:00+00:00",
        "regime_mode": "defensive",
        "selected": ["AMD"],
        "candidates": [{
            "symbol": "AMD",
            "strategy": "daily_mean_reversion",
            "signal": "daily_mean_reversion",
            "rank_score": 101.5,
            "selection_quality_score": 177.25,
            "entry_type": "mean_reversion",
            "breakout_level": 510.0,
            "stop_price": 492.0,
            "target_price": 532.0,
        }],
    }])
    monkeypatch.setattr(app, "SCAN_HISTORY", [])
    monkeypatch.setattr(app, "DECISIONS", [])
    monkeypatch.setattr(app, "PAPER_LIFECYCLE_HISTORY", [])
    monkeypatch.setattr(app, "LAST_PAPER_LIFECYCLE", {})
    monkeypatch.setattr(app, "_read_journal", lambda *args, **kwargs: [])

    meta = app._infer_recovered_plan_attribution("AMD")

    assert meta["strategy_name"] == "daily_mean_reversion"
    assert meta["signal"] == "daily_mean_reversion"
    assert meta["rank_score"] == 101.5
    assert meta["selection_quality_score"] == 177.25
    assert meta["attribution_source"] == "candidate_history_selected"


def test_patch_222_reconcile_recovered_position_preserves_strategy_attribution(monkeypatch):
    monkeypatch.setattr(app, "CANDIDATE_HISTORY", [{
        "selected": ["AMD"],
        "candidates": [{"symbol": "AMD", "strategy": "daily_mean_reversion", "rank_score": 99.0, "entry_type": "mean_reversion"}],
    }])
    monkeypatch.setattr(app, "SCAN_HISTORY", [])
    monkeypatch.setattr(app, "DECISIONS", [])
    monkeypatch.setattr(app, "PAPER_LIFECYCLE_HISTORY", [])
    monkeypatch.setattr(app, "LAST_PAPER_LIFECYCLE", {})
    monkeypatch.setattr(app, "_read_journal", lambda *args, **kwargs: [])
    monkeypatch.setattr(app, "list_open_positions_details_allowed", lambda: [{"symbol": "AMD", "qty": "2", "avg_entry_price": "500"}])
    monkeypatch.setattr(app, "list_open_orders_safe", lambda *args, **kwargs: [])
    monkeypatch.setattr(app, "persist_positions_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(app, "_ensure_exit_arm_for_symbol", lambda *args, **kwargs: None)
    monkeypatch.setattr(app, "record_decision", lambda *args, **kwargs: None)
    app.TRADE_PLAN.clear()

    actions = app.reconcile_trade_plans_from_alpaca()
    plan = app.TRADE_PLAN["AMD"]

    assert actions and actions[0]["action"] == "recovered_plan"
    assert plan["recovered"] is True
    assert plan["signal"] == "daily_mean_reversion"
    assert plan["strategy_name"] == "daily_mean_reversion"
    assert plan["attribution_backfilled"] is True
    assert plan["attribution_source"] == "candidate_history_selected"
    assert plan["recovered_original_signal"] != plan["signal"]


def test_patch_222_restore_classification_keeps_recovered_provenance_after_backfill():
    restored = app._restore_snapshot_plan_classification({
        "symbol": "AMD",
        "signal": "daily_breakout",
        "strategy_name": "daily_breakout",
        "recovered": True,
        "recovered_at": "2026-06-26T10:00:00-04:00",
        "attribution_backfilled": True,
    })

    assert restored["recovered"] is True
    assert restored["startup_restore_classification"] == "recovered"
    assert restored["signal"] == "daily_breakout"
    assert restored["recovered_at"] == "2026-06-26T10:00:00-04:00"


def test_patch_222_closed_trade_keeps_recovered_provenance_without_recovered_strategy(monkeypatch):
    monkeypatch.setattr(app, "persist_strategy_performance_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(app, "STRATEGY_PERFORMANCE_STATE", {"closed_trades": [], "by_strategy": {}, "kill_switch": {}})
    opened_at = (app.now_ny() - timedelta(days=1)).isoformat()
    plan = {
        "symbol": "AMD",
        "side": "buy",
        "qty": 1,
        "filled_qty": 1,
        "entry_price": 100.0,
        "stop_price": 95.0,
        "signal": "daily_mean_reversion",
        "strategy_name": "daily_mean_reversion",
        "opened_at": opened_at,
        "recovered": True,
        "attribution_backfilled": True,
        "attribution_source": "candidate_history_selected",
    }

    row = app._append_strategy_closed_trade(plan, 103.0, reason="target", source="unit_test")

    assert row["strategy_name"] == "daily_mean_reversion"
    assert row["signal"] == "daily_mean_reversion"
    assert row["strategy_name"] != "recovered"
    assert row["recovered"] is True
    assert row["attribution_backfilled"] is True
    assert row["attribution_source"] == "candidate_history_selected"