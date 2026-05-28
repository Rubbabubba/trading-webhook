import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_175_trade_quality_handles_missing_fields_and_buckets():
    perf_state = {
        "closed_trades": [
            {
                "symbol": "CRM",
                "strategy_name": "daily_breakout",
                "rank_score": 82.5,
                "gross_pnl": 120.0,
                "pnl_r": 1.2,
                "entry_ts_utc": "2026-05-10T14:30:00Z",
                "ts_utc": "2026-05-11T14:30:00Z",
                "reason": "target_hit",
            },
            {"symbol": "NET", "gross_pnl": -50.0},
        ]
    }
    scan_state = {
        "scan_history": [
            {
                "ts_utc": "2026-05-15T14:00:00Z",
                "candidates": [
                    {"symbol": "CRM", "rank_score": 75, "close": 100.0, "rejection_reasons": ["too_far_below_breakout"]},
                    {"symbol": "NET", "rank_score": 35, "close": 50.0, "rejection_reasons": ["rank_score_below_min", "daily_halt_active"]},
                ],
            },
            {
                "ts_utc": "2026-05-15T14:05:00Z",
                "candidates": [
                    {"symbol": "CRM", "rank_score": 76, "close": 103.0, "rejection_reasons": []},
                    {"symbol": "NET", "rank_score": 36, "close": 49.0, "rejection_reasons": []},
                ],
            },
        ]
    }
    snap = {
        "positions": [{"symbol": "CRM", "qty": 2, "avg_entry_price": 100, "current_price": 95}],
        "active_plans": {"CRM": {"active": True, "stop_price": 90, "entry_price": 100, "rank_score": 82.5}},
    }

    out = app._p175_trade_quality_analytics(perf_state=perf_state, position_snapshot=snap, scan_state=scan_state)

    assert out["read_only"] is True
    assert out["closed_trades_by_rank_bucket"]["80+"]["closed_trades"] == 1
    assert out["closed_trades_by_rank_bucket"]["unknown"]["closed_trades"] == 1
    assert out["closed_trades_by_holding_period"]["1 day"]["closed_trades"] == 1
    assert out["closed_trades_by_symbol"]["CRM"]["gross_pnl"] == 120.0
    assert out["open_book_risk"]["open_positions"] == 1
    assert out["open_book_risk"]["total_risk_to_stop"] == 20.0
    focus = out["rejected_setup_follow_through"]["focus_reasons"]
    assert focus["too_far_below_breakout"]["count"] == 1
    assert focus["rank_score_below_min"]["count"] == 1
    assert focus["daily_halt_active"]["count"] == 1
    assert focus["too_far_below_breakout"]["follow_through_count"] == 1
    assert focus["too_far_below_breakout"]["avg_best_follow_through_pct"] == 3.0
    assert focus["rank_score_below_min"]["avg_last_follow_through_pct"] == -2.0


def test_patch_175_rank_score_reads_current_plan_thesis_key():
    row = {"thesis": {"candidate_rank_score": 72.5}}
    assert app._p175_rank_score(row) == 72.5


def test_patch_175_rank_score_and_holding_days_support_legacy_fields():
    row = {
        "rank_meta": {"rank_score": 66.2},
        "opened_at": "2026-05-10T14:00:00+00:00",
        "closed_at": "2026-05-12T14:00:00+00:00",
    }
    assert app._p175_rank_score(row) == 66.2
    assert app._p175_holding_bucket(row) == "2-3 days"


def test_patch_175_holding_days_supports_nested_and_epoch_fields():
    row = {
        "entry": {"timestamp": 1715349600},  # 2024-05-10T14:00:00Z
        "exit": {"timestamp": 1715522400},   # 2024-05-12T14:00:00Z
    }
    assert app._p175_holding_bucket(row) == "2-3 days"


def test_patch_175_rank_score_supports_legacy_rank_and_score_meta_fields():
    assert app._p175_rank_score({"rank": 71.8}) == 71.8
    assert app._p175_rank_score({"score_meta": {"components": {"rank": 64.5}}}) == 64.5
    assert app._p175_rank_score({"thesis": "{\"candidate_rank_score\": 58.4}"}) == 58.4


def test_patch_175_holding_days_supports_json_and_epoch_string_fields():
    row = {
        "entry": "{\"timestamp\": \"1715349600\"}",
        "exit": "{\"timestamp\": \"1715522400000\"}",
    }
    assert app._p175_holding_bucket(row) == "2-3 days"


def test_patch_175_strategy_performance_endpoint_includes_trade_quality():
    state = {"closed_trades": [{"symbol": "CRM", "strategy_name": "x", "gross_pnl": 1, "rank_score": 70}], "by_strategy": {}, "kill_switch": {}}
    out = app._p175_trade_quality_analytics(perf_state=state, position_snapshot={}, scan_state={})
    assert "closed_trades_by_symbol" in out
    assert "open_book_risk" in out
    assert "rejected_setup_follow_through" in out


def test_patch_175_late_enrichment_helper_noop_when_already_complete():
    state = {"closed_trades": [{"symbol": "CRM", "rank_score": 80.0, "holding_days": 2.0}], "by_strategy": {}, "kill_switch": {}}
    out, meta = app._patch175_enrich_state_if_needed(state)
    assert out["closed_trades"][0]["rank_score"] == 80.0
    assert meta["rows_seen"] == 1


def test_patch_175_enrich_closed_trade_row_backfills_rank_and_holding():
    row = {"candidate_rank_score": "77.25", "holding_period": "2.5"}
    out, changed = app._p175_enrich_closed_trade_row(row)
    assert changed is True
    assert app._p175_rank_score(out) == 77.25
    assert out["holding_days"] == 2.5


def test_intraday_launch_readiness_shape():
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/intraday_launch_readiness"})
    out = app.diagnostics_intraday_launch_readiness(req)
    assert out["framework"] == "finra_intraday_margin"
    assert out["effective_date_utc"] == "2026-06-04"
    assert isinstance(out["checks"], list) and len(out["checks"]) >= 5
    assert "strategy_mode" in out
    assert "projection" in out


def test_intraday_daily_limit_overrides_for_intraday_mode():
    prev_mode = app.STRATEGY_MODE
    prev_stop = os.environ.get("INTRADAY_DAILY_STOP_DOLLARS")
    prev_loss = os.environ.get("INTRADAY_DAILY_LOSS_LIMIT")
    try:
        app.STRATEGY_MODE = "intraday"
        os.environ["INTRADAY_DAILY_STOP_DOLLARS"] = "275"
        os.environ["INTRADAY_DAILY_LOSS_LIMIT"] = "325"
        os.environ["INTRADAY_MAX_OPEN_POSITIONS"] = "11"
        os.environ["INTRADAY_MAX_PORTFOLIO_EXPOSURE_PCT"] = "0.97"
        os.environ["INTRADAY_MAX_SYMBOL_EXPOSURE_PCT"] = "0.41"
        assert app._configured_daily_stop_dollars_safe() == 275.0
        assert app._configured_daily_loss_limit_safe() == 325.0
        assert app._effective_max_open_positions() == 11
        assert app._effective_portfolio_exposure_cap_pct() == 0.97
        assert app._effective_symbol_exposure_cap_pct() == 0.41
    finally:
        app.STRATEGY_MODE = prev_mode
        if prev_stop is None:
            os.environ.pop("INTRADAY_DAILY_STOP_DOLLARS", None)
        else:
            os.environ["INTRADAY_DAILY_STOP_DOLLARS"] = prev_stop
        if prev_loss is None:
            os.environ.pop("INTRADAY_DAILY_LOSS_LIMIT", None)
        else:
            os.environ["INTRADAY_DAILY_LOSS_LIMIT"] = prev_loss
        os.environ.pop("INTRADAY_MAX_OPEN_POSITIONS", None)
        os.environ.pop("INTRADAY_MAX_PORTFOLIO_EXPOSURE_PCT", None)
        os.environ.pop("INTRADAY_MAX_SYMBOL_EXPOSURE_PCT", None)

def test_intraday_launch_projection_works_before_mode_switch():
    prev_mode = app.STRATEGY_MODE
    try:
        app.STRATEGY_MODE = "swing"
        os.environ["INTRADAY_MAX_OPEN_POSITIONS"] = "12"
        os.environ["INTRADAY_DAILY_STOP_DOLLARS"] = "450"
        projection = app._intraday_launch_projection()
        assert projection["mode_switch_required"] is True
        assert projection["projected_intraday"]["max_open_positions"] == 12
        assert projection["projected_intraday"]["daily_stop_dollars"] == 450.0
    finally:
        app.STRATEGY_MODE = prev_mode
        os.environ.pop("INTRADAY_MAX_OPEN_POSITIONS", None)
        os.environ.pop("INTRADAY_DAILY_STOP_DOLLARS", None)