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


def test_patch_175_strategy_performance_endpoint_includes_trade_quality():
    state = {"closed_trades": [{"symbol": "CRM", "strategy_name": "x", "gross_pnl": 1, "rank_score": 70}], "by_strategy": {}, "kill_switch": {}}
    out = app._p175_trade_quality_analytics(perf_state=state, position_snapshot={}, scan_state={})
    assert "closed_trades_by_symbol" in out
    assert "open_book_risk" in out
    assert "rejected_setup_follow_through" in out