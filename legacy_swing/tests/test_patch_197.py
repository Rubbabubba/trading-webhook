import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_197_rejection_truth_marks_stale_book_state():
    truth = app._dashboard_rejection_truth_status(
        "AMAT",
        ["plan_or_pending_entry_exists", "position_already_open"],
        current_positions=set(),
        current_active_plans=set(),
    )

    assert truth["status"] == "historical_book_state"
    assert truth["current_position"] is False
    assert truth["current_active_plan"] is False
    assert truth["stale_book_reasons"] == ["position_already_open", "plan_or_pending_entry_exists"]


def test_patch_197_rejection_truth_keeps_current_book_state():
    truth = app._dashboard_rejection_truth_status(
        "AMAT",
        ["position_already_open"],
        current_positions={"AMAT"},
        current_active_plans=set(),
    )

    assert truth["status"] == "current"
    assert truth["current_position"] is True
    assert truth["stale_book_reasons"] == []


def test_patch_197_intraday_signal_debug_payload_from_scan(monkeypatch):
    monkeypatch.setattr(app, "STRATEGY_MODE", "intraday")
    monkeypatch.setattr(app, "INTRADAY_VWAP_RECLAIM_ENABLE", True)
    scan = {
        "ts_utc": "2026-06-09T14:01:00+00:00",
        "scanned": 2,
        "signals": 0,
        "candidate_slots": 10,
        "raw_results_count": 2,
        "results_compacted": True,
        "summary": {
            "strategy_breakdown": {
                "intraday_vwap_reclaim": [{"reason": "vwap_reclaim_missing", "count": 2}],
            },
            "top_component_blockers": [{"reason": "vwap_reclaim_missing", "count": 2}],
            "top_pre_ranked_candidates": [{"symbol": "AAPL", "candidate_path": "intraday_vwap_reclaim", "reason": "vwap_reclaim_missing", "rank_score": 61}],
            "top_signals": [],
        },
        "ignored_ranked_out": [],
        "would_submit": [],
    }

    out = app._intraday_signal_debug_from_scan(scan)

    assert out["ok"] is True
    assert out["actionable_state"] == "waiting_for_reclaim_signal"
    assert out["intraday_vwap_reclaim"]["enabled"] is True
    assert out["intraday_vwap_reclaim"]["breakdown"][0]["reason"] == "vwap_reclaim_missing"
    assert out["top_component_blockers"][0]["reason"] == "vwap_reclaim_missing"
    assert out["top_pre_ranked_candidates"][0]["candidate_path"] == "intraday_vwap_reclaim"


def test_patch_197_intraday_signal_debug_endpoint(monkeypatch):
    monkeypatch.setattr(app, "SCAN_HISTORY", [{"ts_utc": "2026-06-09T14:02:00+00:00", "summary": {"top_signals": []}, "results": []}])
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/intraday_signal_debug"})

    out = app.diagnostics_intraday_signal_debug(req)

    assert out["ok"] is True
    assert out["patch_version"].startswith("patch-197")
