from app import _latest_matching_scan_record, _proof_capture_plan_snapshot
import app
from datetime import datetime, timezone, timedelta


def test_latest_matching_scan_record_prefers_scan_history_over_candidate_history(monkeypatch):
    runtime=["NET","AMZN"]
    old_ts=(datetime.now(timezone.utc)-timedelta(hours=3)).isoformat()
    app.CANDIDATE_HISTORY=[{"ts_utc": old_ts, "symbols": runtime, "selected": ["AMZN"], "candidates": []}]
    app.SCAN_HISTORY=[{"ts_utc": datetime.now(timezone.utc).isoformat(), "symbols": runtime, "summary": {"symbols": runtime, "selected_symbols": []}, "would_submit": []}]
    rec=_latest_matching_scan_record(runtime)
    assert rec.get("_scan_source") == "scan_history_match"


def test_proof_capture_plan_suppresses_candidate_history_submit_truth(monkeypatch):
    monkeypatch.setattr(app, '_current_runtime_preview_snapshot', lambda limit=10: {"runtime_symbols": ["NET"], "preview_source": "current_runtime_env", "top_candidates": [], "selected_symbols": []})
    monkeypatch.setattr(app, '_execution_visibility_snapshot', lambda limit=10: {"truth_source": "current_runtime_preview", "items": [], "selected_count": 0, "planned_count": 0, "submitted_count": 0, "filled_count": 0, "exit_armed_count": 0})
    monkeypatch.setattr(app, '_live_readiness_gate_snapshot', lambda limit=10: {"trade_path_proven": False, "go_live_eligible": False, "worker_status": {}})
    monkeypatch.setattr(app, '_paper_proof_gate_snapshot', lambda: {"paper_orders_permitted": True, "paper_proof_eligible": True, "paper_release_stage": True, "recent_market_scan_ok": True, "same_session_proven": True, "component_ready": True, "worker_status": {}, "session": {"market_open_now": True}, "env": {"dry_run": False, "paper_execution_enabled": True, "scanner_allow_live": True}, "paper_proof_unmet_conditions": []})
    monkeypatch.setattr(app, 'diagnostics_readiness', lambda request: {"market_open": True})
    monkeypatch.setattr(app, '_latest_matching_scan_record', lambda runtime_symbols=None: {"_scan_source": "candidate_history_match", "would_submit": [{"symbol": "NET", "submit_state": "blocked", "submit_reason": "spread_too_wide"}]})
    snap=_proof_capture_plan_snapshot(limit=5)
    assert snap.get("submit_truth_source") == "candidate_history_match"
    assert snap.get("items") == []
