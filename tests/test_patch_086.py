from app import _classify_scan_submit_response, _latest_scan_submit_decision, _proof_capture_plan_snapshot


def test_classify_scan_submit_response_blocked():
    out = _classify_scan_submit_response({"ok": False, "rejected": True, "reason": "spread_too_wide"})
    assert out["state"] == "blocked"
    assert out["reason"] == "spread_too_wide"
    assert out["attempted"] is False


def test_latest_scan_submit_decision_picks_submit_event():
    decisions = [
        {"event": "SCAN", "action": "candidate_selected", "symbol": "PLTR"},
        {"event": "SCAN", "action": "paper_submit_blocked", "symbol": "PLTR", "reason": "spread_too_wide"},
    ]
    out = _latest_scan_submit_decision(decisions)
    assert out["action"] == "paper_submit_blocked"
    assert out["reason"] == "spread_too_wide"
