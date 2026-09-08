from regime_intraday_ledger import empty_ledger, mark_signal_submission, record_signal_shadow_candidates, update_signal_shadow_outcomes


def test_rejected_and_unsubmitted_signal_is_retained_and_scored():
    ledger = empty_ledger()
    signal = {"signal_id": "sig", "base_signal_id": "base", "symbol": "SPY", "underlying_side": "buy", "entry_price": 100, "stop_price": 99, "target_price": 102}
    scan = {"ts_utc": "2026-09-08T14:00:00+00:00", "signals": [signal], "features": {"SPY": {"ready": True, "last_ts": "2026-09-08T10:00:00-04:00"}}}
    plans = [{"signal": {"signal_id": "sig"}, "plan": {"status": "rejected", "reason": "wide_spread"}}]
    record_signal_shadow_candidates(ledger, scan, plans, ts_utc=scan["ts_utc"])
    row = ledger["signal_shadow_candidates"]["sig"]
    assert row["submission_status"] == "option_plan_rejected"
    update_signal_shadow_outcomes(ledger, {"ts_utc": "2026-09-08T14:02:00+00:00", "features": {"SPY": {"ready": True, "last_ts": "2026-09-08T10:01:00-04:00", "last_high": 103, "last_low": 98}}})
    assert row["status"] == "closed"
    assert row["exit_reason"] == "stop"
    assert row["realized_r"] == -1.0
    mark_signal_submission(ledger, "sig", "blocked", "cooldown")
    assert row["submission_reason"] == "cooldown"
