import regime_intraday_runtime as runtime_module
from regime_intraday_ledger import assign_setup_identities, empty_ledger, save_ledger, load_ledger, paper_submission_decision
from regime_intraday_options import spread_quote_evidence


def scan(minute, signal=True, ready=True):
    return {"features": {"SPY": {"ready": ready, "last_ts": f"2026-09-03T10:{minute:02}:00-04:00"}},
            "signals": [{"symbol": "SPY", "strategy": "mr", "underlying_side": "buy", "signal_id": "original"}] if signal else []}


def test_setup_identity_survives_restart_and_requires_two_distinct_absent_bars(tmp_path):
    ledger = empty_ledger()
    initial = scan(1)
    assign_setup_identities(ledger, initial)
    assert initial["signals"][0]["signal_id"] == "original"
    path = str(tmp_path / "ledger.json")
    save_ledger(path, ledger)
    ledger = load_ledger(path)
    assign_setup_identities(ledger, scan(2, False))
    assign_setup_identities(ledger, scan(2, False))
    same = scan(3)
    assign_setup_identities(ledger, same)
    assert same["signals"][0]["signal_id"] == "original"
    assign_setup_identities(ledger, scan(4, False))
    assign_setup_identities(ledger, scan(5, False))
    new = scan(6)
    assign_setup_identities(ledger, new)
    assert new["signals"][0]["signal_id"] != "original"
    repeat = scan(7)
    assign_setup_identities(ledger, repeat)
    assert repeat["signals"][0]["signal_id"] == new["signals"][0]["signal_id"]


def test_terminal_partial_fill_blocks_new_entries_and_syncs_queue(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKER_SECRET", "test")
    monkeypatch.setenv("REGIME_INTRADAY_ALERT_EMAIL_ENABLED", "false")
    runtime = runtime_module.RegimeIntradayRuntime()
    runtime.ledger_path = str(tmp_path / "ledger.json")
    ledger = empty_ledger()
    ledger["orders"]["sig"] = {"order_id": "entry", "status": "new", "plan": {}, "terminal_quotes": {}}
    ledger["pending_candidates"]["sig"] = {"status": "paper_order_submitted"}
    save_ledger(runtime.ledger_path, ledger)
    monkeypatch.setattr(runtime_module, "get_order", lambda *a, **k: {"status": "canceled", "filled_qty": "0", "legs": [{"filled_qty": "1"}]})
    runtime.paper_reconcile({"worker_secret": "test"})
    result = load_ledger(runtime.ledger_path)
    assert result["orders"]["sig"]["status"] == "entry_requires_attention"
    assert result["pending_candidates"]["sig"]["status"] == "entry_requires_attention"
    assert not paper_submission_decision(result, "new", session="2026-09-03")["allowed"]


def test_zero_fill_cancellation_email_failure_preserves_status(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKER_SECRET", "test")
    runtime = runtime_module.RegimeIntradayRuntime()
    runtime.ledger_path = str(tmp_path / "ledger.json")
    ledger = empty_ledger()
    ledger["orders"]["sig"] = {"order_id": "entry", "status": "new", "plan": {}, "terminal_quotes": {}}
    ledger["pending_candidates"]["sig"] = {}
    save_ledger(runtime.ledger_path, ledger)
    monkeypatch.setattr(runtime_module, "get_order", lambda *a, **k: {"status": "canceled", "filled_qty": "0"})
    def fail(**k): raise TimeoutError()
    monkeypatch.setattr(runtime_module, "send_order_outcome_email", fail)
    runtime.paper_reconcile({"worker_secret": "test"})
    result = load_ledger(runtime.ledger_path)
    assert result["pending_candidates"]["sig"]["status"] == "canceled"
    assert result["orders"]["sig"]["outcome_email_error"] == "delivery_failed"


def test_quote_evidence_retains_timestamps_and_both_sides():
    chain = {"snapshots": {"long": {"latestQuote": {"bp": 2, "ap": 2.1, "t": "now"}}, "short": {"latestQuote": {"bp": 1.5, "ap": 1.6, "t": "then"}}}}
    evidence = spread_quote_evidence(chain, {"legs": [{"symbol": "long"}, {"symbol": "short"}]})
    assert evidence["entry_debit_from_quotes"] == .6
    assert evidence["legs"][1]["quote_timestamp"] == "then"


def test_entry_submission_email_failure_is_retried_by_reconcile(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKER_SECRET", "test")
    monkeypatch.setenv("REGIME_INTRADAY_ALERT_EMAIL_ENABLED", "true")
    monkeypatch.setenv("REGIME_INTRADAY_ALERT_EMAIL_TO", "operator@example.com")
    runtime = runtime_module.RegimeIntradayRuntime()
    runtime.ledger_path = str(tmp_path / "ledger.json")
    ledger = empty_ledger()
    ledger["orders"]["sig"] = {"order_id": "entry", "status": "new", "plan": {"underlying": "SPY"}, "entry_lifecycle_notifications": "v1",
                                     "entry_submitted_email_attempts": 1, "entry_submitted_email_error": "provider_error"}
    save_ledger(runtime.ledger_path, ledger)
    monkeypatch.setattr(runtime_module, "get_order", lambda *a, **k: {"status": "new", "submitted_at": "2099-09-03T15:00:00+00:00"})
    monkeypatch.setattr(runtime_module, "send_entry_lifecycle_email", lambda **k: {"sent": True, "message_id": "retry-1"})
    runtime.paper_reconcile({"worker_secret": "test"})
    record = load_ledger(runtime.ledger_path)["orders"]["sig"]
    assert record["entry_submitted_email_sent"] is True
    assert record["entry_submitted_email_attempts"] == 2
    assert "entry_submitted_email_error" not in record
