import pytest

import regime_intraday_runtime as module
from regime_intraday_ledger import empty_ledger, load_ledger, save_ledger


@pytest.fixture
def lifecycle(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKER_SECRET", "test")
    monkeypatch.setenv("REGIME_INTRADAY_PAPER_AUTO_EXIT", "true")
    runtime = module.RegimeIntradayRuntime()
    runtime.ledger_path = str(tmp_path / "ledger.json")
    ledger = empty_ledger()
    ledger["orders"] = {"sig": {"order_id": "entry", "status": "filled", "plan": {"underlying": "SPY", "limit_debit": .3}}}
    save_ledger(runtime.ledger_path, ledger)
    monkeypatch.setattr(module, "fetch_option_chain", lambda *a, **k: {})
    monkeypatch.setattr(module, "value_debit_spread", lambda *a, **k: {"status": "valued", "liquidation_credit": .4})
    monkeypatch.setattr(module, "spread_exit_decision", lambda *a, **k: {"exit": True, "reason": "end_of_day"})
    monkeypatch.setattr(module, "send_exit_email", lambda **k: {"sent": False})
    calls = []
    def submit(*a, **k):
        calls.append(k["client_order_id"])
        return {"order_id": "close", "status": "new"}
    monkeypatch.setattr(module, "submit_mleg_close_order", submit)
    return runtime, calls


def test_email_failure_does_not_block_close(lifecycle, monkeypatch):
    runtime, calls = lifecycle
    monkeypatch.setattr(module, "get_order", lambda *a, **k: {"status": "filled"})
    def fail(**k):
        raise TimeoutError("email unavailable")
    monkeypatch.setattr(module, "send_exit_email", fail)
    runtime.paper_reconcile({"worker_secret": "test"})
    assert len(calls) == 1
    assert load_ledger(runtime.ledger_path)["events"][-2]["event"] == "paper_exit_email_failed"


@pytest.mark.parametrize("status", ["rejected", "canceled", "expired"])
def test_terminal_zero_fill_close_retries_with_new_id(lifecycle, monkeypatch, status):
    runtime, calls = lifecycle
    ledger = load_ledger(runtime.ledger_path)
    ledger["orders"]["sig"]["close_order"] = {"order_id": "old"}
    save_ledger(runtime.ledger_path, ledger)
    monkeypatch.setattr(module, "get_order", lambda *a, **k: {"status": status, "filled_qty": "0"})
    runtime.paper_reconcile({"worker_secret": "test"})
    assert not calls
    monkeypatch.setattr(module, "get_order", lambda *a, **k: {"status": "filled"})
    runtime.paper_reconcile({"worker_secret": "test"})
    assert calls == [module.paper_client_order_id("close:sig:retry:2")]


@pytest.mark.parametrize("qty,attempt", [("1", 1), (None, 1), ("0", 3)])
def test_partial_unknown_or_exhausted_close_needs_attention(lifecycle, monkeypatch, qty, attempt):
    runtime, calls = lifecycle
    ledger = load_ledger(runtime.ledger_path)
    ledger["orders"]["sig"].update(close_order={"order_id": "old"}, close_attempt=attempt)
    save_ledger(runtime.ledger_path, ledger)
    monkeypatch.setattr(module, "get_order", lambda *a, **k: {"status": "canceled", "filled_qty": qty})
    runtime.paper_reconcile({"worker_secret": "test"})
    assert not calls
    assert load_ledger(runtime.ledger_path)["orders"]["sig"]["status"] == "close_requires_attention"


def test_stale_close_cancels_without_submitting_replacement(lifecycle, monkeypatch):
    runtime, calls = lifecycle
    ledger = load_ledger(runtime.ledger_path)
    ledger["orders"]["sig"]["close_order"] = {"order_id": "old"}
    save_ledger(runtime.ledger_path, ledger)
    monkeypatch.setattr(module, "get_order", lambda *a, **k: {"status": "new", "submitted_at": "2020-01-01T00:00:00+00:00"})
    canceled = []
    monkeypatch.setattr(module, "cancel_order", lambda *a, **k: canceled.append(a[2]))
    runtime.paper_reconcile({"worker_secret": "test"})
    assert canceled == ["old"]
    assert not calls
