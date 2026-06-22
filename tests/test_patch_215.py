import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_215_submit_market_order_does_not_rest_retry_nonretryable(monkeypatch):
    calls = {"rest": 0}

    class FakeTradingClient:
        def submit_order(self, order_req):
            raise RuntimeError('{"code":42210000,"message":"fractional orders cannot be sold short"}')

    def fail_rest(*args, **kwargs):
        calls["rest"] += 1
        raise AssertionError("REST fallback should not run for non-retryable order validation errors")

    monkeypatch.setattr(app, "trading_client", FakeTradingClient())
    monkeypatch.setattr(app, "_alpaca_submit_order_rest", fail_rest)

    try:
        app.submit_market_order("AAPL", "sell", 1.25)
        assert False, "submit_market_order should raise a classified non-retryable error"
    except RuntimeError as exc:
        assert "non_retryable_order_error" in str(exc)
        assert "fractional orders cannot be sold short" in str(exc)

    assert calls["rest"] == 0


def test_patch_215_close_position_returns_structured_failure_on_submit_error(monkeypatch):
    monkeypatch.setattr(app, "get_position", lambda symbol: (1.23456789, "long"))
    monkeypatch.setattr(app, "is_live_exit_permitted", lambda source, reason="": True)
    monkeypatch.setattr(app, "list_open_orders_safe", lambda limit=None: [])
    monkeypatch.setattr(app, "submit_market_order", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("non_retryable_order_error:fractional orders cannot be sold short")))
    monkeypatch.setattr(app, "persist_positions_snapshot", lambda reason="", extra=None: {"reason": reason, "extra": extra or {}})
    monkeypatch.setattr(app, "record_decision", lambda *args, **kwargs: None)

    app.TRADE_PLAN["AAPL"] = {"active": True}
    out = app.close_position("AAPL", reason="stall_exit", source="worker_exit")

    assert out["closed"] is False
    assert out["submit_error"] is True
    assert out["nonretryable"] is True
    assert out["reason"] == "exit_submit_failed"
    assert out["qty"] == 1.234568
    assert app.TRADE_PLAN["AAPL"]["last_exit_submit_error_reason"] == "stall_exit"

    app.TRADE_PLAN.pop("AAPL", None)


def test_patch_215_close_position_skips_when_pending_close_order_exists(monkeypatch):
    submitted = {"called": False}
    monkeypatch.setattr(app, "get_position", lambda symbol: (2.0, "long"))
    monkeypatch.setattr(app, "list_open_orders_safe", lambda limit=None: [{"id": "close-1", "symbol": "AAPL", "side": "sell", "status": "accepted"}])
    monkeypatch.setattr(app, "record_decision", lambda *args, **kwargs: None)

    def fake_submit(*args, **kwargs):
        submitted["called"] = True
        return {"id": "unexpected"}

    monkeypatch.setattr(app, "submit_market_order", fake_submit)

    out = app.close_position("AAPL", reason="stall_exit", source="worker_exit")

    assert out["closed"] is False
    assert out["pending_close_order"] is True
    assert out["reason"] == "pending_close_order_exists"
    assert out["order_id"] == "close-1"
    assert submitted["called"] is False