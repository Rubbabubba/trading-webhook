from datetime import date, datetime
from zoneinfo import ZoneInfo

import regime_intraday_runtime as runtime_module
from regime_intraday_ledger import empty_ledger, paper_submission_decision, pending_candidate, record_broker_order, record_pending_candidate, update_ledger
from regime_intraday_ledger import load_ledger, save_ledger
from regime_intraday_options import parse_occ, select_debit_spread, spread_exit_decision, value_debit_spread
from regime_intraday_executor import build_mleg_close_order, build_mleg_limit_order, paper_client_order_id, submit_mleg_limit_order
from regime_intraday_readiness import readiness_snapshot
from regime_intraday_runtime import RegimeIntradayRuntime


def test_close_order_reverses_both_legs_and_uses_credit_price():
    plan = {"legs": [{"symbol": "SPY260918C00500000", "side": "buy"}, {"symbol": "SPY260918C00501000", "side": "sell"}]}
    payload = build_mleg_close_order(plan, 0.42, client_order_id="close-key")
    assert payload["limit_price"] == "-0.42"
    assert payload["client_order_id"] == "close-key"
    assert [leg["side"] for leg in payload["legs"]] == ["sell", "buy"]
    assert [leg["position_intent"] for leg in payload["legs"]] == ["sell_to_close", "buy_to_close"]


def test_signal_generates_stable_broker_idempotency_key():
    first = paper_client_order_id("SPY:mean-reversion:2026-09-03T10:15")
    assert first == paper_client_order_id("SPY:mean-reversion:2026-09-03T10:15")
    assert first != paper_client_order_id("SPY:mean-reversion:2026-09-03T10:16")
    assert len(first) <= 48


def _snapshot(bid, ask, delta):
    return {"latestQuote": {"bp": bid, "ap": ask}, "greeks": {"delta": delta}}


def test_occ_parser():
    parsed = parse_occ("SPY260911C00600000")
    assert parsed["expiration"].isoformat() == "2026-09-11"
    assert parsed["strike"] == 600.0
    assert parsed["option_type"] == "call"


def test_defined_risk_call_spread_selection():
    chain = {"snapshots": {
        "SPY260911C00600000": _snapshot(0.48, 0.50, 0.62),
        "SPY260911C00601000": _snapshot(0.18, 0.20, 0.42),
    }}
    plan = select_debit_spread(
        chain,
        {"underlying": "SPY", "option_type": "call", "min_dte": 7, "max_dte": 21, "target_delta_range": [0.55, 0.70], "max_bid_ask_spread_pct": 0.12},
        as_of=date(2026, 9, 2),
        max_loss_dollars=100,
    )
    assert plan["status"] == "selected"
    assert plan["limit_debit"] == 0.32
    assert plan["max_loss_dollars"] == 32.0
    assert plan["live_submission"] is False


def test_ledger_opens_then_closes_at_target():
    signal = {"symbol": "SPY", "strategy": "opening_range_momentum", "underlying_side": "buy", "entry_price": 100.0, "stop_price": 99.0, "target_price": 102.0}
    first = update_ledger(empty_ledger(), {"ts_utc": "2026-09-02T15:00:00+00:00", "signals": [signal], "features": {"SPY": {"price": 100.0}}})
    assert first["summary"]["open_count"] == 1
    second = update_ledger(first, {"ts_utc": "2026-09-02T16:00:00+00:00", "signals": [], "features": {"SPY": {"price": 102.1}}})
    assert second["summary"]["open_count"] == 0
    assert second["closed"][-1]["exit_reason"] == "target"
    assert second["closed"][-1]["realized_r"] == 2.0


def test_live_transport_is_closed_without_gate():
    plan = {"status": "selected", "order_class": "mleg", "quantity": 1, "limit_debit": 0.32, "max_loss_dollars": 32.0, "legs": [
        {"symbol": "SPY260911C00600000", "side": "buy", "position_intent": "buy_to_open"},
        {"symbol": "SPY260911C00601000", "side": "sell", "position_intent": "sell_to_open"},
    ]}
    payload = build_mleg_limit_order(plan)
    assert payload["type"] == "limit"
    assert payload["qty"] == "1"
    try:
        submit_mleg_limit_order("key", "secret", plan, paper=False, live_enabled=False)
    except PermissionError:
        pass
    else:
        raise AssertionError("live transport opened without its gate")


def test_indicative_feed_can_select_paper_plan_without_greeks():
    chain = {"snapshots": {
        "SPY260911C00600000": _snapshot(0.48, 0.50, 0.0),
        "SPY260911C00601000": _snapshot(0.18, 0.20, 0.0),
    }}
    plan = select_debit_spread(chain, {"underlying": "SPY", "underlying_price": 600.5, "option_type": "call", "min_dte": 7, "max_dte": 21, "target_delta_range": [0.55, 0.70], "max_bid_ask_spread_pct": 0.12}, as_of=date(2026, 9, 2))
    assert plan["status"] == "selected"
    assert plan["quote_basis"]["selection_source"] == "near_money_fallback"
    assert plan["live_eligible"] is False


def test_conservative_spread_valuation_and_exit():
    plan = {"limit_debit": 0.30, "max_profit_dollars": 70.0, "legs": [{"symbol": "LONG"}, {"symbol": "SHORT"}]}
    chain = {"snapshots": {"LONG": _snapshot(0.65, 0.67, 0.6), "SHORT": _snapshot(0.19, 0.20, 0.4)}}
    valuation = value_debit_spread(chain, plan)
    assert valuation["liquidation_credit"] == 0.45
    assert valuation["unrealized_dollars"] == 15.0
    assert spread_exit_decision(plan, valuation, minutes_to_close=10)["reason"] == "end_of_day"


def test_durable_signal_order_deduplication():
    ledger = empty_ledger()
    assert paper_submission_decision(ledger, "sig-1", session="2026-09-02")["allowed"] is True
    record_broker_order(ledger, "sig-1", {"order_id": "order-1", "status": "new"}, ts_utc="2026-09-02T15:00:00+00:00")
    decision = paper_submission_decision(ledger, "sig-1", session="2026-09-02")
    assert decision["allowed"] is False
    assert decision["reason"] == "duplicate_signal_order"


def test_paper_lifecycle_locks_active_order_daily_limit_and_loss_streak():
    active = empty_ledger()
    record_broker_order(active, "sig-1", {"order_id": "order-1", "status": "partially_filled"}, ts_utc="2026-09-02T15:00:00+00:00")
    assert paper_submission_decision(active, "sig-2", session="2026-09-02")["reason"] == "active_paper_order_or_position"

    daily = empty_ledger()
    daily["orders"] = {"a": {"session": "2026-09-02", "status": "canceled"}, "b": {"session": "2026-09-02", "status": "rejected"}}
    assert paper_submission_decision(daily, "sig-3", session="2026-09-02", max_trades_per_day=2)["reason"] == "daily_trade_limit"

    losses = empty_ledger()
    losses["closed"] = [{"paper_signal_id": "a", "session": "2026-09-02", "realized_dollars": -10}, {"paper_signal_id": "b", "session": "2026-09-02", "realized_dollars": -5}]
    assert paper_submission_decision(losses, "sig-4", session="2026-09-02", max_consecutive_losses=2)["reason"] == "consecutive_loss_lock"

    daily_loss = empty_ledger()
    daily_loss["closed"] = [{"paper_signal_id": "a", "session": "2026-09-02", "realized_dollars": -200}]
    assert paper_submission_decision(daily_loss, "sig-5", session="2026-09-02", max_daily_loss_dollars=200)["reason"] == "daily_loss_lock"


def test_live_readiness_requires_opra_and_closed_paper_roundtrip():
    snapshot = readiness_snapshot(config={"paper_submit_enabled": True, "option_feed": "indicative", "max_scan_age_sec": 10**9, "min_shadow_closed": 1}, ledger=empty_ledger(), last_scan={"ts_utc": "2026-09-02T18:00:00+00:00", "option_plans": []}, paper_credentials_present=True)
    assert snapshot["live_ready"] is False
    assert "opra_feed_required" in snapshot["live_blockers"]
    assert "paper_order_roundtrip_required" in snapshot["live_blockers"]


def test_pending_candidate_survives_scan_but_expires_closed():
    ledger = empty_ledger()
    signal = {"signal_id": "sig-queued", "symbol": "SPY"}
    plan = {"status": "selected", "limit_debit": 0.42}
    record_pending_candidate(ledger, signal, plan, ts_utc="2026-09-02T15:00:00+00:00", expires_at="2026-09-02T15:10:00+00:00")
    update_ledger(ledger, {"ts_utc": "2026-09-02T15:01:00+00:00", "signals": [], "features": {}})
    assert pending_candidate(ledger, "sig-queued", now_utc="2026-09-02T15:09:59+00:00") is not None
    assert pending_candidate(ledger, "sig-queued", now_utc="2026-09-02T15:10:00+00:00") is None


def test_scan_worker_auto_submits_one_selected_paper_plan(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKER_SECRET", "worker")
    monkeypatch.setenv("REGIME_INTRADAY_PAPER_AUTO_SUBMIT", "true")
    runtime = RegimeIntradayRuntime()
    monkeypatch.setattr(runtime, "scan", lambda: {
        "status": "completed",
        "option_plans": [
            {"signal": {"signal_id": "sig-1"}, "plan": {"status": "selected"}},
            {"signal": {"signal_id": "sig-2"}, "plan": {"status": "selected"}},
        ],
    })
    submitted = []
    monkeypatch.setattr(runtime, "paper_roundtrip", lambda body: submitted.append(body["signal_id"]) or {"ok": True, "signal_id": body["signal_id"]})

    result = runtime.scan_worker({"worker_secret": "worker"})

    assert submitted == ["sig-1"]
    assert result["auto_submission"]["signal_id"] == "sig-1"
    assert result["paper_auto_submit_enabled"] is True


def test_reconcile_auto_closes_and_records_filled_paper_roundtrip(monkeypatch, tmp_path):
    ledger_path = tmp_path / "ledger.json"
    monkeypatch.setenv("WORKER_SECRET", "worker")
    monkeypatch.setenv("ALPACA_PAPER_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_PAPER_API_SECRET_KEY", "secret")
    monkeypatch.setenv("REGIME_INTRADAY_LEDGER_PATH", str(ledger_path))
    monkeypatch.setenv("REGIME_INTRADAY_PAPER_AUTO_EXIT", "true")
    ledger = empty_ledger()
    ledger["orders"] = {"sig-1": {
        "order_id": "entry-1",
        "status": "filled",
        "session": "2026-09-02",
        "plan": {"underlying": "SPY", "limit_debit": 0.30, "legs": [{"symbol": "LONG"}, {"symbol": "SHORT"}]},
    }}
    save_ledger(str(ledger_path), ledger)
    monkeypatch.setattr(runtime_module, "get_order", lambda *_args, **_kwargs: {"status": "filled", "filled_at": "2026-09-02T20:00:00+00:00"})
    monkeypatch.setattr(runtime_module, "fetch_option_chain", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(runtime_module, "value_debit_spread", lambda *_args, **_kwargs: {"status": "valued", "liquidation_credit": 0.50})
    monkeypatch.setattr(runtime_module, "spread_exit_decision", lambda *_args, **_kwargs: {"exit": True, "reason": "profit_target"})
    monkeypatch.setattr(runtime_module, "send_exit_email", lambda **_kwargs: {"sent": True, "message_id": "email-1"})
    monkeypatch.setattr(runtime_module, "submit_mleg_close_order", lambda *_args, **_kwargs: {"submitted": True, "paper": True, "order_id": "close-1", "status": "new"})
    runtime = RegimeIntradayRuntime()

    first = runtime.paper_reconcile({"worker_secret": "worker"})
    assert first["automatic_exit_submission"] is True
    assert load_ledger(str(ledger_path))["orders"]["sig-1"]["status"] == "close_submitted"

    runtime.paper_reconcile({"worker_secret": "worker"})
    saved = load_ledger(str(ledger_path))
    assert saved["orders"]["sig-1"]["status"] == "filled_closed"
    assert saved["closed"][-1]["paper_signal_id"] == "sig-1"
    assert saved["closed"][-1]["realized_dollars"] == 20.0


def test_paper_submit_recovers_order_after_transport_timeout(monkeypatch, tmp_path):
    ledger_path = tmp_path / "ledger.json"
    monkeypatch.setenv("WORKER_SECRET", "worker")
    monkeypatch.setenv("ALPACA_PAPER_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_PAPER_API_SECRET_KEY", "secret")
    monkeypatch.setenv("REGIME_INTRADAY_LEDGER_PATH", str(ledger_path))
    monkeypatch.setenv("REGIME_INTRADAY_PAPER_SUBMIT_ENABLED", "true")
    ledger = empty_ledger()
    record_pending_candidate(
        ledger,
        {"signal_id": "sig-timeout", "symbol": "SPY"},
        {"status": "selected", "order_class": "mleg", "quantity": 1, "limit_debit": 0.40, "max_loss_dollars": 40, "legs": []},
        ts_utc="2026-09-03T15:00:00+00:00",
        expires_at="2099-09-03T15:10:00+00:00",
    )
    save_ledger(str(ledger_path), ledger)
    monkeypatch.setattr(runtime_module, "now_ny", lambda: datetime(2026, 9, 3, 10, 1, tzinfo=ZoneInfo("America/New_York")))
    monkeypatch.setattr(runtime_module, "submit_mleg_limit_order", lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("response lost")))
    monkeypatch.setattr(runtime_module, "get_order_by_client_id", lambda *_args, **_kwargs: {"id": "accepted-1", "status": "new", "order_class": "mleg"})
    runtime = RegimeIntradayRuntime()

    result = runtime.paper_roundtrip({"worker_secret": "worker", "signal_id": "sig-timeout"})

    assert result["result"]["recovered_after_transport_error"] is True
    saved = load_ledger(str(ledger_path))
    assert saved["orders"]["sig-timeout"]["order_id"] == "accepted-1"


def test_live_market_scan_combines_spy_and_dia_paper_sleeves(monkeypatch, tmp_path):
    now = datetime(2026, 9, 3, 10, 15, tzinfo=ZoneInfo("America/New_York"))
    monkeypatch.setenv("REGIME_INTRADAY_LEDGER_PATH", str(tmp_path / "ledger.json"))
    monkeypatch.setenv("REGIME_INTRADAY_DIA_PAPER_ENABLED", "true")
    monkeypatch.setenv("REGIME_INTRADAY_OPTION_CHAIN_ENABLED", "false")
    monkeypatch.setattr(runtime_module, "now_ny", lambda: now)
    monkeypatch.setattr(runtime_module, "is_regular_market_time", lambda *_args: True)
    monkeypatch.setattr(runtime_module, "fetch_recent_minute_bars", lambda symbols: ({symbol: [{"ts_ny": now}] for symbol in symbols}, {"count": len(symbols)}))

    def fake_evaluate(_bars, config):
        symbol = config.trade_symbols[0]
        return {
            "ok": True,
            "regime": {"name": "range"},
            "signals": [{"signal_id": f"signal-{symbol}", "symbol": symbol, "strategy": "vwap_mean_reversion", "underlying_side": "buy", "entry_price": 100, "stop_price": 99, "target_price": 102}],
            "features": {item: {"price": 100, "ready": True, "last_ts": "2026-09-03T10:14:00-04:00"} for item in config.symbols},
            "config": {},
        }

    monkeypatch.setattr(runtime_module, "evaluate_regime_intraday", fake_evaluate)
    result = RegimeIntradayRuntime().scan()

    assert {row["symbol"] for row in result["signals"]} == {"SPY", "DIA"}
    assert result["sleeves"]["spy_mean_reversion"]["execution"] == "paper"
    assert result["sleeves"]["dia_mean_reversion"]["execution"] == "paper"
    assert result["live_submission"] is False
