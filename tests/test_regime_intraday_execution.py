from datetime import date

from regime_intraday_ledger import empty_ledger, paper_submission_decision, pending_candidate, record_broker_order, record_pending_candidate, update_ledger
from regime_intraday_options import parse_occ, select_debit_spread, spread_exit_decision, value_debit_spread
from regime_intraday_executor import build_mleg_close_order, build_mleg_limit_order, submit_mleg_limit_order
from regime_intraday_readiness import readiness_snapshot


def test_close_order_reverses_both_legs_and_uses_credit_price():
    plan = {"legs": [{"symbol": "SPY260918C00500000", "side": "buy"}, {"symbol": "SPY260918C00501000", "side": "sell"}]}
    payload = build_mleg_close_order(plan, 0.42)
    assert payload["limit_price"] == "-0.42"
    assert [leg["side"] for leg in payload["legs"]] == ["sell", "buy"]
    assert [leg["position_intent"] for leg in payload["legs"]] == ["sell_to_close", "buy_to_close"]


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
    assert second["closed"][-1]["realized_r"] == 2.1


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
    losses["closed"] = [{"session": "2026-09-02", "realized_dollars": -10}, {"session": "2026-09-02", "realized_dollars": -5}]
    assert paper_submission_decision(losses, "sig-4", session="2026-09-02", max_consecutive_losses=2)["reason"] == "consecutive_loss_lock"


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
