from datetime import date

from regime_intraday_ledger import empty_ledger, update_ledger
from regime_intraday_options import parse_occ, select_debit_spread
from regime_intraday_executor import build_mleg_limit_order, submit_mleg_limit_order


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
