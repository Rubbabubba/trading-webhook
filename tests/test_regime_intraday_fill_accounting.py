from regime_intraday_fill_accounting import verified_roundtrip


PLAN = {"legs": [{"symbol": "LONG", "side": "buy"}, {"symbol": "SHORT", "side": "sell"}]}


def test_roundtrip_uses_account_fill_activities_and_aggregates_partial_events():
    record = {
        "plan": PLAN,
        "broker": {"filled_avg_price": "999", "fill_activities": [
            {"symbol": "LONG", "side": "buy", "qty": ".5", "price": "9.50"},
            {"symbol": "LONG", "side": "buy", "qty": ".5", "price": "9.50"},
            {"symbol": "SHORT", "side": "sell_short", "qty": "1", "price": "8.60"},
        ]},
        "close_order": {"broker": {"filled_avg_price": "999", "fill_activities": [
            {"symbol": "LONG", "side": "sell", "qty": "1", "price": "9.95"},
            {"symbol": "SHORT", "side": "buy", "qty": "1", "price": "8.75"},
        ]}},
    }
    result = verified_roundtrip(record)
    assert result["complete"] is True
    assert result["entry_debit"] == .9
    assert result["exit_credit"] == 1.2
    assert result["realized_dollars"] == 30.0


def test_roundtrip_accepts_nested_leg_fills():
    record = {"plan": PLAN,
              "broker": {"legs": [{"symbol": "LONG", "side": "buy", "filled_qty": "1", "filled_avg_price": "1.00"}, {"symbol": "SHORT", "side": "sell", "filled_qty": "1", "filled_avg_price": ".58"}]},
              "close_order": {"broker": {"legs": [{"symbol": "LONG", "side": "sell", "filled_qty": "1", "filled_avg_price": "1.10"}, {"symbol": "SHORT", "side": "buy", "filled_qty": "1", "filled_avg_price": ".42"}]}}}
    result = verified_roundtrip(record)
    assert result["complete"] is True
    assert result["realized_dollars"] == 26.0


def test_parent_prices_and_quotes_never_become_actual_pnl():
    record = {"plan": {**PLAN, "limit_debit": .9}, "broker": {"filled_avg_price": .9},
              "valuation": {"liquidation_credit": 1.2}, "close_order": {"broker": {"filled_avg_price": 1.2}}}
    result = verified_roundtrip(record)
    assert result["complete"] is False
    assert result["realized_dollars"] is None
    assert result["problems"] == ["verified_leg_fills_missing", "verified_leg_fills_missing"]


def test_unequal_leg_quantities_are_unresolved():
    record = {"plan": PLAN,
              "broker": {"fill_activities": [{"symbol": "LONG", "side": "buy", "qty": "1", "price": "1"}, {"symbol": "SHORT", "side": "sell", "qty": ".5", "price": ".5"}]},
              "close_order": {"broker": {"fill_activities": [{"symbol": "LONG", "side": "sell", "qty": "1", "price": "1"}, {"symbol": "SHORT", "side": "buy", "qty": "1", "price": ".5"}]}}}
    assert verified_roundtrip(record)["complete"] is False
