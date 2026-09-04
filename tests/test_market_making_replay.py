from opportunity_lab.prediction_market_making import replay_quote


def test_replay_requires_queue_clearance_before_crediting_fill():
    previous = {"ticker": "M", "yes_bid": .40, "yes_ask": .45, "bid_depth": 5, "ask_depth": 4,
                "modeled_quote_size": 3}
    current = {"ticker": "M", "yes_bid": .41, "yes_ask": .46}
    trades = [
        {"ticker": "M", "yes_price_dollars": ".40", "count_fp": "7", "taker_side": "no"},
        {"ticker": "M", "yes_price_dollars": ".45", "count_fp": "6", "taker_side": "yes"},
    ]
    result = replay_quote(previous, current, trades, maker_fee_coefficient=0)
    assert result["simulated_buy_fills"] == 2
    assert result["simulated_sell_fills"] == 2
    assert result["paired_round_trips"] == 2
    assert result["gross_marked_pnl"] == .1
    assert result["profitable"] is True
    assert result["eligible"] is False


def test_replay_marks_one_sided_inventory_at_next_executable_quote():
    previous = {"ticker": "M", "yes_bid": .40, "yes_ask": .45, "bid_depth": 1, "ask_depth": 1,
                "modeled_quote_size": 2}
    current = {"ticker": "M", "yes_bid": .35, "yes_ask": .40}
    trades = [{"ticker": "M", "yes_price_dollars": ".40", "count_fp": "3", "taker_side": "no"}]
    result = replay_quote(previous, current, trades, maker_fee_coefficient=0)
    assert result["ending_long_inventory"] == 2
    assert result["gross_marked_pnl"] == -.1
    assert result["profitable"] is False
