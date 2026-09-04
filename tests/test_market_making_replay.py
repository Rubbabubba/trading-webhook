from opportunity_lab.prediction_market_making import replay_quote
from opportunity_lab.store import _maker_replay_summary


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


def test_maker_replay_summary_reports_fills_and_pnl():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    rows = [(now, .10, 1.0, True, {"simulated_buy_fills": 2, "simulated_sell_fills": 1,
                                    "paired_round_trips": 1}),
            (now, -.04, -1.0, False, {"simulated_buy_fills": 0, "simulated_sell_fills": 0,
                                      "paired_round_trips": 0})]
    result = _maker_replay_summary(rows)
    assert result["replay_count"] == 2
    assert result["replays_with_any_fill"] == 1
    assert result["net_marked_pnl"] == .06
    assert result["total_paired_round_trips"] == 1
