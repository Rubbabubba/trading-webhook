from datetime import datetime, timezone

from opportunity_lab.kalshi_market_data import rank_event_dislocations


def test_rank_event_dislocation_is_never_marked_executable():
    result = rank_event_dislocations([{
        "event_ticker": "EVENT-1", "title": "Winner", "category": "Sports",
        "mutually_exclusive": True, "collateral_return_type": "binary",
        "markets": [
            {"ticker": "A", "yes_sub_title": "A", "yes_ask_dollars": "0.4500", "yes_ask_size_fp": "10.00"},
            {"ticker": "B", "yes_sub_title": "B", "yes_ask_dollars": "0.5000", "yes_ask_size_fp": "5.00"},
        ],
    }], category="Sports")
    row = result["candidates"][0]
    assert row["price_dislocation"] is True
    assert row["displayed_yes_ask_sum"] == .95
    assert row["gross_profit_at_displayed_size"] == .25
    assert row["eligible"] is False
    assert "fees_not_modeled" in row["blockers"]


def test_ranker_rejects_nonexclusive_and_unquoted_events():
    result = rank_event_dislocations([
        {"category": "Sports", "mutually_exclusive": False, "markets": []},
        {"category": "Sports", "mutually_exclusive": True, "markets": [{"yes_ask_dollars": "0"}]},
    ])
    assert result["candidate_count"] == 0
    assert result["skipped"]["not_mutually_exclusive"] == 1
    assert result["skipped"]["too_few_quoted_markets"] == 1


def test_mutually_exclusive_no_pair_models_fees_and_time():
    result = rank_event_dislocations([{
        "event_ticker": "RACE", "title": "Race", "category": "Sports", "mutually_exclusive": True,
        "markets": [
            {"ticker": "A", "yes_sub_title": "A", "yes_ask_dollars": ".50", "yes_ask_size_fp": "20", "yes_bid_dollars": ".60", "yes_bid_size_fp": "10", "close_time": "2027-01-01T00:00:00Z"},
            {"ticker": "B", "yes_sub_title": "B", "yes_ask_dollars": ".50", "yes_ask_size_fp": "20", "yes_bid_dollars": ".60", "yes_bid_size_fp": "8", "close_time": "2027-01-01T00:00:00Z"},
        ],
    }], now=datetime(2026, 1, 1, tzinfo=timezone.utc))
    pair = result["mutually_exclusive_no_pairs"][0]
    assert pair["contracts"] == 8
    assert pair["cost_before_fees"] == 6.4
    assert pair["estimated_taker_fees"] == .28
    assert pair["estimated_net_profit"] == 1.32
    assert pair["eligible"] is False
