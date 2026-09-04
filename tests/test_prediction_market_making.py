from opportunity_lab.prediction_market_making import screen_market_making


def test_market_making_screen_models_fill_and_inventory_risk():
    result = screen_market_making([{"event_ticker": "E", "category": "Sports", "markets": [{
        "ticker": "M", "title": "Market", "yes_bid_dollars": ".40", "yes_ask_dollars": ".45",
        "yes_bid_size_fp": "20", "yes_ask_size_fp": "8",
    }]}])
    row = result["candidates"][0]
    assert row["modeled_quote_size"] == 8
    assert row["spread_dollars"] == .05
    assert row["queue_position_modeled"] is False
    assert row["eligible"] is False
    assert "series_fee_not_verified" in row["blockers"]
    assert set(row["scenarios"]) == {"optimistic", "base", "conservative"}


def test_market_making_screen_rejects_crossed_or_incomplete_quotes():
    result = screen_market_making([{"markets": [
        {"ticker": "CROSSED", "yes_bid_dollars": ".6", "yes_ask_dollars": ".5", "yes_bid_size_fp": "2", "yes_ask_size_fp": "2"},
        {"ticker": "EMPTY", "yes_bid_dollars": ".4"},
    ]}])
    assert result["market_count"] == 0
    assert result["conservative_positive_count"] == 0
