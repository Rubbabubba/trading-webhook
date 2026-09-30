from copy import deepcopy

from opportunity_lab.kalshi_external_sleeves import (
    confirm_structural_candidate, favorite_longshot_observations,
    favorite_maker_observations,
    research_relevance, structural_candidates,
)


def market(strike=10, *, yes_ask=".40", no_ask=".61"):
    return {
        "ticker": f"E-{strike}", "event_ticker": "E", "status": "active",
        "market_type": "binary", "exchange_index": 0, "strike_type": "greater",
        "floor_strike": strike, "yes_ask_dollars": yes_ask,
        "no_ask_dollars": no_ask, "rules_primary": f"Value is above {strike} units",
        "rules_secondary": "Official source; same rounding", "category": "Economics",
        "close_time": "2030-01-01T00:00:00Z",
        "expiration_time": "2030-01-02T00:00:00Z",
        "occurrence_datetime": "2030-01-01T00:00:00Z",
    }


def test_structural_candidate_requires_exact_relationship_and_cost_surplus():
    low = market(10, yes_ask=".30", no_ask=".71")
    high = market(20, yes_ask=".80", no_ask=".19")
    result = structural_candidates([low, high])
    assert len(result) == 1
    assert result[0]["indicative_stressed_surplus_cents"] == 45
    assert result[0]["fresh_book_required"] and not result[0]["execution_enabled"]


def test_structural_candidate_fails_closed_on_rule_or_event_mismatch():
    low, high = market(10), market(20, no_ask=".19")
    high["rules_secondary"] = "Different source"
    assert structural_candidates([low, high]) == []
    high = market(20, no_ask=".19"); high["event_ticker"] = "OTHER"
    assert structural_candidates([low, high]) == []


def test_nonprofitable_threshold_pair_is_not_signal():
    assert structural_candidates([market(10, yes_ask=".60"),
                                  market(20, no_ask=".39")]) == []


def test_fresh_structural_confirmation_requires_depth_and_time_alignment():
    low = market(10, yes_ask=".30", no_ask=".71")
    high = market(20, yes_ask=".80", no_ask=".19")
    candidate = structural_candidates([low, high])[0]
    low_quote = {"market": low, "orderbook_fp": {"no_dollars": [[".70", "2"]]},
                 "observed_at": 100.0}
    high_quote = {"market": high, "orderbook_fp": {"yes_dollars": [[".80", "3"]]},
                  "observed_at": 104.0}
    signal = confirm_structural_candidate(candidate, low_quote, high_quote, now=105.0)
    assert signal["fully_executable_snapshot"]
    assert signal["fresh_stressed_surplus_cents"] == 44
    high_quote["observed_at"] = 106.0
    import pytest
    with pytest.raises(ValueError, match="stale_or_asynchronous_books"):
        confirm_structural_candidate(candidate, low_quote, high_quote, now=106.0)


def test_favorite_longshot_are_symmetric_and_sports_is_separate():
    row = market(10, yes_ask=".95", no_ask=".05")
    row["event_ticker"] = "KXNFLGAME-30"
    observations = favorite_longshot_observations(row, "2026-09-28T12:34:56+00:00")
    assert {item["classification"] for item in observations} == {"favorite", "longshot"}
    assert {item["stratum"] for item in observations} == {"sports"}
    assert all(not item["fill_assumed"] for item in observations)


def test_relevance_keeps_only_registered_inputs():
    row = market(10, yes_ask=".95", no_ask=".05")
    assert set(research_relevance(row)) == {"favorite_longshot", "nested_threshold"}
    ordinary = deepcopy(row); ordinary.update(yes_ask_dollars=".50", no_ask_dollars=".51",
                                               strike_type="custom")
    assert research_relevance(ordinary) == []


def test_favorite_maker_is_family_time_liquidity_and_event_guarded():
    rows = []
    for ticker, event, family, expiry, bid, ask, volume in (
        ("CRYPTO-A", "CRYPTO-E", "Crypto", "2026-10-01T12:00:00Z", ".92", ".94", "10"),
        ("CRYPTO-B", "CRYPTO-E", "Crypto", "2026-10-01T10:00:00Z", ".91", ".93", "1"),
        ("POL-A", "POL-E", "Politics", "2026-10-02T00:00:00Z", ".95", ".97", "2"),
        ("SPORT-A", "KXNFLGAME-30", "Sports", "2026-10-01T00:00:00Z", ".95", ".97", "2"),
        ("ECON-A", "ECON-E", "Economics", "2026-10-01T00:00:00Z", ".95", ".97", "2"),
        ("WIDE-A", "WIDE-E", "Politics", "2026-10-01T00:00:00Z", ".91", ".98", "2"),
    ):
        row = market(10, yes_ask=ask, no_ask=str(1 - float(bid)))
        row.update(ticker=ticker, event_ticker=event, category=family,
                   expiration_time=expiry, close_time=expiry,
                   yes_bid_dollars=bid, yes_bid_size_fp="2", volume_24h_fp=volume)
        rows.append(row)
    observations = favorite_maker_observations(
        rows, "2026-09-30T12:00:00+00:00")
    assert [(row["event_id"], row["ticker"]) for row in observations] == [
        ("CRYPTO-E", "CRYPTO-B"), ("POL-E", "POL-A")]
    assert all(not row["fill_assumed"] and not row["execution_enabled"]
               for row in observations)
    assert all(row["passive_price_cents"] >= 90 for row in observations)


def test_relevance_routes_tradeable_favorite_maker_even_without_extreme_ask():
    row = market(10, yes_ask=".99", no_ask=".11")
    row.update(category="Crypto", yes_bid_dollars=".95", yes_bid_size_fp="3")
    assert research_relevance(row) == ["favorite_maker", "nested_threshold"]
