from fractions import Fraction

from opportunity_lab.kalshi_maker_v12 import STRATEGY_ID, shadow_decision


def frame(*, yes_bid=".40", no_bid=".54", yes_depth="12", no_depth="8"):
    return {
        "received_at": 400,
        "orderbook_fp": {
            "yes_dollars": [[yes_bid, yes_depth]],
            "no_dollars": [[no_bid, no_depth]],
        },
    }


def test_v12_accepts_microprice_value_without_trend_history():
    signal, reason = shadow_decision([], frame())
    assert reason == "signal"
    assert signal["strategy_id"] == STRATEGY_ID
    assert signal["outcome"] == "yes"
    assert signal["price_cents"] == 40
    assert Fraction(signal["gross_edge_cents"]) >= 3
    assert Fraction(signal["stressed_edge_cents"]) >= 1


def test_v12_is_outcome_symmetric():
    signal, reason = shadow_decision(
        [(100, Fraction(".99"))],
        frame(yes_bid=".54", no_bid=".40", yes_depth="8", no_depth="12"),
    )
    assert reason == "signal"
    assert signal["outcome"] == "no"
    assert signal["price_cents"] == 40


def test_v12_rejects_balanced_illiquid_and_low_edge_books():
    assert shadow_decision([], frame(yes_depth="10", no_depth="10"))[1] == "balanced_book"
    assert shadow_decision([], frame(yes_depth="2", no_depth="1"))[1] == "depth_out_of_range"
    assert shadow_decision(
        [], frame(yes_bid=".40", no_bid=".56", yes_depth="12", no_depth="8")
    )[1] == "insufficient_microprice_edge"


def test_v12_preserves_price_and_spread_bounds():
    assert shadow_decision(
        [], frame(yes_bid=".10", no_bid=".84", yes_depth="12", no_depth="8")
    )[1] == "midpoint_out_of_range"
    assert shadow_decision(
        [], frame(yes_bid=".40", no_bid=".47", yes_depth="12", no_depth="8")
    )[1] == "spread_out_of_range"
