from fractions import Fraction

from opportunity_lab.kalshi_maker_v5 import maker_quote


def frame(at=200, yes_bid="0.44", no_bid="0.52", yes_size="6", no_size="5"):
    return {
        "received_at": at,
        "orderbook_fp": {
            "yes_dollars": [[yes_bid, yes_size]],
            "no_dollars": [[no_bid, no_size]],
        },
    }


def test_stable_balanced_book_quotes_inside_spread_for_both_outcomes():
    history = [(100, Fraction(46, 100))]
    assert maker_quote(history, frame(), "yes")["price_cents"] == 45
    assert maker_quote(history, frame(), "no")["price_cents"] == 53


def test_rejects_fast_move_narrow_spread_and_one_sided_depth():
    assert maker_quote([(100, Fraction(40, 100))], frame(), "yes") is None
    assert maker_quote([(100, Fraction(46, 100))], frame(no_bid="0.55"), "yes") is None
    assert maker_quote([(100, Fraction(46, 100))], frame(yes_size="20", no_size="3"), "yes") is None


def test_requires_a_separate_older_observation():
    assert maker_quote([(180, Fraction(46, 100))], frame(), "yes") is None
