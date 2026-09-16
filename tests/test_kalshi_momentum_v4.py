from fractions import Fraction

from opportunity_lab.kalshi_momentum_v4 import momentum_entry_signal


def frame(yes_bid, no_bid, yes_size="10", no_size="4"):
    return {
        "received_at": 500,
        "fee_coefficient": ".07",
        "orderbook_fp": {
            "yes_dollars": [[str(yes_bid), yes_size]],
            "no_dollars": [[str(no_bid), no_size]],
        },
    }


CONFIG = {"strategy_id": "momentum_v4", "slippage_cents": 1}


def test_confirmed_yes_momentum_requires_liquidity():
    history = [(100, Fraction(50, 100)), (420, Fraction(55, 100))]
    result = momentum_entry_signal(history, frame("0.57", "0.41"), CONFIG)
    assert result == {"side": "yes", "strategy_id": "momentum_v4"}


def test_confirmed_no_momentum_is_symmetric():
    history = [(100, Fraction(60, 100)), (420, Fraction(55, 100))]
    result = momentum_entry_signal(history, frame("0.51", "0.47", yes_size="4", no_size="10"), CONFIG)
    assert result == {"side": "no", "strategy_id": "momentum_v4"}


def test_rejects_reversal_wide_shallow_and_extreme_books():
    assert momentum_entry_signal([(100, Fraction(".50")), (420, Fraction(".58"))],
                                 frame("0.55", "0.43"), CONFIG) is None
    assert momentum_entry_signal([(100, Fraction(".50")), (420, Fraction(".55"))],
                                 frame("0.58", "0.37"), CONFIG) is None
    assert momentum_entry_signal([(100, Fraction(".50")), (420, Fraction(".55"))],
                                 frame("0.58", "0.40", yes_size="2"), CONFIG) is None
    assert momentum_entry_signal([(100, Fraction(".72")), (420, Fraction(".77"))],
                                 frame("0.81", "0.17"), CONFIG) is None
