from fractions import Fraction

from opportunity_lab.kalshi_maker_v11 import STRATEGY_ID, shadow_decision


def frame(*, bid_depth, ask_depth):
    return {
        "received_at": 400,
        "orderbook_fp": {
            "yes_dollars": [[".40", str(bid_depth)]],
            "no_dollars": [[".54", str(ask_depth)]],
        },
    }


def test_v11_rejects_weak_v10_imbalance_and_accepts_fixed_threshold():
    history = [(100, Fraction(".40")), (200, Fraction(".405")),
               (300, Fraction(".41"))]
    assert shadow_decision(history, frame(bid_depth=12, ask_depth=8)) == (
        None, "weak_imbalance"
    )
    signal, reason = shadow_decision(history, frame(bid_depth=15, ask_depth=9))
    assert reason == "signal"
    assert signal["strategy_id"] == STRATEGY_ID
    assert Fraction(signal["imbalance"]) == Fraction(1, 4)


def test_v11_preserves_v10_rejection_reason():
    history = [(100, Fraction(".40")), (200, Fraction(".405"))]
    assert shadow_decision(history, frame(bid_depth=15, ask_depth=9)) == (
        None, "insufficient_history"
    )
