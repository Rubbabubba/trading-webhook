from fractions import Fraction

import pytest

from opportunity_lab.kalshi_maker_v10 import (
    passive_exit_quote,
    queue_cancel_reason,
    shadow_quote,
    stressed_markout,
)


def frame(*, at=400, yes_bid=".40", no_bid=".54", yes_depth="12", no_depth="6"):
    return {
        "received_at": at,
        "orderbook_fp": {
            "yes_dollars": [[yes_bid, yes_depth]],
            "no_dollars": [[no_bid, no_depth]],
        },
    }


def test_shadow_quote_chooses_side_from_trend_and_depth_not_side_balance():
    history = [(100, Fraction(".40")), (200, Fraction(".405")), (300, Fraction(".41"))]
    result = shadow_quote(history, frame())
    assert result["outcome"] == "yes"
    assert result["price_cents"] == 41
    assert Fraction(result["net_edge_floor_cents"]) >= 3

    falling = [(100, Fraction(".60")), (200, Fraction(".595")), (300, Fraction(".59"))]
    result = shadow_quote(
        falling,
        frame(yes_bid=".54", no_bid=".40", yes_depth="6", no_depth="12"),
    )
    assert result["outcome"] == "no"


def test_shadow_quote_rejects_disagreement_cost_shortfall_and_sparse_history():
    rising = [(100, Fraction(".40")), (200, Fraction(".405")), (300, Fraction(".41"))]
    assert shadow_quote(rising, frame(yes_depth="6", no_depth="12")) is None
    assert shadow_quote(rising[:2], frame()) is None
    assert shadow_quote(rising, frame(yes_bid=".42", no_bid=".55")) is None


def test_queue_cancel_requires_stale_bad_position_or_no_improvement():
    assert queue_cancel_reason([{"queue_position_fp": "6"}], age_seconds=44) is None
    assert queue_cancel_reason([{"queue_position_fp": "6"}], age_seconds=45) == "rear_of_queue"
    improving = [{"queue_position_fp": "4"}, {"queue_position_fp": "3"}, {"queue_position_fp": "1"}]
    assert queue_cancel_reason(improving, age_seconds=90) is None
    stalled = [{"queue_position_fp": "3"}, {"queue_position_fp": "3"}, {"queue_position_fp": "3"}]
    assert queue_cancel_reason(stalled, age_seconds=60) == "queue_not_improving"
    with pytest.raises(ValueError):
        queue_cancel_reason([{"queue_position_fp": "-1"}], age_seconds=1)


def test_passive_exit_preserves_target_and_never_crosses():
    assert passive_exit_quote(frame(), "yes", basis_cents=40) == 46
    assert passive_exit_quote(frame(), "yes", basis_cents=50) == 53
    assert passive_exit_quote(frame(), "no", basis_cents=40) == 60


def test_markout_is_outcome_symmetric_and_cost_stressed():
    yes = stressed_markout({"outcome": "yes", "price_cents": 42}, Fraction(".47"))
    no = stressed_markout({"outcome": "no", "price_cents": 42}, Fraction(".53"))
    assert yes == no == {"gross_cents": "5", "stressed_cents": "3"}
    half = stressed_markout({"outcome": "yes", "price_cents": 42}, Fraction(".445"))
    assert half == {"gross_cents": "2.5", "stressed_cents": "0.5"}
