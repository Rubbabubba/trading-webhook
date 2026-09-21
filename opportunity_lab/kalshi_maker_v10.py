"""Queue-aware, cost-stressed challenger for the Kalshi demo maker worker.

The signal is deliberately pure so it can run in shadow mode before it is
allowed to create an order.  It uses only information available at decision
time and expresses every price in the selected economic outcome's cents.
"""
from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

from .kalshi_shadow import price_book


STRATEGY_ID = "queue_toxicity_maker_v10_shadow"
MIN_NET_EDGE_CENTS = 3
ROUND_TRIP_COST_STRESS_CENTS = 2
MIN_TREND_CENTS = Fraction(1, 2)
MIN_IMBALANCE = Fraction(1, 10)
MAX_FAIR_SHIFT_CENTS = 2
SIGNAL_COOLDOWN_SECONDS = 300
MARKOUT_HORIZONS = (5, 30, 300)


def _median(values):
    ordered = sorted(values)
    size = len(ordered)
    if size % 2:
        return ordered[size // 2]
    return (ordered[size // 2 - 1] + ordered[size // 2]) / 2


def _whole_cents(value):
    cents = value * 100
    if cents.denominator != 1:
        return None
    return cents.numerator


def _decimal_text(value):
    value = Fraction(value)
    return format(Decimal(value.numerator) / Decimal(value.denominator), "f")


def shadow_decision(history, frame):
    """Return ``(quote, reason)`` for one prospective shadow evaluation.

    Direction requires agreement between a 60--300 second midpoint trend and
    current top-of-book depth.  The quoted price must remain passive and retain
    three cents to a bounded microprice/trend fair-value estimate.  This is a
    prospective hypothesis, not a profitability claim.
    """
    at = frame["received_at"]
    anchors = [mid for when, mid in history if 60 <= at - when <= 300]
    if len(anchors) < 3:
        return None, "insufficient_history"
    yes_bid, yes_ask, bid_depth, ask_depth = price_book(frame, "yes")
    yes_mid = (yes_bid + yes_ask) / 2
    if not Fraction(20, 100) <= yes_mid <= Fraction(80, 100):
        return None, "midpoint_out_of_range"
    spread = yes_ask - yes_bid
    if not Fraction(4, 100) <= spread <= Fraction(10, 100):
        return None, "spread_out_of_range"
    if min(bid_depth, ask_depth) < 3 or max(bid_depth, ask_depth) > min(bid_depth, ask_depth) * 4:
        return None, "depth_out_of_range"

    anchor = _median(anchors)
    trend = yes_mid - anchor
    imbalance = Fraction(bid_depth - ask_depth, bid_depth + ask_depth)
    # More bid depth pushes microprice toward the ask; more ask depth pushes it
    # toward the bid.  Require trend and depth to agree before choosing a side.
    microprice = (yes_ask * bid_depth + yes_bid * ask_depth) / (bid_depth + ask_depth)
    micro_shift = microprice - yes_mid
    if trend * 100 >= MIN_TREND_CENTS and imbalance >= MIN_IMBALANCE and micro_shift > 0:
        outcome = "yes"
    elif trend * 100 <= -MIN_TREND_CENTS and imbalance <= -MIN_IMBALANCE and micro_shift < 0:
        outcome = "no"
    else:
        return None, "directional_disagreement"

    raw_shift = (trend + micro_shift) / 2
    bound = Fraction(MAX_FAIR_SHIFT_CENTS, 100)
    fair_yes = yes_mid + max(-bound, min(bound, raw_shift))
    fair = fair_yes if outcome == "yes" else 1 - fair_yes
    side_bid, side_ask, _side_bid_depth, _side_ask_depth = price_book(frame, outcome)
    bid_cents, ask_cents = _whole_cents(side_bid), _whole_cents(side_ask)
    if bid_cents is None or ask_cents is None:
        return None, "fractional_book"
    # Fractions use floor division here so an optimistic fractional cent can
    # never be counted as available edge.
    fair_cents_floor = (fair * 100).numerator // (fair * 100).denominator
    quote_cents = min(ask_cents - 1, fair_cents_floor - MIN_NET_EDGE_CENTS)
    if not bid_cents < quote_cents < ask_cents:
        return None, "insufficient_passive_edge"
    return {
        "strategy_id": STRATEGY_ID,
        "outcome": outcome,
        "price_cents": quote_cents,
        "fair_value_cents": str(fair * 100),
        "net_edge_floor_cents": str(fair * 100 - quote_cents),
        "yes_mid": str(yes_mid),
        "anchor_yes_mid": str(anchor),
        "trend_cents": str(trend * 100),
        "microprice_yes": str(microprice),
        "imbalance": str(imbalance),
        "spread_cents": int(spread * 100),
    }, "signal"


def shadow_quote(history, frame):
    """Return a cost-stressed directional maker quote or ``None``."""
    return shadow_decision(history, frame)[0]


def queue_cancel_reason(observations, *, age_seconds):
    """Reject stale, rear-of-queue quotes before a toxic sweep reaches them."""
    if not observations:
        return None
    positions = [Fraction(str(item["queue_position_fp"])) for item in observations]
    if any(value < 0 for value in positions):
        raise ValueError("invalid_queue_position")
    if age_seconds >= 45 and positions[-1] > 5:
        return "rear_of_queue"
    if age_seconds >= 60 and len(positions) >= 3 and positions[-1] >= positions[0] and positions[-1] > 1:
        return "queue_not_improving"
    return None


def passive_exit_quote(frame, outcome, *, basis_cents, target_cents=2,
                       exit_fee_reserve_cents=1):
    """Return a non-crossing economic sell price that preserves net profit."""
    if outcome not in ("yes", "no") or type(basis_cents) not in (int, float):
        raise ValueError("invalid_exit_terms")
    bid, ask, _bid_depth, _ask_depth = price_book(frame, outcome)
    bid_cents, ask_cents = _whole_cents(bid), _whole_cents(ask)
    if bid_cents is None or ask_cents is None:
        return None
    required = int(basis_cents + target_cents + exit_fee_reserve_cents + Fraction(999, 1000))
    price = max(ask_cents, bid_cents + 1, required)
    return price if 1 <= price <= 99 else None


def stressed_markout(signal, yes_mid):
    """Mark one hypothetical buy to outcome midpoint with a two-cent stress."""
    if signal["outcome"] not in ("yes", "no"):
        raise ValueError("invalid_shadow_signal")
    outcome_mid = yes_mid if signal["outcome"] == "yes" else 1 - yes_mid
    gross = outcome_mid * 100 - signal["price_cents"]
    return {
        "gross_cents": _decimal_text(gross),
        "stressed_cents": _decimal_text(gross - ROUND_TRIP_COST_STRESS_CENTS),
    }
