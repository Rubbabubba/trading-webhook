"""Prospective microprice-value challenger for the Kalshi Demo worker.

V12 is shadow-only.  It removes V10's historical trend requirement and tests
whether a bounded top-of-book imbalance offers enough passive value at the
selected outcome's best bid after a fixed two-cent cost stress.
"""
from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

from .kalshi_maker_v10 import MARKOUT_HORIZONS, stressed_markout
from .kalshi_shadow import price_book


STRATEGY_ID = "microprice_value_maker_v12_shadow"
MIN_ABS_IMBALANCE = Fraction(1, 10)
MIN_GROSS_EDGE_CENTS = 3
SIGNAL_EVENT_COOLDOWN_SECONDS = 30 * 60


def _decimal_text(value):
    value = Fraction(value)
    return format(Decimal(value.numerator) / Decimal(value.denominator), "f")


def _whole_cents(value):
    cents = value * 100
    return cents.numerator if cents.denominator == 1 else None


def shadow_decision(history, frame):
    """Return a passive microprice signal and an auditable decision reason."""
    del history  # The preregistered V12 hypothesis uses only decision-time data.
    yes_bid, yes_ask, bid_depth, ask_depth = price_book(frame, "yes")
    yes_mid = (yes_bid + yes_ask) / 2
    if not Fraction(20, 100) <= yes_mid <= Fraction(80, 100):
        return None, "midpoint_out_of_range"
    spread = yes_ask - yes_bid
    if not Fraction(4, 100) <= spread <= Fraction(12, 100):
        return None, "spread_out_of_range"
    if min(bid_depth, ask_depth) < 3 or max(bid_depth, ask_depth) > min(bid_depth, ask_depth) * 4:
        return None, "depth_out_of_range"

    imbalance = Fraction(bid_depth - ask_depth, bid_depth + ask_depth)
    if abs(imbalance) < MIN_ABS_IMBALANCE:
        return None, "balanced_book"
    outcome = "yes" if imbalance > 0 else "no"
    microprice_yes = (yes_ask * bid_depth + yes_bid * ask_depth) / (bid_depth + ask_depth)
    fair = microprice_yes if outcome == "yes" else 1 - microprice_yes
    side_bid, side_ask, _bid_depth, _ask_depth = price_book(frame, outcome)
    bid_cents = _whole_cents(side_bid)
    ask_cents = _whole_cents(side_ask)
    if bid_cents is None or ask_cents is None:
        return None, "fractional_book"
    gross_edge = fair * 100 - bid_cents
    if gross_edge < MIN_GROSS_EDGE_CENTS:
        return None, "insufficient_microprice_edge"
    return {
        "strategy_id": STRATEGY_ID,
        "outcome": outcome,
        "price_cents": bid_cents,
        "fair_value_cents": _decimal_text(fair * 100),
        "gross_edge_cents": _decimal_text(gross_edge),
        "stressed_edge_cents": _decimal_text(gross_edge - 2),
        "yes_mid": str(yes_mid),
        "microprice_yes": str(microprice_yes),
        "imbalance": str(imbalance),
        "spread_cents": int(spread * 100),
        "quote_location": "selected_outcome_best_bid",
    }, "signal"


def shadow_quote(history, frame):
    return shadow_decision(history, frame)[0]
