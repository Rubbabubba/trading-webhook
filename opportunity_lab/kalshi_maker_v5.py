"""Conservative one-contract maker signal for the Kalshi demo experiment."""
from __future__ import annotations

from fractions import Fraction

from .kalshi_shadow import price_book


def maker_quote(history, frame, preferred_side):
    """Return a passive quote only for a stable, balanced, two-sided book.

    ``history`` contains ``(timestamp, yes_midpoint)`` observations.  The
    returned price is expressed in cents for the selected economic outcome.
    """
    if preferred_side not in ("yes", "no"):
        raise ValueError("invalid_preferred_side")
    at = frame["received_at"]
    anchors = [mid for when, mid in history if 90 <= at - when <= 300]
    if not anchors:
        return None
    yes_bid, yes_ask, yes_bid_size, yes_ask_size = price_book(frame, "yes")
    yes_mid = (yes_bid + yes_ask) / 2
    if abs(yes_mid - anchors[-1]) > Fraction(2, 100):
        return None
    if not Fraction(20, 100) <= yes_mid <= Fraction(80, 100):
        return None
    side_bid, side_ask, bid_size, ask_size = price_book(frame, preferred_side)
    spread = side_ask - side_bid
    if not Fraction(3, 100) <= spread <= Fraction(8, 100):
        return None
    if min(bid_size, ask_size) < 3:
        return None
    # Avoid the one-sided books associated with toxic maker fills.
    if bid_size > ask_size * 2 or ask_size > bid_size * 2:
        return None
    improvement = side_bid + Fraction(1, 100)
    price = improvement if improvement < side_ask else side_bid
    cents = price * 100
    if cents.denominator != 1 or not 1 <= cents <= 99:
        return None
    return {
        "side": preferred_side,
        "price_cents": cents.numerator,
        "yes_mid": str(yes_mid),
        "spread_cents": int(spread * 100),
        "bid_depth": str(bid_size),
        "ask_depth": str(ask_size),
    }
