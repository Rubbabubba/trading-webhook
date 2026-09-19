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
    # Quote as aggressively as the spread permits while retaining at least one
    # cent of gross edge to the contemporaneous midpoint.  The previous fixed
    # one-cent improvement sat at the front of the queue but produced no demo
    # fills, even in six- and eight-cent spreads.  This rule improves wider
    # spreads by more without crossing or paying the displayed ask.
    midpoint_cents = (side_bid + side_ask) * 50
    target_cents = int(midpoint_cents - 1)
    bid_cents = side_bid * 100
    ask_cents = side_ask * 100
    if bid_cents.denominator != 1 or ask_cents.denominator != 1:
        return None
    cents = min(target_cents, ask_cents.numerator - 1)
    if not bid_cents.numerator < cents < ask_cents.numerator or not 1 <= cents <= 99:
        return None
    return {
        "side": preferred_side,
        "price_cents": cents,
        "yes_mid": str(yes_mid),
        "spread_cents": int(spread * 100),
        "gross_edge_to_mid_cents": str(midpoint_cents - cents),
        "bid_depth": str(bid_size),
        "ask_depth": str(ask_size),
    }
