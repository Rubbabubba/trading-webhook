"""Prospective strong-imbalance challenger to the frozen V10 strategy.

V11 deliberately inherits every V10 decision rule and adds one fixed filter.
It remains shadow-only until its separately registered evidence gate is met.
"""
from __future__ import annotations

from fractions import Fraction

from .kalshi_maker_v10 import (
    MARKOUT_HORIZONS,
    shadow_decision as v10_shadow_decision,
    stressed_markout,
)


STRATEGY_ID = "strong_imbalance_maker_v11_shadow"
MIN_ABS_IMBALANCE = Fraction(1, 4)
SIGNAL_EVENT_COOLDOWN_SECONDS = 30 * 60


def shadow_decision(history, frame):
    """Return a V10 candidate only when absolute book imbalance is at least 25%."""
    signal, reason = v10_shadow_decision(history, frame)
    if signal is None:
        return None, reason
    if abs(Fraction(signal["imbalance"])) < MIN_ABS_IMBALANCE:
        return None, "weak_imbalance"
    return {**signal, "strategy_id": STRATEGY_ID}, "signal"


def shadow_quote(history, frame):
    return shadow_decision(history, frame)[0]
