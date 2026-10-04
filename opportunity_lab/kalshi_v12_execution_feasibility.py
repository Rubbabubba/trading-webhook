"""Decision-time execution check for the frozen V12 microprice signal.

This is a diagnostic, not an execution strategy. The microprice is a weighted
average of the two best prices, so crossing the spread cannot have positive
edge under the same fair-value estimate, even before exchange fees.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation


def crossing_diagnostic(signal):
    if not isinstance(signal, dict) or signal.get("strategy_id") != "microprice_value_maker_v12_shadow":
        return {"eligible": False, "reason": "not_v12_signal"}
    try:
        bid = Decimal(str(signal["price_cents"]))
        spread = Decimal(str(signal["spread_cents"]))
        fair = Decimal(str(signal["fair_value_cents"]))
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return {"eligible": False, "reason": "malformed_signal"}
    if not all(value.is_finite() for value in (bid, spread, fair)) or spread <= 0:
        return {"eligible": False, "reason": "malformed_signal"}
    ask = bid + spread
    gross = fair - ask
    return {
        "eligible": gross > 0,
        "reason": "positive_pre_fee_crossing_edge" if gross > 0 else "nonpositive_pre_fee_crossing_edge",
        "bid_cents": float(bid),
        "ask_cents": float(ask),
        "fair_value_cents": float(fair),
        "gross_crossing_edge_cents": float(gross),
        "fees_included": False,
        "execution_enabled": False,
    }
