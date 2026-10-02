"""Bounded Demo fillability trial for the V12 microprice challenger."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from .kalshi_v12_demo_trial import shadow_gate_passed


STRATEGY_ID = "microprice_value_maker_v12_fillability_trial"
CLIENT_ID_PREFIX = "v12-fill-"
EXECUTION_ENABLED = True
MAX_ORDER_ATTEMPTS = 200
MAX_FILLS = 20
MAX_FLAT_BALANCE_LOSS_CENTS = 100
FILL_RATE_CHECKPOINT_ATTEMPTS = 100
MIN_FILLS_AT_CHECKPOINT = 5
PRICE_IMPROVEMENT_CENTS = 1
MIN_GROSS_EDGE_CENTS = Decimal("4")
COST_STRESS_CENTS = Decimal("2")
MIN_REMAINING_STRESSED_EDGE_CENTS = Decimal("1")


def improved_quote(signal):
    """Improve a frozen V12 signal by one tick without consuming its edge.

    The original V12 signal is preserved.  This function creates a separate
    execution context and fails closed if the quote would leave less than one
    cent after the registered two-cent cost stress.
    """
    if not isinstance(signal, dict) or signal.get("strategy_id") != "microprice_value_maker_v12_shadow":
        return None, "not_v12_signal"
    try:
        source_price = int(signal["price_cents"])
        spread = int(signal["spread_cents"])
        gross_edge = Decimal(str(signal["gross_edge_cents"]))
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None, "malformed_v12_signal"
    if gross_edge < MIN_GROSS_EDGE_CENTS:
        return None, "insufficient_edge_for_improvement"
    if spread <= PRICE_IMPROVEMENT_CENTS:
        return None, "spread_too_narrow_for_post_only_improvement"
    price = source_price + PRICE_IMPROVEMENT_CENTS
    if not 1 <= price <= 99:
        return None, "improved_price_out_of_range"
    remaining = gross_edge - PRICE_IMPROVEMENT_CENTS - COST_STRESS_CENTS
    if remaining < MIN_REMAINING_STRESSED_EDGE_CENTS:
        return None, "insufficient_stressed_edge_after_improvement"
    return {
        **signal,
        "execution_strategy_id": STRATEGY_ID,
        "source_signal_price_cents": source_price,
        "price_cents": price,
        "price_improvement_cents": PRICE_IMPROVEMENT_CENTS,
        "quote_location": "selected_outcome_best_bid_plus_one",
        "stressed_edge_after_improvement_cents": format(remaining, "f"),
        "post_only": True,
    }, "eligible"


def trial_decision(*, shadow, attempts, fills, start_balance_cents,
                   current_flat_balance_cents, event_locked):
    """Return an auditable authorization decision for one improved Demo quote."""
    reason = None
    if not EXECUTION_ENABLED:
        reason = "trial_disabled"
    elif not shadow_gate_passed(shadow):
        reason = "shadow_gate_not_passed"
    elif event_locked:
        reason = "event_already_entered"
    elif attempts >= MAX_ORDER_ATTEMPTS:
        reason = "trial_order_cap_reached"
    elif fills >= MAX_FILLS:
        reason = "trial_fill_cap_reached"
    elif attempts >= FILL_RATE_CHECKPOINT_ATTEMPTS and fills < MIN_FILLS_AT_CHECKPOINT:
        reason = "insufficient_fill_rate"
    elif start_balance_cents is None or current_flat_balance_cents is None:
        reason = "flat_balance_unavailable"
    elif current_flat_balance_cents <= start_balance_cents - MAX_FLAT_BALANCE_LOSS_CENTS:
        reason = "trial_loss_stop"
    return {
        "allowed": reason is None,
        "reason": reason or "authorized",
        "strategy_id": STRATEGY_ID,
        "attempts": attempts,
        "fills": fills,
        "fill_rate": fills / attempts if attempts else 0.0,
        "start_balance_cents": start_balance_cents,
        "current_flat_balance_cents": current_flat_balance_cents,
        "flat_pnl_cents": (
            None if start_balance_cents is None or current_flat_balance_cents is None
            else current_flat_balance_cents - start_balance_cents
        ),
        "loss_stop_cents": MAX_FLAT_BALANCE_LOSS_CENTS,
        "max_order_attempts": MAX_ORDER_ATTEMPTS,
        "max_fills": MAX_FILLS,
        "fill_rate_checkpoint_attempts": FILL_RATE_CHECKPOINT_ATTEMPTS,
        "minimum_fills_at_checkpoint": MIN_FILLS_AT_CHECKPOINT,
        "price_improvement_cents": PRICE_IMPROVEMENT_CENTS,
        "minimum_gross_edge_cents": int(MIN_GROSS_EDGE_CENTS),
        "minimum_remaining_stressed_edge_cents": int(MIN_REMAINING_STRESSED_EDGE_CENTS),
        "shadow_gate_passed": shadow_gate_passed(shadow),
    }
