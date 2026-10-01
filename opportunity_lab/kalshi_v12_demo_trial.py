"""Bounded Demo execution policy for the V12 microprice challenger."""
from __future__ import annotations


STRATEGY_ID = "microprice_value_maker_v12_demo_trial"
CLIENT_ID_PREFIX = "v12-trial-"
EXECUTION_ENABLED = True
MAX_ORDER_ATTEMPTS = 50
MAX_FILLS = 10
MAX_FLAT_BALANCE_LOSS_CENTS = 25


def shadow_gate_passed(shadow):
    """Fail closed unless every preregistered V12 shadow requirement passed."""
    horizons = ("5", "30", "300")
    signals = int(shadow.get("signals") or 0)
    complete = int(shadow.get("complete_signals") or 0)
    events = int(shadow.get("independent_events") or 0)
    marks = shadow.get("markout_records", {})
    pnl = shadow.get("stressed_markout_pnl_cents", {})
    lcbs = shadow.get("event_cluster_lcb_cents", {})
    return (
        shadow.get("strategy_id") == "microprice_value_maker_v12_shadow"
        and shadow.get("execution_enabled") is False
        and shadow.get("automatic_rejection_triggered") is False
        and signals >= 100
        and complete >= 100
        and events >= 30
        # New shadow signals can be awaiting their 300-second observation while
        # an earlier, fully observed sample already satisfies the gate.  Evaluate
        # the registered complete sample rather than requiring the live collector
        # to have no in-flight observations at the instant of an entry decision.
        and signals >= complete
        and all(int(marks.get(h) or 0) >= complete for h in horizons)
        and all(float(pnl.get(h) or 0) > 0 for h in horizons)
        and all(lcbs.get(h) is not None and float(lcbs[h]) > 0 for h in horizons)
    )


def trial_decision(*, shadow, attempts, fills, start_balance_cents,
                   current_flat_balance_cents, event_locked):
    """Return an auditable authorization decision for one new Demo quote."""
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
        "start_balance_cents": start_balance_cents,
        "current_flat_balance_cents": current_flat_balance_cents,
        "loss_stop_cents": MAX_FLAT_BALANCE_LOSS_CENTS,
        "max_order_attempts": MAX_ORDER_ATTEMPTS,
        "max_fills": MAX_FILLS,
        "shadow_gate_passed": shadow_gate_passed(shadow),
    }
