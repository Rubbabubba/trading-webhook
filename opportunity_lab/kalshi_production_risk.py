"""Read-only production risk diagnostics. This module cannot authorize execution."""
import json
from datetime import datetime, timezone
from fractions import Fraction
import math

from .kalshi_execution_risk import replay, report, amount


def evaluate(db, snapshot, policy=None, *, now=None, reserved_client_id=None):
    """Use a consistent journal snapshot; policy limits are integer cents.

    No default live allocation is inferred from the demo policy or account cash.
    Passing these checks is necessary, but not sufficient, for live release.
    """
    now = now or datetime.now(timezone.utc)
    blockers = []
    result = {"execution_enabled": False, "live_ready": False,
              "risk_checks_passed": False, "blockers": blockers}
    if now.tzinfo is None:
        raise ValueError("naive_risk_time")
    if policy is None:
        blockers.append("production_risk_policy_missing")
    elif (not isinstance(policy, dict) or set(policy) != {"capital_limit_cents", "daily_loss_limit_cents"}
          or any(type(v) is not int or v <= 0 for v in policy.values())):
        blockers.append("production_risk_policy_invalid")
        policy = None
    try:
        start, end = snapshot["started_at"], snapshot["observed_at"]
        if (any(type(v) not in (int, float) or not math.isfinite(v) for v in (start, end))
                or not 0 <= end - start <= 30 or not 0 <= now.timestamp() - end <= 60):
            blockers.append("account_snapshot_stale_or_inconsistent")
        if snapshot.get("environment") != "production" or type(snapshot.get("subaccount")) is not int or snapshot["subaccount"] != 0:
            blockers.append("account_scope_mismatch")
        cash = snapshot["balance"]["balance"]
        if type(cash) is not int or cash < 0:
            raise ValueError("invalid_cash")
        positions = {}
        if not isinstance(snapshot["positions"], list):
            raise ValueError("invalid_positions")
        for row in snapshot["positions"]:
            ticker = row["ticker"]
            qty = amount(row["position_fp"])
            if not isinstance(ticker, str) or not ticker or ticker in positions or qty.denominator != 1:
                raise ValueError("unsupported_position")
            positions[ticker] = int(qty)
        if not isinstance(snapshot["resting_orders"], list):
            raise ValueError("invalid_orders")
        if snapshot["resting_orders"]:
            blockers.append("resting_orders_present")
    except (KeyError, TypeError, ValueError):
        blockers.append("invalid_account_snapshot")
        return result
    own_transaction = not db.in_transaction
    if own_transaction:
        db.execute("BEGIN")
    try:
        scope = db.execute("SELECT environment FROM journal_scope WHERE id=1").fetchone()
        if scope != ("production",):
            blockers.append("production_journal_scope_required")
            return result
        if db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0]:
            blockers.append("journal_stopped")
        records = [dict(payload=json.loads(r[0]), reserve=r[1], state=r[2], filled=r[3], broker_id=r[4])
                   for r in db.execute("SELECT payload,reserve,state,filled,broker_id FROM intents ORDER BY rowid")]
        if any(r["state"] != "terminal" and not (r["state"] == "reserved"
               and r["payload"]["client_order_id"] == reserved_client_id) for r in records):
            blockers.append("orders_not_terminal")
        evidence = {r[0]: json.loads(r[1]) for r in db.execute("SELECT client_id,detail FROM broker_evidence")}
        settlements = [json.loads(r[0]) for r in db.execute("SELECT detail FROM settlements")]
        state = replay(records, evidence, as_of=now, settlements=settlements)
        result["accounting"] = report(state)
        if state["positions"] != {k:v for k,v in positions.items() if v}:
            blockers.append("external_or_unreconciled_position")
        if state["pending_reserves"] * 100 > cash:
            blockers.append("insufficient_available_cash")
        if db.execute("SELECT 1 FROM risk_stops WHERE day=?", (state["day"],)).fetchone():
            blockers.append("daily_loss_stop_latched")
        if policy:
            if state["capital_at_risk"] * 100 > policy["capital_limit_cents"]:
                blockers.append("capital_limit_exceeded")
            if state["daily_low"] <= -Fraction(policy["daily_loss_limit_cents"], 100):
                blockers.append("daily_loss_limit_reached")
    except Exception:
        # Malformed/missing ledger evidence cannot become a passing diagnostic.
        blockers.append("invalid_production_journal")
    finally:
        if own_transaction:
            db.execute("ROLLBACK")
    result["risk_checks_passed"] = not blockers
    return result
