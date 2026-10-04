"""Deterministic Kalshi health and evidence change detector.

This module never calls an AI service.  It turns the worker's bounded status
document into a compact review packet, durable checkpoint, and usage counters.
"""
from __future__ import annotations

from datetime import datetime, timezone
import argparse
import hashlib
import json
import math
from pathlib import Path
import time


CHECK_INTERVAL_SECONDS = 60
STATUS_STALE_SECONDS = 180
PERSISTENT_FAULT_CHECKS = 3
EVIDENCE_STALE_SECONDS = 15 * 60
DAILY_REVIEW_SECONDS = 24 * 60 * 60
EXPECTED_STRATEGY = "stable_balanced_maker_v9"
EXPECTED_EXECUTION_POLICY = "v9_retired_after_8_losses_20260924"
EXPECTED_V10 = "queue_toxicity_maker_v10_shadow"
EXPECTED_V11 = "strong_imbalance_maker_v11_shadow"
EXPECTED_V12 = "microprice_value_maker_v12_shadow"
EXPECTED_V12_TRIAL = "microprice_value_maker_v12_demo_trial"
EXPECTED_V12_TRIAL_POLICY = "v12_one_contract_demo_trial_20261001"
EXPECTED_V12_FILLABILITY = "microprice_value_maker_v12_fillability_trial"
EXPECTED_V12_FILLABILITY_POLICY = "v12_one_tick_fillability_trial_20261002"
MAX_PACKET_BYTES = 16_000


def _iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def _read(path, default):
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def _research_snapshot(path, *, now):
    """Bound the independent research sleeves' read-only status for Life OS."""
    source = _read(path, {})
    if not isinstance(source, dict):
        return None
    if source.get("schema") != "kalshi_sleeve_comparison_v1" or source.get("execution_enabled") is not False:
        return None
    try:
        generated = datetime.fromisoformat(source["generated_at"].replace("Z", "+00:00"))
        age = now - generated.timestamp()
    except (KeyError, TypeError, ValueError):
        return None
    if generated.tzinfo is None or age < -300 or age > 900:
        return None
    rows = source.get("sleeves")
    if not isinstance(rows, list) or len(rows) > 12:
        return None
    sleeves = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("strategy_id"), str):
            return None
        if row.get("execution_enabled") is not False or len(row["strategy_id"]) > 120:
            return None
        counts = {}
        for key in ("independent_events", "complete_observations"):
            value = row.get(key)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 1_000_000):
                return None
            counts[key] = value
        metrics = {}
        for key in ("cost_stressed_net_cents", "event_clustered_95pct_lower_bound_cents", "maximum_drawdown_cents"):
            value = row.get(key)
            if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))
                                      or not math.isfinite(value) or abs(value) > 1_000_000_000):
                return None
            metrics[key] = value
        sleeves.append({"strategy_id": row["strategy_id"], **counts, **metrics})
    coverage = source.get("coverage") or {}
    if not isinstance(coverage, dict):
        return None
    mve = coverage.get("multivariate_coverage") or {}
    if not isinstance(mve, dict):
        return None
    multivariate_scanned = mve.get("markets_scanned")
    if multivariate_scanned is not None and (isinstance(multivariate_scanned, bool)
            or not isinstance(multivariate_scanned, int) or not 0 <= multivariate_scanned <= 1_000_000_000):
        return None
    factory = source.get("strategy_factory") or {}
    if (not isinstance(factory, dict)
            or factory.get("schema") not in (None, "kalshi_strategy_factory_v1")
            or factory.get("execution_enabled") not in (None, False)):
        return None
    factory_rows = factory.get("candidates") or []
    if not isinstance(factory_rows, list) or len(factory_rows) > 8:
        return None
    candidates = []
    for row in factory_rows:
        if not isinstance(row, dict) or row.get("execution_enabled") is not False:
            return None
        spec = row.get("spec") or {}
        if (not isinstance(spec, dict)
                or spec.get("primitive") != "buy_at_observed_ask_to_settlement"
                or spec.get("stratum") not in ("sports", "non_sports")
                or spec.get("price_bin") not in ("2-5", "5-10", "90-95", "95-98")
                or row.get("state") not in ("shadow", "rejected", "demo_trial_candidate")):
            return None
        name = row.get("strategy_id")
        if not isinstance(name, str) or len(name) > 80:
            return None
        count = row.get("complete_independent_events")
        if isinstance(count, bool) or not isinstance(count, int) or not 0 <= count <= 1_000_000:
            return None
        held_count = row.get("holdout_complete_independent_events", 0)
        if isinstance(held_count, bool) or not isinstance(held_count, int) or not 0 <= held_count <= 1_000_000:
            return None
        for metric in ("cost_stressed_net_cents", "event_cluster_lower_bound_cents",
                       "holdout_cost_stressed_net_cents", "holdout_event_cluster_lower_bound_cents"):
            value = row.get(metric)
            if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))
                                      or not math.isfinite(value) or abs(value) > 1_000_000_000):
                return None
        candidates.append({
            "strategy_id": name, "state": row["state"],
            "stratum": spec["stratum"], "price_bin": spec["price_bin"],
            "registered_at": row.get("registered_at"),
            "complete_independent_events": count,
            "cost_stressed_net_cents": row.get("cost_stressed_net_cents"),
            "event_cluster_lower_bound_cents": row.get("event_cluster_lower_bound_cents"),
            "holdout_started_at": row.get("holdout_started_at"),
            "holdout_state": row.get("holdout_state"),
            "holdout_reason": row.get("holdout_reason"),
            "holdout_complete_independent_events": held_count,
            "holdout_cost_stressed_net_cents": row.get("holdout_cost_stressed_net_cents"),
            "holdout_event_cluster_lower_bound_cents": row.get("holdout_event_cluster_lower_bound_cents"),
            "reason": row.get("reason"),
        })
    return {
        "generated_at": generated.isoformat(),
        "execution_enabled": False,
        "catalog_complete": coverage.get("coverage_complete") is True,
        "multivariate_scanned": multivariate_scanned,
        "sleeves": sleeves,
        "strategy_factory": {"execution_enabled": False, "candidates": candidates},
    }


def _atomic(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if len(text.encode()) > MAX_PACKET_BYTES:
        raise ValueError("monitor_packet_too_large")
    temporary.write_text(text)
    temporary.replace(path)


def _number(value, default=0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def gate_state(status, registration, evidence_key="v10_shadow"):
    """Evaluate a registered shadow gate without inference or missing-data passes."""
    gate = registration.get("shadow_gate", {})
    shadow = status.get("evidence", {}).get(evidence_key, {})
    horizons = [str(value) for value in gate.get(
        "required_markout_seconds",
        registration.get("fixed_parameters", {}).get("required_markout_seconds", []),
    )]
    marks = shadow.get("markout_records", {})
    pnl = shadow.get("stressed_markout_pnl_cents", {})
    lcbs = shadow.get("event_cluster_lcb_cents", {})
    signals = int(shadow.get("signals") or 0)
    complete = int(shadow.get("complete_signals") or 0)
    events = int(shadow.get("independent_events") or 0)
    requirements = {
        "execution_disabled": shadow.get("execution_enabled") is False,
        "minimum_independent_events": events >= int(gate.get("minimum_independent_events", 0)),
        "minimum_complete_signals": complete >= int(gate.get("minimum_complete_signals", 0)),
        "complete_horizons": signals >= complete > 0 and bool(horizons) and all(
            int(marks.get(h, 0)) >= complete for h in horizons
        ),
        "positive_stressed_net": bool(horizons) and all(_number(pnl.get(h)) > 0 for h in horizons),
        "positive_event_cluster_lcb": bool(horizons) and all(
            lcbs.get(h) is not None and _number(lcbs[h]) > 0 for h in horizons
        ),
    }
    return {
        "state": "passed" if all(requirements.values()) else "collecting",
        "requirements": requirements,
        "signals": signals,
        "complete_signals": complete,
        "independent_events": events,
        "markout_records": {h: int(marks.get(h, 0)) for h in horizons},
        "stressed_markout_pnl_cents": {h: _number(pnl.get(h)) for h in horizons},
        "event_cluster_lcb_cents": {h: lcbs.get(h) for h in horizons},
    }


def faults(status, *, now):
    """Return stable fault codes suitable for deduplication."""
    result = []
    try:
        observed = datetime.fromisoformat(status["at"].replace("Z", "+00:00")).timestamp()
    except (KeyError, TypeError, ValueError):
        observed = 0
    if now - observed > STATUS_STALE_SECONDS:
        result.append("status_stale")
    if status.get("phase") not in {"running", "reconciling"}:
        result.append("worker_not_running")
    if status.get("errors"):
        result.append("worker_errors")
    if status.get("environment") != "demo":
        result.append("environment_not_demo")
    if status.get("production_execution_enabled") is not False:
        result.append("production_execution_changed")
    if status.get("strategy_id") != EXPECTED_STRATEGY:
        result.append("strategy_changed")
    if status.get("execution_policy_id") != EXPECTED_EXECUTION_POLICY:
        result.append("execution_policy_changed")
    if status.get("strategy_execution_enabled") is not False:
        result.append("retired_strategy_execution_enabled")
    evidence = status.get("evidence", {})
    shadow = evidence.get("v10_shadow", {})
    if shadow.get("strategy_id") != EXPECTED_V10:
        result.append("v10_strategy_changed")
    if shadow.get("execution_enabled") is not False:
        result.append("v10_execution_enabled")
    challenger = evidence.get("v11_shadow", {})
    if challenger.get("strategy_id") != EXPECTED_V11:
        result.append("v11_strategy_changed")
    if challenger.get("execution_enabled") is not False:
        result.append("v11_execution_enabled")
    v12 = evidence.get("v12_shadow", {})
    if v12.get("strategy_id") != EXPECTED_V12:
        result.append("v12_strategy_changed")
    if v12.get("execution_enabled") is not False:
        result.append("v12_execution_enabled")
    trial = status.get("v12_demo_trial", {})
    if status.get("v12_demo_trial_enabled") is not True:
        result.append("v12_demo_trial_not_enabled")
    if status.get("v12_demo_trial_policy_id") != EXPECTED_V12_TRIAL_POLICY:
        result.append("v12_demo_trial_policy_changed")
    if trial.get("strategy_id") != EXPECTED_V12_TRIAL:
        result.append("v12_demo_trial_strategy_changed")
    if trial.get("max_order_attempts") != 50 or trial.get("max_fills") != 10:
        result.append("v12_demo_trial_limits_changed")
    if trial.get("loss_stop_cents") != 25:
        result.append("v12_demo_trial_loss_stop_changed")
    if trial.get("shadow_gate_passed") is not True:
        result.append("v12_demo_trial_shadow_gate_lost")
    fillability = status.get("v12_fillability_trial", {})
    if status.get("v12_fillability_trial_enabled") is not True:
        result.append("v12_fillability_trial_not_enabled")
    if status.get("v12_fillability_trial_policy_id") != EXPECTED_V12_FILLABILITY_POLICY:
        result.append("v12_fillability_trial_policy_changed")
    if fillability.get("strategy_id") != EXPECTED_V12_FILLABILITY:
        result.append("v12_fillability_trial_strategy_changed")
    if (fillability.get("max_order_attempts") != 200
            or fillability.get("max_fills") != 20):
        result.append("v12_fillability_trial_limits_changed")
    if fillability.get("loss_stop_cents") != 100:
        result.append("v12_fillability_trial_loss_stop_changed")
    if fillability.get("price_improvement_cents") != 1:
        result.append("v12_fillability_trial_quote_changed")
    if fillability.get("shadow_gate_passed") is not True:
        result.append("v12_fillability_trial_shadow_gate_lost")
    if int(evidence.get("ending_position_contracts") or 0) > 1:
        result.append("inventory_limit_breached")
    # A working post-only order is represented as unresolved.  More than one
    # violates the frozen single-order invariant.
    if int(evidence.get("unresolved_orders") or 0) > 1:
        result.append("multiple_unresolved_orders")
    return sorted(set(result))


def _evidence_snapshot(status):
    evidence = status.get("evidence", {})
    discovery = evidence.get("market_discovery", {})
    last_complete = discovery.get("last_completed") or {}
    complete = (
        discovery if discovery.get("in_progress") is False
        and discovery.get("coverage_accounting_complete") is True
        else (last_complete or discovery)
    )
    complete_at = complete.get("completed_at")
    try:
        complete_at = _iso(float(complete_at)) if complete_at is not None else None
    except (TypeError, ValueError, OverflowError):
        complete_at = None
    families = complete.get("market_families")
    family_count = len(families) if isinstance(families, dict) else int(families or 0)
    shadow = evidence.get("v10_shadow", {})
    challenger = evidence.get("v11_shadow", {})
    v12 = evidence.get("v12_shadow", {})
    holdout = evidence.get("v12_quote_holdout", {})
    crossing = evidence.get("v12_crossing_feasibility", {})
    trial = status.get("v12_demo_trial", {})
    fillability = status.get("v12_fillability_trial", {})
    return {
        "market_discovery_complete": bool(complete_at and complete.get("coverage_accounting_complete") is True),
        "market_discovery_last_complete_at": complete_at,
        "market_discovery_scanned": int(complete.get("markets_scanned") or discovery.get("markets_scanned") or 0),
        "market_discovery_eligible": int(complete.get("eligible_markets") or discovery.get("eligible_markets") or 0),
        "market_discovery_families": family_count,
        "post_only_attempts": int(evidence.get("post_only_attempts") or 0),
        "maker_fills": int(evidence.get("maker_fills") or 0),
        "terminal_orders": int(evidence.get("terminal_orders") or 0),
        "unresolved_orders": int(evidence.get("unresolved_orders") or 0),
        "ending_position_contracts": int(evidence.get("ending_position_contracts") or 0),
        "v10_signals": int(shadow.get("signals") or 0),
        "v10_evaluations": int(shadow.get("evaluations") or 0),
        "v10_last_evaluation_at": shadow.get("last_evaluation_at"),
        "v10_rejection_reasons": shadow.get("rejection_reasons", {}),
        "v10_complete_signals": int(shadow.get("complete_signals") or 0),
        "v10_independent_events": int(shadow.get("independent_events") or 0),
        "v10_markout_records": shadow.get("markout_records", {}),
        "v11_signals": int(challenger.get("signals") or 0),
        "v11_evaluations": int(challenger.get("evaluations") or 0),
        "v11_last_evaluation_at": challenger.get("last_evaluation_at"),
        "v11_rejection_reasons": challenger.get("rejection_reasons", {}),
        "v11_complete_signals": int(challenger.get("complete_signals") or 0),
        "v11_independent_events": int(challenger.get("independent_events") or 0),
        "v11_markout_records": challenger.get("markout_records", {}),
        "v11_stressed_markout_pnl_cents": challenger.get(
            "stressed_markout_pnl_cents", {}
        ),
        "v11_event_cluster_lcb_cents": challenger.get("event_cluster_lcb_cents", {}),
        "v11_automatic_rejection_triggered": bool(
            challenger.get("automatic_rejection_triggered")
        ),
        "v12_signals": int(v12.get("signals") or 0),
        "v12_evaluations": int(v12.get("evaluations") or 0),
        "v12_last_evaluation_at": v12.get("last_evaluation_at"),
        "v12_rejection_reasons": v12.get("rejection_reasons", {}),
        "v12_complete_signals": int(v12.get("complete_signals") or 0),
        "v12_independent_events": int(v12.get("independent_events") or 0),
        "v12_markout_records": v12.get("markout_records", {}),
        "v12_stressed_markout_pnl_cents": v12.get("stressed_markout_pnl_cents", {}),
        "v12_event_cluster_lcb_cents": v12.get("event_cluster_lcb_cents", {}),
        "v12_automatic_rejection_triggered": bool(v12.get("automatic_rejection_triggered")),
        "v12_holdout_start_at": holdout.get("holdout_start_at"),
        "v12_holdout_registration_sha256": holdout.get("registration_sha256"),
        "v12_holdout_code_frozen": bool(
            (holdout.get("gates") or {}).get("strategy_code_frozen")
            and (holdout.get("gates") or {}).get("evaluator_code_frozen")
        ),
        "v12_holdout_complete_signals": int(holdout.get("complete_signals") or 0),
        "v12_holdout_independent_events": int(holdout.get("independent_events") or 0),
        "v12_holdout_active_days": int(holdout.get("active_days") or 0),
        "v12_holdout_event_cluster_lcb_cents": holdout.get("event_cluster_lower_bound_cents", {}),
        "v12_holdout_passed": bool(holdout.get("passed")),
        "v12_crossing_gross_edge_cents": crossing.get("gross_crossing_edge_cents"),
        "v12_crossing_reason": crossing.get("reason"),
        "v12_trial_attempts": int(trial.get("attempts") or 0),
        "v12_trial_fills": int(trial.get("fills") or 0),
        "v12_trial_shadow_gate_passed": bool(trial.get("shadow_gate_passed")),
        "v12_trial_stop_reason": trial.get("reason"),
        "v12_fillability_attempts": int(fillability.get("attempts") or 0),
        "v12_fillability_fills": int(fillability.get("fills") or 0),
        "v12_fillability_terminal_orders": int(
            fillability.get("terminal_orders") or 0
        ),
        "v12_fillability_independent_events": int(
            fillability.get("attempted_independent_events") or 0
        ),
        "v12_fillability_market_families": int(
            fillability.get("attempted_market_families") or 0
        ),
        "v12_fillability_flat_pnl_cents": fillability.get("flat_pnl_cents"),
        "v12_fillability_stop_reason": fillability.get("reason"),
    }


def _delta(current, previous):
    result = {}
    for key, value in current.items():
        old = previous.get(key)
        if isinstance(value, int) and isinstance(old, int):
            result[key] = value - old
        elif value != old:
            result[key] = {"before": old, "after": value}
    return result


def check(status, checkpoint, registration, *, now, v12_registration=None):
    active = faults(status, now=now)
    previous_faults = checkpoint.get("active_faults", [])
    current_evidence = _evidence_snapshot(status)
    if (current_evidence["v12_holdout_start_at"]
            and not current_evidence["v12_holdout_code_frozen"]):
        active = sorted(set(active) | {"v12_holdout_code_changed"})
    prior_evaluations = checkpoint.get("evidence", {}).get("v10_evaluations")
    evaluations = current_evidence["v10_evaluations"]
    last_progress_at = float(checkpoint.get("last_v10_progress_at") or now)
    if prior_evaluations is None or evaluations != prior_evaluations:
        last_progress_at = now
    elif (status.get("phase") == "running"
          and now - last_progress_at >= EVIDENCE_STALE_SECONDS):
        active = sorted(set(active) | {"v10_evidence_stalled"})

    repeats = (int(checkpoint.get("fault_repeats") or 0) + 1
               if active == previous_faults and active else (1 if active else 0))
    gate = gate_state(status, registration)
    v12_gate = gate_state(
        status, v12_registration or registration, evidence_key="v12_shadow"
    )
    prior_gate = checkpoint.get("gate_state")
    prior_v12_gate = checkpoint.get("v12_gate_state")
    v12_rejected = current_evidence["v12_automatic_rejection_triggered"]
    holdout_passed = current_evidence["v12_holdout_passed"]
    prior_holdout_passed = checkpoint.get("v12_holdout_passed")
    prior_v12_rejected = bool(checkpoint.get("v12_automatic_rejection_triggered"))
    new_faults = sorted(set(active) - set(previous_faults))
    recovered = sorted(set(previous_faults) - set(active))
    persistent = active if repeats == PERSISTENT_FAULT_CHECKS else []
    gate_transition = prior_gate is not None and gate["state"] != prior_gate
    v12_gate_transition = (
        prior_v12_gate is not None and v12_gate["state"] != prior_v12_gate
    )
    daily_due = now - float(checkpoint.get("last_daily_review_at") or 0) >= DAILY_REVIEW_SECONDS
    triggers = []
    if new_faults:
        triggers.append("new_fault")
    if persistent:
        triggers.append("persistent_fault")
    if recovered:
        triggers.append("recovery")
    if gate_transition:
        triggers.append("evidence_gate_transition")
    if v12_gate_transition:
        triggers.append("v12_evidence_gate_transition")
    if v12_rejected and not prior_v12_rejected:
        triggers.append("v12_automatic_rejection")
    if prior_holdout_passed is False and holdout_passed:
        triggers.append("v12_holdout_gate_passed")
    if daily_due:
        triggers.append("daily_review")
    packet = {
        "schema": "kalshi_compact_review_packet_v1",
        "generated_at": _iso(now),
        "worker": {
            "strategy_id": status.get("strategy_id"),
            "environment": status.get("environment"),
            "phase": status.get("phase"),
            "production_execution_enabled": status.get("production_execution_enabled"),
            "strategy_execution_enabled": status.get("strategy_execution_enabled"),
            "execution_policy_id": status.get("execution_policy_id"),
            "v12_demo_trial_enabled": status.get("v12_demo_trial_enabled"),
            "v12_demo_trial_policy_id": status.get("v12_demo_trial_policy_id"),
            "v12_fillability_trial_enabled": status.get(
                "v12_fillability_trial_enabled"
            ),
            "v12_fillability_trial_policy_id": status.get(
                "v12_fillability_trial_policy_id"
            ),
            "status_at": status.get("at"),
            "status_age_seconds": max(0, round(now - datetime.fromisoformat(
                status.get("at", "1970-01-01T00:00:00+00:00").replace("Z", "+00:00")
            ).timestamp(), 1)),
        },
        "health": {"faults": active, "new_faults": new_faults,
                   "persistent_faults": persistent, "recovered": recovered},
        "evidence": current_evidence,
        "evidence_delta": _delta(current_evidence, checkpoint.get("evidence", {})),
        "v10_gate": gate,
        "v12_gate": v12_gate,
        "investigation_needed": bool(triggers),
        "trigger_categories": triggers,
        "references": {
            "status": "status.json",
            "worker_database": "worker.sqlite3",
            "order_journal": "journal.sqlite3",
            "registration": "configs/kalshi_maker_v10_20260921/registration.json",
            "challenger_registration": "configs/kalshi_maker_v11_20260927/registration.json",
            "frequency_challenger_registration": "configs/kalshi_maker_v12_20260930/registration.json",
            "v12_demo_trial_registration": "configs/kalshi_maker_v12_demo_trial_20261001/registration.json",
            "v12_fillability_trial_registration": "configs/kalshi_maker_v12_fillability_trial_20261002/registration.json",
        },
    }
    state_fingerprint = hashlib.sha256(json.dumps({
        "faults": active, "gate": gate["state"], "v12_gate": v12_gate["state"],
        "v12_rejected": v12_rejected,
    }, sort_keys=True).encode()).hexdigest()[:16]
    fingerprint = hashlib.sha256(json.dumps({
        "faults": active, "gate": gate["state"], "v12_gate": v12_gate["state"],
        "v12_rejected": v12_rejected, "triggers": triggers,
    }, sort_keys=True).encode()).hexdigest()[:16]
    duplicate = bool(
        (triggers and fingerprint == checkpoint.get("last_escalation_fingerprint"))
        or (not triggers and active and state_fingerprint == checkpoint.get("last_fault_fingerprint"))
    )
    if duplicate:
        packet["investigation_needed"] = False
        packet["trigger_categories"] = []
        packet["duplicate_escalation_suppressed"] = True
    next_checkpoint = {
        "schema": "kalshi_monitor_checkpoint_v1",
        "last_check_at": now,
        "active_faults": active,
        "fault_repeats": repeats,
        "gate_state": gate["state"],
        "v12_gate_state": v12_gate["state"],
        "v12_automatic_rejection_triggered": v12_rejected,
        "v12_holdout_passed": holdout_passed,
        "evidence": current_evidence,
        "last_daily_review_at": now if daily_due else checkpoint.get("last_daily_review_at", now),
        "last_escalation_fingerprint": (
            fingerprint if packet["investigation_needed"] else checkpoint.get("last_escalation_fingerprint")
        ),
        "last_fault_fingerprint": state_fingerprint if active else None,
        "last_v10_progress_at": last_progress_at,
    }
    return packet, next_checkpoint, duplicate


def run_check(data_root, status=None, *, now=None, force=False):
    """Run one cheap check and persist its bounded artifacts."""
    root = Path(data_root)
    monitor = root / "monitor"
    checkpoint_path = monitor / "checkpoint.json"
    packet_path = monitor / "review_packet.json"
    metrics_path = monitor / "metrics.json"
    escalation_path = monitor / "escalation.json"
    checkpoint = _read(checkpoint_path, {})
    now = time.time() if now is None else float(now)
    if not force and now - float(checkpoint.get("last_check_at") or 0) < CHECK_INTERVAL_SECONDS:
        return {"checked": False, "reason": "interval_not_due"}
    if status is None:
        status = _read(root / "status.json", {})
    registration = _read(
        Path(__file__).resolve().parent.parent / "configs" /
        "kalshi_maker_v10_20260921" / "registration.json", {}
    )
    v12_registration = _read(
        Path(__file__).resolve().parent.parent / "configs" /
        "kalshi_maker_v12_20260930" / "registration.json", {}
    )
    packet, next_checkpoint, duplicate = check(
        status, checkpoint, registration, now=now, v12_registration=v12_registration
    )
    trial = status.get("factory_demo_trial")
    if isinstance(trial, dict):
        packet["factory_demo_trial"] = {
            "protocol": trial.get("protocol"),
            "attempts": trial.get("attempts"),
            "fills": trial.get("fills"),
            "attempts_today": trial.get("attempts_today"),
            "independent_events": trial.get("independent_events"),
            "independent_days": trial.get("independent_days"),
            "terminal_orders": trial.get("terminal_orders"),
            "unresolved_orders": trial.get("unresolved_orders"),
            "realized_net_cents": trial.get("realized_net_cents"),
            "fees_cents": trial.get("fees_cents"),
            "flat_at_review": trial.get("flat_at_review"),
            "fees_reconciled": trial.get("fees_reconciled"),
            "execution_environment": trial.get("execution_environment"),
            "live_execution_enabled": trial.get("live_execution_enabled"),
        }
    packet["research_sleeves"] = _research_snapshot(root / "sleeve_comparison.json", now=now)
    metrics = _read(metrics_path, {
        "schema": "kalshi_monitor_metrics_v1", "checks": 0,
        "checks_without_ai": 0, "investigations_requested": 0,
        "duplicate_escalations_suppressed": 0, "trigger_counts": {},
    })
    metrics["checks"] = int(metrics.get("checks") or 0) + 1
    # This checker has no model client. Every invocation itself is zero-AI.
    metrics["checks_without_ai"] = int(metrics.get("checks_without_ai") or 0) + 1
    if packet["investigation_needed"]:
        metrics["investigations_requested"] = int(metrics.get("investigations_requested") or 0) + 1
        for category in packet["trigger_categories"]:
            counts = metrics.setdefault("trigger_counts", {})
            counts[category] = int(counts.get(category) or 0) + 1
        _atomic(escalation_path, packet)
    if duplicate:
        metrics["duplicate_escalations_suppressed"] = int(
            metrics.get("duplicate_escalations_suppressed") or 0
        ) + 1
    metrics["updated_at"] = _iso(now)
    _atomic(packet_path, packet)
    _atomic(checkpoint_path, next_checkpoint)
    _atomic(metrics_path, metrics)
    return {"checked": True, "investigation_needed": packet["investigation_needed"],
            "triggers": packet["trigger_categories"], "packet": str(packet_path)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="/var/data/kalshi-demo-v9")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps(run_check(args.data_root, force=args.force), sort_keys=True))


if __name__ == "__main__":
    main()
