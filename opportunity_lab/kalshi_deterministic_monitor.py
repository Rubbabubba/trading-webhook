"""Deterministic Kalshi health and evidence change detector.

This module never calls an AI service.  It turns the worker's bounded status
document into a compact review packet, durable checkpoint, and usage counters.
"""
from __future__ import annotations

from datetime import datetime, timezone
import argparse
import hashlib
import json
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
MAX_PACKET_BYTES = 12_000


def _iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def _read(path, default):
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


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


def gate_state(status, registration):
    """Evaluate the frozen V10 gate without inference or missing-data passes."""
    gate = registration.get("shadow_gate", {})
    shadow = status.get("evidence", {}).get("v10_shadow", {})
    horizons = [str(value) for value in gate.get("required_markout_seconds", [])]
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
        "complete_horizons": signals > 0 and bool(horizons) and all(
            int(marks.get(h, 0)) >= signals for h in horizons
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
    if int(evidence.get("ending_position_contracts") or 0) > 1:
        result.append("inventory_limit_breached")
    # A working post-only order is represented as unresolved.  More than one
    # violates the frozen single-order invariant.
    if int(evidence.get("unresolved_orders") or 0) > 1:
        result.append("multiple_unresolved_orders")
    return sorted(set(result))


def _evidence_snapshot(status):
    evidence = status.get("evidence", {})
    shadow = evidence.get("v10_shadow", {})
    return {
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


def check(status, checkpoint, registration, *, now):
    active = faults(status, now=now)
    previous_faults = checkpoint.get("active_faults", [])
    current_evidence = _evidence_snapshot(status)
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
    prior_gate = checkpoint.get("gate_state")
    new_faults = sorted(set(active) - set(previous_faults))
    recovered = sorted(set(previous_faults) - set(active))
    persistent = active if repeats == PERSISTENT_FAULT_CHECKS else []
    gate_transition = prior_gate is not None and gate["state"] != prior_gate
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
        "investigation_needed": bool(triggers),
        "trigger_categories": triggers,
        "references": {
            "status": "status.json",
            "worker_database": "worker.sqlite3",
            "order_journal": "journal.sqlite3",
            "registration": "configs/kalshi_maker_v10_20260921/registration.json",
        },
    }
    state_fingerprint = hashlib.sha256(json.dumps({
        "faults": active, "gate": gate["state"],
    }, sort_keys=True).encode()).hexdigest()[:16]
    fingerprint = hashlib.sha256(json.dumps({
        "faults": active, "gate": gate["state"], "triggers": triggers,
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
    packet, next_checkpoint, duplicate = check(status, checkpoint, registration, now=now)
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
