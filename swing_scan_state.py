"""Broker-free swing scan state/truth helpers.

This module does not own runtime globals. app.py still owns live scan state for
now; this file only shapes scan truth objects passed into it.
"""

from __future__ import annotations

from typing import Any


SWING_SCAN_STATE_MODULE_VERSION = "patch-705-scanner-ownership-extraction"


def candidate_bearing_truth(scan: dict | None) -> dict:
    scan = dict(scan or {})
    summary = dict(scan.get("summary") or {})
    incremental = dict(summary.get("incremental_scan") or {})
    evaluation = dict(incremental.get("evaluation") or {})

    symbols_eval_total = int(
        summary.get("symbols_eval_total")
        or evaluation.get("evaluated_count")
        or scan.get("scanned")
        or 0
    )
    candidates_total = int(summary.get("candidates_total") or 0)
    eligible_total = int(summary.get("eligible_total") or scan.get("eligible_total") or 0)
    selected_total = int(summary.get("selected_total") or scan.get("selected_total") or 0)

    has_candidate_rows = bool(
        summary.get("top_candidates")
        or summary.get("top_rejection_reasons")
        or summary.get("rejection_counts")
    )

    candidate_bearing = bool(
        symbols_eval_total > 0
        or candidates_total > 0
        or eligible_total > 0
        or selected_total > 0
        or has_candidate_rows
    )

    phase = str(summary.get("scan_truth_phase") or "").strip()
    regime_only = bool(
        phase in {
            "regime_to_candidate_fast_publish",
            "regime_truth_only_before_candidate_eval",
        }
        or summary.get("p474_regime_fast_publish")
        or summary.get("p470_regime_budget_close")
        or (
            int(scan.get("scanned") or 0) <= 0
            and symbols_eval_total <= 0
            and not has_candidate_rows
        )
    )

    return {
        "candidate_bearing": candidate_bearing,
        "trade_judgable": bool(candidate_bearing and not regime_only),
        "regime_only_non_actionable": bool(regime_only and not candidate_bearing),
        "symbols_eval_total": symbols_eval_total,
        "candidates_total": candidates_total,
        "eligible_total": eligible_total,
        "selected_total": selected_total,
        "has_candidate_rows": has_candidate_rows,
        "scan_truth_phase": phase or None,
    }


def normalize_scan_truth_contract(scan: dict | None, source: str = "") -> dict:
    row = dict(scan or {})
    if not row:
        return row

    summary = dict(row.get("summary") or {})
    truth = candidate_bearing_truth(row)

    row["candidate_bearing_scan"] = bool(truth.get("candidate_bearing"))
    row["trade_judgable"] = bool(truth.get("trade_judgable"))
    row["regime_only_non_actionable"] = bool(truth.get("regime_only_non_actionable"))
    row["p475_scan_truth_contract"] = dict(truth)
    row["p475_scan_truth_source"] = source or "scan_truth_contract"

    summary["candidate_bearing_scan"] = bool(truth.get("candidate_bearing"))
    summary["trade_judgable"] = bool(truth.get("trade_judgable"))
    summary["regime_only_non_actionable"] = bool(truth.get("regime_only_non_actionable"))
    summary["p475_scan_truth_contract"] = dict(truth)

    reason = str(row.get("reason") or summary.get("scan_reason") or "").strip()
    if bool(truth.get("regime_only_non_actionable")):
        if reason in {"scan_completed", "partial_scan_completed", ""}:
            row["reason_before_p475_contract"] = reason or None
            row["reason"] = "regime_only_non_actionable"
        if str(summary.get("scan_reason") or "") in {"scan_completed", "partial_scan_completed", ""}:
            summary["scan_reason_before_p475_contract"] = summary.get("scan_reason") or None
            summary["scan_reason"] = "regime_only_non_actionable"

    row["summary"] = summary
    return row


def build_scan_brief(scan: dict | None, truth: dict | None = None) -> dict:
    scan = dict(scan or {})
    summary = dict(scan.get("summary") or {})
    truth = dict(truth or {})
    return {
        "ts_utc": scan.get("ts_utc"),
        "reason": scan.get("reason") or summary.get("scan_reason"),
        "raw_latest_reason": scan.get("raw_latest_reason") or summary.get("raw_latest_reason"),
        "source": scan.get("_scan_source") or scan.get("source"),
        "scanned": scan.get("scanned"),
        "duration_ms": scan.get("duration_ms"),
        "symbols_eval_total": int(truth.get("symbols_eval_total") or 0),
        "candidates_total": int(truth.get("candidates_total") or 0),
        "eligible_total": int(truth.get("eligible_total") or 0),
        "selected_total": int(truth.get("selected_total") or 0),
        "candidate_bearing": bool(truth.get("candidate_bearing")),
        "trade_judgable": bool(truth.get("trade_judgable")),
        "regime_only_non_actionable": bool(truth.get("regime_only_non_actionable")),
    }


def build_canonical_scan_contract(
    *,
    raw_latest_scan: dict | None,
    last_candidate_bearing_scan: dict | None,
    effective_trade_scan: dict | None,
    after_hours_does_not_replace_candidate_truth: bool,
    effective_scan_source: str | None,
    candidate_cache_adopted: bool,
    candidate_cache_source: str | None,
    used_candidate_bearing_fallback: bool,
    raw_latest_reason: str | None,
) -> dict:
    return {
        "raw_latest_scan": dict(raw_latest_scan or {}),
        "last_candidate_bearing_scan": dict(last_candidate_bearing_scan or {}),
        "effective_trade_scan": dict(effective_trade_scan or {}),
        "after_hours_does_not_replace_candidate_truth": bool(after_hours_does_not_replace_candidate_truth),
        "effective_scan_source": effective_scan_source,
        "candidate_cache_adopted": bool(candidate_cache_adopted),
        "candidate_cache_source": candidate_cache_source,
        "used_candidate_bearing_fallback": bool(used_candidate_bearing_fallback),
        "raw_latest_reason": raw_latest_reason or "",
        "module": "swing_scan_state",
        "module_version": SWING_SCAN_STATE_MODULE_VERSION,
    }


def tombstone_historical_background_failure(background_truth: dict | None) -> dict:
    bg = dict(background_truth or {})
    if not bg:
        return {}

    active = bool(bg.get("active"))
    terminal = bool(bg.get("terminal"))
    status = str(bg.get("status") or "").strip().lower()
    reason = str(bg.get("reason") or "").strip()
    exception_type = str(bg.get("exception_type") or "").strip()

    historical_failure = bool(
        terminal
        and not active
        and (
            status == "failed"
            or reason == "background_scan_runtime_budget_exceeded"
            or exception_type == "BackgroundScanRuntimeBudgetExceeded"
        )
    )

    if not historical_failure:
        bg["tombstoned_historical_failure"] = False
        return bg

    return {
        "enabled": bool(bg.get("enabled", True)),
        "status": "historical_tombstone",
        "active": False,
        "terminal": True,
        "historical": True,
        "tombstoned_historical_failure": True,
        "reason": "historical_background_failure_tombstoned",
        "original_reason": reason or None,
        "original_status": status or None,
        "original_exception_type": exception_type or None,
        "scan_attempt_id": bg.get("scan_attempt_id"),
        "started_utc": bg.get("started_utc"),
        "completed_utc": bg.get("completed_utc"),
        "duration_ms": bg.get("duration_ms"),
        "symbols_scanned": bg.get("symbols_scanned"),
        "signals": bg.get("signals"),
        "would_trade": bg.get("would_trade"),
        "blocked": bg.get("blocked"),
        "module": "swing_scan_state",
        "module_version": SWING_SCAN_STATE_MODULE_VERSION,
        "recommended_action": "ignore_historical_background_failure_monitor_current_scanner_status",
    }


def build_scanner_state_contract(
    *,
    scanner_status: str | None,
    latest_scan: dict | None,
    background_truth: dict | None,
    active_warning_codes: list | None,
    recommended_action: str | None = None,
) -> dict:
    def _intish(value: Any) -> int:
        try:
            return int(float(str(value).strip()))
        except Exception:
            return 0

    latest = dict(latest_scan or {})
    background = dict(background_truth or {})
    warnings = [str(x) for x in list(active_warning_codes or []) if str(x or "").strip()]

    reason = str(latest.get("reason") or "").strip()
    selected_symbols = [
        str(sym or "").strip().upper()
        for sym in list(latest.get("selected_symbols") or [])
        if str(sym or "").strip()
    ]
    status = str(scanner_status or "").strip() or "unknown"
    background_active = bool(background.get("active"))
    background_terminal = bool(background.get("terminal"))
    latest_has_candidate_truth = bool(
        _intish(latest.get("scanned")) > 0
        or _intish(latest.get("eligible_total")) > 0
        or _intish(latest.get("selected_total")) > 0
        or bool(selected_symbols)
    )
    latest_terminal = bool(
        reason
        and reason
        not in {
            "outside_market_hours",
            "regime_only_non_actionable",
            "scan_request_received",
            "scan_background_started",
        }
    )
    actionable_scan_available = bool(latest_terminal and latest_has_candidate_truth)

    if background_active and not background_terminal:
        next_action = "wait_for_background_scan_terminal_close"
    elif actionable_scan_available and selected_symbols:
        next_action = "monitor_submission_and_active_position_truth"
    elif actionable_scan_available:
        next_action = "monitor_next_scan_or_candidate_gate_pressure"
    elif warnings:
        next_action = "inspect_scanner_light_warning_codes"
    else:
        next_action = recommended_action or "monitor_next_scan"

    return {
        "ok": True,
        "module": "swing_scan_state",
        "module_version": SWING_SCAN_STATE_MODULE_VERSION,
        "status": status,
        "healthy": status in {"healthy", "background_scan_running"},
        "actionable_scan_available": actionable_scan_available,
        "latest_scan": {
            "ts_utc": latest.get("ts_utc"),
            "reason": reason or None,
            "source": latest.get("source") or latest.get("_scan_source"),
            "scanned": latest.get("scanned"),
            "signals": latest.get("signals"),
            "would_trade": latest.get("would_trade"),
            "blocked": latest.get("blocked"),
            "eligible_total": latest.get("eligible_total"),
            "selected_total": latest.get("selected_total"),
            "selected_symbols": selected_symbols,
            "duration_ms": latest.get("duration_ms"),
        },
        "background": {
            "active": background_active,
            "terminal": background_terminal,
            "status": background.get("status"),
            "stage": background.get("stage"),
            "reason": background.get("reason"),
            "scan_attempt_id": background.get("scan_attempt_id"),
        },
        "active_warning_codes": warnings,
        "recommended_action": next_action,
        "extraction_phase": "scanner_ownership_extraction",
    }


def build_scanner_ownership_contract(*, patch_version: str, scanner_light_payload: dict | None) -> dict:
    payload = dict(scanner_light_payload or {})
    latest_scan = dict(payload.get("latest_scan") or {})
    background_truth = dict(payload.get("scan_background_completion_truth") or {})
    state_contract = dict(payload.get("p611_scanner_state_contract") or {})
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_scan_state",
        "module_version": SWING_SCAN_STATE_MODULE_VERSION,
        "scanner_state_owner": "swing_scan_state",
        "scanner_route_owner": "app.py",
        "scanner_runtime_owner": "app.py",
        "broker_calls": False,
        "submits_orders": False,
        "fetches_market_data": False,
        "route_adapter_only": True,
        "latest_scan_reason": latest_scan.get("reason"),
        "latest_scan_source": latest_scan.get("source") or latest_scan.get("_scan_source"),
        "latest_scan_scanned": latest_scan.get("scanned"),
        "scanner_status": payload.get("scanner_status"),
        "background_status": background_truth.get("status"),
        "background_active": bool(background_truth.get("active")),
        "actionable_scan_available": bool(state_contract.get("actionable_scan_available")),
        "recommended_action": state_contract.get("recommended_action") or payload.get("recommended_action"),
        "extraction_phase": "scanner_light_contract_owned_by_swing_scan_state",
        "next_extraction_target": "move_scanner_publish_state_mutation_behind_module_api",
    }


def attach_scanner_light_contracts(*, patch_version: str, scanner_light_payload: dict | None) -> dict:
    payload = dict(scanner_light_payload or {})
    payload["p611_scanner_state_contract"] = build_scanner_state_contract(
        scanner_status=payload.get("scanner_status"),
        latest_scan=payload.get("latest_scan"),
        background_truth=payload.get("scan_background_completion_truth"),
        active_warning_codes=list(payload.get("active_warning_codes") or []),
        recommended_action=payload.get("recommended_action"),
    )
    payload["p705_scanner_ownership_contract"] = build_scanner_ownership_contract(
        patch_version=patch_version,
        scanner_light_payload=payload,
    )
    payload["swing_scan_state_module_status"] = scan_state_module_status(patch_version=patch_version)
    return payload


def scan_state_module_status(*, patch_version: str) -> dict:
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_scan_state",
        "module_version": SWING_SCAN_STATE_MODULE_VERSION,
        "owns_runtime_state": False,
        "owns_scanner_light_contract": True,
        "owns_scanner_ownership_contract": True,
        "broker_calls": False,
        "app_globals_required": False,
        "extraction_phase": "scanner_ownership_extraction",
        "responsibilities": [
            "scan_brief_shape",
            "candidate_bearing_truth",
            "scan_truth_contract_normalization",
            "canonical_scan_contract_shape",
            "historical_background_failure_tombstone_shape",
            "scanner_state_contract_shape",
            "scanner_light_contract_attachment",
            "scanner_ownership_contract_shape",
        ],
        "next_extraction_target": "move_scanner_publish_state_mutation_behind_module_api",
    }
