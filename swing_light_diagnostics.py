"""Broker-free swing light diagnostic response builders.

These helpers only shape already-collected runtime data. They must not import
FastAPI, Alpaca clients, app globals, or broker submission logic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


SWING_LIGHT_DIAGNOSTICS_MODULE_VERSION = "patch-580-submit-trace-canonical-scan-consumer-sync"


def selected_submission_truth_light_snapshot(
    *,
    patch_version: str,
    latest_scan: dict,
    selected_symbols: list[str],
    rows: list[dict],
) -> dict:
    stale_rows = [
        dict(row)
        for row in rows
        if row.get("symbol") and bool(row.get("stale_selected_submit_timeout_suppressed"))
    ]
    active_rows = [
        dict(row)
        for row in rows
        if not bool(row.get("stale_selected_submit_timeout_suppressed"))
    ]
    rows = active_rows
    stale_selected_submit_timeout_symbols = [
        row.get("symbol")
        for row in stale_rows
        if row.get("symbol")
    ]
    selected_symbols = [
        sym
        for sym in list(selected_symbols or [])
        if sym not in set(stale_selected_submit_timeout_symbols)
    ]
    submit_gap_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol")
        and bool(row.get("submit_gap"))
        and bool(row.get("submit_gap_is_actionable", True))
    ]
    after_hours_selected_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and bool(row.get("after_hours_selected_not_submitted"))
    ]
    execution_quality_block_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and bool(row.get("execution_quality_blocked"))
    ]
    retryable_spread_block_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and bool((row.get("retryable_spread_block") or {}).get("retryable"))
    ]
    missing_side_effect_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol")
        and not row.get("actual_submit_side_effect", row.get("side_effect_detected_light"))
        and row.get("symbol") not in set(execution_quality_block_symbols)
        and row.get("symbol") not in set(retryable_spread_block_symbols)
        and row.get("symbol") not in set(after_hours_selected_symbols)
        and bool(row.get("submit_gap_is_actionable", True))
    ]
    candidate_selected_only = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and row.get("candidate_selected_only")
    ]
    limit_order_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and str(row.get("plan_order_type") or "").lower() == "limit"
    ]
    filled_plan_backfill_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and bool(row.get("p330_filled_plan_backfill"))
    ]
    retry_resolved_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol")
        and str(row.get("retry_evidence_status") or "") == "resolved_by_active_plan_or_submit_side_effect"
    ]
    retry_waiting_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol")
        and str(row.get("retry_evidence_status") or "") in {"waiting_for_spread_retry", "waiting_for_rate_limit_retry"}
    ]
    rate_limited_retry_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and bool(row.get("rate_limited_retryable"))
    ]
    submit_gap_unattempted_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and str(row.get("submit_gap_type") or "") == "unattempted"
    ]
    submit_gap_terminal_failed_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and str(row.get("submit_gap_type") or "") == "terminal_failed"
    ]
    submit_pending_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and bool(row.get("submit_pending"))
    ]
    pending_order_sync_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol")
        and (
            bool(row.get("pending_order_only_plan"))
            or str(row.get("retry_evidence_status") or "") == "pending_entry_order_needs_broker_status_sync"
            or str(row.get("submit_gap_type") or "") == "pending_entry_order_needs_broker_status_sync"
        )
    ]
    selected_submit_timeout_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol")
        and bool(row.get("selected_submit_timeout"))
        and not bool(row.get("actual_submit_side_effect", row.get("side_effect_detected_light")))
        and row.get("symbol") not in set(pending_order_sync_symbols)
        and str(row.get("retry_evidence_status") or "") != "resolved_by_active_plan_or_submit_side_effect"
    ]
    resolved_submit_timeout_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol")
        and bool(row.get("selected_submit_timeout"))
        and (
            bool(row.get("actual_submit_side_effect", row.get("side_effect_detected_light")))
            or str(row.get("retry_evidence_status") or "") == "resolved_by_active_plan_or_submit_side_effect"
        )
    ]
    submit_pending_symbol_set = set(submit_pending_symbols)
    selected_submit_timeout_symbol_set = set(selected_submit_timeout_symbols)
    stale_selected_submit_timeout_symbol_set = set(stale_selected_submit_timeout_symbols)
    non_actionable_gap_symbols = [
        row.get("symbol")
        for row in rows
        if row.get("symbol")
        and bool(row.get("submit_gap"))
        and not bool(row.get("submit_gap_is_actionable", True))
    ]
    missing_side_effect_symbols = [
        sym for sym in missing_side_effect_symbols
        if sym not in submit_pending_symbol_set
        and sym not in selected_submit_timeout_symbol_set
        and sym not in stale_selected_submit_timeout_symbol_set
    ]

    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "selected_submission_truth_light",
        "source": "runtime_memory_no_broker_no_bundle",
        "module": "swing_light_diagnostics",
        "module_version": SWING_LIGHT_DIAGNOSTICS_MODULE_VERSION,
        "latest_scan_ts_utc": latest_scan.get("ts_utc"),
        "latest_scan_reason": latest_scan.get("reason"),
        "selected_symbols": list(selected_symbols),
        "selected_count": len(selected_symbols),
        "side_effect_symbols": [
            row.get("symbol")
            for row in rows
            if row.get("actual_submit_side_effect")
        ],
        "missing_side_effect_symbols": missing_side_effect_symbols,
        "selected_without_side_effect": bool(missing_side_effect_symbols),
        "candidate_selected_only_symbols": candidate_selected_only,
        "execution_quality_block_symbols": execution_quality_block_symbols,
        "execution_quality_block_count": len(execution_quality_block_symbols),
        "retryable_spread_block_symbols": retryable_spread_block_symbols,
        "retryable_spread_block_count": len(retryable_spread_block_symbols),
        "submit_gap_symbols": submit_gap_symbols,
        "submit_gap_count": len(submit_gap_symbols),
        "submit_gap_is_actionable": bool(submit_gap_symbols),
        "non_actionable_submit_gap_symbols": non_actionable_gap_symbols,
        "non_actionable_submit_gap_count": len(non_actionable_gap_symbols),
        "market_hours_submit_possible": bool(any(row.get("market_hours_submit_possible") for row in rows)),
        "latest_post_deploy_submit_cycle_seen": bool(any(row.get("latest_post_deploy_submit_cycle_seen") for row in rows)),
        "p577_submit_consumption_seen": bool(any(row.get("p577_submit_consumption_seen") for row in rows)),
        "after_hours_selected_symbols": after_hours_selected_symbols,
        "after_hours_selected_count": len(after_hours_selected_symbols),
        "after_hours_selected_not_submitted": bool(after_hours_selected_symbols),
        "limit_order_symbols": limit_order_symbols,
        "limit_order_count": len(limit_order_symbols),
        "filled_plan_backfill_symbols": filled_plan_backfill_symbols,
        "filled_plan_backfill_count": len(filled_plan_backfill_symbols),
        "retry_resolved_symbols": retry_resolved_symbols,
        "retry_resolved_count": len(retry_resolved_symbols),
        "retry_waiting_symbols": retry_waiting_symbols,
        "retry_waiting_count": len(retry_waiting_symbols),
        "rate_limited_retry_symbols": rate_limited_retry_symbols,
        "rate_limited_retry_count": len(rate_limited_retry_symbols),
        "submit_gap_unattempted_symbols": submit_gap_unattempted_symbols,
        "submit_gap_unattempted_count": len(submit_gap_unattempted_symbols),
        "submit_gap_terminal_failed_symbols": submit_gap_terminal_failed_symbols,
        "submit_gap_terminal_failed_count": len(submit_gap_terminal_failed_symbols),
        "submit_pending_symbols": submit_pending_symbols,
        "submit_pending_count": len(submit_pending_symbols),
        "pending_order_sync_symbols": pending_order_sync_symbols,
        "pending_order_sync_count": len(pending_order_sync_symbols),
        "selected_submit_timeout_symbols": selected_submit_timeout_symbols,
        "selected_submit_timeout_count": len(selected_submit_timeout_symbols),
        "resolved_submit_timeout_symbols": resolved_submit_timeout_symbols,
        "resolved_submit_timeout_count": len(resolved_submit_timeout_symbols),
        "stale_selected_submit_timeout_symbols": stale_selected_submit_timeout_symbols,
        "stale_selected_submit_timeout_count": len(stale_selected_submit_timeout_symbols),
        "stale_selected_submit_timeout_rows_tombstoned": stale_rows,
        "p574_stale_selected_timeout_tombstone": {
            "enabled": True,
            "tombstoned_count": len(stale_rows),
            "tombstoned_symbols": stale_selected_submit_timeout_symbols,
            "current_rows_count": len(rows),
            "reason": (
                "stale_selected_submit_timeout_rows_removed_from_current_submission_truth"
                if stale_rows
                else "no_stale_selected_submit_timeout_rows"
            ),
        },
        "rows": list(rows),
        "recommended_action": (
            "selected_candidate_submit_pending"
            if submit_pending_symbols
            else "sync_pending_entry_order_status_with_broker"
            if pending_order_sync_symbols
            else "selected_submit_timeout_requires_reconcile"
            if selected_submit_timeout_symbols
            else "selected_timeout_resolved_by_active_plan"
            if resolved_submit_timeout_symbols
            else "rate_limited_selected_submit_waiting_for_retry"
            if rate_limited_retry_symbols
            else "wait_for_next_market_scan"
            if after_hours_selected_symbols
            else "selected_candidate_submit_gap_detected"
            if submit_gap_symbols
            else "wait_for_next_market_scan"
            if non_actionable_gap_symbols or after_hours_selected_symbols
            else "selected_candidate_blocked_by_execution_quality"
            if execution_quality_block_symbols
            else "retryable_spread_block_waiting_for_quote_improvement"
            if retryable_spread_block_symbols or retry_waiting_symbols
            else "selected_symbols_have_actual_submit_side_effect"
            if selected_symbols
            else "stale_selected_timeout_rows_tombstoned_monitor_next_scan"
            if stale_selected_submit_timeout_symbols
            else "no_selected_symbols_found"
        ),
    }

def _scanner_light_iso_age_sec(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None

def _scanner_runtime_hotspot_truth(latest_scan: dict, scan_summary: dict, budget_sec: int) -> dict:
    timing_ms = {}
    for source in (
        scan_summary.get("timing_ms"),
        latest_scan.get("timing_ms"),
        (latest_scan.get("summary") or {}).get("timing_ms") if isinstance(latest_scan.get("summary"), dict) else {},
    ):
        if isinstance(source, dict) and source:
            timing_ms = dict(source)
            break
    cleaned = {}
    for key, value in timing_ms.items():
        try:
            cleaned[str(key)] = int(float(value or 0))
        except Exception:
            continue
    sorted_stages = sorted(
        [{"stage": stage, "duration_ms": ms} for stage, ms in cleaned.items() if stage != "total"],
        key=lambda row: int(row.get("duration_ms") or 0),
        reverse=True,
    )
    try:
        duration_ms = int(float(latest_scan.get("duration_ms") or scan_summary.get("duration_ms") or cleaned.get("total") or 0))
    except Exception:
        duration_ms = 0
    budget = max(1, int(budget_sec or 240))
    budget_ms = budget * 1000
    return {
        "enabled": True,
        "duration_ms": duration_ms,
        "duration_sec": round(duration_ms / 1000.0, 2),
        "budget_sec": budget,
        "budget_ms": budget_ms,
        "over_budget": bool(duration_ms > budget_ms),
        "stage_timing_available": bool(cleaned),
        "top_stage": sorted_stages[0].get("stage") if sorted_stages else None,
        "top_stage_ms": sorted_stages[0].get("duration_ms") if sorted_stages else None,
        "top_stages": sorted_stages[:5],
        "timing_ms": cleaned,
        "recommended_action": (
            "scanner_hotspot_identified_reduce_top_stage_cost"
            if duration_ms > budget_ms and cleaned
            else "scanner_over_budget_but_stage_timing_missing"
            if duration_ms > budget_ms
            else "scanner_runtime_within_budget"
        ),
    }

def scanner_light_snapshot(
    *,
    patch_version: str,
    telemetry: dict,
    telemetry_summary: dict,
    latest_scan: dict,
    scan_summary: dict,
    in_flight_grace_sec: int = 900,
    scan_runtime_budget_sec: int = 240,
) -> dict:
    active_warning_codes = [
        str(code or "").strip().lower()
        for code in list(telemetry_summary.get("active_warning_codes") or [])
        if str(code or "").strip()
    ]
    recovered_warning_codes = [
        str(code or "").strip().lower()
        for code in list(telemetry_summary.get("recovered_warning_codes") or [])
        if str(code or "").strip()
    ]
    historical_warning_codes = [
        str(code or "").strip().lower()
        for code in list(telemetry_summary.get("historical_warning_codes") or [])
        if str(code or "").strip()
    ]

    current_error = telemetry.get("last_error") or telemetry.get("error")
    latest_scan_completed = str(latest_scan.get("reason") or "").strip().lower() == "scan_completed"
    latest_status_ok = str(telemetry.get("status") or telemetry.get("last_status") or "").strip().lower() in {
        "ok",
        "success",
        "skipped",
        "sleep",
    }
    in_flight = bool(telemetry_summary.get("in_flight_run"))
    last_event_age_sec = _scanner_light_iso_age_sec(telemetry.get("last_event_utc"))
    last_closed_age_sec = _scanner_light_iso_age_sec(telemetry_summary.get("last_closed_utc") or telemetry.get("last_closed_utc"))
    grace_sec = max(60, int(in_flight_grace_sec or 900))
    budget_sec = max(60, int(scan_runtime_budget_sec or 240))
    running_age_sec = last_event_age_sec if in_flight else None

    in_flight_grace_active = bool(in_flight and running_age_sec is not None and running_age_sec <= grace_sec)
    in_flight_over_grace = bool(in_flight and running_age_sec is not None and running_age_sec > grace_sec)
    in_flight_over_budget = bool(in_flight and running_age_sec is not None and running_age_sec > budget_sec)
    in_flight_reconciled_by_completed_scan = bool(latest_scan_completed and latest_status_ok and in_flight and not in_flight_grace_active)
    effective_in_flight = bool(in_flight and not in_flight_reconciled_by_completed_scan and not in_flight_grace_active)

    if latest_scan_completed and latest_status_ok and "dispatch_failure" in active_warning_codes:
        active_warning_codes = [code for code in active_warning_codes if code != "dispatch_failure"]
        if "dispatch_failure_recovered_by_scan_success" not in recovered_warning_codes:
            recovered_warning_codes.append("dispatch_failure_recovered_by_scan_success")
        if "dispatch_failure" not in historical_warning_codes:
            historical_warning_codes.append("dispatch_failure")

    if in_flight_reconciled_by_completed_scan and "partial_run_open" in active_warning_codes:
        active_warning_codes = [code for code in active_warning_codes if code != "partial_run_open"]
        if "partial_run_open_reconciled_by_completed_scan" not in recovered_warning_codes:
            recovered_warning_codes.append("partial_run_open_reconciled_by_completed_scan")
        if "partial_run_open" not in historical_warning_codes:
            historical_warning_codes.append("partial_run_open")

    if in_flight_grace_active:
        if "partial_run_open" in active_warning_codes:
            active_warning_codes = [code for code in active_warning_codes if code != "partial_run_open"]
            if "partial_run_open_within_grace_window" not in recovered_warning_codes:
                recovered_warning_codes.append("partial_run_open_within_grace_window")
            if "partial_run_open" not in historical_warning_codes:
                historical_warning_codes.append("partial_run_open")
        if "dispatch_failure" in active_warning_codes and not in_flight_over_budget:
            active_warning_codes = [code for code in active_warning_codes if code != "dispatch_failure"]
            if "dispatch_failure_pending_scan_close_within_grace_window" not in recovered_warning_codes:
                recovered_warning_codes.append("dispatch_failure_pending_scan_close_within_grace_window")
            if "dispatch_failure" not in historical_warning_codes:
                historical_warning_codes.append("dispatch_failure")

    dispatch_failure_recovered_by_closed_scan = (
        "dispatch_failure_recovered_by_closed_scan" in recovered_warning_codes
    )

    post_open_scan_missing = bool(scan_summary.get("post_open_scan_missing") or latest_scan.get("post_open_scan_missing"))
    scanner_failure_root_cause = dict(
        scan_summary.get("scanner_failure_root_cause")
        or latest_scan.get("scanner_failure_root_cause")
        or {}
    )
    scan_background_completion_truth = dict(
        scan_summary.get("scan_background_completion_truth")
        or latest_scan.get("scan_background_completion_truth")
        or {}
    )
    background_status = str(scan_background_completion_truth.get("status") or "").strip().lower()
    background_duration_ms = 0
    try:
        background_duration_ms = int(float(scan_background_completion_truth.get("duration_ms") or 0))
    except Exception:
        background_duration_ms = 0
    background_budget_sec = max(60, int(scan_runtime_budget_sec or 240))
    background_timeout_sec = background_budget_sec + 60
    background_completed_over_budget = bool(
        background_status == "completed"
        and background_duration_ms > int(background_budget_sec * 1000)
    )
    if background_completed_over_budget:
        scan_background_completion_truth["status"] = "failed"
        scan_background_completion_truth["active"] = False
        scan_background_completion_truth["terminal"] = True
        scan_background_completion_truth["reason_before_runtime_budget_sanitize"] = scan_background_completion_truth.get("reason")
        scan_background_completion_truth["reason"] = "background_scan_runtime_budget_exceeded"
        scan_background_completion_truth["exception_type"] = scan_background_completion_truth.get("exception_type") or "BackgroundScanRuntimeBudgetExceeded"
        scan_background_completion_truth["error"] = scan_background_completion_truth.get("error") or "persisted completed background scan exceeded runtime budget"
        scan_background_completion_truth["runtime_budget_sanitized"] = True
        scan_background_completion_truth["runtime_budget_ms"] = int(background_budget_sec * 1000)
        background_status = "failed"

    background_scan_active = background_status in {"accepted", "running"}
    background_scan_terminal = bool(
        background_status in {"completed", "failed", "timeout", "canceled"}
        or bool(scan_background_completion_truth.get("terminal"))
    )
    background_started_age_sec = _scanner_light_iso_age_sec(scan_background_completion_truth.get("started_utc"))
    background_heartbeat_age_sec = _scanner_light_iso_age_sec(scan_background_completion_truth.get("updated_utc"))
    background_age_sec = background_started_age_sec
    background_over_budget = bool(background_scan_active and background_started_age_sec is not None and background_started_age_sec > background_budget_sec)
    background_timed_out = bool(background_scan_active and background_started_age_sec is not None and background_started_age_sec > background_timeout_sec)
    background_heartbeat_stale = bool(background_scan_active and background_heartbeat_age_sec is not None and background_heartbeat_age_sec > 90)
    background_stage = str(scan_background_completion_truth.get("stage") or "").strip().lower()
    background_thread_start_proof_timeout_sec = max(15, min(45, int(background_budget_sec // 4)))
    background_thread_entry_missing = bool(
        background_scan_active
        and background_stage in {"accepted", "thread_starting", "thread_started"}
        and background_started_age_sec is not None
        and background_started_age_sec > background_thread_start_proof_timeout_sec
    )
    background_restart_lost = bool(
        str(scan_background_completion_truth.get("exception_type") or "").strip() == "BackgroundScanLostAfterRestart"
        or str(scan_background_completion_truth.get("reason") or "").strip().lower() == "background_scan_lost_after_restart"
        or str(scanner_failure_root_cause.get("exception_type") or "").strip() == "BackgroundScanLostAfterRestart"
        or str(scanner_failure_root_cause.get("root_cause") or "").strip().lower() == "background_scan_failed"
        and str(scanner_failure_root_cause.get("error") or "").strip().lower() == "background scan was active before process restart and cannot be resumed"
    )

    if background_restart_lost and not background_scan_active:
        effective_in_flight = False
        in_flight_grace_active = False
        in_flight_over_grace = False
        in_flight_over_budget = False
        active_warning_codes = [
            code for code in active_warning_codes
            if code not in {"partial_run_open", "dispatch_failure"}
        ]
        for code in ["background_scan_lost_after_restart_aged", "partial_run_open", "dispatch_failure"]:
            if code not in historical_warning_codes:
                historical_warning_codes.append(code)
        if "background_scan_lost_after_restart_aged" not in recovered_warning_codes:
            recovered_warning_codes.append("background_scan_lost_after_restart_aged")

    terminal_scan_request_without_active_worker = bool(
        latest_scan_completed
        and in_flight_grace_active
        and not background_scan_active
        and not background_over_budget
        and not background_timed_out
        and not background_thread_entry_missing
    )
    if terminal_scan_request_without_active_worker:
        in_flight_grace_active = False
        effective_in_flight = False
        if "scan_request_reconciled_by_terminal_scan" not in recovered_warning_codes:
            recovered_warning_codes.append("scan_request_reconciled_by_terminal_scan")

    if background_scan_terminal and not background_scan_active:
        in_flight_grace_active = False
        effective_in_flight = False
        in_flight_over_grace = False
        in_flight_over_budget = False
        active_warning_codes = [
            code for code in active_warning_codes
            if code not in {"partial_run_open", "dispatch_failure"}
        ]
        if "completed_background_scan_cleared_grace" not in recovered_warning_codes:
            recovered_warning_codes.append("completed_background_scan_cleared_grace")

    grace_has_active_scan = bool(in_flight_grace_active and (effective_in_flight or in_flight or background_scan_active))
    scanner_status = (
        "background_scan_thread_start_unproven"
        if background_thread_entry_missing
        else "background_scan_runtime_budget_exceeded"
        if background_completed_over_budget
        else "background_scan_timeout"
        if background_timed_out
        else "background_scan_over_budget"
        if background_over_budget
        else "background_scan_running"
        if background_scan_active
        else "scan_running_within_grace"
        if grace_has_active_scan and not in_flight_over_budget
        else "scan_request_pending_terminal_scan"
        if in_flight_grace_active and not in_flight_over_budget
        else "scan_running_over_budget"
        if in_flight_over_budget
        else "scan_stale_failed"
        if effective_in_flight or active_warning_codes
        else "healthy"
    )

    scanner_currently_ok = bool(
        scanner_status in {"healthy", "scan_running_within_grace", "background_scan_running"}
        and not background_over_budget
        and not background_timed_out
        and not background_thread_entry_missing
        and (background_scan_active or not post_open_scan_missing)
        and (background_scan_active or not active_warning_codes)
    )

    scanner_failure_root_cause_historical = {}
    if background_restart_lost and not background_scan_active:
        scanner_failure_root_cause_historical = dict(scanner_failure_root_cause or {})
        scanner_failure_root_cause_historical["active"] = False
        scanner_failure_root_cause_historical["aged_reason"] = "restart_lost_background_scan_is_historical_and_next_scan_is_unlocked"
        scanner_failure_root_cause = {
            "active": False,
            "root_cause": "none",
            "recovery_action": "run_or_wait_for_fresh_market_scan",
            "aged_from": "BackgroundScanLostAfterRestart",
            "aged_reason": "restart_lost_background_scan_is_historical_and_next_scan_is_unlocked",
        }
        if "background_scan_lost_after_restart_aged" not in recovered_warning_codes:
            recovered_warning_codes.append("background_scan_lost_after_restart_aged")
        if "background_scan_lost_after_restart" not in historical_warning_codes:
            historical_warning_codes.append("background_scan_lost_after_restart")

    swing_candidate_eval_module_version = (
        scan_summary.get("swing_candidate_eval_module_version")
        or latest_scan.get("swing_candidate_eval_module_version")
    )
    swing_candidate_eval_module_status = dict(
        scan_summary.get("swing_candidate_eval_module_status")
        or latest_scan.get("swing_candidate_eval_module_status")
        or {}
    )
    p484_candidate_eval_module = dict(
        scan_summary.get("p484_candidate_eval_module")
        or latest_scan.get("p484_candidate_eval_module")
        or swing_candidate_eval_module_status
        or {}
    )
    scanner_runtime_hotspot_truth = _scanner_runtime_hotspot_truth(latest_scan, scan_summary, budget_sec)

    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "scanner_light",
        "source": "runtime_memory",
        "module": "swing_light_diagnostics",
        "module_version": SWING_LIGHT_DIAGNOSTICS_MODULE_VERSION,
        "last_event": telemetry.get("event") or telemetry.get("last_event"),
        "last_status": telemetry.get("status") or telemetry.get("last_status"),
        "last_event_utc": telemetry.get("last_event_utc"),
        "last_closed_utc": telemetry_summary.get("last_closed_utc") or telemetry.get("last_closed_utc"),
        "last_error": None if scanner_currently_ok else current_error,
        "last_error_historical": current_error if scanner_currently_ok else None,
        "scanner_status": scanner_status,
        "scanner_failure_root_cause": scanner_failure_root_cause,
        "scanner_failure_root_cause_historical": scanner_failure_root_cause_historical,
        "swing_candidate_eval_module_version": swing_candidate_eval_module_version,
        "swing_candidate_eval_module_status": swing_candidate_eval_module_status,
        "p484_candidate_eval_module": p484_candidate_eval_module,
        "scanner_runtime_hotspot_truth": scanner_runtime_hotspot_truth,
        "scan_background_completion_truth": scan_background_completion_truth,
        "background_scan_runtime_truth": {
            "active": bool(background_scan_active),
            "age_sec": round(background_age_sec, 2) if background_age_sec is not None else None,
            "started_age_sec": round(background_started_age_sec, 2) if background_started_age_sec is not None else None,
            "heartbeat_age_sec": round(background_heartbeat_age_sec, 2) if background_heartbeat_age_sec is not None else None,
            "heartbeat_stale": bool(background_heartbeat_stale),
            "thread_entry_missing": bool(background_thread_entry_missing),
            "thread_start_proof_timeout_sec": int(background_thread_start_proof_timeout_sec),
            "budget_sec": int(background_budget_sec),
            "timeout_sec": int(background_timeout_sec),
            "over_budget": bool(background_over_budget),
            "timed_out": bool(background_timed_out),
            "completed_over_budget_sanitized": bool(background_completed_over_budget),
            "duration_ms": int(background_duration_ms),
            "stage": scan_background_completion_truth.get("stage"),
            "scan_attempt_id": scan_background_completion_truth.get("scan_attempt_id"),
        },
        "in_flight_run": bool(effective_in_flight or (background_scan_active and not background_timed_out)),
        "raw_in_flight_run": bool(in_flight),
        "in_flight_grace_active": bool(in_flight_grace_active),
        "in_flight_over_grace": bool(in_flight_over_grace),
        "in_flight_over_budget": bool(in_flight_over_budget),
        "in_flight_reconciled_by_completed_scan": bool(in_flight_reconciled_by_completed_scan),
        "last_event_age_sec": round(last_event_age_sec, 2) if last_event_age_sec is not None else None,
        "last_closed_age_sec": round(last_closed_age_sec, 2) if last_closed_age_sec is not None else None,
        "in_flight_grace_sec": int(grace_sec),
        "scan_runtime_budget_sec": int(budget_sec),
        "attempts_today": telemetry_summary.get("attempts_today"),
        "success_today": telemetry_summary.get("success_today"),
        "failure_today": telemetry_summary.get("failure_today"),
        "active_warning_codes": active_warning_codes,
        "recovered_warning_codes": recovered_warning_codes,
        "historical_warning_codes": historical_warning_codes,
        "cleanup_status": {
            "scanner_currently_ok": scanner_currently_ok,
            "scanner_status": scanner_status,
            "in_flight_grace_active": bool(in_flight_grace_active),
            "grace_has_active_scan": bool(grace_has_active_scan),
            "dispatch_failure_aged_as_pending_scan": bool("dispatch_failure_pending_scan_close_within_grace_window" in recovered_warning_codes),
            "partial_run_aged_as_pending_scan": bool("partial_run_open_within_grace_window" in recovered_warning_codes),
            "stale_errors_suppressed": bool(scanner_currently_ok and current_error),
            "manual_requests_are_historical": True,
            "worker_unknown_can_be_cleared_by_recent_heartbeat": True,
            "dispatch_failure_recovered_by_closed_scan": bool(dispatch_failure_recovered_by_closed_scan),
            "dispatch_failure_recovered_by_scan_success": "dispatch_failure_recovered_by_scan_success" in recovered_warning_codes,
            "partial_run_open_reconciled_by_completed_scan": bool(in_flight_reconciled_by_completed_scan),
            "scan_request_reconciled_by_terminal_scan": bool(terminal_scan_request_without_active_worker),
            "post_open_scan_missing": bool(post_open_scan_missing),
            "background_scan_active": bool(background_scan_active),
            "background_scan_over_budget": bool(background_over_budget),
            "background_scan_timed_out": bool(background_timed_out),
            "background_completed_over_budget_sanitized": bool(background_completed_over_budget),
            "background_scan_heartbeat_stale": bool(background_heartbeat_stale),
            "background_thread_entry_missing": bool(background_thread_entry_missing),
            "background_scan_lost_after_restart_aged": bool(scanner_failure_root_cause_historical),
            "scanner_runtime_over_budget": bool(scanner_runtime_hotspot_truth.get("over_budget")),
            "scanner_runtime_top_stage": scanner_runtime_hotspot_truth.get("top_stage"),
        },
        "latest_scan": {
            "ts_utc": latest_scan.get("ts_utc"),
            "reason": latest_scan.get("reason"),
            "scanned": latest_scan.get("scanned"),
            "signals": latest_scan.get("signals"),
            "would_trade": latest_scan.get("would_trade"),
            "blocked": latest_scan.get("blocked"),
            "duration_ms": latest_scan.get("duration_ms"),
            "selected_total": int(scan_summary.get("selected_total") or 0),
            "selected_symbols": list(scan_summary.get("selected_symbols") or []),
            "selected_symbols_before_p525_revalidation": list(scan_summary.get("selected_symbols_before_p525_revalidation") or latest_scan.get("selected_symbols_before_p525_revalidation") or []),
            "stale_revalidated_blocked_symbols": list(scan_summary.get("stale_revalidated_blocked_symbols") or latest_scan.get("stale_revalidated_blocked_symbols") or []),
            "p525_scanner_selection_revalidation": dict(scan_summary.get("p525_scanner_selection_revalidation") or latest_scan.get("p525_scanner_selection_revalidation") or {}),
            "eligible_total": int(scan_summary.get("eligible_total") or scan_summary.get("eligible_count") or 0),
            "stale_preopen_scan": bool(scan_summary.get("stale_preopen_scan") or latest_scan.get("stale_preopen_scan")),
            "post_open_scan_missing": bool(post_open_scan_missing),
            "scan_background_completion_truth": scan_background_completion_truth,
            "swing_candidate_eval_module_version": swing_candidate_eval_module_version,
            "swing_candidate_eval_module_status": swing_candidate_eval_module_status,
            "p484_candidate_eval_module": p484_candidate_eval_module,
        },
    }


def swing_cleanup_status_snapshot(
    *,
    patch_version: str,
    strategy_mode: str,
    live_swing_runtime: bool,
    retired_paths: dict[str, dict[str, Any]],
) -> dict:
    retired = dict(retired_paths or {})
    active_retired_paths = [
        name
        for name, row in retired.items()
        if isinstance(row, dict) and not bool(row.get("disabled"))
    ]
    fully_disabled = not bool(active_retired_paths)

    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "swing_cleanup_status",
        "module": "swing_light_diagnostics",
        "module_version": SWING_LIGHT_DIAGNOSTICS_MODULE_VERSION,
        "strategy_mode": strategy_mode,
        "live_swing_runtime": bool(live_swing_runtime),
        "cleanup_phase": "swing_production_core_cleanup",
        "cleanup_principle": "protect_working_swing_lane_remove_or_disable_retired_side_paths",
        "retired_paths": retired,
        "active_retired_path_count": len(active_retired_paths),
        "active_retired_paths": active_retired_paths,
        "retired_paths_fully_disabled": fully_disabled,
        "retained_runtime_paths": {
            "swing_scanner": {
                "status": "active",
                "reason": "primary_candidate_source",
            },
            "swing_production_contract": {
                "status": "active",
                "reason": "single_entry_contract",
            },
            "direct_submit_path": {
                "status": "active",
                "reason": "selected_candidate_to_broker_order",
            },
            "broker_reconcile": {
                "status": "active",
                "reason": "position_truth_and_fill_recovery",
            },
            "worker_exit": {
                "status": "active",
                "reason": "normal_exit_management",
            },
            "light_diagnostics": {
                "status": "active",
                "reason": "operator_truth_without_heavy_bundles",
            },
            "intraday_runtime": {
                "status": "retained_dormant",
                "reason": "intraday_code_retained_for_future_separation_but_not_part_of_swing_operator_flow",
            },
        },
        "active_light_endpoints": [
            "/diagnostics/live_positions",
            "/diagnostics/reconcile",
            "/diagnostics/scanner_light",
            "/diagnostics/selected_submission_truth_light",
            "/diagnostics/market_open_selection_audit_light",
            "/diagnostics/live_positions_light",
            "/diagnostics/reconcile_light",
            "/diagnostics/no_trade_brief?refresh_live=false&limit=10",
            "/diagnostics/swing_cleanup_status",
            "/diagnostics/swing_core_status",
            "/diagnostics/swing_light_endpoint_manifest",
            "/diagnostics/swing_runtime_config",
            "/diagnostics/swing_selection_contract_module_status",
            "/diagnostics/swing_execution_module_status",
            "/diagnostics/swing_submit_split_readiness",
            "/diagnostics/protective_limit_submit_evidence",
            "/diagnostics/intraday_runtime_isolation_status",
        ],
        "removed_from_operator_default_flow": [
            "heavy_operator_bundle",
            "selected_entry_intent_queue",
            "selected_entry_finalizer",
            "selected_submission_finalizer",
            "fast_swing_scan_trigger",
            "intraday_shadow_inside_swing_scan",
            "intraday_live",
        ],
        "next_cleanup_focus": [
            "use_swing_light_endpoint_manifest_for_operator_pulls",
            "verify_protective_limit_submit_path_on_next_live_selected_candidate",
            "prepare_submit_function_split_after_limit_path_is_live_proven",
            "keep_intraday_runtime_retained_dormant_until_separate_service",
        ],
        "recommended_action": (
            "cleanup_state_ready_for_next_code_split"
            if fully_disabled
            else "disable_active_retired_paths_before_deeper_removal"
        ),
    }
