"""Broker-free swing light diagnostic response builders.

These helpers only shape already-collected runtime data. They must not import
FastAPI, Alpaca clients, app globals, or broker submission logic.
"""

from __future__ import annotations

from typing import Any


SWING_LIGHT_DIAGNOSTICS_MODULE_VERSION = "patch-336-swing-core-status-cleanup-submit-split-readiness-truth"


def selected_submission_truth_light_snapshot(
    *,
    patch_version: str,
    latest_scan: dict,
    selected_symbols: list[str],
    rows: list[dict],
) -> dict:
    missing = [
        row.get("symbol")
        for row in rows
        if row.get("symbol") and not row.get("actual_submit_side_effect", row.get("side_effect_detected_light"))
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
        "missing_side_effect_symbols": missing,
        "selected_without_side_effect": bool(missing),
        "candidate_selected_only_symbols": candidate_selected_only,
        "submit_gap_symbols": missing,
        "submit_gap_count": len(missing),
        "limit_order_symbols": limit_order_symbols,
        "limit_order_count": len(limit_order_symbols),
        "filled_plan_backfill_symbols": filled_plan_backfill_symbols,
        "filled_plan_backfill_count": len(filled_plan_backfill_symbols),
        "rows": list(rows),
        "recommended_action": (
            "selected_candidate_submit_gap_detected"
            if missing
            else "selected_symbols_have_actual_submit_side_effect"
            if selected_symbols
            else "no_selected_symbols_found"
        ),
    }


def scanner_light_snapshot(
    *,
    patch_version: str,
    telemetry: dict,
    telemetry_summary: dict,
    latest_scan: dict,
    scan_summary: dict,
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

    if latest_scan_completed and latest_status_ok and not in_flight and "dispatch_failure" in active_warning_codes:
        active_warning_codes = [code for code in active_warning_codes if code != "dispatch_failure"]
        if "dispatch_failure_recovered_by_scan_success" not in recovered_warning_codes:
            recovered_warning_codes.append("dispatch_failure_recovered_by_scan_success")
        if "dispatch_failure" not in historical_warning_codes:
            historical_warning_codes.append("dispatch_failure")

    dispatch_failure_recovered_by_closed_scan = (
        "dispatch_failure_recovered_by_closed_scan" in recovered_warning_codes
    )

    post_open_scan_missing = bool(scan_summary.get("post_open_scan_missing") or latest_scan.get("post_open_scan_missing"))

    scanner_currently_ok = bool(
        not active_warning_codes
        and not post_open_scan_missing
        and not in_flight
        and str(telemetry.get("status") or telemetry.get("last_status") or "")
        .strip()
        .lower()
        in {"ok", "success", "skipped", ""}
    )

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
        "in_flight_run": bool(in_flight),
        "attempts_today": telemetry_summary.get("attempts_today"),
        "success_today": telemetry_summary.get("success_today"),
        "failure_today": telemetry_summary.get("failure_today"),
        "active_warning_codes": active_warning_codes,
        "recovered_warning_codes": recovered_warning_codes,
        "historical_warning_codes": historical_warning_codes,
        "cleanup_status": {
            "scanner_currently_ok": scanner_currently_ok,
            "stale_errors_suppressed": bool(scanner_currently_ok and current_error),
            "manual_requests_are_historical": True,
            "worker_unknown_can_be_cleared_by_recent_heartbeat": True,
            "dispatch_failure_recovered_by_closed_scan": bool(dispatch_failure_recovered_by_closed_scan),
            "post_open_scan_missing": bool(post_open_scan_missing),
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
            "eligible_total": int(scan_summary.get("eligible_total") or scan_summary.get("eligible_count") or 0),
            "stale_preopen_scan": bool(scan_summary.get("stale_preopen_scan") or latest_scan.get("stale_preopen_scan")),
            "post_open_scan_missing": bool(post_open_scan_missing),
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
            "/diagnostics/swing_runtime_config",
            "/diagnostics/swing_selection_contract_module_status",
            "/diagnostics/swing_execution_module_status",
            "/diagnostics/swing_submit_split_readiness",
            "/diagnostics/protective_limit_submit_evidence",
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
            "verify_protective_limit_submit_path_on_next_wide_spread_live_candidate",
            "prepare_submit_function_split_after_limit_path_is_live_proven",
            "delete_selected_entry_intent_queue_code_after_another_clean_live_session",
            "keep_intraday_code_retained_but_out_of_swing_runtime_until_separate_service",
        ],
        "recommended_action": (
            "cleanup_state_ready_for_next_code_split"
            if fully_disabled
            else "disable_active_retired_paths_before_deeper_removal"
        ),
    }