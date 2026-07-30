"""Broker-free swing light diagnostic response builders.

These helpers only shape already-collected runtime data. They must not import
FastAPI, Alpaca clients, app globals, or broker submission logic.
"""

from __future__ import annotations

from typing import Any


SWING_LIGHT_DIAGNOSTICS_MODULE_VERSION = "patch-308-swing-light-diagnostics-module-split"


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
        if row.get("symbol") and not row.get("side_effect_detected_light")
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
            if row.get("side_effect_detected_light")
        ],
        "missing_side_effect_symbols": missing,
        "selected_without_side_effect": bool(missing),
        "rows": list(rows),
        "recommended_action": (
            "investigate_submit_path_for_selected_symbols"
            if missing
            else "selected_symbols_have_runtime_plan_or_submit_event"
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
    active_warning_codes = list(telemetry_summary.get("active_warning_codes") or [])
    recovered_warning_codes = list(telemetry_summary.get("recovered_warning_codes") or [])
    historical_warning_codes = list(telemetry_summary.get("historical_warning_codes") or [])
    current_error = telemetry.get("last_error") or telemetry.get("error")
    dispatch_failure_recovered_by_closed_scan = (
        "dispatch_failure_recovered_by_closed_scan" in recovered_warning_codes
    )

    scanner_currently_ok = bool(
        not active_warning_codes
        and not bool(telemetry_summary.get("in_flight_run"))
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
        "in_flight_run": bool(telemetry_summary.get("in_flight_run")),
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
        },
    }


def swing_cleanup_status_snapshot(
    *,
    patch_version: str,
    strategy_mode: str,
    live_swing_runtime: bool,
    retired_paths: dict[str, dict[str, Any]],
) -> dict:
    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "swing_cleanup_status",
        "module": "swing_light_diagnostics",
        "module_version": SWING_LIGHT_DIAGNOSTICS_MODULE_VERSION,
        "strategy_mode": strategy_mode,
        "live_swing_runtime": bool(live_swing_runtime),
        "retired_paths": dict(retired_paths),
        "active_light_endpoints": [
            "/diagnostics/scanner_light",
            "/diagnostics/market_open_selection_audit_light",
            "/diagnostics/selected_submission_truth_light",
            "/diagnostics/live_positions_light",
            "/diagnostics/reconcile_light",
            "/diagnostics/no_trade_brief?refresh_live=false&limit=10",
        ],
        "next_cleanup_patch": "patch-309-swing-runtime-config-module-split",
    }