"""Broker-free swing scan state/truth helpers.

This module does not own runtime globals. app.py still owns live scan state for
now; this file only shapes scan truth objects passed into it.
"""

from __future__ import annotations

from typing import Any


SWING_SCAN_STATE_MODULE_VERSION = "patch-483-swing-scan-state-module-extraction-prep"


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


def scan_state_module_status(*, patch_version: str) -> dict:
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_scan_state",
        "module_version": SWING_SCAN_STATE_MODULE_VERSION,
        "owns_runtime_state": False,
        "broker_calls": False,
        "app_globals_required": False,
        "extraction_phase": "prep",
        "responsibilities": [
            "scan_brief_shape",
            "canonical_scan_contract_shape",
            "historical_background_failure_tombstone_shape",
        ],
        "next_extraction_target": "move_last_candidate_bearing_scan_lookup_after_market_proof",
    }