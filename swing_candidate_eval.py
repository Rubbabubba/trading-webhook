"""Broker-free swing candidate evaluation truth helpers.

This module does not evaluate symbols or submit orders. app.py still owns the
runtime scanner loop for now; this file only shapes candidate-eval status,
timeouts, and module adoption truth.
"""

from __future__ import annotations

from typing import Any


SWING_CANDIDATE_EVAL_MODULE_VERSION = "patch-485-candidate-eval-progress-summary-builder-extraction"


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen = set()
    out = []
    for value in values:
        key = str(value)
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def initial_eval_truth(
    *,
    incremental_enabled: bool,
    budget_sec: int,
    reserve_sec: int,
) -> dict:
    return {
        "enabled": bool(incremental_enabled),
        "evaluated_symbols": [],
        "skipped_symbols": [],
        "stopped_for_budget": False,
        "budget_sec": int(budget_sec or 0),
        "reserve_sec": int(reserve_sec or 0),
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
    }


def initial_progress_publish() -> dict:
    return {
        "published": False,
        "publish_count": 0,
        "last_reason": None,
        "last_symbols_eval_total": 0,
        "last_candidate_count": 0,
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
    }


def initial_terminal_partial_close(*, batch_size: int = 5) -> dict:
    return {
        "applied": False,
        "reason": "not_needed",
        "stage": None,
        "evaluated_count": 0,
        "candidate_count": 0,
        "remaining_sec": None,
        "elapsed_sec": None,
        "batch_size": int(batch_size or 5),
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
    }


def build_progress_summary(
    *,
    strategy_name: str,
    scan_reason: str,
    publish_reason: str,
    index_symbol: str,
    index_alignment_ok: bool,
    regime: dict,
    regime_mode: str,
    scan_symbols: list[str],
    requested_symbols: list[str],
    fetch_truth: dict,
    eval_truth: dict,
    runtime_slim: dict,
    hot_path_slim: dict,
    stage_snapshot: dict,
    candidates: list[dict],
    selected_symbols: list[str],
    approved_rows: list[dict],
    approved_symbols: list[str],
    rejection_rows: list[dict],
    patch_version: str,
) -> dict:
    evaluated_count = len(list((eval_truth or {}).get("evaluated_symbols") or []))
    candidate_count = len(list(candidates or []))
    selected_symbols = _dedupe_keep_order([
        str(symbol or "").strip().upper()
        for symbol in list(selected_symbols or [])
        if str(symbol or "").strip()
    ])
    approved_symbols = _dedupe_keep_order([
        str(symbol or "").strip().upper()
        for symbol in list(approved_symbols or [])
        if str(symbol or "").strip()
    ])
    skipped_symbols = _dedupe_keep_order(
        list((fetch_truth or {}).get("skipped_symbols") or [])
        + list((eval_truth or {}).get("skipped_symbols") or [])
    )[:50]

    out = {
        "strategy_name": strategy_name,
        "scan_reason": scan_reason,
        "scan_truth_phase": "candidate_eval_progress",
        "candidate_truth_published_before_reports": True,
        "p476_candidate_eval_progress_publish": True,
        "p476_publish_reason": publish_reason,
        "heavy_reports_deferred_from_hot_path": True,
        "index_symbol": index_symbol,
        "index_alignment_ok": bool(index_alignment_ok),
        "regime": dict(regime or {}),
        "regime_mode": regime_mode,
        "symbols": list(scan_symbols or []),
        "symbols_total": len(list(scan_symbols or [])),
        "symbols_requested_total": len(list(requested_symbols or [])),
        "symbols_fetched_total": int((fetch_truth or {}).get("fetched_count") or 0),
        "symbols_eval_total": int(evaluated_count),
        "symbols_skipped_for_budget": skipped_symbols,
        "runtime_slim": dict(runtime_slim or {}),
        "hot_path_slim": dict(hot_path_slim or {}),
        "scan_stage_checkpoint": dict(stage_snapshot or {}),
        "incremental_scan": {
            "fetch": dict(fetch_truth or {}),
            "evaluation": dict(eval_truth or {}),
            "partial_scan": True,
            "partial_scan_publishable": bool(candidate_count > 0),
            "partial_publish_reason": "candidate_eval_progress_before_selection",
        },
        "candidates_total": int(candidate_count),
        "eligible_total": len(list(approved_rows or [])),
        "selected_total": len(selected_symbols),
        "selected_symbols": list(selected_symbols),
        "production_contract_selected_symbols": list(selected_symbols),
        "approved_symbols": list(approved_symbols),
        "candidate_bearing_scan": bool(candidate_count > 0 or evaluated_count > 0),
        "trade_judgable": bool(candidate_count > 0 or evaluated_count > 0),
        "regime_only_non_actionable": False,
        "top_candidates": [dict(row or {}) for row in list(candidates or [])[:5]],
        "top_rejection_reasons": [dict(row or {}) for row in list(rejection_rows or [])],
        "production_contract_miss_reasons": {
            "ok": True,
            "deferred": True,
            "reason": "p476_candidate_eval_progress_before_selection",
            "endpoint": "/diagnostics/production_contract_miss_reasons",
            "candidate_count": int(candidate_count),
            "selected_symbols": list(selected_symbols),
        },
        "target_path_opportunity_expansion_lab": {
            "ok": True,
            "deferred": True,
            "reason": "p476_candidate_eval_progress_before_selection",
            "endpoint": "/diagnostics/target_path_opportunity_expansion_lab",
            "candidate_count": int(candidate_count),
        },
        "p485_candidate_eval_progress_builder": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "broker_calls": False,
            "submits_orders": False,
        },
    }
    return attach_candidate_eval_module_status(out, patch_version=patch_version)


def symbol_eval_timeout_row(
    *,
    symbol: str,
    strategy: str,
    signal: str,
    scan_ts_utc: str,
    isolation_enabled: bool,
    timeout_sec: float,
    elapsed_sec: float,
    stage: str,
) -> dict:
    return {
        "symbol": str(symbol or "").strip().upper(),
        "eligible": False,
        "selected": False,
        "strategy": str(strategy or ""),
        "signal": str(signal or ""),
        "scan_ts_utc": scan_ts_utc,
        "rejection_reasons": ["candidate_eval_timeout"],
        "candidate_eval_timeout": True,
        "p478_symbol_eval_isolation": {
            "enabled": bool(isolation_enabled),
            "timeout_sec": round(float(timeout_sec or 0.0), 3),
            "elapsed_sec": round(float(elapsed_sec or 0.0), 3),
            "stage": str(stage or ""),
            "reason": "symbol_eval_exceeded_timeout",
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
        },
    }


def candidate_eval_module_status(*, patch_version: str) -> dict:
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
        "owns_runtime_state": False,
        "broker_calls": False,
        "submits_orders": False,
        "app_globals_required": False,
        "extraction_phase": "prep",
        "responsibilities": [
            "candidate_eval_truth_shape",
            "candidate_eval_progress_publish_shape",
            "candidate_eval_timeout_row_shape",
            "candidate_eval_progress_summary_shape",
        ],
        "next_extraction_target": "candidate_eval_terminal_partial_summary_builder",
    }


def attach_candidate_eval_module_status(payload: dict | None, *, patch_version: str) -> dict:
    out = dict(payload or {})
    out["swing_candidate_eval_module_version"] = SWING_CANDIDATE_EVAL_MODULE_VERSION
    out["swing_candidate_eval_module_status"] = candidate_eval_module_status(
        patch_version=patch_version
    )
    return out
