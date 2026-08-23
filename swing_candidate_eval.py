"""Broker-free swing candidate evaluation truth helpers.

This module does not evaluate symbols or submit orders. app.py still owns the
runtime scanner loop for now; this file only shapes candidate-eval status,
timeouts, and module adoption truth.
"""

from __future__ import annotations

from typing import Any


SWING_CANDIDATE_EVAL_MODULE_VERSION = "patch-484-candidate-evaluation-module-extraction-prep"


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
        ],
        "next_extraction_target": "candidate_eval_progress_summary_builder",
    }


def attach_candidate_eval_module_status(payload: dict | None, *, patch_version: str) -> dict:
    out = dict(payload or {})
    out["swing_candidate_eval_module_version"] = SWING_CANDIDATE_EVAL_MODULE_VERSION
    out["swing_candidate_eval_module_status"] = candidate_eval_module_status(
        patch_version=patch_version
    )
    return out