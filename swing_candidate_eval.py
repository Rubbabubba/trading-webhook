"""Broker-free swing candidate evaluation truth helpers.

This module does not evaluate symbols or submit orders. app.py still owns the
runtime scanner loop for now; this file only shapes candidate-eval status,
timeouts, and module adoption truth.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, wait
from typing import Any


SWING_CANDIDATE_EVAL_MODULE_VERSION = "patch-498-candidate-eval-result-branch-helper-extraction"


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


def initial_budget_state() -> dict:
    return {
        "enforced": False,
        "stopped_symbols": [],
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
    }


def mark_budget_stopped_symbol(state: dict | None, symbol: str) -> dict:
    out = dict(state or initial_budget_state())
    symbol_clean = str(symbol or "").strip().upper()
    if symbol_clean:
        out["enforced"] = True
        stopped = list(out.get("stopped_symbols") or [])
        stopped.append(symbol_clean)
        out["stopped_symbols"] = _dedupe_keep_order(stopped)
    return out


def mark_budget_stopped_symbols(state: dict | None, symbols: list[str]) -> dict:
    out = dict(state or initial_budget_state())
    for symbol in list(symbols or []):
        out = mark_budget_stopped_symbol(out, symbol)
    return out


def budget_state_summary(state: dict | None, *, max_symbols: int = 25) -> dict:
    row = dict(state or initial_budget_state())
    stopped_symbols = _dedupe_keep_order(list(row.get("stopped_symbols") or []))
    return {
        "runtime_budget_enforced": bool(row.get("enforced")),
        "runtime_budget_stopped_count": len(stopped_symbols),
        "runtime_budget_stopped_symbols": stopped_symbols[: max(0, int(max_symbols or 25))],
        "p488_candidate_eval_budget_state_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def merge_eval_output(target: dict | None, output: dict | None) -> dict:
    out = dict(target or {"results": [], "signals": [], "blocked": 0})
    result = dict(output or {})
    out["results"] = list(out.get("results") or []) + list(result.get("results") or [])
    out["signals"] = list(out.get("signals") or []) + list(result.get("signals") or [])
    out["blocked"] = int(out.get("blocked") or 0) + int(result.get("blocked") or 0)
    return out


def merge_eval_exception(target: dict | None, *, symbol: str, error: str) -> dict:
    out = dict(target or {"results": [], "signals": [], "blocked": 0})
    results = list(out.get("results") or [])
    results.append({
        "symbol": str(symbol or "").strip().upper(),
        "action": "blocked",
        "reason": "scan_future_exception",
        "err": str(error or ""),
        "price": None,
        "stop": None,
        "take": None,
        "p489_candidate_eval_result_merge_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "broker_calls": False,
            "submits_orders": False,
        },
    })
    out["results"] = results
    out["signals"] = list(out.get("signals") or [])
    out["blocked"] = int(out.get("blocked") or 0) + 1
    return out


def result_merge_summary(state: dict | None) -> dict:
    row = dict(state or {"results": [], "signals": [], "blocked": 0})
    return {
        "results": list(row.get("results") or []),
        "signals": list(row.get("signals") or []),
        "blocked": int(row.get("blocked") or 0),
        "p489_candidate_eval_result_merge_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def shutdown_executor(executor: Any) -> dict:
    truth = {
        "ok": True,
        "used_cancel_futures": False,
        "fallback_used": False,
        "error": None,
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
        "broker_calls": False,
        "submits_orders": False,
    }
    try:
        executor.shutdown(wait=False, cancel_futures=True)
        truth["used_cancel_futures"] = True
    except TypeError:
        truth["fallback_used"] = True
        executor.shutdown(wait=False)
    except Exception as exc:
        truth["ok"] = False
        truth["error"] = str(exc)
    return truth


def cancel_pending_futures(pending: Any, future_to_symbol: dict | None) -> dict:
    symbols = []
    errors = []
    canceled_count = 0
    future_map = dict(future_to_symbol or {})
    for fut in list(pending or []):
        symbol = str(future_map.get(fut) or "").strip().upper()
        if symbol:
            symbols.append(symbol)
        try:
            fut.cancel()
            canceled_count += 1
        except Exception as exc:
            errors.append({
                "symbol": symbol,
                "error": str(exc),
            })
    return {
        "pending_symbols": _dedupe_keep_order(symbols),
        "pending_count": len(list(pending or [])),
        "canceled_count": int(canceled_count),
        "error_count": len(errors),
        "errors": errors[:10],
        "p491_candidate_eval_pending_future_cancel_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def submit_eval_future(executor: Any, future_to_symbol: dict | None, eval_fn: Any, symbol: str) -> Any:
    symbol_clean = str(symbol or "").strip().upper()
    fut = executor.submit(eval_fn, symbol)
    if future_to_symbol is not None:
        future_to_symbol[fut] = symbol_clean or symbol
    return fut


def future_submit_summary(future_to_symbol: dict | None) -> dict:
    return {
        "p494_candidate_eval_future_submit_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "tracked_future_count": len(dict(future_to_symbol or {})),
            "tracked_symbols": _dedupe_keep_order([
                str(symbol or "").strip().upper()
                for symbol in dict(future_to_symbol or {}).values()
                if str(symbol or "").strip()
            ])[:50],
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def wait_timeout_state() -> dict:
    return {
        "empty_wait_count": 0,
        "last_wait_timeout_sec": None,
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
    }


def capped_wait_timeout_sec(remaining_sec: float, *, max_wait_sec: float = 2.0) -> float:
    return min(max(0.0, float(remaining_sec or 0.0)), max(0.0, float(max_wait_sec or 0.0)))


def wait_for_completed_future(pending: Any, *, timeout_sec: float) -> tuple[Any, Any, dict]:
    done, still_pending = wait(
        pending,
        timeout=float(timeout_sec or 0.0),
        return_when=FIRST_COMPLETED,
    )
    return done, still_pending, {
        "p495_candidate_eval_pending_wait_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "done_count": len(done),
            "pending_count": len(still_pending),
            "timeout_sec": round(float(timeout_sec or 0.0), 4),
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def resolve_completed_future(future: Any, future_to_symbol: dict | None) -> dict:
    symbol = str(dict(future_to_symbol or {}).get(future) or "").strip().upper()
    try:
        output = future.result()
        return {
            "ok": True,
            "symbol": symbol,
            "output": output,
            "error": None,
            "exception": None,
            "p496_candidate_eval_future_result_helper": {
                "module": "swing_candidate_eval",
                "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
                "symbol": symbol,
                "ok": True,
                "output_present": output is not None,
                "broker_calls": False,
                "submits_orders": False,
            },
        }
    except Exception as exc:
        return {
            "ok": False,
            "symbol": symbol,
            "output": None,
            "error": str(exc),
            "exception": exc,
            "p496_candidate_eval_future_result_helper": {
                "module": "swing_candidate_eval",
                "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
                "symbol": symbol,
                "ok": False,
                "error": str(exc),
                "exception_type": type(exc).__name__,
                "broker_calls": False,
                "submits_orders": False,
            },
        }


def future_exception_log_record(future_result: dict | None) -> dict:
    row = dict(future_result or {})
    exc = row.get("exception")
    return {
        "symbol": str(row.get("symbol") or "").strip().upper(),
        "error": str(row.get("error") or ""),
        "exc_info": (type(exc), exc, exc.__traceback__) if exc is not None else None,
        "p497_candidate_eval_exception_log_contract": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "symbol": str(row.get("symbol") or "").strip().upper(),
            "has_exception_object": exc is not None,
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def apply_future_result_to_merge_state(merge_state: dict | None, future_result: dict | None) -> dict:
    row = dict(future_result or {})
    if row.get("ok"):
        next_merge_state = merge_eval_output(merge_state, row.get("output"))
        exception_log = {}
    else:
        exception_log = future_exception_log_record(row)
        next_merge_state = merge_eval_exception(
            merge_state,
            symbol=exception_log.get("symbol"),
            error=str(exception_log.get("error") or ""),
        )
    return {
        "merge_state": next_merge_state,
        "exception_log": exception_log,
        "p498_candidate_eval_result_branch_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "future_ok": bool(row.get("ok")),
            "symbol": str(row.get("symbol") or "").strip().upper(),
            "exception_log_required": bool(exception_log),
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def remaining_budget_sec(*, budget_sec: float, elapsed_sec: float) -> float:
    return max(0.0, float(budget_sec or 0.0) - float(elapsed_sec or 0.0))


def remaining_budget_summary(*, budget_sec: float, elapsed_sec: float, remaining_sec: float) -> dict:
    return {
        "p493_candidate_eval_remaining_budget_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "budget_sec": round(float(budget_sec or 0.0), 4),
            "elapsed_sec": round(float(elapsed_sec or 0.0), 4),
            "remaining_sec": round(float(remaining_sec or 0.0), 4),
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def record_wait_result(state: dict | None, *, wait_timeout_sec: float, completed_count: int) -> dict:
    out = dict(state or wait_timeout_state())
    out["last_wait_timeout_sec"] = round(float(wait_timeout_sec or 0.0), 4)
    if int(completed_count or 0) <= 0:
        out["empty_wait_count"] = int(out.get("empty_wait_count") or 0) + 1
    return out


def wait_timeout_summary(state: dict | None) -> dict:
    row = dict(state or wait_timeout_state())
    return {
        "p492_candidate_eval_wait_timeout_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "empty_wait_count": int(row.get("empty_wait_count") or 0),
            "last_wait_timeout_sec": row.get("last_wait_timeout_sec"),
            "broker_calls": False,
            "submits_orders": False,
        },
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


def build_terminal_partial_summary(
    *,
    latest_summary: dict,
    terminal_partial_close: dict,
    publish_truth: dict,
    candidate_count: int,
    evaluated_count: int,
    patch_version: str,
) -> dict:
    out = dict(latest_summary or {})
    out["p477_terminal_partial_close"] = dict(terminal_partial_close or {})
    out["p477_terminal_partial_publish"] = dict(publish_truth or {})
    out["scan_truth_phase"] = "candidate_eval_terminal_partial_close"
    out["candidate_truth_published_before_reports"] = True
    out["candidate_bearing_scan"] = bool(int(candidate_count or 0) > 0 or int(evaluated_count or 0) > 0)
    out["trade_judgable"] = bool(int(candidate_count or 0) > 0 or int(evaluated_count or 0) > 0)
    out["regime_only_non_actionable"] = False
    out["p486_candidate_eval_terminal_partial_summary_builder"] = {
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
        "broker_calls": False,
        "submits_orders": False,
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


def symbol_eval_exception_row(
    *,
    symbol: str,
    strategy: str,
    error: str,
    exception_type: str | None = None,
    selected: bool = False,
) -> dict:
    return {
        "symbol": str(symbol or "").strip().upper(),
        "eligible": False,
        "selected": bool(selected),
        "strategy": str(strategy or ""),
        "rejection_reasons": ["candidate_eval_exception"],
        "candidate_eval_exception": str(error or ""),
        "exception_type": str(exception_type or ""),
        "p487_candidate_eval_result_row_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "broker_calls": False,
            "submits_orders": False,
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
            "candidate_eval_terminal_partial_summary_shape",
            "candidate_eval_exception_row_shape",
            "candidate_eval_loop_budget_state_shape",
            "candidate_eval_loop_result_merge_shape",
            "candidate_eval_executor_shutdown_shape",
            "candidate_eval_pending_future_cancel_shape",
            "candidate_eval_wait_timeout_shape",
            "candidate_eval_remaining_budget_shape",
            "candidate_eval_future_submit_shape",
            "candidate_eval_pending_wait_shape",
            "candidate_eval_future_result_shape",
            "candidate_eval_exception_log_contract_shape",
            "candidate_eval_result_branch_shape",
        ],
        "next_extraction_target": "candidate_eval_loop_compact_runner_prep",
    }


def attach_candidate_eval_module_status(payload: dict | None, *, patch_version: str) -> dict:
    out = dict(payload or {})
    out["swing_candidate_eval_module_version"] = SWING_CANDIDATE_EVAL_MODULE_VERSION
    out["swing_candidate_eval_module_status"] = candidate_eval_module_status(
        patch_version=patch_version
    )
    return out
