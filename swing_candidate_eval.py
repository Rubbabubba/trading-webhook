"""Broker-free swing candidate evaluation truth helpers.

This module does not evaluate symbols or submit orders. app.py still owns the
runtime scanner loop for now; this file only shapes candidate-eval status,
timeouts, and module adoption truth.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Callable


SWING_CANDIDATE_EVAL_MODULE_VERSION = "patch-706-candidate-evaluation-ownership"


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


def initial_loop_state() -> dict:
    return {
        "runtime_budget_state": initial_budget_state(),
        "merge_state": {"results": [], "signals": [], "blocked": 0},
        "shutdown_truth": {},
        "pending_cancel_truth": {},
        "future_submit_truth": {},
        "wait_timeout_state": wait_timeout_state(),
        "pending_wait_truth": {},
        "future_result_truth": {},
        "exception_log_truth": {},
        "result_branch_truth": {},
        "completed_futures_truth": {},
        "budget_wait_contract_truth": {},
        "wait_process_step_truth": {},
        "loop_iteration_truth": {},
        "loop_iteration_state_truth": {},
        "remaining_budget_truth": {},
        "future_to_symbol": {},
        "p499_candidate_eval_loop_state_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "broker_calls": False,
            "submits_orders": False,
        },
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


def submit_eval_future(
    executor: Any,
    future_to_symbol: dict | None,
    eval_fn: Any,
    symbol: str,
    future_started_at: dict | None = None,
    now_perf: float | None = None,
) -> Any:
    symbol_clean = str(symbol or "").strip().upper()
    fut = executor.submit(eval_fn, symbol)
    if future_to_symbol is not None:
        future_to_symbol[fut] = symbol_clean or symbol
    if future_started_at is not None:
        future_started_at[fut] = float(now_perf if now_perf is not None else 0.0)
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


def prune_timed_out_pending_futures(
    *,
    pending: Any,
    future_to_symbol: dict | None,
    future_started_at: dict | None,
    merge_state: dict | None,
    timeout_sec: float,
    now_perf: float,
    timeout_row_fn: Callable[[str, float, float, str], dict],
) -> dict:
    pending_set = set(pending or set())
    future_map = future_to_symbol if isinstance(future_to_symbol, dict) else {}
    started_map = future_started_at if isinstance(future_started_at, dict) else {}
    next_merge_state = dict(merge_state or {"results": [], "signals": [], "blocked": 0})
    timeout_symbols: list[str] = []
    timeout_rows: list[dict] = []
    timed_out_futures = []
    threshold = max(1.0, float(timeout_sec or 0.0))
    now_value = float(now_perf or 0.0)

    for fut in list(pending_set):
        started = started_map.get(fut)
        if started is None:
            continue
        elapsed = max(0.0, now_value - float(started or 0.0))
        if elapsed < threshold:
            continue
        symbol = str(future_map.get(fut) or "").strip().upper()
        if not symbol:
            continue
        try:
            fut.cancel()
        except Exception:
            pass
        row = dict(timeout_row_fn(symbol, elapsed, threshold, "candidate_eval_future_timeout") or {})
        timeout_symbols.append(symbol)
        timeout_rows.append(row)
        timed_out_futures.append(fut)
        pending_set.discard(fut)
        future_map.pop(fut, None)
        started_map.pop(fut, None)
        next_merge_state = merge_eval_output(
            next_merge_state,
            {"results": [row], "signals": [], "blocked": 1},
        )

    return {
        "pending": pending_set,
        "future_to_symbol": future_map,
        "future_started_at": started_map,
        "merge_state": next_merge_state,
        "timed_out_futures": timed_out_futures,
        "timed_out_symbols": _dedupe_keep_order(timeout_symbols),
        "timeout_rows": timeout_rows,
        "p510_candidate_eval_future_timeout_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "enabled": True,
            "timeout_sec": round(threshold, 3),
            "timed_out_count": len(timeout_symbols),
            "timed_out_symbols": _dedupe_keep_order(timeout_symbols),
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


def process_completed_futures(
    completed_futures: Any,
    future_to_symbol: dict | None,
    merge_state: dict | None,
) -> dict:
    next_merge_state = dict(merge_state or {"results": [], "signals": [], "blocked": 0})
    future_result_truth = {}
    exception_log_truth = {}
    result_branch_truth = {}
    exception_logs = []
    processed_count = 0

    for fut in list(completed_futures or []):
        processed_count += 1
        future_result = resolve_completed_future(fut, future_to_symbol)
        future_result_truth = {
            "p496_candidate_eval_future_result_helper": dict(
                future_result.get("p496_candidate_eval_future_result_helper") or {}
            )
        }
        branch_result = apply_future_result_to_merge_state(next_merge_state, future_result)
        next_merge_state = branch_result.get("merge_state") or next_merge_state
        result_branch_truth = {
            "p498_candidate_eval_result_branch_helper": dict(
                branch_result.get("p498_candidate_eval_result_branch_helper") or {}
            )
        }
        exception_log = dict(branch_result.get("exception_log") or {})
        if exception_log:
            exception_logs.append(exception_log)
            exception_log_truth = {
                "p497_candidate_eval_exception_log_contract": dict(
                    exception_log.get("p497_candidate_eval_exception_log_contract") or {}
                )
            }

    return {
        "merge_state": next_merge_state,
        "exception_logs": exception_logs,
        "future_result_truth": future_result_truth,
        "exception_log_truth": exception_log_truth,
        "result_branch_truth": result_branch_truth,
        "p500_candidate_eval_completed_futures_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "processed_count": int(processed_count),
            "exception_count": len(exception_logs),
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


def budget_wait_contract(
    *,
    budget_sec: float,
    elapsed_sec: float,
    pending: Any,
    future_to_symbol: dict | None,
    max_wait_sec: float = 2.0,
) -> dict:
    remaining_sec = remaining_budget_sec(
        budget_sec=budget_sec,
        elapsed_sec=elapsed_sec,
    )
    pending_cancel_truth = {}
    if remaining_sec <= 0:
        pending_cancel_truth = cancel_pending_futures(pending, future_to_symbol)
    return {
        "remaining_sec": remaining_sec,
        "wait_timeout_sec": capped_wait_timeout_sec(remaining_sec, max_wait_sec=max_wait_sec),
        "cancel_for_budget": remaining_sec <= 0,
        "pending_cancel_truth": pending_cancel_truth,
        "remaining_budget_truth": remaining_budget_summary(
            budget_sec=budget_sec,
            elapsed_sec=elapsed_sec,
            remaining_sec=remaining_sec,
        ),
        "p501_candidate_eval_budget_wait_contract": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "budget_sec": round(float(budget_sec or 0.0), 4),
            "elapsed_sec": round(float(elapsed_sec or 0.0), 4),
            "remaining_sec": round(float(remaining_sec or 0.0), 4),
            "cancel_for_budget": remaining_sec <= 0,
            "pending_count": len(list(pending or [])),
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


def wait_and_process_pending_step(
    *,
    pending: Any,
    future_to_symbol: dict | None,
    merge_state: dict | None,
    wait_timeout_state_row: dict | None,
    wait_timeout_sec: float,
) -> dict:
    done, still_pending, pending_wait_truth = wait_for_completed_future(
        pending,
        timeout_sec=wait_timeout_sec,
    )
    next_wait_timeout_state = record_wait_result(
        wait_timeout_state_row,
        wait_timeout_sec=wait_timeout_sec,
        completed_count=len(done),
    )
    if not done:
        return {
            "done": done,
            "pending": still_pending,
            "merge_state": merge_state,
            "wait_timeout_state": next_wait_timeout_state,
            "pending_wait_truth": pending_wait_truth,
            "future_result_truth": {},
            "exception_log_truth": {},
            "result_branch_truth": {},
            "completed_futures_truth": {},
            "exception_logs": [],
            "p502_candidate_eval_wait_process_step_helper": {
                "module": "swing_candidate_eval",
                "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
                "done_count": 0,
                "pending_count": len(still_pending),
                "processed_completed_futures": False,
                "broker_calls": False,
                "submits_orders": False,
            },
        }

    completed_result = process_completed_futures(
        done,
        future_to_symbol,
        merge_state,
    )
    return {
        "done": done,
        "pending": still_pending,
        "merge_state": completed_result.get("merge_state") or merge_state,
        "wait_timeout_state": next_wait_timeout_state,
        "pending_wait_truth": pending_wait_truth,
        "future_result_truth": completed_result.get("future_result_truth") or {},
        "exception_log_truth": completed_result.get("exception_log_truth") or {},
        "result_branch_truth": completed_result.get("result_branch_truth") or {},
        "completed_futures_truth": {
            "p500_candidate_eval_completed_futures_helper": dict(
                completed_result.get("p500_candidate_eval_completed_futures_helper") or {}
            )
        },
        "exception_logs": list(completed_result.get("exception_logs") or []),
        "p502_candidate_eval_wait_process_step_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "done_count": len(done),
            "pending_count": len(still_pending),
            "processed_completed_futures": True,
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def pending_loop_iteration(
    *,
    budget_sec: float,
    elapsed_sec: float,
    pending: Any,
    future_to_symbol: dict | None,
    merge_state: dict | None,
    wait_timeout_state_row: dict | None,
) -> dict:
    contract = budget_wait_contract(
        budget_sec=budget_sec,
        elapsed_sec=elapsed_sec,
        pending=pending,
        future_to_symbol=future_to_symbol,
    )
    remaining_budget_truth = dict(contract.get("remaining_budget_truth") or {})
    budget_wait_contract_truth = {
        "p501_candidate_eval_budget_wait_contract": dict(
            contract.get("p501_candidate_eval_budget_wait_contract") or {}
        )
    }
    if contract.get("cancel_for_budget"):
        return {
            "pending": pending,
            "merge_state": merge_state,
            "wait_timeout_state": wait_timeout_state_row,
            "pending_cancel_truth": dict(contract.get("pending_cancel_truth") or {}),
            "remaining_budget_truth": remaining_budget_truth,
            "budget_wait_contract_truth": budget_wait_contract_truth,
            "pending_wait_truth": {},
            "future_result_truth": {},
            "exception_log_truth": {},
            "result_branch_truth": {},
            "completed_futures_truth": {},
            "wait_process_step_truth": {},
            "exception_logs": [],
            "continue_loop": False,
            "break_loop": True,
            "budget_canceled": True,
            "p503_candidate_eval_loop_iteration_helper": {
                "module": "swing_candidate_eval",
                "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
                "action": "budget_cancel",
                "pending_count": len(list(pending or [])),
                "broker_calls": False,
                "submits_orders": False,
            },
        }

    step = wait_and_process_pending_step(
        pending=pending,
        future_to_symbol=future_to_symbol,
        merge_state=merge_state,
        wait_timeout_state_row=wait_timeout_state_row,
        wait_timeout_sec=float(contract.get("wait_timeout_sec") or 0.0),
    )
    return {
        "pending": step.get("pending") or set(),
        "merge_state": step.get("merge_state") or merge_state,
        "wait_timeout_state": step.get("wait_timeout_state") or wait_timeout_state_row,
        "pending_cancel_truth": {},
        "remaining_budget_truth": remaining_budget_truth,
        "budget_wait_contract_truth": budget_wait_contract_truth,
        "pending_wait_truth": step.get("pending_wait_truth") or {},
        "future_result_truth": step.get("future_result_truth") or {},
        "exception_log_truth": step.get("exception_log_truth") or {},
        "result_branch_truth": step.get("result_branch_truth") or {},
        "completed_futures_truth": step.get("completed_futures_truth") or {},
        "wait_process_step_truth": {
            "p502_candidate_eval_wait_process_step_helper": dict(
                step.get("p502_candidate_eval_wait_process_step_helper") or {}
            )
        },
        "exception_logs": list(step.get("exception_logs") or []),
        "continue_loop": not bool(step.get("done")),
        "break_loop": False,
        "budget_canceled": False,
        "p503_candidate_eval_loop_iteration_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "action": "wait_process",
            "done_count": len(list(step.get("done") or [])),
            "pending_count": len(list(step.get("pending") or [])),
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def loop_iteration_state_update(
    loop_iteration: dict | None,
    *,
    current_merge_state: dict | None,
    current_wait_timeout_state: dict | None,
    current_pending_cancel_truth: dict | None,
) -> dict:
    row = dict(loop_iteration or {})
    pending_cancel_truth = row.get("pending_cancel_truth") or current_pending_cancel_truth or {}
    return {
        "pending": row.get("pending") or set(),
        "merge_state": row.get("merge_state") or current_merge_state,
        "wait_timeout_state": row.get("wait_timeout_state") or current_wait_timeout_state,
        "pending_cancel_truth": pending_cancel_truth,
        "remaining_budget_truth": row.get("remaining_budget_truth") or {},
        "budget_wait_contract_truth": row.get("budget_wait_contract_truth") or {},
        "pending_wait_truth": row.get("pending_wait_truth") or {},
        "future_result_truth": row.get("future_result_truth") or {},
        "exception_log_truth": row.get("exception_log_truth") or {},
        "result_branch_truth": row.get("result_branch_truth") or {},
        "completed_futures_truth": row.get("completed_futures_truth") or {},
        "wait_process_step_truth": row.get("wait_process_step_truth") or {},
        "loop_iteration_truth": {
            "p503_candidate_eval_loop_iteration_helper": dict(
                row.get("p503_candidate_eval_loop_iteration_helper") or {}
            )
        },
        "break_loop": bool(row.get("break_loop")),
        "continue_loop": bool(row.get("continue_loop")),
        "exception_logs": list(row.get("exception_logs") or []),
        "budget_stopped_symbols": list(dict(pending_cancel_truth or {}).get("pending_symbols") or []),
        "p504_candidate_eval_loop_state_update_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "break_loop": bool(row.get("break_loop")),
            "continue_loop": bool(row.get("continue_loop")),
            "exception_log_count": len(list(row.get("exception_logs") or [])),
            "broker_calls": False,
            "submits_orders": False,
        },
    }


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


def runner_boundary_truth(
    *,
    symbol_count: int,
    future_count: int,
    max_workers: int,
    runtime_budget_sec: float,
) -> dict:
    return {
        "p506_candidate_eval_runner_boundary": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "runner_owned_by_app": True,
            "state_shape_owned_by_module": True,
            "eval_function_owned_by_app": True,
            "broker_calls": False,
            "submits_orders": False,
            "symbol_count": int(symbol_count or 0),
            "future_count": int(future_count or 0),
            "max_workers": int(max_workers or 0),
            "runtime_budget_sec": round(float(runtime_budget_sec or 0.0), 4),
            "next_safe_move": "move_runner_scaffolding_after_market_scan_proof",
        }
    }


def open_runner_scaffold(
    *,
    max_workers: int,
    runtime_budget_sec: float,
) -> dict:
    worker_count = max(1, int(max_workers or 1))
    budget_sec = float(runtime_budget_sec or 1.0)
    return {
        "executor": ThreadPoolExecutor(max_workers=worker_count),
        "runtime_budget_sec": budget_sec,
        "p507_candidate_eval_runner_scaffold": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "executor_created": True,
            "max_workers": worker_count,
            "runtime_budget_sec": round(budget_sec, 4),
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def runner_wait_scaffold(
    *,
    future_to_symbol: dict | None,
    runtime_budget_sec: float,
) -> dict:
    futures = dict(future_to_symbol or {})
    return {
        "pending": set(futures.keys()),
        "runtime_budget_sec": float(runtime_budget_sec or 1.0),
        "p507_candidate_eval_runner_wait_scaffold": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "future_count": len(futures),
            "pending_count": len(futures),
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def submit_symbol_futures(
    *,
    executor: Any,
    symbols: list[str],
    future_to_symbol: dict | None,
    eval_fn: Callable[[str], Any],
    over_budget_fn: Callable[[], bool],
    runtime_budget_state: dict | None,
    start_index: int = 0,
    max_submit: int | None = None,
    future_started_at: dict | None = None,
    now_perf_fn: Callable[[], float] | None = None,
) -> dict:
    symbol_list = list(symbols or [])
    idx = max(0, int(start_index or 0))
    submit_limit = len(symbol_list) - idx if max_submit is None else max(0, int(max_submit or 0))
    end_idx = min(len(symbol_list), idx + submit_limit)
    futures = future_to_symbol if isinstance(future_to_symbol, dict) else {}
    started_at = future_started_at if isinstance(future_started_at, dict) else {}
    budget_state = dict(runtime_budget_state or initial_budget_state())
    submitted_futures: list[Any] = []
    submitted_symbols: list[str] = []
    budget_stopped_symbols: list[str] = []
    for symbol in symbol_list[idx:end_idx]:
        symbol_clean = str(symbol or "").strip().upper()
        if over_budget_fn():
            budget_state = mark_budget_stopped_symbol(budget_state, symbol_clean)
            if symbol_clean:
                budget_stopped_symbols.append(symbol_clean)
            continue
        now_perf = float(now_perf_fn()) if now_perf_fn is not None else 0.0
        fut = submit_eval_future(
            executor,
            futures,
            eval_fn,
            symbol_clean,
            future_started_at=started_at,
            now_perf=now_perf,
        )
        submitted_futures.append(fut)
        if symbol_clean:
            submitted_symbols.append(symbol_clean)
    next_index = end_idx
    return {
        "future_to_symbol": futures,
        "future_started_at": started_at,
        "runtime_budget_state": budget_state,
        "submitted_futures": submitted_futures,
        "next_index": next_index,
        "remaining_symbols": symbol_list[next_index:],
        "batch_complete": next_index >= len(symbol_list),
        "future_submit_truth": future_submit_summary(futures),
        "p508_candidate_eval_submission_loop_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "symbol_count": len(symbol_list),
            "submitted_count": len(submitted_symbols),
            "submitted_symbols": submitted_symbols,
            "budget_stopped_count": len(budget_stopped_symbols),
            "budget_stopped_symbols": _dedupe_keep_order(budget_stopped_symbols),
            "broker_calls": False,
            "submits_orders": False,
        },
        "p509_candidate_eval_batch_submission_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "symbol_count": len(symbol_list),
            "start_index": idx,
            "end_index": end_idx,
            "next_index": next_index,
            "max_submit": submit_limit,
            "submitted_count": len(submitted_symbols),
            "submitted_symbols": submitted_symbols,
            "remaining_count": max(0, len(symbol_list) - next_index),
            "batch_complete": next_index >= len(symbol_list),
            "budget_stopped_count": len(budget_stopped_symbols),
            "budget_stopped_symbols": _dedupe_keep_order(budget_stopped_symbols),
            "broker_calls": False,
            "submits_orders": False,
        },
    }


def build_eval_loop_summary(
    *,
    runtime_budget_state: dict | None,
    merge_state: dict | None,
    shutdown_truth: dict | None,
    pending_cancel_truth: dict | None,
    future_submit_truth: dict | None,
    pending_wait_truth: dict | None,
    future_result_truth: dict | None,
    exception_log_truth: dict | None,
    result_branch_truth: dict | None,
    completed_futures_truth: dict | None,
    budget_wait_contract_truth: dict | None,
    wait_process_step_truth: dict | None,
    loop_iteration_truth: dict | None,
    loop_iteration_state_truth: dict | None,
    wait_timeout_state: dict | None,
    remaining_budget_truth: dict | None,
    loop_state: dict | None,
    runner_boundary: dict | None = None,
    runner_scaffold: dict | None = None,
    runner_wait_scaffold: dict | None = None,
    runner_submission_loop: dict | None = None,
) -> dict:
    budget_summary = budget_state_summary(runtime_budget_state)
    merge_summary = result_merge_summary(merge_state)
    wait_summary = wait_timeout_summary(wait_timeout_state)
    summary = {
        **budget_summary,
        "p489_candidate_eval_result_merge_helper": merge_summary.get(
            "p489_candidate_eval_result_merge_helper"
        ),
        "p490_candidate_eval_executor_shutdown_helper": dict(shutdown_truth or {}),
        "p491_candidate_eval_pending_future_cancel_helper": dict(pending_cancel_truth or {}),
        **dict(future_submit_truth or {}),
        **dict(pending_wait_truth or {}),
        **dict(future_result_truth or {}),
        **dict(exception_log_truth or {}),
        **dict(result_branch_truth or {}),
        **dict(completed_futures_truth or {}),
        **dict(budget_wait_contract_truth or {}),
        **dict(wait_process_step_truth or {}),
        **dict(loop_iteration_truth or {}),
        **dict(loop_iteration_state_truth or {}),
        "p499_candidate_eval_loop_state_helper": dict(
            dict(loop_state or {}).get("p499_candidate_eval_loop_state_helper") or {}
        ),
        **wait_summary,
        **dict(remaining_budget_truth or {}),
        **dict(runner_boundary or {}),
        **dict(runner_scaffold or {}),
        **dict(runner_wait_scaffold or {}),
        **dict(runner_submission_loop or {}),
        "p505_candidate_eval_loop_summary_helper": {
            "module": "swing_candidate_eval",
            "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
            "broker_calls": False,
            "submits_orders": False,
            "summary_fields_owned": [
                "budget",
                "merge",
                "shutdown",
                "pending_cancel",
                "future_submit",
                "pending_wait",
                "future_result",
                "exception_log",
                "result_branch",
                "completed_futures",
                "budget_wait_contract",
                "wait_process_step",
                "loop_iteration",
                "loop_state_update",
                "wait_timeout",
                "remaining_budget",
            ],
        },
    }
    return {
        "results": list(merge_summary.get("results") or []),
        "signals": list(merge_summary.get("signals") or []),
        "blocked": int(merge_summary.get("blocked") or 0),
        "summary": summary,
        "runtime_budget_summary": budget_summary,
        "merge_summary": merge_summary,
        "wait_timeout_summary": wait_summary,
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
        "owns_candidate_eval_contract": True,
        "owns_candidate_eval_status_attachment": True,
        "broker_calls": False,
        "submits_orders": False,
        "app_globals_required": False,
        "extraction_phase": "candidate_evaluation_ownership",
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
            "candidate_eval_loop_state_shape",
            "candidate_eval_completed_futures_shape",
            "candidate_eval_budget_wait_contract_shape",
            "candidate_eval_wait_process_step_shape",
            "candidate_eval_loop_iteration_shape",
            "candidate_eval_loop_state_update_shape",
            "candidate_eval_loop_summary_shape",
            "candidate_eval_runner_boundary_shape",
            "candidate_eval_runner_scaffold_shape",
            "candidate_eval_runner_submission_loop_shape",
            "candidate_eval_module_status_attachment",
            "candidate_eval_ownership_contract_shape",
        ],
        "next_extraction_target": "move_candidate_row_evaluation_pipeline_behind_module_api",
    }


def build_candidate_eval_ownership_contract(*, patch_version: str, payload: dict | None) -> dict:
    row = dict(payload or {})
    latest_scan = dict(row.get("latest_scan") or {})
    incremental = dict(row.get("incremental_scan") or latest_scan.get("incremental_scan") or {})
    evaluation = dict(incremental.get("evaluation") or {})
    candidate_count = (
        row.get("candidate_count")
        or row.get("candidates_total")
        or latest_scan.get("candidate_count")
        or latest_scan.get("candidates_total")
        or 0
    )
    selected_symbols = list(
        row.get("selected_symbols")
        or latest_scan.get("selected_symbols")
        or []
    )
    evaluated_count = (
        row.get("symbols_eval_total")
        or latest_scan.get("symbols_eval_total")
        or evaluation.get("evaluated_count")
        or 0
    )
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_candidate_eval",
        "module_version": SWING_CANDIDATE_EVAL_MODULE_VERSION,
        "candidate_eval_owner": "swing_candidate_eval",
        "scanner_route_owner": "app.py",
        "scanner_runtime_owner": "app.py",
        "broker_calls": False,
        "submits_orders": False,
        "fetches_market_data": False,
        "route_adapter_only": True,
        "evaluated_count": int(evaluated_count or 0),
        "candidate_count": int(candidate_count or 0),
        "selected_count": len(selected_symbols),
        "selected_symbols": [
            str(symbol or "").strip().upper()
            for symbol in selected_symbols
            if str(symbol or "").strip()
        ],
        "candidate_truth_phase": row.get("scan_truth_phase") or latest_scan.get("scan_truth_phase"),
        "trade_judgable": bool(row.get("trade_judgable") or latest_scan.get("trade_judgable")),
        "extraction_phase": "candidate_eval_contract_owned_by_swing_candidate_eval",
        "next_extraction_target": "move_candidate_row_evaluation_pipeline_behind_module_api",
    }


def attach_candidate_eval_module_status(payload: dict | None, *, patch_version: str) -> dict:
    out = dict(payload or {})
    out["swing_candidate_eval_module_version"] = SWING_CANDIDATE_EVAL_MODULE_VERSION
    module_status = candidate_eval_module_status(patch_version=patch_version)
    out["swing_candidate_eval_module_status"] = module_status
    out["p484_candidate_eval_module"] = dict(module_status)
    out["p706_candidate_eval_ownership_contract"] = build_candidate_eval_ownership_contract(
        patch_version=patch_version,
        payload=out,
    )
    return out
