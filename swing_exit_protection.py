"""Broker-free swing exit protection snapshot helpers.

app.py still owns live exit behavior for now. This module shapes fast,
snapshot-only exit protection diagnostics so operator surfaces can stay light
while the full exit engine is extracted in later cleanup phases.
"""

from __future__ import annotations

from typing import Any


SWING_EXIT_PROTECTION_MODULE_VERSION = "patch-620-exit-protection-module-extraction-prep"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


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


def _position_row(symbol: str, position: dict | None, plan: dict | None) -> dict:
    pos = dict(position or {})
    trade_plan = dict(plan or {})
    has_plan = bool(trade_plan and trade_plan.get("active"))
    qty = _safe_float(pos.get("qty") or trade_plan.get("filled_qty") or trade_plan.get("submitted_qty") or 0.0)
    current_price = _safe_float(pos.get("current_price") or pos.get("last_price") or 0.0)
    entry_price = _safe_float(
        pos.get("avg_entry_price")
        or trade_plan.get("avg_fill_price")
        or trade_plan.get("entry_price")
        or 0.0
    )
    stop_price = _safe_float(trade_plan.get("stop_price") or 0.0)
    target_price = _safe_float(trade_plan.get("take_price") or trade_plan.get("target_price") or 0.0)
    profit_lock_price = _safe_float(trade_plan.get("profit_lock_price") or 0.0)
    has_exit_level = bool(stop_price > 0 or target_price > 0 or profit_lock_price > 0)
    has_enough_data = bool(abs(qty) > 0 and current_price > 0 and entry_price > 0)

    hit_stop = bool(stop_price > 0 and current_price > 0 and current_price <= stop_price)
    hit_profit_lock = bool(profit_lock_price > 0 and current_price > 0 and current_price <= profit_lock_price)
    hit_target = bool(target_price > 0 and current_price > 0 and current_price >= target_price)
    closest_exit_reason = "none"
    if hit_stop:
        closest_exit_reason = "stop"
    elif hit_profit_lock:
        closest_exit_reason = "profit_lock_stop"
    elif hit_target:
        closest_exit_reason = "target"

    protection_status = (
        "protected"
        if has_plan and has_enough_data and has_exit_level
        else "plan_missing"
        if not has_plan
        else "price_or_qty_missing"
        if not has_enough_data
        else "missing_exit_levels"
    )

    return {
        "symbol": symbol,
        "has_broker_position_snapshot": bool(pos),
        "has_active_plan": has_plan,
        "qty": qty,
        "entry_price": entry_price,
        "current_price": current_price,
        "unrealized_pl": _safe_float(pos.get("unrealized_pl") or 0.0),
        "stop_price": stop_price or None,
        "target_price": target_price or None,
        "profit_lock_price": profit_lock_price or None,
        "closest_exit_reason": closest_exit_reason,
        "exit_trigger_now": bool(hit_stop or hit_profit_lock or hit_target),
        "exit_worker_has_enough_data": has_enough_data,
        "protection_status": protection_status,
        "order_status": trade_plan.get("order_status"),
        "source": trade_plan.get("source"),
        "fast_snapshot_only": True,
    }


def build_fast_active_exit_snapshot(
    *,
    positions_by_symbol: dict | None,
    trade_plan: dict | None,
    patch_version: str,
    limit: int = 20,
) -> dict:
    positions = {
        str(sym or "").strip().upper(): dict(pos or {})
        for sym, pos in dict(positions_by_symbol or {}).items()
        if str(sym or "").strip() and isinstance(pos, dict)
    }
    plans = {
        str(sym or "").strip().upper(): dict(plan or {})
        for sym, plan in dict(trade_plan or {}).items()
        if str(sym or "").strip() and isinstance(plan, dict) and bool(plan.get("active"))
    }
    symbols = sorted(set(positions) | set(plans))
    rows = [_position_row(sym, positions.get(sym), plans.get(sym)) for sym in symbols]
    missing = [row.get("symbol") for row in rows if row.get("protection_status") != "protected"]
    watch = [row for row in rows if bool(row.get("exit_trigger_now")) or row.get("protection_status") != "protected"]
    due_symbols = _dedupe_keep_order([
        str(row.get("symbol") or "").strip().upper()
        for row in watch
        if bool(row.get("exit_trigger_now")) and str(row.get("symbol") or "").strip()
    ])
    lim = max(1, min(int(limit or 20), 100))

    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "active_exit_protection_truth",
        "payload_mode": "fast_snapshot",
        "source": "swing_exit_protection_snapshot_no_dynamic_rebuild",
        "read_only": True,
        "fast_contract": "positions_snapshot_and_trade_plan_memory_no_dynamic_exit_rebuild_no_broker",
        "heavy_available": True,
        "heavy_endpoint": "/diagnostics/active_exit_protection_truth?detail=heavy&limit=20",
        "summary": {
            "position_count": len(positions),
            "active_plan_count": len(plans),
            "row_count": len(rows),
            "missing_protection_count": len(missing),
            "exit_watch_count": len(watch),
            "giveback_exit_due_count": None,
            "giveback_exit_due_symbols": [],
            "failed_followthrough_exit_due_count": None,
            "failed_followthrough_exit_due_symbols": [],
            "all_active_positions_protected": len(missing) == 0,
            "fast_snapshot_only": True,
        },
        "missing_protection_symbols": missing,
        "actionable_exit_watch": watch[:lim],
        "rows": rows[:lim],
        "fast_omitted_fields": [
            "daily_breakout_profit_giveback",
            "daily_breakout_failed_followthrough",
            "breakout_partial_profit_bias",
            "breakout_stall_loss_reduce_first",
            "dynamic_exit_preview",
            "forbidden_short_cleanup",
        ],
        "module_status": exit_protection_module_status(patch_version=patch_version),
        "recommended_action": (
            "inspect_active_exit_protection_truth_heavy_for_due_exit_details"
            if due_symbols
            else "inspect_missing_exit_protection_symbols"
            if missing
            else "none"
        ),
    }


def exit_protection_module_status(*, patch_version: str) -> dict:
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_exit_protection",
        "module_version": SWING_EXIT_PROTECTION_MODULE_VERSION,
        "owns_runtime_state": False,
        "broker_calls": False,
        "submits_orders": False,
        "app_globals_required": False,
        "extraction_phase": "prep",
        "responsibilities": [
            "active_exit_fast_snapshot_shape",
            "exit_protection_module_status",
        ],
        "next_extraction_target": "move_active_exit_protection_truth_out_of_app_py",
    }
