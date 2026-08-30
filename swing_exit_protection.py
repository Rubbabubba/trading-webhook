"""Broker-free swing exit protection snapshot helpers.

app.py still owns live exit behavior for now. This module shapes fast,
snapshot-only exit protection diagnostics so operator surfaces can stay light
while the full exit engine is extracted in later cleanup phases.
"""

from __future__ import annotations

from typing import Any


SWING_EXIT_PROTECTION_MODULE_VERSION = "patch-628-exit-preview-wrapper-deletion"


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


def is_pending_entry_plan(plan: dict | None) -> bool:
    trade_plan = dict(plan or {})
    status = str(trade_plan.get("status") or "").strip().lower()
    order_status = str(trade_plan.get("order_status") or "").strip().lower()
    execution_state = str(trade_plan.get("execution_state") or "").strip().lower()
    filled_qty = _safe_float(trade_plan.get("filled_qty") or 0.0)
    submitted_qty = _safe_float(trade_plan.get("submitted_qty") or 0.0)

    return bool(
        status in {"pending", "submitted", "accepted", "open_order", "entry_submitted"}
        or order_status in {"new", "accepted", "pending_new", "submitted", "partially_filled"}
        or execution_state in {"pending", "submitted", "accepted", "open_order"}
        or (submitted_qty > 0 and filled_qty <= 0 and order_status not in {"filled", "canceled", "cancelled", "expired", "rejected"})
    )


def classify_exit_reason(
    *,
    current_price: float,
    stop_price: float,
    target_price: float,
    profit_lock_price: float,
) -> dict:
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
    return {
        "closest_exit_reason": closest_exit_reason,
        "exit_trigger_now": bool(hit_stop or hit_profit_lock or hit_target),
        "hit_stop": hit_stop,
        "hit_profit_lock": hit_profit_lock,
        "hit_target": hit_target,
    }


def classify_protection_status(
    *,
    has_plan: bool,
    has_broker_position_snapshot: bool,
    pending_entry_without_position: bool,
    has_enough_data: bool,
    has_exit_level: bool,
) -> str:
    if pending_entry_without_position:
        return "pending_entry_waiting_for_fill"
    if has_plan and has_enough_data and has_exit_level:
        return "protected"
    if has_broker_position_snapshot and not has_plan:
        return "broker_position_plan_recovery_needed"
    if not has_plan:
        return "plan_missing"
    if not has_enough_data:
        return "price_or_qty_missing"
    return "missing_exit_levels"


def _position_row(symbol: str, position: dict | None, plan: dict | None) -> dict:
    pos = dict(position or {})
    trade_plan = dict(plan or {})
    has_plan = bool(trade_plan and trade_plan.get("active"))
    has_broker_position_snapshot = bool(pos)
    pending_entry_plan = bool(is_pending_entry_plan(trade_plan))
    pending_entry_without_position = bool(pending_entry_plan and not has_broker_position_snapshot)
    recovery_needed = bool(has_broker_position_snapshot and not has_plan)
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
    has_enough_data = bool(has_plan and abs(qty) > 0 and current_price > 0 and entry_price > 0)
    exit_reason = classify_exit_reason(
        current_price=current_price,
        stop_price=stop_price,
        target_price=target_price,
        profit_lock_price=profit_lock_price,
    )
    protection_status = classify_protection_status(
        has_plan=has_plan,
        has_broker_position_snapshot=has_broker_position_snapshot,
        pending_entry_without_position=pending_entry_without_position,
        has_enough_data=has_enough_data,
        has_exit_level=has_exit_level,
    )

    return {
        "symbol": symbol,
        "has_broker_position_snapshot": has_broker_position_snapshot,
        "has_active_plan": has_plan,
        "p605_broker_position_plan_recovery_needed": recovery_needed,
        "pending_entry_plan": pending_entry_plan,
        "pending_entry_without_position": pending_entry_without_position,
        "qty": qty,
        "entry_price": entry_price,
        "current_price": current_price,
        "unrealized_pl": _safe_float(pos.get("unrealized_pl") or 0.0),
        "stop_price": stop_price or None,
        "target_price": target_price or None,
        "profit_lock_price": profit_lock_price or None,
        "closest_exit_reason": exit_reason["closest_exit_reason"],
        "exit_trigger_now": bool(exit_reason["exit_trigger_now"]),
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
    pending_entry_rows = [row for row in rows if bool(row.get("pending_entry_without_position"))]
    recovery_needed_symbols = [
        row.get("symbol")
        for row in rows
        if bool(row.get("p605_broker_position_plan_recovery_needed"))
    ]
    missing = [
        row.get("symbol")
        for row in rows
        if row.get("protection_status") != "protected" and not bool(row.get("pending_entry_without_position"))
    ]
    watch = [
        row for row in rows
        if bool(row.get("exit_trigger_now"))
        or (row.get("protection_status") != "protected" and not bool(row.get("pending_entry_without_position")))
    ]
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
            "pending_entry_protection_pending_count": len(pending_entry_rows),
            "pending_entry_protection_pending_symbols": [
                row.get("symbol")
                for row in pending_entry_rows
                if row.get("symbol")
            ],
            "giveback_exit_due_count": None,
            "giveback_exit_due_symbols": [],
            "failed_followthrough_exit_due_count": None,
            "failed_followthrough_exit_due_symbols": [],
            "forbidden_short_cleanup_due_count": None,
            "forbidden_short_cleanup_due_symbols": [],
            "all_active_positions_protected": len(missing) == 0,
            "p605_broker_position_plan_recovery_needed_count": len(recovery_needed_symbols),
            "p605_broker_position_plan_recovery_needed_symbols": recovery_needed_symbols,
            "fast_snapshot_only": True,
            "status_parity_with_heavy": "core_position_plan_protection_fields",
        },
        "missing_protection_symbols": missing,
        "pending_entry_protection_pending_symbols": [
            row.get("symbol")
            for row in pending_entry_rows
            if row.get("symbol")
        ],
        "pending_entry_protection_pending": pending_entry_rows[:lim],
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
        "p600_exit_watch_verification": {
            "enabled": True,
            "watch_count": len(watch),
            "watch_symbols": [
                row.get("symbol")
                for row in watch
                if row.get("symbol")
            ],
            "watch_reasons_by_symbol": {
                str(row.get("symbol") or ""): str(row.get("closest_exit_reason") or "none")
                for row in watch
                if row.get("symbol")
            },
            "all_active_positions_protected": len(missing) == 0,
            "fast_snapshot_only": True,
            "recommended_action": (
                "worker_exit_should_attempt_or_monitor_watch_symbols"
                if watch
                else "no_exit_watch_symbols"
            ),
        },
        "p605_broker_position_plan_recovery_truth": {
            "enabled": True,
            "fresh_snapshot": None,
            "snapshot_age_sec": None,
            "snapshot_reason": "omitted_from_fast_snapshot",
            "recovery_needed_symbols": recovery_needed_symbols,
            "stale_deactivation_guard": "fresh_snapshot_positions_are_never_deactivated_as_orphans",
            "recommended_action": (
                "run_or_wait_for_worker_exit_reconcile"
                if recovery_needed_symbols
                else "none"
            ),
        },
        "recommended_action": (
            "run_or_wait_for_worker_exit_reconcile"
            if recovery_needed_symbols
            else "inspect_active_exit_protection_truth_heavy_for_due_exit_details"
            if due_symbols
            else "inspect_missing_exit_protection_symbols"
            if missing
            else "monitor_pending_entry_order_status"
            if pending_entry_rows
            else "none"
        ),
    }


def build_breakout_partial_profit_bias_state(
    *,
    symbol: str,
    qty: float,
    qty_source: str,
    qty_to_close: float,
    unrealized_r: float,
    is_daily_breakout: bool,
    partial_taken: bool,
    enabled: bool,
    trigger_r: float,
    fraction: float,
    min_qty: float,
) -> dict:
    sym = str(symbol or "").strip().upper()
    available_qty = _safe_float(qty)
    close_qty = _safe_float(qty_to_close)
    trigger = _safe_float(trigger_r, 0.75)
    applies = bool(
        enabled
        and is_daily_breakout
        and available_qty > 0
        and not partial_taken
        and unrealized_r >= trigger
        and close_qty >= _safe_float(min_qty)
        and close_qty < available_qty
    )

    return {
        "enabled": bool(enabled),
        "symbol": sym,
        "is_daily_breakout": bool(is_daily_breakout),
        "applies": applies,
        "reason": "breakout_partial_profit_bias_ready" if applies else "not_ready",
        "unrealized_r": round(_safe_float(unrealized_r), 4),
        "trigger_r": trigger,
        "qty": round(available_qty, 4),
        "qty_source": str(qty_source or "unknown"),
        "qty_to_close": round(close_qty, 4),
        "fraction": round(_safe_float(fraction), 4),
        "partial_taken": bool(partial_taken),
        "module_contract": "breakout_partial_profit_bias_state",
    }


def build_dynamic_exit_preview_base(*, strategy_mode: str, side: str) -> dict:
    out = {
        "updates": {},
        "flags": [],
        "stall_exit": False,
        "stall_r": 0.0,
        "partial_profit_ready": False,
        "partial_profit_qty": 0.0,
        "oversized_winner_preservation_ready": False,
        "oversized_winner_preservation_qty": 0.0,
        "oversized_winner_preservation": {},
        "legacy_oversized_normalization_ready": False,
        "legacy_oversized_normalization_qty": 0.0,
        "legacy_oversized_normalization": {},
        "time_exit_grace": False,
        "module_contract": "dynamic_exit_preview_base",
    }
    if str(strategy_mode or "").strip().lower() != "swing":
        out["inactive_reason"] = "strategy_mode_not_swing"
    elif str(side or "buy").strip().lower() != "buy":
        out["inactive_reason"] = "non_long_position"
    return out


def build_partial_profit_state(
    *,
    qty: float,
    unrealized_r: float,
    partial_taken: bool,
    enabled: bool,
    trigger_r: float,
    fraction: float,
    min_qty: float,
    qty_to_close: float,
) -> dict:
    available_qty = _safe_float(qty)
    close_qty = _safe_float(qty_to_close)
    applies = bool(
        enabled
        and not partial_taken
        and _safe_float(unrealized_r) >= _safe_float(trigger_r)
        and close_qty >= _safe_float(min_qty)
        and close_qty < available_qty
    )
    return {
        "enabled": bool(enabled),
        "applies": applies,
        "reason": "partial_profit_ready" if applies else "not_ready",
        "unrealized_r": round(_safe_float(unrealized_r), 4),
        "trigger_r": _safe_float(trigger_r),
        "qty": round(available_qty, 4),
        "qty_to_close": round(close_qty, 4),
        "fraction": round(_safe_float(fraction), 4),
        "partial_taken": bool(partial_taken),
        "module_contract": "partial_profit_state",
    }


def build_time_exit_grace_state(
    *,
    hold_days: int | float,
    max_hold_days: int | float,
    unrealized_r: float,
    grace_r: float,
    grace_days: int | float,
) -> dict:
    hold = _safe_float(hold_days)
    max_hold = _safe_float(max_hold_days)
    grace_window_days = max(int(_safe_float(grace_days)), 0)
    applies = bool(
        max_hold > 0
        and hold >= max_hold
        and _safe_float(unrealized_r) >= _safe_float(grace_r)
        and hold < (max_hold + grace_window_days)
    )
    return {
        "applies": applies,
        "reason": "time_exit_grace" if applies else "not_ready",
        "hold_days": round(hold, 4),
        "max_hold_days": int(max_hold),
        "unrealized_r": round(_safe_float(unrealized_r), 4),
        "grace_r": _safe_float(grace_r),
        "grace_days": grace_window_days,
        "module_contract": "time_exit_grace_state",
    }


def _append_flag(dynamic_exit: dict, flag: str) -> None:
    flags = dynamic_exit.setdefault("flags", [])
    if flag and flag not in flags:
        flags.append(flag)


def apply_daily_breakout_giveback_state(
    dynamic_exit: dict,
    state: dict | None,
) -> None:
    giveback = dict(state or {})
    dynamic_exit["daily_breakout_profit_giveback"] = giveback
    if giveback.get("updates"):
        dynamic_exit.setdefault("updates", {}).update(dict(giveback.get("updates") or {}))
    if giveback.get("profit_lock_armed"):
        _append_flag(dynamic_exit, "daily_breakout_profit_lock_armed")
    if giveback.get("triggered"):
        dynamic_exit["daily_breakout_profit_giveback_exit"] = True
        _append_flag(dynamic_exit, "daily_breakout_profit_giveback_preservation_exit")


def apply_failed_followthrough_state(
    dynamic_exit: dict,
    state: dict | None,
) -> None:
    followthrough = dict(state or {})
    dynamic_exit["daily_breakout_failed_followthrough"] = followthrough
    if followthrough.get("triggered"):
        dynamic_exit["daily_breakout_failed_followthrough_exit"] = True
        _append_flag(dynamic_exit, "daily_breakout_failed_followthrough_exit")


def apply_triggered_qty_state(
    dynamic_exit: dict,
    state: dict | None,
    *,
    state_key: str,
    ready_key: str,
    qty_key: str,
    flag: str,
    entry_price: float,
    proposed_profit_lock: float,
) -> float:
    payload = dict(state or {})
    dynamic_exit[state_key] = payload
    if not bool(payload.get("triggered")):
        return _safe_float(proposed_profit_lock)

    dynamic_exit[ready_key] = True
    dynamic_exit[qty_key] = _safe_float(payload.get("qty_to_close"))
    _append_flag(dynamic_exit, flag)
    dynamic_exit.setdefault("updates", {})["break_even_armed"] = True
    return max(_safe_float(proposed_profit_lock), _safe_float(entry_price))


def apply_partial_profit_state(
    dynamic_exit: dict,
    state: dict | None,
    *,
    state_key: str,
    reason: str,
    flag: str,
    entry_price: float = 0.0,
    proposed_profit_lock: float = 0.0,
    arm_break_even: bool = False,
) -> float:
    payload = dict(state or {})
    dynamic_exit[state_key] = payload
    if not bool(payload.get("applies")):
        return _safe_float(proposed_profit_lock)

    dynamic_exit["partial_profit_ready"] = True
    dynamic_exit["partial_profit_qty"] = payload.get("qty_to_close")
    dynamic_exit["partial_profit_reason"] = reason
    _append_flag(dynamic_exit, flag)
    if arm_break_even:
        dynamic_exit.setdefault("updates", {})["break_even_armed"] = True
        return max(_safe_float(proposed_profit_lock), _safe_float(entry_price))
    return _safe_float(proposed_profit_lock)


def apply_time_exit_grace_state(
    dynamic_exit: dict,
    state: dict | None,
    *,
    entry_price: float,
    proposed_profit_lock: float,
) -> float:
    payload = dict(state or {})
    dynamic_exit["time_exit_grace_state"] = payload
    if not bool(payload.get("applies")):
        return _safe_float(proposed_profit_lock)

    dynamic_exit["time_exit_grace"] = True
    dynamic_exit.setdefault("updates", {})["time_exit_grace_active"] = True
    _append_flag(dynamic_exit, "time_exit_grace")
    return max(_safe_float(proposed_profit_lock), _safe_float(entry_price))


def apply_stall_loss_reduce_first_state(
    dynamic_exit: dict,
    state: dict | None,
) -> None:
    payload = dict(state or {})
    dynamic_exit["breakout_stall_loss_reduce_first"] = payload
    if not bool(payload.get("applies")):
        return

    dynamic_exit["stall_exit"] = False
    dynamic_exit["stall_loss_guard"] = False
    dynamic_exit["partial_profit_ready"] = True
    dynamic_exit["partial_profit_qty"] = payload.get("qty_to_close")
    dynamic_exit["partial_profit_reason"] = "breakout_stall_loss_reduce_first"
    _append_flag(dynamic_exit, "breakout_stall_loss_reduce_first_ready")


def dynamic_exit_preview_contract_status(*, heavy_requested: bool = False) -> dict:
    return {
        "dynamic_exit_preview_base_owner": "swing_exit_protection",
        "dynamic_exit_apply_helpers_owner": "swing_exit_protection",
        "partial_profit_state_owner": "swing_exit_protection",
        "time_exit_grace_state_owner": "swing_exit_protection",
        "breakout_partial_profit_bias_state_owner": "swing_exit_protection",
        "breakout_stall_loss_reduce_first_state_owner": "swing_exit_protection",
        "app_wrapper_status": "deleted",
        "app_wrappers_remaining": [],
        "runtime_adapter_owner": "app_runtime_facts_only",
        "active_exit_heavy_uses_module_contract": bool(heavy_requested),
    }


def build_breakout_stall_loss_reduce_first_state(
    *,
    symbol: str,
    qty: float,
    qty_source: str,
    qty_to_close: float,
    unrealized_r: float,
    is_daily_breakout: bool,
    stall_loss_due: bool,
    already_taken: bool,
    enabled: bool,
    fraction: float,
    min_qty: float,
) -> dict:
    sym = str(symbol or "").strip().upper()
    available_qty = _safe_float(qty)
    close_qty = _safe_float(qty_to_close)
    applies = bool(
        enabled
        and is_daily_breakout
        and stall_loss_due
        and close_qty >= _safe_float(min_qty)
        and close_qty < available_qty
        and not already_taken
    )

    return {
        "enabled": bool(enabled),
        "symbol": sym,
        "is_daily_breakout": bool(is_daily_breakout),
        "applies": applies,
        "reason": "breakout_stall_loss_reduce_first_ready" if applies else "not_ready",
        "stall_loss_due": bool(stall_loss_due),
        "unrealized_r": round(_safe_float(unrealized_r), 4),
        "qty": round(available_qty, 4),
        "qty_source": str(qty_source or "unknown"),
        "qty_to_close": round(close_qty, 4),
        "fraction": round(_safe_float(fraction), 4),
        "already_taken": bool(already_taken),
        "module_contract": "breakout_stall_loss_reduce_first_state",
    }


def breakout_dynamic_evidence_row(row: dict | None) -> dict | None:
    source = dict(row or {})
    dynamic = dict(source.get("dynamic_exit_preview") or {})
    partial_bias = dict(source.get("breakout_partial_profit_bias") or {})
    reduce_first = dict(source.get("breakout_stall_loss_reduce_first") or {})
    if not (partial_bias.get("is_daily_breakout") or reduce_first.get("is_daily_breakout")):
        return None
    return {
        "symbol": source.get("symbol"),
        "qty": source.get("qty"),
        "entry_price": source.get("entry_price"),
        "current_price": source.get("current_price"),
        "unrealized_pl": source.get("unrealized_pl"),
        "closest_exit_reason": source.get("closest_exit_reason"),
        "dynamic_flags": dynamic.get("flags"),
        "stall_r": dynamic.get("stall_r"),
        "partial_profit_ready": dynamic.get("partial_profit_ready"),
        "partial_profit_qty": dynamic.get("partial_profit_qty"),
        "partial_profit_reason": dynamic.get("partial_profit_reason"),
        "stall_exit": dynamic.get("stall_exit"),
        "stall_loss_guard": dynamic.get("stall_loss_guard"),
        "breakout_partial_profit_bias": partial_bias,
        "breakout_stall_loss_reduce_first": reduce_first,
    }


def build_breakout_stall_loss_containment_report(
    *,
    active_exit_truth: dict | None,
    config: dict | None,
    patch_version: str,
) -> dict:
    rows = []
    for row in list((active_exit_truth or {}).get("rows") or []):
        if not isinstance(row, dict):
            continue
        evidence = breakout_dynamic_evidence_row(row)
        if evidence:
            rows.append(evidence)

    partial_ready = [
        row.get("symbol") for row in rows
        if bool((row.get("breakout_partial_profit_bias") or {}).get("applies"))
    ]
    reduce_ready = [
        row.get("symbol") for row in rows
        if bool((row.get("breakout_stall_loss_reduce_first") or {}).get("applies"))
    ]

    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "breakout_stall_loss_containment",
        "source": "swing_exit_protection_dynamic_evidence_report",
        "enabled": dict((config or {}).get("enabled") or {}),
        "config": dict((config or {}).get("config") or {}),
        "breakout_position_count": len(rows),
        "partial_profit_bias_ready_symbols": partial_ready,
        "stall_loss_reduce_first_ready_symbols": reduce_ready,
        "rows": rows,
        "module_status": exit_protection_module_status(patch_version=patch_version),
        "recommended_action": (
            "worker_exit_should_reduce_breakout_stall_loss_candidates"
            if reduce_ready
            else "worker_exit_should_take_breakout_partial_profit"
            if partial_ready
            else "monitor_active_breakout_positions"
        ),
    }


def build_breakout_stall_loss_fast_snapshot(
    *,
    active_exit_snapshot: dict | None,
    config: dict | None,
    patch_version: str,
    limit: int = 20,
) -> dict:
    snapshot = dict(active_exit_snapshot or {})
    summary = dict(snapshot.get("summary") or {})
    rows = [
        dict(row)
        for row in list(snapshot.get("rows") or [])
        if isinstance(row, dict)
    ]
    safe_limit = max(1, min(int(limit or 20), 50))

    preview = []
    for row in rows[:safe_limit]:
        preview.append({
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "qty": row.get("qty"),
            "entry_price": row.get("entry_price"),
            "current_price": row.get("current_price"),
            "unrealized_pl": row.get("unrealized_pl"),
            "closest_exit_reason": row.get("closest_exit_reason"),
            "protection_status": row.get("protection_status"),
            "exit_trigger_now": row.get("exit_trigger_now"),
            "stop_price": row.get("stop_price"),
            "target_price": row.get("target_price"),
            "profit_lock_price": row.get("profit_lock_price"),
        })

    exit_watch_count = int(
        snapshot.get("exit_watch_count")
        or summary.get("exit_watch_count")
        or 0
    )
    missing_protection_count = int(
        summary.get("missing_protection_count")
        or snapshot.get("missing_protection_count")
        or 0
    )

    if exit_watch_count or missing_protection_count:
        recommended_action = "inspect_breakout_stall_loss_containment_heavy"
    else:
        recommended_action = "none"

    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "breakout_stall_loss_containment",
        "payload_mode": "fast_snapshot",
        "source": "swing_exit_protection_fast_active_exit_snapshot",
        "read_only": True,
        "dynamic_evidence_available": False,
        "dynamic_evidence_deferred": True,
        "heavy_available": True,
        "heavy_endpoint": "/diagnostics/breakout_stall_loss_containment?detail=heavy&limit=20",
        "enabled": dict((config or {}).get("enabled") or {}),
        "config": dict((config or {}).get("config") or {}),
        "active_exit_summary": {
            "position_count": summary.get("position_count"),
            "active_plan_count": summary.get("active_plan_count"),
            "missing_protection_count": missing_protection_count,
            "exit_watch_count": exit_watch_count,
            "all_active_positions_protected": summary.get("all_active_positions_protected"),
        },
        "breakout_position_count": None,
        "breakout_position_count_reason": "dynamic breakout evidence is deferred in light mode",
        "partial_profit_bias_ready_symbols": [],
        "stall_loss_reduce_first_ready_symbols": [],
        "rows": [],
        "active_exit_rows_preview": preview,
        "module_status": exit_protection_module_status(patch_version=patch_version),
        "recommended_action": recommended_action,
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
            "exit_reason_core_classifier",
            "position_protection_status_classifier",
            "pending_entry_protection_contract",
            "broker_position_plan_recovery_contract",
            "dynamic_exit_preview_base",
            "partial_profit_state",
            "time_exit_grace_state",
            "dynamic_exit_apply_helpers",
            "dynamic_exit_preview_contract_status",
            "breakout_partial_profit_bias_state",
            "breakout_stall_loss_reduce_first_state",
            "breakout_dynamic_evidence_report_shape",
            "breakout_stall_loss_fast_snapshot_shape",
            "exit_protection_module_status",
        ],
        "compatibility_wrappers_remaining": [],
        "runtime_adapter_owner": "app_runtime_facts_only",
        "next_extraction_target": "move_runtime_fact_adapter_to_exit_module_boundary",
    }
