"""Broker-free swing performance report helpers.

app.py still owns live broker/runtime assembly for now. This module owns pure
report-shaping contracts for daily goal, profit path, and capital rotation
truth so performance reporting can be extracted without changing trade behavior.
"""

from __future__ import annotations

from typing import Any


SWING_PERFORMANCE_REPORTS_MODULE_VERSION = "patch-648-performance-report-module-extraction-prep"


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
    for value in list(values or []):
        key = str(value or "").strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def build_goal_gap_rotation_operator_plan(
    *,
    patch_version: str,
    primary: float,
    target_low: float,
    remaining_low: float,
    clean_upside: float,
    all_upside: float,
    recovery_upside: float,
    clean_goal_symbols: list,
    drag_symbols: list,
    near_stop_symbols: list,
    actionable_selected_symbols: list,
    retry_waiting_selected_symbols: list,
    non_actionable_selected_symbols: list,
) -> dict:
    clean_gap = max(0.0, _safe_float(remaining_low, 0.0) - max(0.0, _safe_float(clean_upside, 0.0)))
    all_gap = max(0.0, _safe_float(remaining_low, 0.0) - max(0.0, _safe_float(all_upside, 0.0)))
    drag_dependency = max(0.0, clean_gap - all_gap)
    clean_goal_symbols = _dedupe_keep_order(clean_goal_symbols or [])
    drag_symbols = _dedupe_keep_order(drag_symbols or [])
    near_stop_symbols = _dedupe_keep_order(near_stop_symbols or [])
    actionable_selected_symbols = _dedupe_keep_order(actionable_selected_symbols or [])
    retry_waiting_selected_symbols = _dedupe_keep_order(retry_waiting_selected_symbols or [])
    non_actionable_selected_symbols = _dedupe_keep_order(non_actionable_selected_symbols or [])
    goal_hit = bool(_safe_float(primary, 0.0) >= max(0.01, _safe_float(target_low, 100.0)))
    clean_reaches_goal = bool(clean_gap <= 0)
    drag_reaches_goal = bool(not clean_reaches_goal and all_gap <= 0 and drag_symbols)
    needs_fresh_quality = bool(not goal_hit and not clean_reaches_goal and not drag_reaches_goal)
    rotation_review_symbols = _dedupe_keep_order(near_stop_symbols + drag_symbols)
    action = (
        "preserve_profit_and_avoid_low_quality_new_entries"
        if goal_hit
        else "fix_actionable_selected_submit_gap"
        if actionable_selected_symbols
        else "let_clean_goal_gap_closers_work"
        if clean_reaches_goal and clean_goal_symbols
        else "review_drag_dependent_goal_path_for_rotation"
        if drag_reaches_goal and rotation_review_symbols
        else "wait_for_retry_or_next_quality_scan"
        if retry_waiting_selected_symbols
        else "wait_for_next_scan_quality_candidate_or_reduce_drag"
        if needs_fresh_quality
        else "monitor_goal_path"
    )
    return {
        "enabled": True,
        "patch_version": patch_version,
        "module": "swing_performance_reports",
        "module_version": SWING_PERFORMANCE_REPORTS_MODULE_VERSION,
        "status": (
            "goal_low_hit"
            if goal_hit
            else "clean_open_targets_can_reach_goal"
            if clean_reaches_goal
            else "drag_dependent_goal_path"
            if drag_reaches_goal
            else "fresh_quality_entry_needed"
        ),
        "clean_gap_after_open_targets": round(clean_gap, 4),
        "all_gap_after_open_targets": round(all_gap, 4),
        "drag_dependency_dollars": round(drag_dependency, 4),
        "recovery_upside_dollars": round(max(0.0, _safe_float(recovery_upside, 0.0)), 4),
        "clean_goal_symbols": clean_goal_symbols,
        "rotation_review_symbols": rotation_review_symbols,
        "near_stop_symbols": near_stop_symbols,
        "drag_symbols": drag_symbols,
        "actionable_selected_symbols": actionable_selected_symbols,
        "retry_waiting_selected_symbols": retry_waiting_selected_symbols,
        "non_actionable_selected_symbols": non_actionable_selected_symbols,
        "profit_improvement_focus": (
            "preserve_realized_goal"
            if goal_hit
            else "clean_winners"
            if clean_reaches_goal
            else "capital_rotation"
            if drag_reaches_goal
            else "fresh_quality_entry"
        ),
        "operator_guidance": (
            "goal already hit; avoid forcing low quality adds"
            if goal_hit
            else "clean winners can reach the low goal without relying on losers"
            if clean_reaches_goal
            else "goal is reachable only if drag names recover; review rotation candidates without manual babysitting"
            if drag_reaches_goal
            else "current open targets do not cleanly close the goal gap; wait for a fresh high quality scan or reduce drag"
        ),
        "recommended_action": action,
        "read_only": True,
        "does_not_submit_orders": True,
        "adds_trade_gate": False,
        "changes_submit_behavior": False,
        "changes_exit_behavior": False,
    }


def build_capital_rotation_action_contract(
    *,
    patch_version: str,
    rotation_rows: list,
    active_exit_truth: dict | None,
    rotation_plan: dict | None,
) -> dict:
    active_rows = {
        str(row.get("symbol") or "").strip().upper(): dict(row or {})
        for row in list((active_exit_truth or {}).get("rows") or [])
        if isinstance(row, dict) and str(row.get("symbol") or "").strip()
    }
    plan = dict(rotation_plan or {})
    candidates = [
        dict(row or {})
        for row in list(rotation_rows or [])
        if isinstance(row, dict) and bool(row.get("rotation_candidate"))
    ]
    action_rows = []
    for row in candidates:
        sym = str(row.get("symbol") or "").strip().upper()
        exit_row = active_rows.get(sym, {})
        protection_status = str(exit_row.get("protection_status") or "unknown")
        exit_actionable = bool(exit_row.get("exit_actionable_now"))
        exit_trigger = bool(exit_row.get("exit_trigger_now"))
        protected = protection_status == "protected"
        near_stop = bool(row.get("near_stop"))
        capital_drag = bool(row.get("capital_drag"))
        action_status = (
            "worker_exit_actionable_first"
            if exit_actionable
            else "protected_near_stop_rotation_watch"
            if protected and near_stop
            else "protected_drag_rotation_watch"
            if protected and capital_drag
            else "protection_recovery_before_rotation"
            if protection_status in {"broker_position_plan_recovery_needed", "plan_missing", "missing_exit_levels", "price_or_qty_missing"}
            else "monitor"
        )
        action_rows.append({
            "symbol": sym,
            "rotation_reason": row.get("rotation_reason"),
            "action_status": action_status,
            "protected": protected,
            "protection_status": protection_status,
            "exit_actionable_now": exit_actionable,
            "exit_trigger_now": exit_trigger,
            "near_stop": near_stop,
            "capital_drag": capital_drag,
            "unrealized_pl": row.get("unrealized_pl"),
            "market_value": row.get("market_value"),
            "risk_to_stop_dollars": row.get("risk_to_stop_dollars"),
            "distance_to_stop_dollars": row.get("distance_to_stop_dollars"),
            "readiness_score": row.get("readiness_score"),
            "replacement_focus": list(row.get("replacement_focus") or []),
            "operator_note": (
                "worker_exit_or_existing_stop_should_resolve_before_any_rotation_change"
                if exit_actionable
                else "protected_position_review_for_rotation_slot_release"
                if protected and (near_stop or capital_drag)
                else "fix_protection_truth_before_rotation_review"
                if protection_status != "protected"
                else "monitor"
            ),
        })
    protected_candidates = [row for row in action_rows if bool(row.get("protected"))]
    actionable_exit_rows = [row for row in action_rows if bool(row.get("exit_actionable_now"))]
    near_stop_rows = [row for row in action_rows if bool(row.get("near_stop"))]
    missing_protection_rows = [row for row in action_rows if not bool(row.get("protected"))]
    recommended_action = (
        "wait_for_worker_exit_before_rotation"
        if actionable_exit_rows
        else "fix_rotation_candidate_protection_before_action"
        if missing_protection_rows
        else "prepare_rotation_slot_release_review"
        if protected_candidates and plan.get("profit_improvement_focus") == "capital_rotation"
        else "no_rotation_action_needed"
    )
    return {
        "enabled": True,
        "patch_version": patch_version,
        "module": "swing_performance_reports",
        "module_version": SWING_PERFORMANCE_REPORTS_MODULE_VERSION,
        "source": "capital_rotation_readiness_and_fast_exit_protection_snapshot",
        "candidate_count": len(action_rows),
        "candidate_symbols": [row.get("symbol") for row in action_rows],
        "protected_candidate_count": len(protected_candidates),
        "protected_candidate_symbols": [row.get("symbol") for row in protected_candidates],
        "near_stop_candidate_count": len(near_stop_rows),
        "near_stop_candidate_symbols": [row.get("symbol") for row in near_stop_rows],
        "exit_actionable_candidate_count": len(actionable_exit_rows),
        "exit_actionable_candidate_symbols": [row.get("symbol") for row in actionable_exit_rows],
        "missing_protection_candidate_count": len(missing_protection_rows),
        "missing_protection_candidate_symbols": [row.get("symbol") for row in missing_protection_rows],
        "candidate_market_value": round(sum(_safe_float(row.get("market_value"), 0.0) for row in action_rows), 4),
        "candidate_unrealized_pl": round(sum(_safe_float(row.get("unrealized_pl"), 0.0) for row in action_rows), 4),
        "profit_improvement_focus": plan.get("profit_improvement_focus"),
        "drag_dependency_dollars": plan.get("drag_dependency_dollars"),
        "rows": action_rows,
        "recommended_action": recommended_action,
        "read_only": True,
        "does_not_submit_orders": True,
        "adds_trade_gate": False,
        "changes_submit_behavior": False,
        "changes_exit_behavior": False,
    }


def performance_reports_module_status(*, patch_version: str) -> dict:
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_performance_reports",
        "module_version": SWING_PERFORMANCE_REPORTS_MODULE_VERSION,
        "owns_live_broker_calls": False,
        "submits_orders": False,
        "changes_trade_behavior": False,
        "extraction_phase": "prep",
        "responsibilities": [
            "daily_goal_operator_plan_shape",
            "capital_rotation_action_contract_shape",
            "profit_path_truth_contract_shape",
        ],
        "next_extraction_target": "move_daily_goal_path_truth_and_capital_rotation_report_assembly",
    }
