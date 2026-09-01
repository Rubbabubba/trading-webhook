"""Broker-free swing performance report helpers.

app.py still owns live broker/runtime assembly for now. This module owns pure
report-shaping contracts for daily goal, profit path, and capital rotation
truth so performance reporting can be extracted without changing trade behavior.
"""

from __future__ import annotations

from typing import Any


SWING_PERFORMANCE_REPORTS_MODULE_VERSION = "patch-650-performance-report-daily-goal-assembly-extraction"


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


def build_daily_goal_path_truth(
    *,
    patch_version: str,
    goal_payload: dict | None,
    profit_payload: dict | None,
    drag_payload: dict | None,
    limit: int = 25,
) -> dict:
    goal_payload = dict(goal_payload or {})
    profit = dict(profit_payload or {})
    drag = dict(drag_payload or {})
    goal = dict(profit.get("daily_goal_progress") or {})
    rows = list(profit.get("rows") or [])
    primary = _safe_float(goal.get("primary_daily_pnl"), 0.0)
    target_low = max(0.01, _safe_float(goal.get("target_low"), 100.0))
    target_high = max(target_low, _safe_float(goal.get("target_high"), 200.0))
    remaining_low = max(0.0, _safe_float(goal.get("remaining_to_low"), target_low - primary))
    remaining_high = max(0.0, _safe_float(goal.get("remaining_to_high"), target_high - primary))
    path_rows = []
    for row in rows:
        row = dict(row or {})
        upside = max(0.0, _safe_float(row.get("target_upside_dollars"), 0.0))
        unreal = _safe_float(row.get("unrealized_pl"), 0.0)
        risk_to_stop = _safe_float(row.get("risk_to_stop_dollars"), 0.0)
        distance_to_stop = _safe_float(row.get("distance_to_stop_dollars"), 0.0)
        is_drag = bool(str(row.get("profit_capture_status") or "") == "drag_watch" or unreal < 0)
        clean_goal_candidate = bool(not is_drag and upside > 0)
        contribution_to_low = min(upside, remaining_low) if remaining_low > 0 else 0.0
        contribution_pct = (contribution_to_low / remaining_low * 100.0) if remaining_low > 0 else 100.0
        downside_pressure = abs(min(0.0, unreal)) + max(0.0, risk_to_stop - max(0.0, distance_to_stop))
        clean_contribution_to_low = contribution_to_low if clean_goal_candidate else 0.0
        path_quality_score = clean_contribution_to_low + max(0.0, unreal) - (downside_pressure * 0.5)
        role = (
            "goal_gap_closer"
            if clean_goal_candidate and contribution_to_low >= max(10.0, remaining_low * 0.25)
            else "supporting_winner"
            if unreal > 0
            else "recovery_upside_with_drag"
            if is_drag and upside > 0
            else "capital_drag"
            if unreal < 0
            else "neutral"
        )
        path_rows.append({
            "symbol": row.get("symbol"),
            "role": role,
            "unrealized_pl": round(unreal, 4),
            "target_upside_dollars": round(upside, 4),
            "contribution_to_low_target_dollars": round(contribution_to_low, 4),
            "clean_contribution_to_low_target_dollars": round(clean_contribution_to_low, 4),
            "contribution_to_low_target_pct": round(contribution_pct, 2),
            "risk_to_stop_dollars": round(risk_to_stop, 4),
            "distance_to_stop_dollars": row.get("distance_to_stop_dollars"),
            "clean_goal_candidate": clean_goal_candidate,
            "recovery_upside_only": bool(is_drag and upside > 0),
            "partial_profit_status": row.get("partial_profit_status"),
            "profit_capture_status": row.get("profit_capture_status"),
            "exit_trigger_now": bool(row.get("exit_trigger_now")),
            "path_quality_score": round(path_quality_score, 4),
            "recommended_read": (
                "primary_goal_path_symbol"
                if role == "goal_gap_closer"
                else "let_winner_work"
                if role == "supporting_winner"
                else "recovery_upside_not_clean_goal_path"
                if role == "recovery_upside_with_drag"
                else "review_capital_drag"
                if role == "capital_drag"
                else "monitor"
            ),
        })
    path_rows.sort(key=lambda r: _safe_float(r.get("path_quality_score"), 0.0), reverse=True)
    positive_upside = sum(max(0.0, _safe_float(r.get("target_upside_dollars"), 0.0)) for r in rows)
    clean_positive_upside = sum(
        max(0.0, _safe_float(r.get("target_upside_dollars"), 0.0))
        for r in rows
        if _safe_float(r.get("unrealized_pl"), 0.0) >= 0
    )
    recovery_upside = max(0.0, positive_upside - clean_positive_upside)
    drag_symbols = list(drag.get("capital_drag_symbols") or [])
    near_stop_symbols = list(drag.get("near_stop_symbols") or [])
    lim = max(1, min(int(limit or 25), 100))
    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "daily_goal_path_truth",
        "module": "swing_performance_reports",
        "module_version": SWING_PERFORMANCE_REPORTS_MODULE_VERSION,
        "source": "profit_capture_readiness_truth_and_capital_drag_audit",
        "read_only": True,
        "does_not_submit_orders": True,
        "daily_goal_progress": goal,
        "p639_broker_daily_goal_snapshot_consistency": goal_payload.get("p639_broker_daily_goal_snapshot_consistency") or profit.get("p639_broker_daily_goal_snapshot_consistency") or {},
        "summary": {
            "broker_daily_pnl": round(primary, 4),
            "target_low": round(target_low, 4),
            "target_high": round(target_high, 4),
            "remaining_to_low": round(remaining_low, 4),
            "remaining_to_high": round(remaining_high, 4),
            "open_unrealized_pl": _safe_float((profit.get("summary") or {}).get("open_unrealized_pl"), 0.0),
            "positive_target_upside_dollars": round(positive_upside, 4),
            "clean_positive_target_upside_dollars": round(clean_positive_upside, 4),
            "recovery_upside_dollars": round(recovery_upside, 4),
            "goal_low_reachable_from_clean_open_targets": bool(primary + clean_positive_upside >= target_low),
            "goal_high_reachable_from_clean_open_targets": bool(primary + clean_positive_upside >= target_high),
            "goal_low_reachable_from_all_open_targets": bool(primary + positive_upside >= target_low),
            "goal_high_reachable_from_all_open_targets": bool(primary + positive_upside >= target_high),
            "goal_gap_closer_count": len([r for r in path_rows if r.get("role") == "goal_gap_closer"]),
            "recovery_upside_drag_count": len([r for r in path_rows if r.get("role") == "recovery_upside_with_drag"]),
            "capital_drag_count": int((drag.get("summary") or {}).get("capital_drag_count") or 0),
            "near_stop_drag_count": int((drag.get("summary") or {}).get("near_stop_drag_count") or 0),
        },
        "goal_gap_closer_symbols": [r.get("symbol") for r in path_rows if r.get("role") == "goal_gap_closer"],
        "capital_drag_symbols": drag_symbols,
        "near_stop_symbols": near_stop_symbols,
        "rows": path_rows[:lim],
        "recommended_action": (
            "goal_low_hit_preserve_profit"
            if primary >= target_low
            else "let_goal_gap_closers_work_watch_drag"
            if primary + clean_positive_upside >= target_low and path_rows
            else "review_candidate_quality_and_capital_rotation"
            if drag_symbols
            else "wait_for_next_scan_or_winner_expansion"
        ),
    }


def build_daily_goal_opportunity_map(
    *,
    patch_version: str,
    path: dict | None,
    profit: dict | None,
    selected_candidate_truth: dict | None,
    operator_plan: dict | None,
    limit: int = 25,
) -> dict:
    path = dict(path or {})
    profit = dict(profit or {})
    selected_candidate_truth = dict(selected_candidate_truth or {})
    operator_plan = dict(operator_plan or {})
    goal = dict(path.get("daily_goal_progress") or {})
    summary = dict(path.get("summary") or {})
    profit_summary = dict(profit.get("summary") or {})
    primary = _safe_float(goal.get("primary_daily_pnl"), _safe_float(summary.get("broker_daily_pnl"), 0.0))
    target_low = max(0.01, _safe_float(goal.get("target_low"), _safe_float(summary.get("target_low"), 100.0)))
    target_high = max(target_low, _safe_float(goal.get("target_high"), _safe_float(summary.get("target_high"), 200.0)))
    remaining_low = max(0.0, _safe_float(goal.get("remaining_to_low"), target_low - primary))
    remaining_high = max(0.0, _safe_float(goal.get("remaining_to_high"), target_high - primary))
    clean_upside = max(0.0, _safe_float(summary.get("clean_positive_target_upside_dollars"), 0.0))
    all_upside = max(0.0, _safe_float(summary.get("positive_target_upside_dollars"), 0.0))
    recovery_upside = max(0.0, _safe_float(summary.get("recovery_upside_dollars"), max(0.0, all_upside - clean_upside)))
    actionable_selected_symbols = list(selected_candidate_truth.get("submit_gap_symbols") or [])
    non_actionable_selected_symbols = list(selected_candidate_truth.get("non_actionable_symbols") or [])
    retry_waiting_selected_symbols = list(selected_candidate_truth.get("retry_waiting_symbols") or [])
    clean_goal_symbols = list(path.get("goal_gap_closer_symbols") or [])
    drag_symbols = list(path.get("capital_drag_symbols") or [])
    near_stop_symbols = list(path.get("near_stop_symbols") or [])
    clean_gap_after_open_targets = max(0.0, remaining_low - clean_upside)
    all_gap_after_open_targets = max(0.0, remaining_low - all_upside)
    selected_candidate_status = str(selected_candidate_truth.get("status") or "")
    daily_goal_state = (
        "goal_low_hit"
        if primary >= target_low
        else "clean_open_targets_can_reach_goal"
        if clean_gap_after_open_targets <= 0
        else "open_targets_can_reach_goal_but_drag_dependent"
        if all_gap_after_open_targets <= 0
        else "fresh_quality_entry_needed"
    )
    profit_path = (
        "preserve_profit"
        if primary >= target_low
        else "let_clean_winners_work"
        if clean_gap_after_open_targets <= 0 and clean_goal_symbols
        else "manage_drag_and_wait_for_recovery"
        if all_gap_after_open_targets <= 0 and drag_symbols
        else "needs_new_high_quality_candidate"
    )
    candidate_path = (
        "selected_candidate_actionable_submit_gap"
        if actionable_selected_symbols
        else "selected_candidate_waiting_retry"
        if retry_waiting_selected_symbols
        else "selected_candidate_non_actionable"
        if non_actionable_selected_symbols
        else selected_candidate_status or "unknown"
    )
    rows = []
    for row in list(path.get("rows") or [])[:max(1, min(int(limit or 25), 100))]:
        row = dict(row or {})
        role = str(row.get("role") or "")
        rows.append({
            "symbol": row.get("symbol"),
            "role": role,
            "unrealized_pl": row.get("unrealized_pl"),
            "target_upside_dollars": row.get("target_upside_dollars"),
            "clean_contribution_to_low_target_dollars": row.get("clean_contribution_to_low_target_dollars"),
            "risk_to_stop_dollars": row.get("risk_to_stop_dollars"),
            "distance_to_stop_dollars": row.get("distance_to_stop_dollars"),
            "partial_profit_status": row.get("partial_profit_status"),
            "profit_capture_status": row.get("profit_capture_status"),
            "operator_priority": (
                "primary_goal_closer"
                if role == "goal_gap_closer"
                else "let_winner_work"
                if role == "supporting_winner"
                else "drag_watch_no_manual_babysit"
                if role in {"recovery_upside_with_drag", "capital_drag"}
                else "monitor"
            ),
        })
    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "daily_goal_opportunity_map",
        "module": "swing_performance_reports",
        "module_version": SWING_PERFORMANCE_REPORTS_MODULE_VERSION,
        "source": "daily_goal_path_truth_profit_capture_and_capital_drag",
        "read_only": True,
        "does_not_submit_orders": True,
        "daily_goal_state": daily_goal_state,
        "profit_path": profit_path,
        "candidate_path": candidate_path,
        "summary": {
            "broker_daily_pnl": round(primary, 4),
            "target_low": round(target_low, 4),
            "target_high": round(target_high, 4),
            "remaining_to_low": round(remaining_low, 4),
            "remaining_to_high": round(remaining_high, 4),
            "open_unrealized_pl": _safe_float(profit_summary.get("open_unrealized_pl"), 0.0),
            "clean_positive_target_upside_dollars": round(clean_upside, 4),
            "all_positive_target_upside_dollars": round(all_upside, 4),
            "recovery_upside_dollars": round(recovery_upside, 4),
            "drag_dependency_dollars": operator_plan.get("drag_dependency_dollars"),
            "gap_after_clean_open_targets": round(clean_gap_after_open_targets, 4),
            "gap_after_all_open_targets": round(all_gap_after_open_targets, 4),
            "profit_improvement_focus": operator_plan.get("profit_improvement_focus"),
            "goal_gap_closer_count": len(clean_goal_symbols),
            "capital_drag_count": len(drag_symbols),
            "near_stop_drag_count": len(near_stop_symbols),
            "ready_partial_profit_count": int(profit_summary.get("ready_partial_profit_count") or 0),
            "actionable_exit_due_count": int(profit_summary.get("actionable_exit_due_count") or 0),
            "selected_candidate_count": int(selected_candidate_truth.get("selected_count") or 0),
            "actionable_selected_candidate_count": len(actionable_selected_symbols),
            "non_actionable_selected_candidate_count": len(non_actionable_selected_symbols),
            "retry_waiting_selected_candidate_count": len(retry_waiting_selected_symbols),
        },
        "goal_gap_closer_symbols": clean_goal_symbols,
        "capital_drag_symbols": drag_symbols,
        "near_stop_symbols": near_stop_symbols,
        "selected_candidate_symbols": list(selected_candidate_truth.get("selected_symbols") or []),
        "actionable_selected_candidate_symbols": actionable_selected_symbols,
        "non_actionable_selected_candidate_symbols": non_actionable_selected_symbols,
        "retry_waiting_selected_candidate_symbols": retry_waiting_selected_symbols,
        "rows": rows,
        "selected_candidate_operator_truth": selected_candidate_truth,
        "p645_profit_path_selected_candidate_sync": {
            "enabled": True,
            "candidate_path": candidate_path,
            "profit_path_remains_primary_when_clean_open_targets_can_reach_goal": True,
            "selected_candidate_truth_is_operator_context_not_new_gate": True,
            "adds_trade_gate": False,
            "changes_submit_behavior": False,
            "changes_exit_behavior": False,
            "does_not_submit_orders": True,
        },
        "p646_goal_gap_rotation_operator_plan": operator_plan,
        "p644_clean_profit_path_contract": {
            "enabled": True,
            "uses_existing_reports_only": True,
            "module_owned_report_shape": True,
            "adds_trade_gate": False,
            "changes_submit_behavior": False,
            "changes_exit_behavior": False,
            "daily_goal_operator_truth_consolidated": True,
        },
        "recommended_action": operator_plan.get("recommended_action") or "monitor_goal_path",
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
            "daily_goal_path_truth_report_shape",
            "daily_goal_opportunity_map_report_shape",
            "capital_rotation_action_contract_shape",
            "profit_path_truth_contract_shape",
        ],
        "next_extraction_target": "move_capital_rotation_readiness_report_assembly",
    }
