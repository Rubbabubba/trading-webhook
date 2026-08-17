"""Pure swing selection contract helpers.

This module must stay broker-free and FastAPI-free. It shapes candidate rows
using explicit config passed in by app.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Any


SWING_SELECTION_CONTRACT_MODULE_VERSION = "patch-435-swing-thrive-fast-cycle-first-2k-productive-sleeve"


@dataclass(frozen=True)
class SwingProductionContractConfig:
    production_reset_enabled: bool
    min_rank_score: float
    min_avg_dollar_volume: float
    max_risk_per_share_pct: float
    max_below_breakout_pct: float
    max_above_breakout_pct: float
    min_close_to_high_pct: float
    min_return_20d_pct: float
    allow_mean_reversion: bool
    mean_reversion_strategy_name: str
    breakout_strategy_name: str
    max_entries_per_scan: int
    risk_calibration_enabled: bool = False
    risk_calibration_max_risk_pct: float = 0.20
    risk_calibration_min_rank_score: float = 105.0
    risk_calibration_min_close_to_high_pct: float = 0.982
    risk_calibration_max_above_breakout_pct: float = 0.025
    risk_calibration_max_entries_per_scan: int = 1
    near_rank_revival_enabled: bool = False
    near_rank_revival_min_rank_score: float = 99.0
    near_rank_revival_min_close_to_high_pct: float = 0.995
    near_rank_revival_max_risk_pct: float = 0.08
    near_rank_revival_min_target_path_score: float = 10.0
    near_rank_revival_max_entries_per_scan: int = 1
    first_2k_geometry_sleeve_enabled: bool = False
    first_2k_geometry_sleeve_symbols: str = ""
    first_2k_geometry_sleeve_min_rank_score: float = 97.0
    first_2k_geometry_sleeve_max_below_breakout_pct: float = 0.01
    first_2k_geometry_sleeve_min_close_to_high_pct: float = 0.985
    first_2k_geometry_sleeve_max_risk_pct: float = 0.06
    first_2k_geometry_sleeve_min_target_path_score: float = 40.0
    first_2k_geometry_sleeve_max_entries_per_scan: int = 2


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _dedupe_reasons(values: list | tuple | None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in list(values or []):
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _value(candidate: dict | None, *keys):
    c = dict(candidate or {})
    for key in keys:
        val = c.get(key)
        if val is not None and str(val).strip() != "":
            return val
    return None

def _pct_decimal(value):
    if value is None or str(value).strip() == "":
        return None
    val = float(_safe_float(value))

    # Candidate rows store percent fields as percent-style numbers:
    # breakout_distance_pct=-0.67 means -0.67%, risk_per_share_pct=1.8 means 1.8%.
    # Contract/env thresholds are decimal ratios: 0.01 means 1%.
    # Convert all candidate pct values to decimal ratios before comparing.
    return val / 100.0

def _symbol_set(csv_text: str | None) -> set[str]:
    return {
        str(part or "").strip().upper()
        for part in str(csv_text or "").replace(";", ",").split(",")
        if str(part or "").strip()
    }

def swing_production_contract(
    candidate: dict | None,
    *,
    config: SwingProductionContractConfig,
    global_block_reasons: list | None = None,
    sizing_truth_fn: Callable[[dict], dict] | None = None,
) -> dict:
    c = dict(candidate or {})
    symbol = str(c.get("symbol") or "").strip().upper()
    strategy = str(c.get("strategy") or c.get("signal") or "").strip().lower()
    original_eligible = bool(c.get("eligible"))
    original_reasons = _dedupe_reasons(c.get("rejection_reasons") or [])
    global_reasons = _dedupe_reasons(global_block_reasons or [])

    hard_reasons = {
        "insufficient_daily_bars",
        "price_below_min",
        "avg_dollar_volume_below_min",
        "low_volume",
        "internal_sizing_qty_zero",
        "broker_insufficient_buying_power",
        "insufficient_buying_power",
        "daily_halt_active",
        "daily_stop_hit",
        "kill_switch_enabled",
        "plan_or_pending_entry_exists",
        "position_already_open",
        "pending_order_entry_freeze",
        "same_day_symbol_loss_cooldown",
        "same_day_stall_churn_reentry_cooldown",
        "strategy_kill_switch_active",
        "correlation_group_limit",
        "symbol_exposure_limit",
        "portfolio_exposure_limit",
        "portfolio_already_over_cap_total",
        "portfolio_already_over_cap_strategy",
        "swing_loss_day_entry_throttle",
    }

    legacy_advisory_reasons = {
        "weak_tape",
        "rank_score_below_min",
        "close_not_near_high",
        "too_far_below_breakout",
        "target_profile_breakout_gate",
        "defensive_daily_breakout_rollback",
        "defensive_risk_per_share_too_wide",
        "defensive_breakout_extension_too_high",
        "defensive_20d_return_too_extended",
        "stall_loss_entry_feedback",
        "swing_post_change_drawdown_circuit",
    }

    blockers: list[str] = []
    advisory: list[str] = []

    for reason in original_reasons + global_reasons:
        reason = str(reason or "").strip()
        if not reason:
            continue
        if reason in hard_reasons:
            blockers.append(reason)
        elif reason in legacy_advisory_reasons:
            advisory.append(reason)
        else:
            advisory.append(reason)

    if sizing_truth_fn is not None:
        sizing_truth = dict(c.get("executable_sizing_truth") or sizing_truth_fn(c) or {})
    else:
        sizing_truth = dict(c.get("executable_sizing_truth") or {})
    executable = bool(sizing_truth.get("executable"))

    rank_score = float(_safe_float(c.get("rank_score")))
    avg_dollar_volume = float(_safe_float(_value(c, "avg_dollar_volume", "avg_dollar_volume_20d")))
    risk_pct = _pct_decimal(c.get("risk_per_share_pct"))
    breakout_distance_pct = _pct_decimal(c.get("breakout_distance_pct"))
    close_to_high_pct = _pct_decimal(c.get("close_to_high_pct"))
    return_20d_pct = _pct_decimal(c.get("return_20d_pct"))

    rank_score_ok = rank_score >= float(config.min_rank_score)
    liquidity_ok = avg_dollar_volume >= float(config.min_avg_dollar_volume)
    base_risk_ok = risk_pct is not None and risk_pct <= float(config.max_risk_per_share_pct)
    not_too_far_below_breakout = breakout_distance_pct is not None and breakout_distance_pct >= -abs(float(config.max_below_breakout_pct))
    not_too_extended_above_breakout = breakout_distance_pct is not None and breakout_distance_pct <= abs(float(config.max_above_breakout_pct))
    close_to_high_ok = close_to_high_pct is not None and close_to_high_pct >= float(config.min_close_to_high_pct)
    return_20d_ok = return_20d_pct is None or return_20d_pct >= float(config.min_return_20d_pct)

    target_path_score = float(_safe_float(
        (c.get("target_path_profit") or {}).get("score")
        if isinstance(c.get("target_path_profit"), dict)
        else c.get("target_path_score")
    ))

    base_contract_ok = bool(
        executable
        and rank_score_ok
        and liquidity_ok
        and base_risk_ok
        and not_too_far_below_breakout
        and not_too_extended_above_breakout
        and close_to_high_ok
        and return_20d_ok
    )

    risk_calibrated_starter_ok = bool(
        config.risk_calibration_enabled
        and executable
        and risk_pct is not None
        and risk_pct <= float(config.risk_calibration_max_risk_pct)
        and rank_score >= float(config.risk_calibration_min_rank_score)
        and liquidity_ok
        and close_to_high_pct is not None
        and close_to_high_pct >= float(config.risk_calibration_min_close_to_high_pct)
        and not_too_far_below_breakout
        and breakout_distance_pct is not None
        and breakout_distance_pct <= abs(float(config.risk_calibration_max_above_breakout_pct))
        and return_20d_ok
    )

    near_rank_revival_ok = bool(
        config.near_rank_revival_enabled
        and executable
        and rank_score >= float(config.near_rank_revival_min_rank_score)
        and liquidity_ok
        and risk_pct is not None
        and risk_pct <= float(config.near_rank_revival_max_risk_pct)
        and close_to_high_pct is not None
        and close_to_high_pct >= float(config.near_rank_revival_min_close_to_high_pct)
        and not_too_far_below_breakout
        and not_too_extended_above_breakout
        and return_20d_ok
        and target_path_score >= float(config.near_rank_revival_min_target_path_score)
    )

    sleeve_symbols = _symbol_set(config.first_2k_geometry_sleeve_symbols)
    first_2k_geometry_sleeve_ok = bool(
        config.first_2k_geometry_sleeve_enabled
        and symbol in sleeve_symbols
        and executable
        and liquidity_ok
        and rank_score >= float(config.first_2k_geometry_sleeve_min_rank_score)
        and risk_pct is not None
        and risk_pct <= float(config.first_2k_geometry_sleeve_max_risk_pct)
        and close_to_high_pct is not None
        and close_to_high_pct >= float(config.first_2k_geometry_sleeve_min_close_to_high_pct)
        and breakout_distance_pct is not None
        and breakout_distance_pct >= -abs(float(config.first_2k_geometry_sleeve_max_below_breakout_pct))
        and not_too_extended_above_breakout
        and return_20d_ok
        and target_path_score >= float(config.first_2k_geometry_sleeve_min_target_path_score)
    )

    contract_path_ok = bool(
        base_contract_ok
        or risk_calibrated_starter_ok
        or near_rank_revival_ok
        or first_2k_geometry_sleeve_ok
    )

    checks = {
        "executable": executable,
        "rank_score_ok": rank_score_ok,
        "liquidity_ok": liquidity_ok,
        "risk_ok": bool(base_risk_ok or risk_calibrated_starter_ok or near_rank_revival_ok or first_2k_geometry_sleeve_ok),
        "base_risk_ok": bool(base_risk_ok),
        "base_contract_ok": bool(base_contract_ok),
        "risk_calibrated_starter_ok": bool(risk_calibrated_starter_ok),
        "near_rank_revival_ok": bool(near_rank_revival_ok),
        "first_2k_geometry_sleeve_ok": bool(first_2k_geometry_sleeve_ok),
        "not_too_far_below_breakout": not_too_far_below_breakout,
        "not_too_extended_above_breakout": not_too_extended_above_breakout,
        "close_to_high_ok": close_to_high_ok,
        "return_20d_ok": return_20d_ok,
        "target_path_score": target_path_score,
        "contract_path_ok": contract_path_ok,
    }

    if not executable:
        blockers.append(str(sizing_truth.get("sizing_block_reason") or "internal_sizing_qty_zero"))

    if strategy == str(config.mean_reversion_strategy_name or "").strip().lower():
        if not bool(config.allow_mean_reversion):
            blockers.append("production_contract_mean_reversion_disabled")
        if not original_eligible and not blockers:
            blockers.append("production_contract_mean_reversion_original_rules_not_met")
    elif not contract_path_ok:
        if bool(config.first_2k_geometry_sleeve_enabled) and symbol in _symbol_set(config.first_2k_geometry_sleeve_symbols):
            if rank_score < float(config.first_2k_geometry_sleeve_min_rank_score):
                blockers.append("first_2k_geometry_rank_below_min")
            if risk_pct is None or risk_pct > float(config.first_2k_geometry_sleeve_max_risk_pct):
                blockers.append("first_2k_geometry_risk_too_wide")
            if close_to_high_pct is None or close_to_high_pct < float(config.first_2k_geometry_sleeve_min_close_to_high_pct):
                blockers.append("first_2k_geometry_not_close_to_high")
            if breakout_distance_pct is None or breakout_distance_pct < -abs(float(config.first_2k_geometry_sleeve_max_below_breakout_pct)):
                blockers.append("first_2k_geometry_too_far_below_breakout")
            if target_path_score < float(config.first_2k_geometry_sleeve_min_target_path_score):
                blockers.append("first_2k_geometry_target_path_score_below_min")

        if not rank_score_ok:
            blockers.append("production_contract_rank_below_min")
        if not liquidity_ok:
            blockers.append("production_contract_liquidity_below_min")
        if not checks["risk_ok"]:
            blockers.append("production_contract_risk_too_wide")
        if not not_too_far_below_breakout:
            blockers.append("production_contract_too_far_below_breakout")
        if not not_too_extended_above_breakout:
            blockers.append("production_contract_too_extended_above_breakout")
        if not close_to_high_ok:
            blockers.append("production_contract_not_close_to_high")
        if not return_20d_ok:
            blockers.append("production_contract_20d_return_below_floor")

    blockers = _dedupe_reasons(blockers)
    advisory = _dedupe_reasons(advisory)

    return {
        "enabled": bool(config.production_reset_enabled),
        "symbol": symbol,
        "strategy": strategy,
        "approved": bool(config.production_reset_enabled) and not blockers,
        "blockers": blockers,
        "advisory_legacy_reasons": advisory,
        "checks": checks,
        "original_eligible": original_eligible,
        "original_rejection_reasons": original_reasons,
        "executable_sizing_truth": sizing_truth,
        "rank_score": rank_score,
        "avg_dollar_volume": avg_dollar_volume,
        "risk_per_share_pct": risk_pct,
        "breakout_distance_pct": breakout_distance_pct,
        "close_to_high_pct": close_to_high_pct,
        "return_20d_pct": return_20d_pct,
    }



def apply_swing_production_contract(
    candidate: dict | None,
    *,
    config: SwingProductionContractConfig,
    global_block_reasons: list | None = None,
    sizing_truth_fn: Callable[[dict], dict] | None = None,
) -> dict:
    c = dict(candidate or {})
    prior_contract = dict(c.get("swing_production_contract") or {})

    if "pre_p323_eligible" not in c:
        c["pre_p323_eligible"] = bool(c.get("eligible"))
    if "pre_p323_rejection_reasons" not in c:
        c["pre_p323_rejection_reasons"] = list(c.get("rejection_reasons") or [])

    contract = swing_production_contract(
        c,
        config=config,
        global_block_reasons=global_block_reasons,
        sizing_truth_fn=sizing_truth_fn,
    )
    c["swing_production_contract"] = contract
    c["legacy_gate_mode"] = "diagnostic_only" if bool(config.production_reset_enabled) else "legacy_live"

    if bool(config.production_reset_enabled):
        approved = bool(contract.get("approved"))
        c["production_contract_approved"] = approved
        c["eligible"] = approved
        c["selected"] = False
        c["rejection_reasons"] = list(contract.get("blockers") or [])
        c["advisory_legacy_rejection_reasons"] = list(contract.get("advisory_legacy_reasons") or [])

        if approved:
            checks = dict(contract.get("checks") or {})
            if bool(checks.get("first_2k_geometry_sleeve_ok")):
                c["entry_type"] = "swing_production_first_2k_geometry_sleeve"
                c["selected_source"] = "swing_production_first_2k_geometry_sleeve"
                c["first_2k_geometry_sleeve_applied"] = True
            elif bool(checks.get("near_rank_revival_ok")):
                c["entry_type"] = "swing_production_near_rank_revival"
                c["selected_source"] = "swing_production_near_rank_revival"
                c["near_rank_revival_applied"] = True
            elif bool(checks.get("risk_calibrated_starter_ok")):
                c["entry_type"] = "swing_production_risk_calibrated_starter"
                c["selected_source"] = "swing_production_risk_calibration"
                c["risk_calibration_applied"] = True
            else:
                c["entry_type"] = "swing_production_contract"
                c["selected_source"] = "swing_production_reset"
        elif prior_contract:
            c["selected_source"] = c.get("selected_source") or "swing_production_reset_blocked"

    return c


def swing_production_sort_key(candidate: dict | None, *, config: SwingProductionContractConfig) -> tuple:
    c = dict(candidate or {})
    contract = dict(c.get("swing_production_contract") or {})
    return (
        1 if str(c.get("strategy") or "").strip().lower() == str(config.breakout_strategy_name or "").strip().lower() else 0,
        float(_safe_float(contract.get("rank_score") or c.get("rank_score"))),
        float(_safe_float((c.get("target_path_profit") or {}).get("score") or c.get("target_path_score"))),
        float(_safe_float(c.get("selection_quality_score"))),
    )


def enforce_production_contract_selection(
    rows: list | None,
    *,
    config: SwingProductionContractConfig,
    global_block_reasons: list | None = None,
    sizing_truth_fn: Callable[[dict], dict] | None = None,
) -> list[dict]:
    enforced: list[dict] = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        c = apply_swing_production_contract(
            row,
            config=config,
            global_block_reasons=global_block_reasons,
            sizing_truth_fn=sizing_truth_fn,
        )
        if bool(config.production_reset_enabled):
            c["eligible"] = bool((c.get("swing_production_contract") or {}).get("approved"))
            c["rejection_reasons"] = list((c.get("swing_production_contract") or {}).get("blockers") or [])
        enforced.append(c)
    return enforced


def approved_production_contract_rows(
    rows: list | None,
    *,
    config: SwingProductionContractConfig,
    sizing_truth_fn: Callable[[dict], dict] | None = None,
) -> list[dict]:
    approved = [
        r for r in list(rows or [])
        if isinstance(r, dict)
        and bool((r.get("swing_production_contract") or {}).get("approved"))
        and bool(
            (
                r.get("executable_sizing_truth")
                or (sizing_truth_fn(r) if sizing_truth_fn is not None else {})
            ).get("executable")
        )
    ]
    approved.sort(key=lambda row: swing_production_sort_key(row, config=config), reverse=True)
    return approved


def finalize_production_contract_selection(
    rows: list | None,
    *,
    config: SwingProductionContractConfig,
    max_new_entries: int,
    global_block_reasons: list | None = None,
    sizing_truth_fn: Callable[[dict], dict] | None = None,
) -> dict:
    enforced = enforce_production_contract_selection(
        list(rows or []),
        config=config,
        global_block_reasons=global_block_reasons,
        sizing_truth_fn=sizing_truth_fn,
    )
    approved = approved_production_contract_rows(
        enforced,
        config=config,
        sizing_truth_fn=sizing_truth_fn,
    )

    max_total = max(0, int(max_new_entries or 0))
    max_calibrated = max(0, int(config.risk_calibration_max_entries_per_scan or 0))
    max_near_rank = max(0, int(config.near_rank_revival_max_entries_per_scan or 0))
    max_first_2k_geometry = max(0, int(config.first_2k_geometry_sleeve_max_entries_per_scan or 0))
    selected = []
    calibrated_count = 0
    near_rank_count = 0
    first_2k_geometry_count = 0

    for row in approved:
        if len(selected) >= max_total:
            break
        contract = dict((row or {}).get("swing_production_contract") or {})
        checks = dict(contract.get("checks") or {})
        is_first_2k_geometry = bool(checks.get("first_2k_geometry_sleeve_ok"))
        is_near_rank = bool(checks.get("near_rank_revival_ok")) and not is_first_2k_geometry
        is_calibrated = bool(checks.get("risk_calibrated_starter_ok")) and not is_near_rank and not is_first_2k_geometry
        if is_first_2k_geometry and first_2k_geometry_count >= max_first_2k_geometry:
            continue
        if is_near_rank and near_rank_count >= max_near_rank:
            continue
        if is_calibrated and calibrated_count >= max_calibrated:
            continue
        selected.append(dict(row or {}))
        if is_first_2k_geometry:
            first_2k_geometry_count += 1
        elif is_near_rank:
            near_rank_count += 1
        elif is_calibrated:
            calibrated_count += 1

    selected_symbols = {
        str((row or {}).get("symbol") or "").strip().upper()
        for row in selected
        if str((row or {}).get("symbol") or "").strip()
    }

    finalized_rows: list[dict] = []
    for row in enforced:
        c = dict(row or {})
        sym = str(c.get("symbol") or "").strip().upper()
        is_selected = bool(sym and sym in selected_symbols)
        c["selected"] = is_selected
        if bool(config.production_reset_enabled):
            c["eligible"] = bool((c.get("swing_production_contract") or {}).get("approved"))
            c["production_contract_approved"] = bool((c.get("swing_production_contract") or {}).get("approved"))
            c["rejection_reasons"] = list((c.get("swing_production_contract") or {}).get("blockers") or [])
            c["legacy_gate_mode"] = "diagnostic_only"
        if is_selected:
            contract = dict(c.get("swing_production_contract") or {})
            checks = dict(contract.get("checks") or {})
            if bool(checks.get("first_2k_geometry_sleeve_ok")):
                c["entry_type"] = "swing_production_first_2k_geometry_sleeve"
                c["selected_source"] = "swing_production_first_2k_geometry_sleeve"
                c["first_2k_geometry_sleeve_applied"] = True
            elif bool(checks.get("near_rank_revival_ok")):
                c["entry_type"] = "swing_production_near_rank_revival"
                c["selected_source"] = "swing_production_near_rank_revival"
                c["near_rank_revival_applied"] = True
            elif bool(checks.get("risk_calibrated_starter_ok")):
                c["entry_type"] = "swing_production_risk_calibrated_starter"
                c["selected_source"] = "swing_production_risk_calibration"
                c["risk_calibration_applied"] = True
            else:
                c["entry_type"] = "swing_production_contract"
                c["selected_source"] = "swing_production_reset"
            c["selection_finalizer"] = "p326_approved_production_contract_selection_finalizer"
        finalized_rows.append(c)

    selected_by_symbol = {
        str((row or {}).get("symbol") or "").strip().upper(): dict(row or {})
        for row in finalized_rows
        if str((row or {}).get("symbol") or "").strip().upper() in selected_symbols
    }
    selected_final = [
        selected_by_symbol.get(str((row or {}).get("symbol") or "").strip().upper(), dict(row or {}))
        for row in selected
        if str((row or {}).get("symbol") or "").strip().upper()
    ]

    return {
        "rows": finalized_rows,
        "approved": approved,
        "selected": selected_final,
        "selected_symbols": [
            str((row or {}).get("symbol") or "").strip().upper()
            for row in selected_final
            if str((row or {}).get("symbol") or "").strip()
        ],
        "approved_symbols": [
            str((row or {}).get("symbol") or "").strip().upper()
            for row in approved
            if str((row or {}).get("symbol") or "").strip()
        ],
        "max_new_entries": max(0, int(max_new_entries or 0)),
        "first_2k_geometry_sleeve": {
            "enabled": bool(config.first_2k_geometry_sleeve_enabled),
            "max_entries_per_scan": int(config.first_2k_geometry_sleeve_max_entries_per_scan or 0),
            "selected_count": int(first_2k_geometry_count),
            "selected_symbols": [
                str((row or {}).get("symbol") or "").strip().upper()
                for row in selected_final
                if bool(((row.get("swing_production_contract") or {}).get("checks") or {}).get("first_2k_geometry_sleeve_ok"))
            ],
        },
    }