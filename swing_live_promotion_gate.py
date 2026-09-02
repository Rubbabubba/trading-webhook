"""Live promotion go/no-go contract helpers.

The gate is read-only and snapshot-driven. It should never fetch broker history,
fetch bars, submit orders, or repair state inline.
"""

from __future__ import annotations

from typing import Any


SWING_LIVE_PROMOTION_GATE_MODULE_VERSION = (
    "patch-714-full-live-promotion-gate-operator-go-no-go-contract"
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text if text else default


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = dict(payload or {})
    return dict(data.get("summary") or {})


def _ledger_status(ledger: dict[str, Any] | None, *, max_age_sec: int, min_trades: int) -> dict[str, Any]:
    data = dict(ledger or {})
    summary = _summary(data)
    cache_age = data.get("cache_age_sec")
    cache_age_ok = cache_age is not None and _safe_float(cache_age, max_age_sec + 1) <= max(1, max_age_sec)
    closed_trades = _safe_int(
        summary.get("closed_trades", summary.get("closed_trade_count", data.get("closed_trade_count"))),
        0,
    )
    blockers = []
    if not data.get("ok"):
        blockers.append("broker_ledger_not_ok")
    if not data.get("cache_hit"):
        blockers.append("broker_ledger_cache_missing")
    if not cache_age_ok:
        blockers.append("broker_ledger_stale")
    if closed_trades < max(0, min_trades):
        blockers.append("broker_ledger_below_min_trade_count")
    return {
        "ok": bool(data.get("ok")),
        "cache_hit": bool(data.get("cache_hit")),
        "cache_status": data.get("cache_status"),
        "cache_age_sec": cache_age,
        "max_age_sec": max_age_sec,
        "cache_age_ok": bool(cache_age_ok),
        "closed_trades": closed_trades,
        "min_trades": min_trades,
        "gross_pnl": _safe_float(summary.get("gross_pnl"), 0.0),
        "expectancy_per_trade": _safe_float(summary.get("expectancy_per_trade"), 0.0),
        "blockers": blockers,
        "fresh_for_full_live": not blockers,
    }


def _scanner_status(scanner: dict[str, Any] | None) -> dict[str, Any]:
    data = dict(scanner or {})
    latest_scan = dict(data.get("latest_scan") or {})
    warning_codes = _list(data.get("active_warning_codes"))
    background = dict(latest_scan.get("scan_background_completion_truth") or {})
    in_flight = bool(data.get("in_flight_run") or background.get("in_flight_run"))
    stale_or_running = str(data.get("scanner_status") or data.get("last_status") or "").lower() in {
        "background_scan_running",
        "scan_running",
        "started",
    }
    trade_judgable = bool(
        latest_scan.get("trade_judgable")
        or data.get("trade_judgable")
        or latest_scan.get("candidate_bearing_scan")
    )
    blockers = []
    if not data.get("ok"):
        blockers.append("scanner_light_not_ok")
    if in_flight or stale_or_running:
        blockers.append("scanner_in_flight_or_running")
    if warning_codes:
        blockers.append("scanner_active_warnings")
    if not trade_judgable:
        blockers.append("scanner_latest_scan_not_trade_judgable")
    return {
        "ok": bool(data.get("ok")),
        "healthy": bool(data.get("ok") and not blockers),
        "latest_scan_ts_utc": latest_scan.get("ts_utc") or data.get("latest_scan_ts_utc"),
        "latest_scan_reason": latest_scan.get("reason") or data.get("latest_scan_reason"),
        "trade_judgable": trade_judgable,
        "in_flight": in_flight,
        "active_warning_codes": warning_codes,
        "blockers": blockers,
    }


def _worker_status(worker_exit: dict[str, Any] | None) -> dict[str, Any]:
    data = dict(worker_exit or {})
    blockers = []
    if not data.get("ok"):
        blockers.append("worker_exit_not_ok")
    if not bool(data.get("healthy")):
        blockers.append("worker_exit_not_healthy")
    if _safe_int(data.get("error_like_recent_count"), 0) > 0:
        blockers.append("worker_exit_recent_errors")
    if bool(data.get("started_stale")):
        blockers.append("worker_exit_started_stale")
    return {
        "ok": bool(data.get("ok")),
        "healthy": bool(data.get("ok") and bool(data.get("healthy")) and not blockers),
        "heartbeat_status": data.get("heartbeat_status"),
        "heartbeat_age_sec": data.get("heartbeat_age_sec"),
        "recommended_action": data.get("recommended_action"),
        "blockers": blockers,
    }


def _protection_status(active_exit: dict[str, Any] | None) -> dict[str, Any]:
    data = dict(active_exit or {})
    summary = _summary(data)
    missing = _safe_int(summary.get("missing_protection_count"), 0)
    actionable_due = _safe_int(summary.get("actionable_exit_due_count", summary.get("exit_watch_count")), 0)
    pending_entry = _safe_int(summary.get("pending_entry_protection_pending_count"), 0)
    blockers = []
    if not data.get("ok"):
        blockers.append("active_exit_protection_not_ok")
    if missing > 0:
        blockers.append("open_position_missing_protection")
    if actionable_due > 0:
        blockers.append("actionable_exit_due_pending_worker_drain")
    return {
        "ok": bool(data.get("ok")),
        "clean": bool(data.get("ok") and not blockers),
        "missing_protection_count": missing,
        "actionable_exit_due_count": actionable_due,
        "pending_entry_protection_pending_count": pending_entry,
        "recommended_action": data.get("recommended_action"),
        "blockers": blockers,
    }


def _registry_status(registry: dict[str, Any] | None, *, min_variants: int) -> dict[str, Any]:
    data = dict(registry or {})
    eligible_count = _safe_int(data.get("capital_eligible_count"), 0)
    blockers = []
    if not data.get("ok"):
        blockers.append("replay_registry_not_ok")
    if eligible_count < max(1, min_variants):
        blockers.append("replay_registry_below_min_capital_eligible_variants")
    return {
        "ok": bool(data.get("ok")),
        "eligible": bool(data.get("ok") and not blockers),
        "registry_entry_count": _safe_int(data.get("registry_entry_count"), 0),
        "capital_eligible_count": eligible_count,
        "min_capital_eligible_variants": min_variants,
        "capital_eligible_strategies": _list(data.get("capital_eligible_strategies")),
        "capital_eligible_symbols": _list(data.get("capital_eligible_symbols")),
        "recommended_action": data.get("recommended_action"),
        "blockers": blockers,
    }


def build_full_live_promotion_gate(
    *,
    patch_version: str,
    live_risk: dict[str, Any] | None,
    broker_ledger: dict[str, Any] | None,
    scanner: dict[str, Any] | None,
    worker_exit: dict[str, Any] | None,
    active_exit: dict[str, Any] | None,
    replay_registry: dict[str, Any] | None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = dict(config or {})
    ledger = _ledger_status(
        broker_ledger,
        max_age_sec=max(1, _safe_int(cfg.get("broker_ledger_max_age_sec"), 86400)),
        min_trades=max(0, _safe_int(cfg.get("broker_ledger_min_trades"), 0)),
    )
    scanner_status = _scanner_status(scanner)
    worker = _worker_status(worker_exit)
    protection = _protection_status(active_exit)
    registry = _registry_status(
        replay_registry,
        min_variants=max(1, _safe_int(cfg.get("min_registry_variants"), 1)),
    )
    risk = dict(live_risk or {})
    promoted = dict(risk.get("p712_reduced_risk_promotion_contract") or {})
    promoted_enabled = bool(promoted.get("enabled"))
    validation_paused = bool(risk.get("validation_pause_entries"))
    current_mode = _text(risk.get("canonical_mode"), "unknown")

    reduced_blockers = []
    if not registry.get("eligible"):
        reduced_blockers.extend(registry.get("blockers") or [])
    if not scanner_status.get("healthy"):
        reduced_blockers.extend(scanner_status.get("blockers") or [])
    if not worker.get("healthy"):
        reduced_blockers.extend(worker.get("blockers") or [])
    if not protection.get("clean"):
        reduced_blockers.extend(protection.get("blockers") or [])
    reduced_ready = not reduced_blockers

    full_blockers = list(reduced_blockers)
    if not ledger.get("fresh_for_full_live"):
        full_blockers.extend(ledger.get("blockers") or [])
    if validation_paused:
        full_blockers.append("current_mode_is_validation_pause_entries")
    if current_mode == "reduced_risk":
        full_blockers.append("current_mode_is_reduced_risk_not_normal")
    full_ready = not full_blockers

    override_enabled = bool(cfg.get("operator_override_enabled"))
    normal_safe = bool(full_ready or override_enabled)
    recommended_action = (
        "normal_live_mode_ready"
        if full_ready
        else "operator_override_allows_normal_live_despite_blockers"
        if override_enabled
        else "enable_validation_promoted_live_reduced_risk_for_registered_variants"
        if reduced_ready
        else "stay_in_validation_pause_and_clear_gate_blockers"
    )
    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "full_live_promotion_gate",
        "module": "swing_live_promotion_gate",
        "module_version": SWING_LIVE_PROMOTION_GATE_MODULE_VERSION,
        "read_only": True,
        "does_not_submit_orders": True,
        "does_not_fetch_bars": True,
        "does_not_refresh_broker_history": True,
        "changes_trade_behavior": False,
        "current_live_risk_mode": current_mode,
        "reduced_risk_ready": bool(reduced_ready),
        "full_live_ready": bool(full_ready),
        "normal_mode_safe_to_enable": bool(normal_safe),
        "operator_override_enabled": override_enabled,
        "validation_promoted_live_env_enabled": promoted_enabled,
        "validation_promoted_live_env_required_for_reduced_risk_activation": True,
        "reduced_risk_blockers": sorted(set(str(x) for x in reduced_blockers if str(x or "").strip())),
        "full_live_blockers": sorted(set(str(x) for x in full_blockers if str(x or "").strip())),
        "evidence": {
            "live_risk": {
                "canonical_mode": current_mode,
                "entry_orders_permitted": bool(risk.get("entry_orders_permitted")),
                "validation_pause_entries": validation_paused,
                "p712_classification": promoted.get("classification"),
            },
            "broker_ledger": ledger,
            "scanner": scanner_status,
            "worker_exit": worker,
            "active_exit_protection": protection,
            "replay_registry": registry,
        },
        "rollback_criteria": [
            "broker_fill_ledger_stale_or_missing",
            "any_open_position_missing_protection",
            "worker_exit_unhealthy_or_recent_errors",
            "scanner_not_trade_judgable_or_running_stale",
            "promoted_live_variant_loses_registry_eligibility",
            "promoted_live_trade_outcomes_show_negative_expectancy",
        ],
        "recommended_action": recommended_action,
    }


def live_promotion_gate_module_status(*, patch_version: str) -> dict[str, Any]:
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_live_promotion_gate",
        "module_version": SWING_LIVE_PROMOTION_GATE_MODULE_VERSION,
        "owns_live_broker_calls": False,
        "submits_orders": False,
        "fetches_market_data": False,
        "changes_trade_behavior": False,
        "owns_full_live_promotion_gate_contract": True,
        "roadmap_step": "Patch 714",
        "roadmap_focus": "full_live_promotion_gate_operator_go_no_go_contract",
    }
