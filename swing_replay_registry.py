"""Replay-passed variant registry helpers.

This module is intentionally broker-free. It turns replay promotion evidence into
one canonical registry that submit/risk code can query without re-running replay,
fetching bars, or touching live order state.
"""

from __future__ import annotations

from typing import Any


SWING_REPLAY_REGISTRY_MODULE_VERSION = (
    "patch-713-replay-passed-variant-registry-strategy-capital-eligibility"
)


def _text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text if text else default


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


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "pass", "passed", "ready"}


def _tokens(value: Any, *, upper: bool = False) -> list[str]:
    if value is None:
        return []
    raw_items = (
        value
        if isinstance(value, (list, tuple, set))
        else str(value).replace(";", ",").replace("|", ",").split(",")
    )
    out: list[str] = []
    seen = set()
    for item in raw_items:
        token = _text(item)
        if not token:
            continue
        token = token.upper() if upper else token.lower()
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _first(*values: Any, default: str = "") -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return default


def _nested(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    for nested_key in ("promotion_contract", "replay_promotion_gate", "p713_replay_variant_registry_match"):
        nested = row.get(nested_key)
        if isinstance(nested, dict):
            for key in keys:
                if key in nested and nested.get(key) not in (None, ""):
                    return nested.get(key)
    return None


def normalize_strategy(value: Any = "", scenario: Any = "") -> str:
    raw = _first(value, scenario).lower().replace("-", "_").replace(" ", "_")
    if "mean_reversion" in raw or raw in {"mr", "reversion"}:
        return "intraday_mean_reversion"
    if "momentum" in raw or raw in {"intraday_momo", "momo"}:
        return "intraday_momentum"
    if "daily_breakout" in raw or "breakout" in raw:
        return "daily_breakout"
    if raw in {"daily_breakout", "intraday_momentum", "intraday_mean_reversion"}:
        return raw
    return "unknown"


def normalize_variant_identity(*, meta: dict[str, Any] | None = None, candidate: dict[str, Any] | None = None) -> dict[str, Any]:
    meta = dict(meta or {})
    candidate = dict(candidate or {})
    scenario = _first(
        _nested(candidate, "scenario", "variant", "variant_name"),
        _nested(meta, "scenario", "variant", "variant_name"),
    )
    strategy = normalize_strategy(
        _first(
            _nested(candidate, "strategy", "strategy_name", "signal"),
            _nested(meta, "strategy", "strategy_name", "signal"),
        ),
        scenario,
    )
    sleeve = _first(
        _nested(candidate, "sleeve", "strategy_sleeve", "entry_type", "selection_source"),
        _nested(meta, "sleeve", "strategy_sleeve", "entry_type", "selection_source"),
    ).lower().replace("-", "_").replace(" ", "_")
    if not sleeve or sleeve == "unknown":
        sleeve = strategy
    return {
        "symbol": _first(_nested(candidate, "symbol"), _nested(meta, "symbol")).upper(),
        "strategy": strategy or "unknown",
        "sleeve": sleeve or "unknown",
        "regime_profile": _first(
            _nested(candidate, "regime_profile", "profile", "market_regime"),
            _nested(meta, "regime_profile", "profile", "market_regime"),
            default="unknown",
        ).lower().replace("-", "_").replace(" ", "_"),
        "risk_profile": _first(
            _nested(candidate, "risk_profile", "risk_tier", "risk_mode"),
            _nested(meta, "risk_profile", "risk_tier", "risk_mode"),
            default="unknown",
        ).lower().replace("-", "_").replace(" ", "_"),
        "scenario": scenario or "unknown",
    }


def _scenario_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_ids = set()
    for source_name in (
        "deployable_top_scenarios",
        "promotion_ready_scenarios",
        "scenario_rows",
        "research_candidate_scenarios",
        "watch_scenarios",
        "failed_scenarios",
    ):
        for row in list(payload.get(source_name) or []):
            if not isinstance(row, dict):
                continue
            item = dict(row)
            item["_registry_source"] = source_name
            row_id = (
                source_name,
                _text(item.get("scenario")),
                _safe_int(item.get("windows")),
                _safe_int(item.get("total_trades")),
                _safe_float(item.get("total_pnl")),
            )
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            rows.append(item)
    return rows


def _row_entry(row: dict[str, Any], *, min_windows: int = 1) -> dict[str, Any]:
    scenario = _text(row.get("scenario"), "unknown")
    strategy = normalize_strategy(_nested(row, "strategy", "strategy_name", "signal"), scenario)
    sleeve = _first(_nested(row, "sleeve", "strategy_sleeve", "entry_type", "selection_source")).lower()
    sleeve = sleeve.replace("-", "_").replace(" ", "_") if sleeve else strategy
    symbols = _tokens(_nested(row, "symbols", "symbol_universe", "runtime_symbols", "symbol"), upper=True)
    regime_profile = _first(_nested(row, "regime_profile", "profile", "market_regime"), default="unknown")
    risk_profile = _first(_nested(row, "risk_profile", "risk_tier", "risk_mode"), default="unknown")
    status = _text(row.get("status"), "unknown").lower()
    promotion_ready = bool(_truthy(row.get("promotion_ready")) or status == "pass")
    windows = _safe_int(row.get("windows"), 0)
    pass_windows = _safe_int(row.get("pass_windows"), 0)
    fail_windows = _safe_int(row.get("fail_windows"), 0)
    blockers = [str(b) for b in list(row.get("blockers") or []) if str(b or "").strip()]
    if strategy == "unknown":
        blockers.append("variant_strategy_unknown")
    if sleeve == "unknown":
        blockers.append("variant_sleeve_unknown")
    if windows < max(1, int(min_windows or 1)):
        blockers.append("below_registry_min_windows")
    if fail_windows > 0:
        blockers.append("registry_has_failed_window")
    capital_eligible = bool(promotion_ready and not blockers)
    symbol_scope = "explicit_symbols" if symbols else "strategy_level"
    registry_key = "|".join([
        strategy or "unknown",
        sleeve or "unknown",
        regime_profile.lower().replace("-", "_").replace(" ", "_") or "unknown",
        risk_profile.lower().replace("-", "_").replace(" ", "_") or "unknown",
        ",".join(symbols) if symbols else "ALL",
    ])
    return {
        "registry_key": registry_key,
        "scenario": scenario,
        "strategy": strategy or "unknown",
        "sleeve": sleeve or "unknown",
        "regime_profile": regime_profile.lower().replace("-", "_").replace(" ", "_") or "unknown",
        "risk_profile": risk_profile.lower().replace("-", "_").replace(" ", "_") or "unknown",
        "symbols": symbols,
        "symbol_scope": symbol_scope,
        "status": "capital_eligible" if capital_eligible else status or "not_eligible",
        "capital_eligible": capital_eligible,
        "promotion_ready": promotion_ready,
        "windows": windows,
        "pass_windows": pass_windows,
        "watch_windows": _safe_int(row.get("watch_windows"), 0),
        "fail_windows": fail_windows,
        "total_trades": _safe_int(row.get("total_trades"), 0),
        "total_pnl": round(_safe_float(row.get("total_pnl"), 0.0), 4),
        "avg_r": round(_safe_float(row.get("avg_r"), 0.0), 4),
        "win_rate": round(_safe_float(row.get("avg_win_rate", row.get("win_rate")), 0.0), 4),
        "max_drawdown": round(_safe_float(row.get("max_drawdown"), 0.0), 4),
        "blockers": sorted(set(blockers)),
        "evidence": {
            "source": row.get("_registry_source"),
            "best_window_pnl": row.get("best_window_pnl"),
            "worst_window_pnl": row.get("worst_window_pnl"),
        },
    }


def build_replay_variant_registry(
    promotion_payload: dict[str, Any] | None,
    *,
    patch_version: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    payload = dict(promotion_payload or {})
    lim = max(1, min(int(limit or 25), 250))
    min_windows = _safe_int((payload.get("promotion_contract") or {}).get("min_windows"), 1)
    entries_by_key: dict[str, dict[str, Any]] = {}
    for row in _scenario_rows(payload):
        entry = _row_entry(row, min_windows=min_windows)
        existing = entries_by_key.get(entry["registry_key"])
        if not existing:
            entries_by_key[entry["registry_key"]] = entry
            continue
        if entry.get("capital_eligible") and not existing.get("capital_eligible"):
            entries_by_key[entry["registry_key"]] = entry
            continue
        if _safe_float(entry.get("total_pnl"), 0.0) > _safe_float(existing.get("total_pnl"), 0.0):
            entries_by_key[entry["registry_key"]] = entry
    entries = list(entries_by_key.values())
    entries.sort(key=lambda row: (
        1 if row.get("capital_eligible") else 0,
        _safe_float(row.get("total_pnl"), 0.0),
        _safe_float(row.get("avg_r"), 0.0),
    ), reverse=True)
    capital_entries = [row for row in entries if bool(row.get("capital_eligible"))]
    unknown_entries = [row for row in entries if row.get("strategy") == "unknown" or row.get("sleeve") == "unknown"]
    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "replay_passed_variant_registry",
        "module": "swing_replay_registry",
        "module_version": SWING_REPLAY_REGISTRY_MODULE_VERSION,
        "read_only": True,
        "does_not_submit_orders": True,
        "does_not_fetch_bars": True,
        "changes_scanner_visibility": False,
        "changes_trade_behavior": False,
        "registry_source_mode": payload.get("mode") or "unknown",
        "registry_entry_count": len(entries),
        "capital_eligible_count": len(capital_entries),
        "unknown_identity_count": len(unknown_entries),
        "capital_eligible_strategies": sorted({row.get("strategy") for row in capital_entries if row.get("strategy")}),
        "capital_eligible_symbols": sorted({
            symbol
            for row in capital_entries
            for symbol in list(row.get("symbols") or [])
            if symbol
        }),
        "entries": entries[:lim],
        "capital_eligible_variants": capital_entries[:lim],
        "registry_contract": {
            "source": "replay_promotion_gate_snapshot",
            "membership_required_for_validation_promoted_live": True,
            "strategy_level_entries_apply_when_replay_output_has_no_symbol_universe": True,
            "unknown_strategy_or_sleeve_is_not_capital_eligible": True,
            "normal_scanner_visibility_unchanged": True,
        },
        "recommended_action": (
            "use_registry_for_validation_promoted_live_capital_eligibility"
            if capital_entries
            else "keep_validation_pause_until_replay_registry_has_capital_eligible_variants"
        ),
    }


def match_replay_variant_registry(
    registry_payload: dict[str, Any] | None,
    *,
    meta: dict[str, Any] | None = None,
    candidate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry = dict(registry_payload or {})
    entries = [
        dict(row or {})
        for row in list(registry.get("capital_eligible_variants") or registry.get("entries") or [])
        if isinstance(row, dict)
    ]
    identity = normalize_variant_identity(meta=meta, candidate=candidate)
    blockers: list[str] = []
    if not entries:
        blockers.append("replay_registry_empty")
    if identity.get("strategy") == "unknown":
        blockers.append("candidate_strategy_unknown")
    if identity.get("sleeve") == "unknown":
        blockers.append("candidate_sleeve_unknown")
    matched: list[dict[str, Any]] = []
    near_matches: list[dict[str, Any]] = []
    for entry in entries:
        if not bool(entry.get("capital_eligible")):
            continue
        if entry.get("strategy") != identity.get("strategy"):
            continue
        sleeve_matches = entry.get("sleeve") in {identity.get("sleeve"), identity.get("strategy")}
        if not sleeve_matches:
            continue
        entry_symbols = {str(s or "").strip().upper() for s in list(entry.get("symbols") or []) if str(s or "").strip()}
        if entry_symbols and identity.get("symbol") not in entry_symbols:
            near_matches.append({
                "registry_key": entry.get("registry_key"),
                "reason": "symbol_not_in_registry_universe",
                "symbols": sorted(entry_symbols),
            })
            continue
        for optional_key in ("regime_profile", "risk_profile"):
            candidate_value = str(identity.get(optional_key) or "unknown")
            entry_value = str(entry.get(optional_key) or "unknown")
            if candidate_value != "unknown" and entry_value != "unknown" and candidate_value != entry_value:
                near_matches.append({
                    "registry_key": entry.get("registry_key"),
                    "reason": f"{optional_key}_mismatch",
                    "candidate": candidate_value,
                    "registry": entry_value,
                })
                break
        else:
            matched.append({
                "registry_key": entry.get("registry_key"),
                "scenario": entry.get("scenario"),
                "strategy": entry.get("strategy"),
                "sleeve": entry.get("sleeve"),
                "symbol_scope": entry.get("symbol_scope"),
                "symbols": entry.get("symbols") or [],
                "windows": entry.get("windows"),
                "pass_windows": entry.get("pass_windows"),
                "total_trades": entry.get("total_trades"),
                "avg_r": entry.get("avg_r"),
                "win_rate": entry.get("win_rate"),
            })
    if not matched and entries and not blockers:
        blockers.append("candidate_variant_not_in_replay_registry")
    capital_eligible = bool(matched and not blockers)
    return {
        "ok": True,
        "module": "swing_replay_registry",
        "module_version": SWING_REPLAY_REGISTRY_MODULE_VERSION,
        "read_only": True,
        "does_not_submit_orders": True,
        "does_not_fetch_bars": True,
        "candidate_identity": identity,
        "registry_available": bool(entries),
        "registry_entry_count": int(registry.get("registry_entry_count") or len(entries)),
        "capital_eligible": capital_eligible,
        "matched_count": len(matched),
        "matched_variants": matched[:5],
        "near_matches": near_matches[:5],
        "blockers": sorted(set(blockers)),
        "classification": "replay_registry_capital_eligible" if capital_eligible else "replay_registry_not_capital_eligible",
        "recommended_action": (
            "allow_validation_promoted_reduced_risk_submit"
            if capital_eligible
            else "keep_candidate_visible_but_do_not_promote_live_capital"
        ),
    }


def replay_registry_module_status(*, patch_version: str = "") -> dict[str, Any]:
    return {
        "ok": True,
        "patch_version": patch_version,
        "module": "swing_replay_registry",
        "module_version": SWING_REPLAY_REGISTRY_MODULE_VERSION,
        "owns_live_broker_calls": False,
        "submits_orders": False,
        "fetches_market_data": False,
        "changes_scanner_visibility": False,
        "changes_trade_behavior": False,
        "owns_replay_variant_registry_contract": True,
        "roadmap_step": "Patch 713",
        "roadmap_focus": "replay_passed_variant_registry_strategy_capital_eligibility",
        "next_extraction_target": "full_live_go_no_go_contract_before_broad_live_restore",
    }
