"""Swing broker-submit extraction prep helpers.

This module is the boundary for the eventual broker submit split. It must stay
free of FastAPI app globals. For now it owns pure submit-adjacent helpers only;
app.py still owns the actual Alpaca client/REST submission.
"""

from __future__ import annotations

from typing import Any, Callable


SWING_BROKER_SUBMIT_MODULE_VERSION = "patch-707-submit-ownership-extraction"


def build_client_order_id(prefix: str, symbol: str, uuid_text: str) -> str:
    clean_prefix = str(prefix or "scan").strip().lower() or "scan"
    clean_symbol = str(symbol or "").strip().lower() or "unknown"
    token = str(uuid_text or "").replace("-", "").strip()[:8] or "00000000"
    return f"{clean_prefix}-{token}-{clean_symbol}"


def alpaca_order_error_text(err: Any) -> str:
    return str(err or "").strip()


def is_nonretryable_alpaca_order_error(err: Any) -> bool:
    text = alpaca_order_error_text(err).lower()
    if not text:
        return False

    nonretryable_markers = [
        "fractional orders cannot be sold short",
        "insufficient qty",
        "insufficient quantity",
        "requested asset is not available for trading",
        "cannot be sold short",
        "unprocessable entity",
        "42210000",
    ]
    return any(marker in text for marker in nonretryable_markers)


def order_id_from_submit_response(order: Any, order_attr_fn: Callable[[Any, str, Any], Any]) -> str:
    if order_attr_fn is None:
        return ""
    return str(order_attr_fn(order, "id", "") or order_attr_fn(order, "order_id", "") or "")


def swing_broker_submit_module_status(*, actual_broker_submit_moved: bool = False) -> dict:
    return {
        "ok": True,
        "module": "swing_broker_submit",
        "module_version": SWING_BROKER_SUBMIT_MODULE_VERSION,
        "broker_free": True,
        "owns_broker_submit_contract": True,
        "owns_submit_error_classification": True,
        "actual_broker_submit_moved": bool(actual_broker_submit_moved),
        "app_py_still_owns_alpaca_client": not bool(actual_broker_submit_moved),
        "extraction_phase": "submit_ownership_extraction",
        "exports": [
            "build_client_order_id",
            "alpaca_order_error_text",
            "is_nonretryable_alpaca_order_error",
            "order_id_from_submit_response",
            "build_submit_ownership_contract",
        ],
        "recommended_action": (
            "actual_broker_submit_moved_verify_live_submit"
            if actual_broker_submit_moved
            else "submit_contract_owned_keep_actual_alpaca_submit_in_app_py_until_transport_promotion"
        ),
    }


def build_submit_ownership_contract(
    *,
    patch_version: str,
    execution_status: dict | None,
    broker_submit_status: dict | None,
    broker_transport_status: dict | None,
    transport_probe: dict | None,
    shadow_probe: dict | None,
    production_submit_uses_transport: bool = False,
    actual_broker_submit_moved: bool = False,
) -> dict:
    execution = dict(execution_status or {})
    broker_submit = dict(broker_submit_status or {})
    transport = dict(broker_transport_status or {})
    probe = dict(transport_probe or {})
    shadow = dict(shadow_probe or {})

    checks = {
        "execution_submit_module_owned": bool(execution.get("owns_submit_decision_contract")),
        "broker_submit_contract_owned": bool(broker_submit.get("owns_broker_submit_contract")),
        "broker_transport_contract_owned": bool(transport.get("owns_transport_contract")),
        "transport_probe_ok": bool(probe.get("ok")),
        "transport_shadow_probe_ok": bool(shadow.get("ok")),
        "actual_submit_not_moved_in_this_patch": not bool(actual_broker_submit_moved),
        "production_transport_not_promoted_in_this_patch": not bool(production_submit_uses_transport),
    }

    return {
        "ok": all(checks.values()),
        "patch_version": patch_version,
        "module": "swing_broker_submit",
        "module_version": SWING_BROKER_SUBMIT_MODULE_VERSION,
        "submit_decision_owner": "swing_execution_submit",
        "broker_submit_contract_owner": "swing_broker_submit",
        "broker_transport_contract_owner": "swing_broker_transport",
        "actual_broker_submit_owner": "app.py",
        "broker_calls": False,
        "submits_orders": False,
        "production_submit_uses_transport": bool(production_submit_uses_transport),
        "actual_broker_submit_moved": bool(actual_broker_submit_moved),
        "checks": checks,
        "mismatch_count": len([name for name, passed in checks.items() if not passed]),
        "extraction_phase": "submit_ownership_extraction",
        "next_extraction_target": "move_direct_submit_retry_limit_finalization_behind_module_api",
        "recommended_action": (
            "submit_ownership_contract_clean_continue_to_transport_promotion"
            if all(checks.values())
            else "fix_submit_ownership_contract_before_transport_promotion"
        ),
    }
