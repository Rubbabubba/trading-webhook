"""Swing broker-submit extraction prep helpers.

This module is the boundary for the eventual broker submit split. It must stay
free of FastAPI app globals. For now it owns pure submit-adjacent helpers only;
app.py still owns the actual Alpaca client/REST submission.
"""

from __future__ import annotations

from typing import Any, Callable


SWING_BROKER_SUBMIT_MODULE_VERSION = "patch-421-swing-broker-submit-function-extraction-prep"


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
        "actual_broker_submit_moved": bool(actual_broker_submit_moved),
        "app_py_still_owns_alpaca_client": not bool(actual_broker_submit_moved),
        "exports": [
            "build_client_order_id",
            "alpaca_order_error_text",
            "is_nonretryable_alpaca_order_error",
            "order_id_from_submit_response",
        ],
        "recommended_action": (
            "actual_broker_submit_moved_verify_live_submit"
            if actual_broker_submit_moved
            else "prep_complete_keep_actual_alpaca_submit_in_app_py"
        ),
    }