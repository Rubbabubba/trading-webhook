"""Swing broker submit transport wrapper prep.

This module defines the broker-submit transport boundary without owning Alpaca
clients, credentials, FastAPI, or app globals. Production submit still lives in
app.py until the transport wrapper is explicitly promoted.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


SWING_BROKER_TRANSPORT_MODULE_VERSION = "patch-423-broker-submit-transport-dry-run-parity-probe"


@dataclass(frozen=True)
class BrokerSubmitTransportConfig:
    sdk_enabled: bool = True
    rest_fallback_enabled: bool = True
    timeout_sec: int = 20


def classify_transport_result(order: Any, *, transport: str, client_order_id: str = "") -> dict:
    ok = order is not None
    return {
        "ok": bool(ok),
        "transport": str(transport or "").strip() or "unknown",
        "client_order_id": str(client_order_id or ""),
        "order_present": bool(order is not None),
        "result_type": type(order).__name__ if order is not None else None,
    }


def classify_transport_error(err: Any, *, transport: str) -> dict:
    text = str(err or "").strip()
    return {
        "ok": False,
        "transport": str(transport or "").strip() or "unknown",
        "error": text,
        "retryable_unknown": bool(text),
    }


def submit_with_injected_transport(
    *,
    sdk_submit_fn: Callable[[], Any] | None,
    rest_submit_fn: Callable[[], Any] | None,
    nonretryable_error_fn: Callable[[Any], bool] | None,
    client_order_id: str = "",
    rest_fallback_enabled: bool = True,
) -> dict:
    """Run an injected SDK submit with optional injected REST fallback.

    This helper is intentionally dependency-injected so it can be tested without
    importing Alpaca clients or touching credentials.
    """

    sdk_error = None

    if sdk_submit_fn is not None:
        try:
            order = sdk_submit_fn()
            if order is None:
                raise RuntimeError("alpaca_sdk_submit_returned_none")
            result = classify_transport_result(order, transport="sdk", client_order_id=client_order_id)
            result["order"] = order
            result["sdk_error"] = None
            return result
        except Exception as exc:
            sdk_error = str(exc)
            if nonretryable_error_fn is not None and bool(nonretryable_error_fn(sdk_error)):
                return {
                    "ok": False,
                    "transport": "sdk",
                    "client_order_id": str(client_order_id or ""),
                    "error": f"non_retryable_order_error:{sdk_error}",
                    "nonretryable": True,
                    "order": None,
                }

    if rest_fallback_enabled and rest_submit_fn is not None:
        try:
            order = rest_submit_fn()
            result = classify_transport_result(order, transport="rest_fallback", client_order_id=client_order_id)
            result["order"] = order
            result["sdk_error"] = sdk_error
            return result
        except Exception as exc:
            return {
                "ok": False,
                "transport": "rest_fallback",
                "client_order_id": str(client_order_id or ""),
                "error": f"sdk:{sdk_error}; rest:{exc}",
                "sdk_error": sdk_error,
                "order": None,
            }

    return {
        "ok": False,
        "transport": "none",
        "client_order_id": str(client_order_id or ""),
        "error": sdk_error or "no_submit_transport_available",
        "sdk_error": sdk_error,
        "order": None,
    }

def broker_transport_dry_run_parity_probe() -> dict:
    """Exercise injected transport paths without importing Alpaca or submitting."""

    fake_sdk_order = {
        "id": "sdk-order-1",
        "client_order_id": "dry-sdk",
        "status": "accepted",
    }
    fake_rest_order = {
        "id": "rest-order-1",
        "client_order_id": "dry-rest",
        "status": "accepted",
    }

    sdk_success = submit_with_injected_transport(
        sdk_submit_fn=lambda: fake_sdk_order,
        rest_submit_fn=lambda: fake_rest_order,
        nonretryable_error_fn=lambda err: False,
        client_order_id="dry-sdk",
        rest_fallback_enabled=True,
    )

    fallback_success = submit_with_injected_transport(
        sdk_submit_fn=lambda: (_ for _ in ()).throw(RuntimeError("temporary sdk timeout")),
        rest_submit_fn=lambda: fake_rest_order,
        nonretryable_error_fn=lambda err: False,
        client_order_id="dry-rest",
        rest_fallback_enabled=True,
    )

    nonretryable_block = submit_with_injected_transport(
        sdk_submit_fn=lambda: (_ for _ in ()).throw(RuntimeError("insufficient qty available")),
        rest_submit_fn=lambda: fake_rest_order,
        nonretryable_error_fn=lambda err: "insufficient qty" in str(err).lower(),
        client_order_id="dry-nonretryable",
        rest_fallback_enabled=True,
    )

    full_failure = submit_with_injected_transport(
        sdk_submit_fn=lambda: (_ for _ in ()).throw(RuntimeError("sdk down")),
        rest_submit_fn=lambda: (_ for _ in ()).throw(RuntimeError("rest down")),
        nonretryable_error_fn=lambda err: False,
        client_order_id="dry-fail",
        rest_fallback_enabled=True,
    )

    checks = {
        "sdk_success_ok": bool(sdk_success.get("ok")) and sdk_success.get("transport") == "sdk",
        "fallback_success_ok": bool(fallback_success.get("ok")) and fallback_success.get("transport") == "rest_fallback",
        "nonretryable_block_ok": (
            not bool(nonretryable_block.get("ok"))
            and bool(nonretryable_block.get("nonretryable"))
            and nonretryable_block.get("transport") == "sdk"
        ),
        "full_failure_ok": (
            not bool(full_failure.get("ok"))
            and full_failure.get("transport") == "rest_fallback"
            and "rest down" in str(full_failure.get("error") or "")
        ),
    }

    return {
        "ok": all(checks.values()),
        "mode": "broker_submit_transport_dry_run_parity_probe",
        "module": "swing_broker_transport",
        "module_version": SWING_BROKER_TRANSPORT_MODULE_VERSION,
        "broker_free": True,
        "production_submit_uses_transport": False,
        "checks": checks,
        "mismatch_count": len([name for name, passed in checks.items() if not passed]),
        "cases": {
            "sdk_success": {k: v for k, v in sdk_success.items() if k != "order"},
            "fallback_success": {k: v for k, v in fallback_success.items() if k != "order"},
            "nonretryable_block": {k: v for k, v in nonretryable_block.items() if k != "order"},
            "full_failure": {k: v for k, v in full_failure.items() if k != "order"},
        },
        "recommended_action": (
            "transport_dry_run_parity_ok_ready_for_opt_in_shadow_routing"
            if all(checks.values())
            else "fix_transport_dry_run_parity_before_live_routing"
        ),
    }

def swing_broker_transport_module_status(*, production_submit_uses_transport: bool = False) -> dict:
    return {
        "ok": True,
        "module": "swing_broker_transport",
        "module_version": SWING_BROKER_TRANSPORT_MODULE_VERSION,
        "broker_free": True,
        "production_submit_uses_transport": bool(production_submit_uses_transport),
        "app_py_still_owns_alpaca_client": not bool(production_submit_uses_transport),
        "exports": [
            "BrokerSubmitTransportConfig",
            "classify_transport_result",
            "classify_transport_error",
            "submit_with_injected_transport",
            "broker_transport_dry_run_parity_probe",
        ],
        "recommended_action": (
            "transport_wrapper_live_in_production_verify_submit"
            if production_submit_uses_transport
            else "transport_wrapper_ready_keep_production_submit_in_app_py"
        ),
    }