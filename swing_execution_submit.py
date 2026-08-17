"""Swing submit decision compatibility helpers.

This module is the next Track 2 extraction point. It intentionally stays
broker-free: it can build/preview submit payloads, but app.py still owns the
actual Alpaca submit call until broker submit movement is explicitly proven.
"""

from __future__ import annotations

from swing_execution import (
    SWING_EXECUTION_MODULE_VERSION,
    SwingLimitEntryConfig,
    available_qty_from_plan,
    build_limit_order_payload,
    build_market_order_payload,
    build_submit_decision,
    clamp_exit_qty,
    format_order_qty,
    limit_entry_preview,
    qty_source_from_plan,
)


SWING_EXECUTION_SUBMIT_MODULE_VERSION = "patch-420-swing-execution-submit-compatibility-module-split"


def swing_execution_submit_module_status() -> dict:
    return {
        "ok": True,
        "module": "swing_execution_submit",
        "module_version": SWING_EXECUTION_SUBMIT_MODULE_VERSION,
        "source_module": "swing_execution",
        "source_module_version": SWING_EXECUTION_MODULE_VERSION,
        "broker_free": True,
        "actual_broker_submit_moved": False,
        "exports": [
            "SwingLimitEntryConfig",
            "available_qty_from_plan",
            "qty_source_from_plan",
            "clamp_exit_qty",
            "format_order_qty",
            "build_market_order_payload",
            "build_limit_order_payload",
            "build_submit_decision",
            "limit_entry_preview",
        ],
        "recommended_action": "compatibility_module_ready_keep_broker_submit_in_app_py",
    }