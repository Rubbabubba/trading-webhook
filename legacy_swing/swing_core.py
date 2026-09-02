"""Pure swing-system helpers.

This module is intentionally broker-free and FastAPI-free. Keep live order
submission, Alpaca calls, and runtime globals in app.py until the split is
proven stable.
"""

from __future__ import annotations

from typing import Any


SWING_CORE_MODULE_VERSION = "patch-360-swing-core-status-version-sync-submit-proof-required-operator-brief"


def control_row(
    name: str,
    enabled: bool,
    category: str,
    live_effect: str,
    env_name: str = "",
    value: Any = None,
    recommendation: str = "keep",
) -> dict:
    return {
        "name": name,
        "enabled": bool(enabled),
        "category": category,
        "live_effect": live_effect,
        "env": env_name or None,
        "value": value,
        "recommendation": recommendation,
    }


def swing_control_surface_snapshot(
    *,
    patch_version: str,
    controls: list[dict],
    recommended_env_updates: list[str] | None = None,
    recommended_action: str = "observe_core_strategy",
) -> dict:
    enabled = [c for c in controls if c.get("enabled")]
    entry_controls = [c for c in enabled if c.get("category") == "entry"]
    exit_controls = [c for c in enabled if c.get("category") == "exit"]
    simplify = [
        c
        for c in controls
        if str(c.get("recommendation") or "").startswith("disable")
    ]

    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "swing_control_surface_audit",
        "swing_core_module_version": SWING_CORE_MODULE_VERSION,
        "module_split": {
            "active": True,
            "broker_free": True,
            "execution_moved": False,
            "diagnostics_moved": ["swing_control_surface"],
        },
        "enabled_control_count": len(enabled),
        "entry_control_count": len(entry_controls),
        "exit_control_count": len(exit_controls),
        "controls": list(controls),
        "recommended_env_updates": list(recommended_env_updates or []),
        "simplification_candidates": [c.get("name") for c in simplify],
        "recommended_action": recommended_action,
    }