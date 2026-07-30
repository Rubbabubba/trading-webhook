"""Broker-free swing runtime config snapshot helpers.

This module does not parse environment variables directly. app.py still owns
runtime globals for now; this file only shapes already-collected config values.
"""

from __future__ import annotations

from typing import Any


SWING_RUNTIME_CONFIG_MODULE_VERSION = "patch-309-swing-runtime-config-module-split"


def build_swing_runtime_config_snapshot(
    *,
    patch_version: str,
    strategy_mode: str,
    live_swing_runtime: bool,
    live_flags: dict[str, Any],
    capacity: dict[str, Any],
    risk_controls: dict[str, Any],
    entry_gates: dict[str, Any],
    exit_guards: dict[str, Any],
    retired_paths: dict[str, Any],
    modules: dict[str, Any],
) -> dict:
    active_entry_blocks = [
        name
        for name, payload in entry_gates.items()
        if isinstance(payload, dict)
        and bool(payload.get("enabled"))
        and bool(payload.get("can_block_entries"))
    ]

    active_exit_guards = [
        name
        for name, payload in exit_guards.items()
        if isinstance(payload, dict) and bool(payload.get("enabled"))
    ]

    retired_not_disabled = [
        name
        for name, payload in retired_paths.items()
        if isinstance(payload, dict) and not bool(payload.get("disabled"))
    ]

    return {
        "ok": True,
        "patch_version": patch_version,
        "mode": "swing_runtime_config",
        "module": "swing_runtime_config",
        "module_version": SWING_RUNTIME_CONFIG_MODULE_VERSION,
        "strategy_mode": strategy_mode,
        "live_swing_runtime": bool(live_swing_runtime),
        "live_flags": dict(live_flags),
        "capacity": dict(capacity),
        "risk_controls": dict(risk_controls),
        "entry_gates": dict(entry_gates),
        "exit_guards": dict(exit_guards),
        "retired_paths": dict(retired_paths),
        "modules": dict(modules),
        "summary": {
            "active_entry_block_count": len(active_entry_blocks),
            "active_entry_blocks": active_entry_blocks,
            "active_exit_guard_count": len(active_exit_guards),
            "active_exit_guards": active_exit_guards,
            "retired_paths_not_disabled": retired_not_disabled,
            "cleanup_green": not retired_not_disabled,
        },
        "operator_read": {
            "purpose": "Single broker-free snapshot of what the swing runtime is configured to do.",
            "env_parsing_moved": False,
            "broker_calls": False,
            "execution_moved": False,
            "recommended_action": (
                "cleanup_green_observe_next_live_scan"
                if not retired_not_disabled
                else "finish_retiring_old_paths_before_more_strategy_changes"
            ),
        },
    }