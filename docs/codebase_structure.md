# Codebase Structure

This project is being cleaned up from a single-file prototype into a modular swing trading system.

## Runtime Entry Points

- `app.py` - FastAPI application shell, routes, compatibility wrappers, and legacy runtime glue.
- `scanner.py` - scanner service entry point.
- `worker.py` - background worker entry point.

## Swing Modules

- `swing_core.py` - swing control/status helpers.
- `swing_execution.py` - swing execution module status and submit-split prep helpers.
- `swing_light_diagnostics.py` - lightweight diagnostics used by operator workflows.
- `swing_runtime_config.py` - runtime config snapshots.
- `swing_selection_contract.py` - production selection contract helpers.

## Cleanup Prep Modules

- `broker_client.py` - broker/env normalization helpers. Larger Alpaca broker code will move here over time.
- `market_clock.py` - pure market/session time helpers. Live Alpaca clock integration remains in `app.py` until safely split.

## Docs And Tools

- `SYSTEM_ENDPOINTS.md` - endpoint manifest. Update whenever routes are added, removed, or renamed.
- `docs/` - operator and codebase documentation.
- `tools/` - local helper scripts, not production runtime.

## Archive

- `archive/` - historical files retained for reference only. Production code must not import from archive.