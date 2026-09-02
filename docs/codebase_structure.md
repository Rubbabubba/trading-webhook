# Codebase Structure

This project is being cleaned up from a single-file prototype into two clearly separated systems: the currently live swing system and a paper-validation regime-routed SPY intraday system.

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

## Regime Intraday Modules

- `regime_intraday.py` - regime classification and underlying signal logic.
- `regime_intraday_options.py` - defined-risk option contract selection and order planning.
- `regime_intraday_paper.py` - paper execution, reconciliation, lifecycle, and performance proof.
- `regime_intraday_notifications.py` - one-time candidate email notification logic.
- `regime_intraday_dashboard.py` - dedicated paper-system operator console renderer.
- `route_catalog.py` - route ownership, lifecycle, mutation, and sensitivity classification.

## Cleanup Prep Modules

- `broker_client.py` - broker/env normalization helpers. Larger Alpaca broker code will move here over time.
- `market_clock.py` - pure market/session time helpers. Live Alpaca clock integration remains in `app.py` until safely split.

## Docs And Tools

- `SYSTEM_ENDPOINTS.md` - generated endpoint manifest. Run `tools/generate_endpoint_manifest.py` whenever routes are added, removed, or renamed.
- `docs/route_migration.md` - route ownership, deprecation, and removal policy.
- `docs/` - operator and codebase documentation.
- `tools/` - local helper scripts, not production runtime.

## Archive

- `archive/` - historical files retained for reference only. Production code must not import from archive.
