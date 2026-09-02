# Codebase Structure

This project is being cleaned up from a single-file prototype into two clearly separated systems: the currently live swing system and a paper-validation regime-routed SPY intraday system.

## Runtime Entry Points

- `app.py` - FastAPI application shell, routes, compatibility wrappers, and legacy runtime glue.
- `scanner.py` - scanner service entry point.
- `worker.py` - background worker entry point.

## Swing Modules

All executable swing modules now live under `legacy_swing/`. They remain runtime-loadable only while the live swing account and its positions require management. New intraday code must not import this package.

Patch-era swing tests live under `legacy_swing/tests/`. Offline swing replay and research utilities are preserved under `archive/legacy_swing_research/` and are not part of Render runtime.

## Regime Intraday Modules

- `regime_intraday.py` - regime classification and underlying signal logic.
- `regime_intraday_options.py` - defined-risk option contract selection and order planning.
- `regime_intraday_paper.py` - paper execution, reconciliation, lifecycle, and performance proof.
- `regime_intraday_notifications.py` - one-time candidate email notification logic.
- `regime_intraday_dashboard.py` - dedicated paper-system operator console renderer.
- `regime_intraday_api.py` - isolated FastAPI router for the new system's read and dashboard endpoints.
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
