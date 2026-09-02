# Current Operator Dashboard Guide

## System Identity

Only the regime-routed SPY intraday system is served in production. It is paper-only until every live-readiness gate passes.

An intraday email, signal, candidate, or paper order is never evidence that a live order was submitted.

## Current Dashboards

| Dashboard | Purpose |
|---|---|
| `/dashboard/intraday` | Primary console for regime, signal, candidate, paper order, exit, performance, and readiness state. |

All dashboards require operator authentication. In a browser, use any username and the Render `ADMIN_SECRET` as the password. API clients may send the same value in `x-admin-secret`.

## Intraday Operating Rule

The intraday system remains paper-only while `live_ready` is false. Do not bypass the listed blockers. Current proof includes signal quality, option-feed quality, defined-risk contract selection, paper entry and exit round trips, and a sufficient closed-trade sample.

Useful endpoints:

- `/health` — public, non-sensitive identity and health summary.
- `/diagnostics/regime_intraday` — latest scan and selected candidates.
- `/diagnostics/regime_intraday_readiness` — paper and live gates.
- `/diagnostics/regime_intraday_ledger` — candidate, order, and event history.
- `/diagnostics/route_catalog` — active and archived route inventory.

## Legacy Swing Rule

Do not disable swing exit management or remove broker state while swing positions remain open. New swing entries should be closed before final retirement. Once the account is free of swing positions and orders, the `legacy_swing` package and its remaining routes can move into the non-runtime archive.

The full historical dashboard guide is preserved at `archive/legacy_swing_docs/dashboard_guide_full.md`.
