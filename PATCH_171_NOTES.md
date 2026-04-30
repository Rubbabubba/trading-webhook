# Patch 171 — Active Position Merge + Dashboard Performance Stabilization

Scope: dashboard/operator UI only.

## Changes
- Dashboard active positions table now merges persisted broker-position snapshot with active plan fields.
- Qty comes from broker-position snapshot first.
- Entry/stop/target/signal/status come from active plan fields when available.
- Dashboard no longer calls live broker/reconcile paths during page render.
- Added lightweight in-process dashboard HTML cache with default 5 second TTL.
- Keeps heavy raw diagnostics in existing diagnostics endpoints.

## No trading changes
- No entry logic changes.
- No exit logic changes.
- No scanner logic changes.
- No risk logic changes.
- No reconcile logic changes.
- No order placement/cancel/retry changes.

## Env changes
None required.
Optional: DASHBOARD_CACHE_TTL_SEC, default 5.
