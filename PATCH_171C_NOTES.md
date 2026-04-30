# Patch 171C — Safe Dashboard Snapshot Merge

Built from Patch 170 baseline.

## Scope
- Restores `/dashboard` route from clean baseline.
- Makes `/dashboard` snapshot-only: no broker calls, no reconcile recompute, no scanner recompute.
- Merges persisted broker-backed positions with active plan metadata for active position display.
- Uses env-safe `DAILY_LOSS_LIMIT` / `DAILY_STOP_DOLLARS`; no undefined globals.

## No behavior changes
- No entry logic changes.
- No exit logic changes.
- No risk logic changes.
- No scanner logic changes.
- No reconcile logic changes.
