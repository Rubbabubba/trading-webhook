patch-173-dashboard-completeness-and-scanner-timeout-closure

## Patch 173
- Restores dashboard completeness from persisted state files only (`/var/data/*`) with fallback aggregation for performance analytics and strategy attribution when per-strategy buckets are missing.
- Active positions now show `snapshot unavailable` instead of blank values for missing last price/unrealized values.
- Lifecycle rows now display symbol, state/event, and reason fallbacks from persisted lifecycle history.
- Today P&L truth applies explicit source fallback labeling (`alpaca_orders` -> `persisted_strategy_state` -> `no_data`).
- Scanner timeout/dispatch exceptions now emit explicit failure close telemetry (`scan_fail`) to avoid stale in-flight state.
- Dashboard table cell overflow styling hardened via `word-break: break-word; white-space: normal`.