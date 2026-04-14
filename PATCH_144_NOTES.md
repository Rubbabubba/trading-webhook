# Patch 144 — scanner timeout alignment + dispatch warning recovery

## What changed
- PATCH_VERSION updated to patch-144-scanner-timeout-alignment-dispatch-warning-recovery
- scanner telemetry warning classification now treats a late worker timeout as recovered when the same scan attempt already closed successfully
- dispatch_failure will no longer remain active on the dashboard when a scheduled scan completed successfully and the worker timed out afterward

## Why
The scanner worker timeout can fire before a long-running scan response returns, causing:
- scan_ok recorded by the main service
- scan_dispatch_error recorded by the worker afterward
This patch prevents that sequence from being shown as an active dispatch failure.

## Env change required
- Increase scanner worker timeout to align with observed scan duration.
