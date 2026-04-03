# Patch 111 — bar fallback diagnostics + acceptance fix

## What changed
- Adds explicit bar fallback diagnostics:
  - bar count
  - bar timestamp
  - bar close
  - bar age
  - exact rejection reason
- Adds a last-resort candidate-scan fallback using the current selected candidate's own `close` and `scan_ts_utc` when:
  - primary IEX quote is stale, and
  - recent 1-minute bar fallback is unavailable or rejected
- Preserves stale primary snapshot under `stale_snapshot`
- Updates patch marker to `patch-111-bar-fallback-diagnostics-and-acceptance-fix`

## Why
Patch 110 proved the system was attempting the 1-minute bar fallback but not activating it. This patch exposes the exact fallback failure details and provides a safe final acceptance path using the current scan candidate metadata already produced in the same scan cycle.

## Risk profile
- No ranking changes
- No regime changes
- No auth/dashboard changes
- No spread loosening
- Fallback only activates after stale primary quote rejection
