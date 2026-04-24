# Patch 166 — Exit Override + Entry Dispatch Truth

## Purpose
Separate protective exit permission from entry-admission freshness gates and expose scanner entry dispatch outcomes on the dashboard.

## Changes
- Allows live exits when the only release-gate blockers are entry-admission controls:
  - `recent_market_scan_missing`
  - `daily_halt_active`
- Keeps hard live-exit blockers intact:
  - global `DRY_RUN`
  - `LIVE_TRADING_ENABLED=false`
  - market-closed / non-tradable gate failures
  - unknown release-gate blockers
- Adds read-only dashboard dispatch truth:
  - selected symbol
  - signal
  - rank
  - submit state
  - submit reason
  - attempted flag
  - order id
  - submit source
- Adds latest selection trace and ranked-out candidate preview.

## No Drift / No Regression
- No entry ranking changes.
- No scanner selection changes.
- No sizing changes.
- No broker execution changes beyond preventing protective exits from being converted to dry-runs by entry freshness blockers.
- No accounting changes.

## Validation
- `py_compile` passed for `app.py`, `scanner.py`, and `worker.py`.

## Env Changes
None.
