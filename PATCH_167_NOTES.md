# Patch 167 — Daily Halt Cause + Today P&L Truth

## Purpose
Adds read-only operator visibility for daily halt cause and current-day P&L truth without changing trading logic.

## Changes
- Adds `/diagnostics/daily_halt_truth`.
- Adds release diagnostic fields:
  - `daily_halt_reason`
  - `daily_halt_truth`
  - `today_pnl_truth`
  - `today_realized_pnl`
  - `today_unrealized_pnl`
  - `today_net_pnl`
- Adds dashboard sections:
  - Daily Halt Truth
  - Today P&L Truth
- Uses broker account equity change as the same broker-first truth basis used by the daily stop brake.
- Separates broker-derived today P&L from strategy-performance recorded trades so accounting gaps are visible.

## No behavior changes
- No entry logic changes.
- No exit logic changes.
- No ranking changes.
- No sizing changes.
- No broker execution changes.
- No accounting write-path changes.

## Env changes
None.
