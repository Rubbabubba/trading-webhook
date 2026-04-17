# Patch 149 — Daily Halt Exit Override

## Goal
Allow protective live exits to continue when the release gate is blocked **only** by `daily_halt_active`, while still blocking new live entries.

## What changed
- Added `is_live_exit_permitted()`
- `close_position()` now uses exit-specific permission checks
- `close_partial_position()` now uses exit-specific permission checks

## Safety behavior
- Still blocks exits when `DRY_RUN=true`
- Still blocks exits when `LIVE_TRADING_ENABLED=false`
- Still blocks exits for other release-gate failures
- Allows exits only when the sole gate blocker is `daily_halt_active`
- Does **not** relax entry permissions

## Expected result
- Daily halt continues to stop new entries
- Profit locks, partial exits, and protective exits can still submit live
