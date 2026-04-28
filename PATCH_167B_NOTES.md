# Patch 167B — Daily Halt Truth NameError Hotfix

## Purpose
Surgical hotfix for Patch 167. Prevents dashboard and release diagnostics from crashing when optional daily halt config names are not defined as module globals.

## Changes
- Adds safe env/config lookup for `DAILY_LOSS_LIMIT`.
- Adds safe env/config lookup for `DAILY_STOP_DOLLARS`.
- Adds fail-safe Daily Halt Truth snapshot.
- Adds fail-safe Today P&L Truth snapshot.
- Surfaces daily halt / today P&L fields in release diagnostics.
- Adds Dashboard visibility cards for Daily Halt Truth and Today P&L Truth.

## No trading logic changes
- No entry logic changes.
- No exit logic changes.
- No scanner ranking changes.
- No sizing changes.
- No broker execution changes.
- No accounting write changes.
