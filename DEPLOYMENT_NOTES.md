# Patch 40

This is a drop-in-ready patch built from the patch 39 baseline.

## Purpose
Reduce overnight dashboard noise without changing release/readiness truth.

## Change included
- Suppress the top-level `freshness_degraded` operator warning when the **only** stale freshness item is `regime` and the market is closed.

## What does NOT change
- `/dashboard`, `/diagnostics/readiness`, and `/diagnostics/release` logic remains intact.
- Release gate behavior remains intact.
- `recent_market_scan_missing` overnight behavior remains intact.
- Regime can still show as stale in the freshness section overnight.

## Expected result
Overnight closed-market dashboard should:
- still show market closed
- still show guarded live blocked
- still show stale regime in freshness diagnostics
- no longer raise the top operator warning solely because overnight regime is stale
