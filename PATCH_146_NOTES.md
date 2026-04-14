# Patch 146 — Adaptive Capacity Utilization

## Goal
Increase trade throughput without loosening hard risk controls.

## What changed
- Added adaptive capacity candidate selection for breakout trades only.
- Adaptive path activates only when:
  - no primary approved candidates exist,
  - capacity remains available,
  - candidate has exactly one soft rejection,
  - candidate still meets high quality thresholds.
- Soft rejection types allowed:
  - `close_not_near_high`
  - `too_far_below_breakout`
  - `rank_score_below_min`
- Adaptive entries use source `worker_scan_adaptive_capacity` and entry type `adaptive_capacity`.
- Added adaptive-capacity summary fields into scan state for observability.

## Hard protections preserved
- No bypass of correlation limits.
- No bypass of portfolio or symbol exposure caps.
- No bypass of market-hours gating.
- No bypass of daily halt / weak-tape global blocks.
- No changes to exit logic, reconcile, or lifecycle management.

## Default thresholds
- min selection quality score: 175
- min rank score: 100
- min close-to-high: 98.0%
- max breakout gap: 1.0%
- max adaptive entries per scan: 1

## Env changes
None required.
Optional tuning envs if needed later:
- `SWING_ADAPTIVE_CAPACITY_ENABLED`
- `SWING_ADAPTIVE_CAPACITY_MAX_ENTRIES_PER_SCAN`
- `SWING_ADAPTIVE_CAPACITY_MIN_SELECTION_SCORE`
- `SWING_ADAPTIVE_CAPACITY_MIN_RANK_SCORE`
- `SWING_ADAPTIVE_CAPACITY_MIN_CLOSE_TO_HIGH_PCT`
- `SWING_ADAPTIVE_CAPACITY_MAX_BREAKOUT_GAP_PCT`
