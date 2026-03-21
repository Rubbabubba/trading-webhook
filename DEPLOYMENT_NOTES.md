# Patch 069 - runtime promotion truth sync

This patch is built from patch 068 as the baseline.

## What changed

- Fixed defensive unlock lab threshold handling so zero-valued thresholds like `return_20d_min_pct = 0` are preserved instead of falling back to legacy defaults.
- Updated current runtime truth snapshot to carry live blockers, remaining entry capacity, regime mode, and active mode thresholds.
- Updated promotion-failure and trade-path diagnostics to prefer current runtime truth over stale history when the active runtime universe has changed.
- Updated promotion-failure filter-pressure output to analyze the current runtime candidate set instead of the last completed historical scan from a different universe.

## Expected outcome

- `/diagnostics/defensive_unlock_lab` should reflect the actual active defensive thresholds.
- `/diagnostics/promotion_failures` should stay aligned with `/diagnostics/current_runtime_preview` for the active runtime universe.
- `/diagnostics/trade_path` should report the active runtime truth instead of stale prior-universe scan history when available.
