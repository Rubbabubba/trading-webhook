Patch 072: filter pressure truth alignment

Baseline: patch-071 active truth source cleanup

## What changed

- Corrected the patch metadata so the deployed build reports `patch-072-filter-pressure-truth-alignment`.
- Fixed `/diagnostics/filter_pressure` to evaluate candidates with the active regime-mode thresholds from current runtime truth instead of the old global trend defaults.
- Preserved the full active runtime candidate set inside current runtime truth so downstream diagnostics do not silently truncate to five rows.
- Split filter eligibility from live selection blockers inside filter-pressure analysis.
- Added baseline visibility for:
  - `filter_eligible_count` / `filter_eligible_symbols`
  - `selection_blocked_count` / `selection_blocked_symbols`
  - active `mode_thresholds` used by the filter-pressure engine
- Updated counterfactual and unlock-combo analysis to keep selection blockers in place, so simulated unlock counts stay closer to live truth.
- Added unit coverage for defensive-mode filter-pressure alignment and full-row runtime-truth preservation.

## Why this patch exists

Patch 071 got current-runtime promotion truth into much better shape, but filter-pressure was still drifting because it recomputed candidates with the old trend-policy thresholds and only a truncated subset of rows. That created mismatches like `eligible_total = 1` while `baseline.eligible_count = 0`. This patch makes filter-pressure evaluate the same active universe under the same active mode thresholds and distinguishes filter failures from downstream selection blockers.

## Expected verification

- `/diagnostics/build`
- `/diagnostics/current_runtime_preview`
- `/diagnostics/runtime_truth`
- `/diagnostics/filter_pressure`
- `/diagnostics/candidates`
- `/diagnostics/promotion_failures`

## Expected behavior

- Build metadata should report patch 072.
- `/diagnostics/filter_pressure` baseline counts should no longer contradict current-runtime preview when the active truth source is current runtime preview.
- If a symbol passes filters but is blocked by portfolio or symbol exposure, it should appear under selection-blocked fields instead of being miscounted as a filter failure.
- Counterfactual unlock output should remain grounded in the active regime-mode thresholds rather than the old static trend thresholds.
