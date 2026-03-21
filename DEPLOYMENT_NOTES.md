# Patch 073 - defensive threshold zero-truth hotfix

This patch is built directly on patch 072 and is intentionally surgical.

## What changed
- Fixed filter-pressure threshold handling so a valid `0` defensive `return_20d_min_pct` is preserved instead of falling back to the legacy current threshold.
- Fixed the defensive unlock lab to preserve valid zero-valued thresholds the same way.
- Enriched `candidate_unlock_requirements` so filter reasons and selection blockers are explicitly separated.

## Expected outcome
- `/diagnostics/filter_pressure` should now report the same active defensive thresholds seen in the runtime preview.
- `candidate_unlock_requirements` should distinguish filter failures from later selection blockers.
- Defensive lab outputs should no longer drift back to the legacy 3% 20-day return floor when defensive mode uses 0%.
