# Patch 070 - promotion truth alignment and selection diagnostics

Baseline: patch-068-defensive-breakout-promotion

## What changed

- Carried forward the intended patch 069 runtime-truth alignment behavior, but rebuilt cleanly from patch 068.
- Upgraded `/diagnostics/current_runtime_preview` to apply the same live selection constraints used by the swing scan for pending plans, open positions, correlation-group limits, symbol exposure, portfolio exposure, and entry-cap limits.
- Added `selected_total`, `selected_symbols`, `remaining_new_entries_today`, and `max_new_entries_effective` to current runtime preview output.
- Updated runtime truth and promotion-failure snapshots to carry current-runtime selected symbols and eligible-but-not-selected diagnostics instead of always reporting zero promoted names.
- Added `/diagnostics/promotion_selection` for a compact promotion-decision view on the active runtime universe.
- Added `selection_blockers` to promotion-failure top-candidate rows for clearer diagnosis when a candidate is rank-worthy but still blocked.

## Why this patch exists

Patch 068 correctly unlocked defensive-mode eligibility for names like CRM and UBER, but promotion diagnostics could still report `eligible_candidates_did_not_promote` because preview truth stopped at eligibility and did not simulate the actual selection layer. This patch makes current-runtime diagnostics reflect the same selection rules the scan uses.

## Expected verification

- `/diagnostics/build`
- `/diagnostics/current_runtime_preview`
- `/diagnostics/promotion_selection`
- `/diagnostics/promotion_failures`
- `/diagnostics/runtime_truth`
- `/diagnostics/defensive_unlock_lab`
- `/diagnostics/candidates`
- `/diagnostics/trade_path`

## Expected behavior

- If the current runtime preview shows eligible candidates and there is entry capacity, selected symbols should now appear instead of staying at zero by diagnostic drift alone.
- Promotion-failure output should align with current-runtime preview on the active universe.
- Eligible-but-not-selected names should be identified explicitly when entry capacity is the reason they were not promoted.
