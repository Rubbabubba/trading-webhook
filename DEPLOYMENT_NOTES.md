# Patch 068 - Defensive Breakout Promotion

Baseline: patch-067-hotfix-drop-in

## What changed

- Promoted the defensive-mode breakout distance default from **1%** to **7%**.
- Kept defensive close-to-high at **98.5%** and defensive 20-day return at **0%**.
- Kept defensive mode trend and index-alignment requirements disabled, matching patch 062/063 policy-switch behavior.
- Added `/diagnostics/defensive_policy` so you can compare the current defensive policy against the prior 1% defensive breakout setting on the current runtime universe.

## Why this patch exists

Patch 067 measured the current runtime universe under defensive-mode rules and showed the narrowest breakout-only unlock at **7%**, with **NET** as the first runtime symbol to unlock. This patch converts that measured result into live policy for defensive mode instead of leaving it as lab-only information.

## Expected verification

After deploy, verify:

- `/diagnostics/build`
- `/diagnostics/current_runtime_preview`
- `/diagnostics/defensive_policy`
- `/diagnostics/defensive_unlock_lab`
- `/diagnostics/promotion_failures`
- `/diagnostics/candidates`
- `/diagnostics/trade_path`

## Expected behavior

- No regression to the patch 067 helper/import stability.
- Defensive mode should now use `breakout_max_distance_pct = 7%`.
- `/diagnostics/defensive_policy` should show the prior 1% policy versus the current 7% policy on the current runtime universe.
- `NET` should become the first current-runtime name eligible under defensive-mode non-market rules when the rest of its defensive checks pass.
