# Patch 067 - Defensive Near-Breakout Lab

Baseline: patch-066-runtime-truth-rebased

This patch builds directly on patch 066 and keeps the runtime-truth fixes intact.

## What changed

- Added `/diagnostics/defensive_unlock_lab`.
- Added a defensive-mode unlock lab that evaluates the **current runtime universe** against a controlled breakout-distance ladder and a narrow close-to-high relaxation matrix.
- Added nearest-unlock diagnostics so you can see exactly how far each top runtime candidate is from passing the current defensive thresholds.
- Added the defensive unlock lab payload into `/diagnostics/promotion_failures` so the failure view shows both the current rejection stack and the smallest defensive-only relaxation that would unlock names.

## Why this patch exists

The system is now truthful about the runtime universe, but it still is not producing paper candidates. At this stage the right move is not changing live behavior blindly. The right move is to measure how close the current runtime names are to qualifying under defensive-mode rules and identify the narrowest controlled relaxation that would unlock candidates.

## Expected verification

After deploy, verify:

- `/diagnostics/build`
- `/diagnostics/current_runtime_preview`
- `/diagnostics/defensive_unlock_lab`
- `/diagnostics/promotion_failures`
- `/diagnostics/candidates`
- `/diagnostics/trade_path`

## Expected behavior

- No regression to the patch 064 helper-reference failures.
- `/diagnostics/defensive_unlock_lab` should report the current runtime symbols and defensive thresholds.
- The lab should show a breakout-distance ladder, a breakout-plus-close matrix, and a narrowest unlock step when one exists.
- `/diagnostics/promotion_failures` should include a `defensive_unlock_lab` section.
