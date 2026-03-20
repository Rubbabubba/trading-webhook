# Patch 066 - Runtime Truth Rebased

Baseline: patch-063-runtime-preview-and-universe-validation

This patch intentionally rebases from patch 063 and does not depend on patch 064 or patch 065.

## What changed

- Added `_latest_matching_scan_record()` to locate the newest candidate/scan record that exactly matches the current runtime universe.
- Added `_current_runtime_truth_snapshot()` to build a scan-like truth snapshot directly from the current runtime universe preview when history is stale or missing.
- Updated `/diagnostics/candidates` to prefer current-runtime-matching truth instead of stale candidate history from an older universe.
- Updated `/diagnostics/trade_path` to prefer current-runtime-matching truth before falling back to generic latest scan history.
- Updated `/diagnostics/promotion_failures` to use the same current-runtime-first truth selection.
- Added `/diagnostics/runtime_truth` for direct inspection of matched-history truth vs current-runtime preview truth.

## Why this patch exists

Patch 063 was the last stable baseline, but it could still report stale candidate/trade-path views when the scanner universe env changed. This patch fixes that drift without introducing the broken helper reference from later patches.

## Expected verification

After deploy, verify:

- `/diagnostics/build`
- `/diagnostics/universe_validation`
- `/diagnostics/current_runtime_preview`
- `/diagnostics/runtime_truth`
- `/diagnostics/candidates`
- `/diagnostics/trade_path`
- `/diagnostics/promotion_failures`

## Expected behavior

- No `NameError` for `_latest_matching_scan_record`.
- Candidate/trade-path/promotion-failures endpoints should reflect the active runtime universe when history is stale.
- Current runtime should now win over older candidate history built from the previous universe.
