# Patch 074 - paper lifecycle proof

This patch is built directly on patch 073 and stays surgical.

## What changed
- Updated the build metadata so diagnostics report `patch-074-paper-lifecycle-proof`.
- Added stitched paper lifecycle proof helpers that tie together candidate state, selection state, plan state, order state, fill state, exit arming, lifecycle events, and recent decisions per symbol.
- Added `/diagnostics/paper_execution_proof` as the one-stop paper audit view for recent runtime symbols and active plan symbols.
- Enriched `/diagnostics/trade_path` with per-symbol lifecycle rows and stage-failure buckets so entry and exit gaps are visible in one place.
- Enriched `/diagnostics/paper_lifecycle` with current stage-failure summaries and active plan symbols for faster operator checks.

## Expected outcome
- You can verify whether a candidate stopped at filter, selection, plan creation, order submission, fill, or exit from a single diagnostics view.
- Trade path diagnostics now expose concrete stage buckets like `selected_but_no_plan`, `plan_without_order`, `order_without_fill`, and `fill_without_exit_arm`.
- Paper lifecycle diagnostics are now usable as an operator view instead of just a raw event dump.
