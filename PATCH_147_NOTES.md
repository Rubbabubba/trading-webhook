# Patch 147 — Diagnostics Performance Split

## Goal
Reduce manual API latency for heavy diagnostics endpoints by splitting default summary responses from full-detail responses.

## Changes
- `PATCH_VERSION` updated to `patch-147-diagnostics-performance-split`
- `/diagnostics/candidates` now returns a summary payload
- added `/diagnostics/candidates_full` for full candidate detail
- `/diagnostics/swing` now returns a summary payload
- added `/diagnostics/swing_full` for full swing detail
- dashboard hero pill updated to Patch 147 label

## Safety
- no trading logic changes
- no scanner selection changes
- no reconcile changes
- no lifecycle changes
- no execution changes
- no env changes
