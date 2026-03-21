# Patch 077 - Current Runtime Fill Truth Isolation

This patch is built directly on patch 076.

## What changed
- Isolated current-runtime fill truth from stale historical fills in lifecycle-derived diagnostics.
- Added `historical_fill_observed` tracking so old fills can still be surfaced without contaminating present-tense pipeline state.
- Updated pipeline guardrail summaries to report `historical_filled_symbols` separately from active/current `filled_symbols`.
- Tightened stage-failure logic so `fill_without_exit_arm` only fires for current or recent fills, not legacy residue.

## Intent
Keep current-runtime promotion and lifecycle diagnostics truthful when older journal or lifecycle events still exist for symbols that are back in the active runtime universe.
