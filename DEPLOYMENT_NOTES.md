# Patch 078 - Paper Execution Guardrail Hotfix

This patch is a surgical stability fix built on patch 077.

## Changes
- Fixes a NameError in `_paper_execution_stage_failures` by deriving `fill_flags` per row with `_proof_row_fill_flags(row)`.
- Restores `/diagnostics/pipeline_guardrails` and downstream snapshots such as `/diagnostics/trade_path` and `/diagnostics/promotion_failures` that depend on paper execution proof generation.
- Preserves patch 077 logic and truth-source behavior without changing entry, selection, or regime rules.

## Expected verification
- `/diagnostics/pipeline_guardrails` returns 200
- `/diagnostics/trade_path` returns 200
- `/diagnostics/promotion_failures` returns 200
