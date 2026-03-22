# Patch 081 – Execution proof lifecycle

This patch builds on patch 080 and turns scanner-selected dry-run candidates into real dry-run plan artifacts so the execution lifecycle can be proven end to end without widening live trading permissions.

What changed:
- Worker scan now records `SCAN/candidate_selected` for top-ranked symbols that win the current submission slots.
- Worker scan now routes selected dry-run candidates through the normal `submit_scan_trade()` path instead of returning a placeholder dry-run stub. That means dry-run selections create the same plan, decision, and lifecycle evidence as other dry-run entries.
- Added `/diagnostics/execution_proof` and `/diagnostics/current_runtime_execution_proof` to show the selected -> plan -> order -> fill -> exit-arm progression for the current truth source only.
- Journal persistence now explicitly keeps `SCAN/candidate_selected` events even when general scan journaling is not the main source of truth.

Safety:
- No live execution permissions were widened.
- Dry-run behavior still remains dry-run.
- This patch is focused on evidence and observability, not on changing trading thresholds.


# Patch 079 – Preview plan activation

This patch builds on patch 078 and activates synthetic preview plan creation for current runtime preview selections without allowing order submission.

What changed:
- Selected symbols in current runtime preview now materialize as preview-only plans in runtime truth and paper execution proof diagnostics.
- Pipeline guardrails now show selected -> planned progression in preview mode without creating real orders.
- Coverage now reflects preview-selected/planned symbols so diagnostics match actual runtime preview state.

Safety:
- No broker orders are submitted.
- No live paper execution permission logic was widened.
- Preview plans are diagnostic-only and marked preview_only=true.


## patch-080-diagnostics-continuity-hotfix
- Fixes `/diagnostics/trade_path` by computing paper execution proof before coverage fields reference it.
- Restores backward-compatible alias route `/diagnostics/runtime_preview` alongside `/diagnostics/current_runtime_preview`.
- Keeps paper execution proof and promotion failure behavior from patch 080 unchanged.
