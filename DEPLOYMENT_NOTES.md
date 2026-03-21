# Patch 075 – Paper Execution Guardrails

This patch is built directly on patch 074 and is intentionally surgical.

## What changed
- Updated build metadata so diagnostics report `patch-075-paper-execution-guardrails`.
- Added a safe paper-execution mode for `worker_scan` that can bypass dry-run **only** when all of the following are true:
  - release stage is `paper`
  - `APCA_PAPER=true`
  - `SCANNER_ALLOW_LIVE=true`
  - `PAPER_EXECUTION_ENABLED=true`
- Centralized entry dry-run resolution with `effective_entry_dry_run(...)` so scanner, diagnostics, and execution paths stay aligned.
- Added `/diagnostics/pipeline_guardrails` to show symbols stuck between selected/plan/order/fill/exit stages.
- Downgraded preview-only `selected_but_no_plan` noise when truth is coming from `current_runtime_preview` rather than an authoritative live scan.
- Added post-scan guardrail logging and decisions for `selected_without_plan` and `plan_without_order`.

## Why
Patch 074 proved lifecycle persistence and surfaced a false-positive drift case where preview-selected symbols looked like pipeline failures even when no authoritative market-hours scan had run. Patch 075 keeps the paper-lifecycle proof intact, adds explicit guardrails, and safely enables real paper execution when the environment is correctly armed.

## Deploy / verify
1. Deploy the zip.
2. Confirm `/diagnostics/build` shows `patch-075-paper-execution-guardrails`.
3. Confirm `/diagnostics/pipeline_guardrails` loads.
4. Confirm `/diagnostics/runtime` (or build/runtime diagnostics) shows:
   - `paper_execution_enabled`
   - `paper_execution_permitted`
5. During market hours, verify selected symbols either create a plan or surface an explicit guardrail violation.
