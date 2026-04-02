# Patch 106 — Final submit source plumbing fix

This patch fixes the remaining scanner submit-path source downgrade.

## Problem
A scheduled scanner path still called `submit_scan_trade(...)` without passing the selected submit source. That caused early-override candidates to be executed with the default source of `worker_scan`, which sent the request down the wrong release/dry-run path.

## Fix
- Preserve the selected submit source end-to-end in the scheduled scanner submit path.
- Add `selected_source` / `effective_submit_source` diagnostics to payload and submit rejection details.
- Keep all strategy, ranking, sizing, and regime logic unchanged.
