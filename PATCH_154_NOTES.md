# Patch 154 — performance state quarantine + clean rebuild

## Purpose
Contain contaminated broker-truth performance mutations and rebuild strategy performance from trusted strategy-owned closed trades only.

## Included
- quarantines broker/manual reconciled performance rows from persisted state
- backs up prior contaminated state to quarantine file
- rebuilds summary metrics from trusted closed trades
- disables opportunistic broker-truth mutation on diagnostics/dashboard reads
- supersedes Patch 153 dedupe intent by hard-stopping mutation at the source

## Not changed
- entry logic
- exit trigger logic
- scanner logic
- release gating
- reconcile blocking behavior
