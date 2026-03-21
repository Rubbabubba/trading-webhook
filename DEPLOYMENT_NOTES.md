# Patch 076 – Bar Truth Sync and Lifecycle Hygiene

This patch is built directly on patch 075 and stays surgical.

## What changed
- Updated build metadata so diagnostics report `patch-076-bar-truth-sync-and-lifecycle-hygiene`.
- Synced bar-path diagnostics to evaluate the **latest available regular session** rather than only "today". This removes false negatives on weekends / after hours when REST has valid 1-minute bars from the most recent market session.
- Extended `/diagnostics/bars_5m` and readiness bar probes with `latest_session_date`, `bars_1m_latest_session`, and `bars_5m_latest_session` for both SDK/fallback and REST paths.
- Added lifecycle hygiene so stale historical recovery/fill artifacts no longer show up as active pipeline guardrail violations unless the symbol is still active or recent.

## Why
Patch 075 showed the guardrails were behaving, but readiness still reported `sample_symbol_5m_bars.ok=false` on a closed market day even though the REST probe showed valid prior-session bars. It also kept surfacing old SPY/AMZN/NVDA lifecycle noise as if it were current-state risk. Patch 076 fixes both without changing trading policy.

## Deploy / verify
1. Deploy the zip.
2. Confirm `/diagnostics/build` shows `patch-076-bar-truth-sync-and-lifecycle-hygiene`.
3. Confirm `/diagnostics/readiness` now reports `sample_symbol_5m_bars.ok=true` when prior-session bars are available.
4. Confirm `/diagnostics/pipeline_guardrails` no longer flags stale historical lifecycle warnings unless the symbol is still active/recent.
