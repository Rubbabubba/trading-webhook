# Trading Webhook Product Roadmap: 700 System Reset

Status: active roadmap
Created: 2026-09-01
Operating mantra: Cleanup, simplify, and get back to the $2k sweet spot.

## Hard Stop Decision

The system should not keep trying to force the $100-$200 daily goal by increasing trade count, risk, or patch-layer complexity. The current live evidence says the daily-breakout implementation is not yet a proven standalone income engine.

Until the engine is revalidated, the system should run in one of these safer modes:

- New entries paused while exits and protection remain active.
- Or new entries allowed only at the performance engine's recommended reduced-risk profile.

The next release sequence must favor larger, tested cleanup releases over narrow hotfix chains.

## Patch Numbering Rule

Roadmap patches use whole numbers starting at Patch 700.

If an error or urgent tangent appears while executing a roadmap patch, tangent patches use the current patch number plus a letter:

- `700A`, `700B`, `700C`: emergency or tangent work while Patch 700 is active.
- After the tangent is resolved, resume with the next whole roadmap patch.
- Example: Patch 700, Patch 700A, Patch 700B, then Patch 701.

Each roadmap patch should include:

- A clear objective.
- Files/modules touched.
- Smoke tests run locally before deploy.
- Endpoints to hit after deploy.
- A rollback or safety note.

## Phase 1: Stop Damage And Establish Broker Truth

### Patch 700: Live Risk Hard Stop + Validation Mode Contract

Status: deployed and verified.

Goal: prevent the bot from manufacturing trades while the edge is under review.

Scope:

- Add one canonical live mode contract: `normal`, `reduced_risk`, or `validation_pause_entries`.
- Keep exit protection active in all modes.
- Make entry submit paths check one shared mode helper instead of scattered env/gate checks.
- Add operator-facing truth explaining whether new entries are paused, reduced, or normal.
- Default unset deployments to `validation_pause_entries` so the hard stop is explicit until validation is complete.

Expected outcome:

- No accidental new entries during validation.
- Exits, stop protection, broker reconciliation, and stale-plan cleanup continue.

Smoke tests:

- Compile all Python files.
- Verify paused mode blocks entries without blocking exits.
- Verify reduced-risk mode changes sizing only, not selection truth.
- Verify normal mode preserves current behavior.

Post-deploy endpoints:

- `/diagnostics/swing_runtime_config`
- `/diagnostics/scanner_light`
- `/diagnostics/current_scan_suppression_truth?limit=10`
- `/diagnostics/swing_submit_path_trace?limit=10`
- `/diagnostics/live_positions_light`
- `/diagnostics/active_exit_protection_truth?detail=heavy&limit=20`

### Patch 701: Broker-Fills-Only Trade Ledger

Status: deployed; endpoint shape was correct but broker-history refresh was too slow inline.

Goal: make broker fills the source of truth for performance review.

Scope:

- Create or complete `swing_performance_reports.py` ownership for broker-only filled trade ledger.
- Build a 200-trade audit report from broker fills only.
- Remove reliance on duplicate worker shadow rows for P/L.
- Report realized P/L, unrealized P/L, win rate, average winner, average loser, expectancy, R-multiple, strategy, regime, symbol, and holding period.
- Add `/diagnostics/broker_fills_only_trade_ledger` as the canonical 200-trade audit starting point.

Expected outcome:

- One trusted performance table.
- No more arguing with strategy-state ghosts.

Smoke tests:

- Broker rows dedupe by order/fill identity.
- Worker-generated duplicate rows do not change P/L.
- Missing strategy attribution is labeled unknown, not guessed.

### Patch 701A: Broker Fill Ledger Snapshot Cache + Bounded Async Refresh

Status: deployed; default cache-first endpoint was fast, but refresh still timed out without cache.

Goal: keep Patch 701 broker-only accounting but make the endpoint safe when broker order history is slow.

Scope:

- Make `/diagnostics/broker_fills_only_trade_ledger` cache-first by default.
- Require `refresh=true` or heavy detail for live broker order-history rebuild.
- Bound refresh with a short timeout and return stale cache or explicit no-cache truth instead of hanging.
- Persist the last successful broker-only ledger snapshot.

Expected outcome:

- The default broker-only ledger endpoint returns quickly.
- Heavy refresh cannot strand the operator request for minutes.
- Worker shadows remain excluded from accounting.

### Patch 701B: Broker Fill History Windowing + Incremental Ledger Refresh

Status: deployed; refresh remained bounded but did not populate cache before timeout.

Goal: make broker-only ledger refresh produce useful partial data even when full Alpaca order history is slow.

Scope:

- Replace one large broker order-history pull with smaller symbol-windowed requests.
- Add per-request timeout, max request count, and refresh deadline truth.
- Persist complete or partial broker-fill ledger snapshots.
- Keep default ledger endpoint cache-first and read-only.

Expected outcome:

- `refresh=true` returns bounded partial or complete broker-only ledger truth.
- The default ledger endpoint stays fast.
- The 200-trade audit can progress incrementally instead of blocking on one slow broker request.

Post-deploy endpoints:

- `/diagnostics/broker_preferred_daily_pnl_dedup`
- `/diagnostics/broker_only_daily_loss_truth`
- `/diagnostics/swing_performance_attribution`
- `/diagnostics/broker_reconciled_strategy_attribution`
- `/diagnostics/live_positions_light`

### Patch 701C: Broker Fill Ledger Background Refresh + Slow Broker History Isolation

Status: deployed; endpoint returned fast, but in-memory background state was not reliable across Render requests/processes.

Goal: finish the 701 tangent by making broker-fill history refresh non-blocking.

Scope:

- Start broker-fill ledger refresh in a daemon background thread when `refresh=true`.
- Return cached or pending truth immediately instead of waiting inline on slow broker history.
- Add `/diagnostics/broker_fills_only_trade_ledger_refresh_status` for running/completed/failed/lost refresh state.
- Keep the default ledger endpoint cache-first and broker-call-free.
- Let `/diagnostics/broker_only_daily_loss_truth` use the cached broker-fill ledger by default, with `?refresh=true` or `?detail=heavy` reserved for legacy heavy broker-order recompute.

Expected outcome:

- Broker-fill audit requests do not hang.
- Broker order-history slowness is isolated behind background status truth.
- Cached broker-only daily loss truth becomes fast enough for operator checks.

Post-deploy endpoints:

- `/diagnostics/broker_fills_only_trade_ledger?limit=200`
- `/diagnostics/broker_fills_only_trade_ledger?refresh=true&limit=200`
- `/diagnostics/broker_fills_only_trade_ledger_refresh_status`
- `/diagnostics/broker_fills_only_trade_ledger?limit=200`
- `/diagnostics/broker_only_daily_loss_truth`

### Patch 701D: Durable Broker Fill Ledger Refresh Pump + Disk Cursor State

Status: deployed; durable cache worked, but status endpoint still did pump work and could be slow.

Goal: replace the fragile in-memory refresh worker with a durable broker-fill refresh pump.

Scope:

- Persist refresh cursor/status/request progress to `/var/data`.
- Advance broker order-history refresh in small bounded chunks per request.
- Let `refresh=true` and the refresh-status endpoint continue the pump without hanging.
- Build and persist the broker-fills-only ledger when all chunks are processed, order limit is reached, or the configured request cap is reached.
- Keep default broker-fill ledger and daily-loss endpoints cache-first and fast.

Expected outcome:

- Broker-fill refresh survives Render process churn.
- Operator/status calls can finish the ledger through repeated bounded checks.
- Slow Alpaca order history no longer blocks diagnostics or worker paths.

Post-deploy endpoints:

- `/diagnostics/broker_fills_only_trade_ledger?limit=200`
- `/diagnostics/broker_fills_only_trade_ledger?refresh=true&limit=200`
- `/diagnostics/broker_fills_only_trade_ledger_refresh_status`
- `/diagnostics/broker_fills_only_trade_ledger_refresh_status`
- `/diagnostics/broker_fills_only_trade_ledger?limit=200`
- `/diagnostics/broker_only_daily_loss_truth`

### Patch 701E: Read-Only Refresh Status + Explicit Pump Endpoint + Legacy Refresh State Sanitizer

Status: deployed and verified.

Goal: keep the durable broker-fill ledger, but make the operational contract clean and fast.

Scope:

- Make `/diagnostics/broker_fills_only_trade_ledger_refresh_status` observe-only with no broker calls.
- Add `/diagnostics/broker_fills_only_trade_ledger_refresh_pump` as the explicit state-mutating refresh step.
- Keep `/diagnostics/broker_fills_only_trade_ledger?refresh=true` as a bounded pump convenience.
- Sanitize old `background_refresh` cache/source markers so stale 701C state cannot confuse current truth.
- Preserve cache-first broker-only daily loss truth.

Expected outcome:

- Status endpoint is always fast.
- Refresh work is explicit and bounded.
- Broker-fill cache/state truth is durable and no longer mixed with retired in-memory refresh metadata.

Post-deploy endpoints:

- `/diagnostics/broker_fills_only_trade_ledger_refresh_status`
- `/diagnostics/broker_fills_only_trade_ledger_refresh_pump?limit=200`
- `/diagnostics/broker_fills_only_trade_ledger_refresh_status`
- `/diagnostics/broker_fills_only_trade_ledger?limit=200`
- `/diagnostics/broker_only_daily_loss_truth`

## Phase 2: Prove Or Reject Each Engine Separately

### Patch 702: Strategy Isolation Switch

Status: local implementation complete.

Goal: test daily breakout, intraday momentum, and intraday mean reversion independently.

Scope:

- Add one canonical strategy isolation contract in `swing_selection_contract.py`.
- Ensure disabled strategies still report why they are disabled.
- Avoid mixed-strategy attribution.
- Make each selected candidate carry strategy, sleeve, regime, and profile identity.
- Add `/diagnostics/strategy_isolation_contract` as the read-only operator truth surface.
- Default to `SWING_STRATEGY_ISOLATION_MODE=all` so current production behavior is preserved unless validation explicitly isolates one engine.

Expected outcome:

- The system can run one engine at a time without hidden cross-contamination.
- Patch 700 validation pause remains the live-entry safety layer; Patch 702 only controls strategy eligibility/attribution.

Smoke tests:

- Daily breakout only.
- Intraday momentum only.
- Intraday mean reversion only.
- All disabled except exits.

Post-deploy endpoints:

- `/diagnostics/strategy_isolation_contract?limit=25`
- `/diagnostics/swing_runtime_config`
- `/diagnostics/current_scan_suppression_truth?limit=10`
- `/diagnostics/swing_submit_path_trace?limit=10`
- `/diagnostics/selected_submission_truth_light`
- `/diagnostics/scanner_light`

### Patch 703: Payoff Imbalance Repair Report

Status: local implementation complete.

Goal: identify why losers are overpowering winners.

Scope:

- Add `/diagnostics/payoff_imbalance_repair_report` as a cache-first broker-fills-only diagnostic.
- Report average planned/inferred risk, realized loss, realized win, stop distance, target distance, and actual exit reason.
- Separate losses caused by entry quality, sizing, stop placement, late exit, failed exit submit, and market regime.
- Promote the report into an operator brief.
- Keep this report read-only: no entry gate, submit path, or exit behavior changes.

Expected outcome:

- A specific list of what must change before risk is increased.
- Risk/trade count stays paused or reduced until broker-fill expectancy and payoff shape improve.

Smoke tests:

- Same symbol with multiple fills dedupes correctly.
- Partial exits attribute correctly.
- Exit reason fallback never invents a reason.

Post-deploy endpoints:

- `/diagnostics/payoff_imbalance_repair_report?limit=25&trade_limit=200`
- `/diagnostics/broker_fills_only_trade_ledger?limit=200`
- `/diagnostics/broker_fills_only_trade_ledger_refresh_status`
- `/diagnostics/broker_reconciled_strategy_attribution`
- `/diagnostics/broker_exit_reason_attribution`
- `/diagnostics/active_exit_protection_truth?detail=heavy&limit=20`
- `/diagnostics/worker_exit_status`

### Patch 704: Out-Of-Sample Replay Promotion Gate

Goal: convert replay outputs into a pass/fail promotion report.

Scope:

- Replay current config across multiple windows.
- Mark each strategy/profile as pass, watch, or fail.
- Require positive out-of-sample expectancy before normal live allocation.
- Store scenario results under `tools`/diagnostics instead of adding route clutter to `app.py`.

Expected outcome:

- Live capital is allocated only to variants with evidence.

Smoke tests:

- Two-week replay.
- One-month replay.
- Prior profitable period replay.
- Down-market day replay.
- Mixed-regime replay.

Post-run local artifacts:

- `C:/Users/matth/TradingDiagnostics/swing_minute_replay/latest_summary.json`
- `C:/Users/matth/TradingDiagnostics/swing_minute_replay/latest_scenario_matrix.csv`
- `C:/Users/matth/TradingDiagnostics/swing_minute_replay/latest_best_scenario_trades.csv`
- `C:/Users/matth/TradingDiagnostics/swing_minute_replay/latest_scenario_symbol_attribution_matrix.csv`

## Phase 3: Rebuild Runtime Around Clean Ownership

### Patch 705: Scanner Ownership Extraction

Goal: move scan orchestration/state/candidate publish ownership out of `app.py`.

Scope:

- `scanner.py`: worker loop and dispatch only.
- `swing_scan_state.py`: canonical scan state, background state, candidate cache.
- `app.py`: route adapter only.

Expected outcome:

- Scanner behavior becomes testable without touching the whole app.

Smoke tests:

- Background scan accept.
- Terminal publish.
- Partial publish.
- Stale scan replacement.
- After-hours non-replacement.

Post-deploy endpoints:

- `/diagnostics/scanner_light`
- `/diagnostics/current_scan_suppression_truth?limit=10`
- `/diagnostics/swing_submit_path_trace?limit=10`

### Patch 706: Candidate Evaluation Ownership

Goal: make `swing_candidate_eval.py` own candidate evaluation, not just shape/status helpers.

Scope:

- Move candidate scoring, sleeve classification, no-trade reasons, and ranking helpers.
- Keep broker submit out of candidate evaluation.
- Keep scanner state writes out of candidate evaluation.

Expected outcome:

- Candidate truth is deterministic, smaller, and easier to replay.

Smoke tests:

- Candidate rows match pre-extraction output.
- No broker calls during pure evaluation.
- Symbol universe coverage remains complete.

Post-deploy endpoints:

- `/diagnostics/current_scan_suppression_truth?limit=10`
- `/diagnostics/candidate_coverage_opportunity_audit`
- `/diagnostics/scanner_light`

### Patch 707: Submit Ownership Extraction

Goal: move direct submit, retry, buying-power truth, and limit-order finalization into submit modules.

Scope:

- `swing_broker_submit.py`: submit decision and retry ownership.
- `swing_broker_transport.py`: broker API transport only.
- `app.py`: route adapter and dependency wiring only.

Expected outcome:

- Submit path becomes one clean pipeline with one retry contract.

Smoke tests:

- Direct submit success.
- Rate-limit queue.
- Buying-power block.
- Limit replacement/fill finalization.
- Dry-run parity.

Post-deploy endpoints:

- `/diagnostics/swing_submit_path_trace?limit=10`
- `/diagnostics/selected_submission_truth_light`
- `/diagnostics/live_positions_light`

### Patch 708: Exit Protection Ownership Extraction

Goal: move protective exits, stale plan recovery, giveback exits, partial profits, broker-qty clamps, and worker fast-close behavior into `swing_exit_protection.py`.

Scope:

- One exit evaluation function.
- One exit execution function.
- One worker no-op/fast-drain contract.
- No heavy diagnostics inside the worker route.

Expected outcome:

- `/worker/exit` becomes a thin route that cannot hang on diagnostic/report work.

Smoke tests:

- No due exits returns fast.
- Stop due submits fast.
- Pending close prevents duplicate exit.
- Missing protection recovers.
- Broker qty clamp prevents over-close.

Post-deploy endpoints:

- `/worker/exit`
- `/diagnostics/worker_exit_status`
- `/diagnostics/active_exit_protection_truth?detail=heavy&limit=20`
- `/diagnostics/live_positions_light`

### Patch 709: Performance Reporting Ownership Extraction

Goal: complete `swing_performance_reports.py` ownership.

Scope:

- Broker-reconciled P/L.
- Strategy attribution.
- R-multiple.
- Daily goal truth.
- Profit opportunity map.

Expected outcome:

- Performance routes stop pulling heavy mixed logic from `app.py`.

Smoke tests:

- All reports return from cached broker ledger.
- Heavy refresh is explicit.
- No endpoint requires worker state mutation.

Post-deploy endpoints:

- `/diagnostics/swing_performance_attribution`
- `/diagnostics/broker_reconciled_strategy_attribution`
- `/diagnostics/broker_daily_goal_truth`
- `/diagnostics/daily_goal_opportunity_map`

## Phase 4: Remove Legacy Patch Paths

### Patch 710: Legacy Route And Compatibility Tombstone Removal

Goal: delete retired compatibility code after ownership modules are stable.

Scope:

- Remove old patch-specific helpers no longer called.
- Remove route aliases that point to obsolete diagnostics.
- Replace old status names with canonical names.
- Keep a migration note in docs.

Expected outcome:

- Smaller `app.py`.
- Fewer false warnings.
- Fewer duplicate truth sources.

Smoke tests:

- Route inventory diff.
- Import compile.
- Core diagnostic bundle.
- Worker POST checks.

Post-deploy endpoints:

- `/diagnostics/swing_core_status`
- `/diagnostics/scanner_light`
- `/diagnostics/worker_exit_status`
- `/diagnostics/swing_submit_path_trace?limit=10`
- `/diagnostics/live_positions_light`

## Future Product Direction

The system should become a product with these stable layers:

- Broker layer: credentials, orders, positions, fills, account, market data.
- Market clock layer: market hours, sessions, holiday/calendar truth.
- Scanner layer: orchestration, timing, symbol universe coverage, scan state.
- Candidate layer: pure strategy evaluation and ranking.
- Submit layer: sizing, buying-power truth, retries, order submission.
- Exit layer: stops, targets, partial profits, stale protection recovery.
- Performance layer: broker-only truth, attribution, replay promotion.
- Dashboard layer: fast cached operator view, heavy diagnostics only by opt-in.

## Previously Locked Work That Still Applies

The hard stop supersedes any work that tries to force more trades, increase risk, or add another entry gate to chase the daily goal. The following previously locked work still applies because it supports cleanup, validation, and product readiness.

### System Cleanup Path

- Keep moving ownership out of `app.py`.
- Keep `broker_client.py` and `swing_broker_transport.py` focused on broker transport only.
- Keep `market_clock.py` as the compatibility wrapper for market-hours truth.
- Keep `swing_runtime_config.py` as the single place for swing runtime config shape.
- Keep `swing_light_diagnostics.py` as the fast endpoint helper layer.
- Keep `swing_scan_state.py` as the canonical scan state/cache owner.
- Keep `swing_candidate_eval.py` moving toward pure candidate evaluation ownership.
- Keep `swing_broker_submit.py` moving toward direct submit, retry, and buying-power ownership.
- Keep `swing_exit_protection.py` moving toward all exit/protection/worker-exit ownership.
- Keep `swing_performance_reports.py` moving toward broker-reconciled performance ownership.
- Keep dashboard cleanup, but only as fast cached operator views plus explicit heavy opt-in routes.
- Archive or delete unused files only after route/import references prove they are unused.

### Testing And Replay Path

- Keep the two-week and minute-bar replay harnesses.
- Expand replay coverage across prior profitable windows, down-market windows, and mixed-regime windows.
- Keep scenario matrices, but convert them into a pass/fail promotion gate instead of standalone research clutter.
- Keep symbol/sleeve attribution outputs for deciding which variants deserve capital.
- Keep local replay tooling outside `app.py`.
- Use broker-fill audit results to calibrate replay assumptions.

### Runtime Reliability Path

- Keep scanner fast-response and terminal-publish guarantees, but move them into scanner ownership modules.
- Keep worker exit fast-close/no-op guarantees, but move them into exit ownership modules.
- Keep single-flight protection, atomic state writes, and stale-state recovery.
- Keep light endpoints as the default operator/status surface.
- Keep heavy endpoints available only by explicit opt-in and never in worker-critical paths.

### Work No Longer In Scope Unless Re-Proven

- Increasing live trade count to manufacture the $100-$200 daily goal.
- Increasing risk before broker-fill expectancy is positive.
- Adding more symbol-specific patches.
- Adding new entry gates without deleting or consolidating old ones.
- Treating daily breakout as the standalone income engine before out-of-sample proof.
- Using strategy-state-only P/L as a decision source.

## Success Criteria

The system is not considered ready for normal live sizing until:

- Worker POST routes return quickly or intentionally hand off.
- Scanner completes without blocking the caller.
- All open broker positions have active protection.
- Broker-fill performance is the only P/L source.
- At least one strategy/profile shows positive out-of-sample expectancy.
- Current config passes the replay promotion gate.
- `app.py` is mostly route wiring, not business logic.

## Immediate Recommendation

Proceed with Patch 700 next. Do not attempt to restore the daily profit quota by increasing risk or trade count. Stabilize live behavior first, audit broker truth second, then promote only proven strategy variants.
