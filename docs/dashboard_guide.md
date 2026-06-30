# Trading Dashboard Guide

> Notion-ready operator guide for the trading-webhook dashboard.
>
> Copy this Markdown page into Notion as a single page. Notion will preserve headings, tables, bullets, and code blocks.

---

## 1. Purpose of the Dashboard

The dashboard is the primary operator console for the trading system. It is designed to answer five questions quickly:

1. **Is the system healthy enough to trust?**
2. **Are live orders permitted, or is anything blocking trading?**
3. **What positions are currently open and how much risk is on the book?**
4. **Is the swing strategy performing well enough to maintain or increase risk?**
5. **Is intraday / hybrid trading ready, or should it remain in shadow/paper proofing?**

The dashboard is intentionally **snapshot-only**. It reads persisted state, snapshots, scanner summaries, and performance data. It should not call the broker, submit orders, reconcile, scan, or run exit logic during page render.

---

## 2. Fast Path vs Full Diagnostic View

| View | URL | Purpose | When to Use |
|---|---|---|---|
| Summary / fast path | `/dashboard` | Fast operator snapshot with core health, positions, risk, readiness, tuning, and proof panels. | Default view for daily monitoring. |
| Full diagnostic view | `/dashboard?detail=full` | Adds heavier rank, symbol, holding-period, exit attribution, rejection totals, correlation, and follow-through tables. | Use when investigating scanner behavior, performance attribution, or rejection patterns. |

### Operator rule

Use `/dashboard` for normal refreshes. Use `/dashboard?detail=full` only when you need deeper diagnostics.

---

## 3. High-Level Operator Checklist

Use this checklist top to bottom.

1. **Operator Alerts**: confirm no blockers, worker healthy, snapshot fresh.
2. **Release Stage**: confirm live orders are intentionally enabled or disabled.
3. **Market Hours**: confirm market state matches expectations.
4. **Regime**: confirm the market regime is favorable if expecting new entries.
5. **Workers / Reconcile**: confirm scanner and reconcile are healthy.
6. **Active Positions**: confirm open symbols, stops, targets, and unrealized P&L look sensible.
7. **Daily Halt Truth**: confirm no daily halt is active.
8. **Exposure & Capacity**: confirm open positions are within configured limits.
9. **Swing Profit / Post-Tuning Validation**: do not increase risk unless validation says ready.
10. **Intraday / Hybrid Panels**: keep intraday shadow/paper unless launch readiness is explicitly true.

---

## 4. Decision Rules

### Do not enter or scale if any of these are true

- `trading_blocked = TRUE`
- `daily_halt_active = TRUE`
- `snapshot_fresh = FALSE` and you need current account truth
- `scanner_dashboard_ready = FALSE`
- `reconcile` is unhealthy or `issue_total > 0`
- `risk_scale_unlock_candidate = FALSE` when considering risk increase
- `intraday_launch_ready = FALSE` when considering intraday launch

### Do not raise swing risk if any of these are true

- `risk_ready_for_40 = FALSE`
- `risk_scale_unlock_candidate = FALSE`
- `risk_blockers` includes `worst_r_too_deep`
- `post_tuning_closed_trades < 10`
- `post_tuning_deep_stall_exits > 0`
- open book is already near max capacity and unrealized P&L is deteriorating

### Do not launch intraday live if any of these are true

- `launch_ready_now = FALSE`
- `market_tradable_now = FALSE`
- `regime_favorable = FALSE`
- `projection_capacity_ready = FALSE`
- `active_strategy_mode != intraday` and you are not deliberately switching modes
- intraday proof/paper metrics are not ready

---

## 5. Dashboard Sections and Metrics

The sections below follow the current dashboard order.

---

# Operator Console Header

## Section purpose

Shows the dashboard title, patch label, read-only status, fast-path status, generation time, and server render time.

| Metric / Label | Meaning | Good / Expected | Action if Concerning |
|---|---|---|---|
| Patch label | Current deployed patch version shown as a badge. | Matches expected deployed patch. | If stale, confirm Render deployed the latest commit. |
| Read-only | Indicates dashboard render does not perform trading actions. | `Read-only`. | If this changes, inspect before using dashboard. |
| Fast path | Indicates summary dashboard mode. | Present on `/dashboard`. | Use `/dashboard?detail=full` for deeper diagnostics. |
| Generated timestamp | Time the dashboard HTML was generated. | Current. | If old, refresh; if still old, inspect service health. |
| Server render ms | Time spent rendering server-side. | Low hundreds of ms or less. | If high, inspect heavy panels, filesystem state, or cold start. |

---

# Operator Alerts

## Section purpose

A top-level red/yellow/green summary of blockers, worker state, and snapshot freshness.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| Blockers present / No current blockers | Human-readable summary of dashboard-visible blockers. | `No current blockers`. | Inspect Current Blockers and Guarded Live Path. |
| Worker status | Normalized scanner status, last event, and in-flight state. | Scanner healthy/ok, heartbeat or scan_ok, `in-flight: False` unless currently scanning. | Check scanner service logs and `/diagnostics/scanner`. |
| Snapshot freshness | Whether `positions_snapshot.json` is recent enough for read-only dashboard truth. | Fresh, low age seconds. | Check background exit/reconcile worker if stale. |

---

# Exception Center

## Section purpose

Summarizes structural system integrity, stale snapshot state, and render timing.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `health_grade` | Overall dashboard/reconcile health grade. | `healthy`. | Inspect reconcile and structural exceptions. |
| `issue_total` | Count of detected reconcile/structural issues. | `0`. | Investigate active plan vs broker mismatch. |
| `trading_blocked` | Whether release/reconcile health blocks trading. | `FALSE`. | Do not add risk until cause is resolved. |
| `snapshot_reason` | Why the current snapshot was produced/restored. | Recent worker/reconcile cycle. | If startup restore or stale, verify worker refresh. |
| `snapshot_age_sec` | Age of the positions snapshot. | Low and fresh. | If high, inspect worker logs and snapshot write path. |
| `snapshot_fresh` | Boolean freshness classification. | `TRUE`. | Treat position/P&L data as potentially stale if false. |
| `server_render_ms` | Server-side dashboard render duration. | Low. | If high, profile dashboard or file I/O. |

---

# Release Stage

## Section purpose

Shows whether the system is in live/paper/dry-run posture.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `stage` | Release stage / environment stage. | `live` only when intentionally live. | Confirm env before market open. |
| `live_orders_permitted` | Whether live order path is allowed. | `TRUE` for live trading, `FALSE` for paper/dry run. | If unexpected, inspect `LIVE_TRADING_ENABLED`, release gate, and kill switch. |
| `dry_run` | Whether scanner/order path is dry-run. | `FALSE` for live swing entries. | If `TRUE`, entries may be simulated only. |
| `kill_switch` | Global kill switch. | `FALSE`. | If `TRUE`, system should not submit new entries. |

---

# Market Hours

## Section purpose

Shows current New York time, market tradability, snapshot time, and last scan time.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `now_ny` | Current New York timestamp. | Current timestamp. | Confirm clock/timezone if odd. |
| `snapshot_ts` | Timestamp of persisted position snapshot. | Recent. | If old, inspect worker refresh. |
| `last_scan_ts` | Most recent scanner summary timestamp. | Recent during market hours. | Check scanner logs if stale. |
| Market headline | Open/closed classification. | Matches actual market. | If wrong, verify market calendar logic. |

---

# Regime

## Section purpose

Displays broad-market regime state used by swing and intraday readiness logic.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `index_symbol` | Market index used for regime, usually `SPY`. | Expected index. | Verify env if unexpected. |
| `score` | Numeric regime score. | Higher is more favorable. | If weak, expect fewer entries or launch blockers. |
| `breadth` | Market breadth estimate. | Higher supports favorable regime. | Weak breadth may reduce signals. |
| `data_complete` | Whether regime data is complete enough. | `TRUE`. | If false, check data provider/scanner. |
| Regime headline | Boolean favorable status. | `TRUE` for normal long swing/intraday launch. | If false, do not force launch. |

---

# Workers

## Section purpose

Shows scanner worker health and whether scans are in-flight.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `raw_scanner_status` | Raw scanner-reported status. | `ok`, `success`, `completed`, or equivalent healthy status. | Check scanner logs if error/timeout. |
| `normalized_health` | Dashboard-normalized scanner health. | `healthy`. | If unhealthy, inspect scanner heartbeat. |
| `in_flight_run` | Whether scanner is currently running. | Usually `FALSE`; can be `TRUE` during active scan. | If stuck true, check scanner timeout/logs. |
| `last_event` | Last scanner event. | `heartbeat`, `scan_ok`, or equivalent. | Error events need investigation. |
| `last_closed_utc` | Timestamp scanner last closed/completed. | Recent during market hours. | If stale, inspect scanner scheduling. |

---

# Reconcile

## Section purpose

Compares broker-backed positions, active plans, open orders, and issue count.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `broker_positions_count` | Count of positions in broker-backed snapshot. | Matches actual account. | If wrong, refresh/reconcile. |
| `active_plan_count` | Count of active local trade plans. | Usually matches broker positions. | Mismatch may require manual reconciliation. |
| `open_order_count` | Count of open broker orders. | Often `0` outside active submissions. | Unexpected open orders need inspection. |
| `issue_total` | Reconcile issue count. | `0`. | Do not scale risk if nonzero. |

---

# Active Position Count

## Section purpose

Quick count and symbol list for currently open positions.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| Metric headline number | Count of merged active positions. | At or below configured max. | If too high, stop new entries and reconcile. |
| `symbols` | Open position symbols. | Matches broker account. | If mismatch, inspect snapshot/reconcile. |
| `snapshot_source` | Data source, typically `positions_snapshot.json`. | Expected snapshot file. | If unexpected, inspect startup restore. |
| `snapshot_age_sec` | Position snapshot age. | Fresh. | If stale, inspect worker refresh. |

---

# Active Positions

## Section purpose

Primary book view. Shows each open position and plan risk fields.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `Symbol` | Ticker symbol. | Expected open holdings. | Investigate unknown symbol. |
| `Qty` | Current quantity. | Matches broker. | Reconcile if mismatch. |
| `Entry` | Plan/account entry price. | Sensible vs fill. | Investigate if missing/wrong. |
| `Last` | Last snapshot price. | Recent snapshot value. | If stale, check snapshot freshness. |
| `U P&L $` | Unrealized profit/loss. | Within risk tolerance. | Watch drawdown clusters. |
| `Stop` | Current stop level from active plan. | Present for all live positions. | Missing stop requires immediate review. |
| `Target` | Current target level from active plan. | Present if strategy uses target. | Missing target may be acceptable only by design. |
| `Signal` | Strategy/signal that opened or recovered the plan. | Expected strategy label. | Investigate unknown signal. |
| `Status` | Plan/order status. | `filled` for open positions. | Non-filled statuses need review. |
| `Days` | Days held. | Within strategy expectations. | Review overstayed positions. |

---

# P&L Summary

## Section purpose

Summary of current open-book unrealized P&L.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `positions_with_snapshot` | Number of open positions with snapshot data. | Matches active positions. | Missing snapshots reduce confidence. |
| `total_unrealized_pl_snapshot` | Total unrealized P&L from snapshot. | Within expected drawdown. | If large negative, inspect stops and daily limits. |
| `winners` | Count of positions currently green. | Informational. | Low winners plus drawdown may signal weak tape. |
| `losers` | Count of positions currently red. | Informational. | Clustered losers require risk review. |

---

# Risk Integrity

## Section purpose

Detects structural mismatches between broker positions, plans, orders, and risk records.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `health_grade` | Overall risk integrity state. | `healthy`. | Do not scale if unhealthy. |
| `issue_total` | Count of risk/reconcile issues. | `0`. | Inspect structural exceptions. |
| `active_plan_count` | Active plan count. | Matches broker positions. | Reconcile mismatches. |
| `broker_positions_count` | Broker-backed position count. | Matches active plans. | Reconcile if mismatched. |
| `trading_blocked` | Whether integrity blocks trading. | `FALSE`. | Resolve before enabling entries. |
| `Structural exceptions` | Missing/stale/orphan/partial plan lists. | all `none`. | Manually reconcile if any symbols listed. |

---

# Exposure & Capacity

## Section purpose

Shows how full the book is, configured capacity, and advisory capacity guidance.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `current_open_positions` | Number of current positions. | At/below max. | If near max, expect no new entries. |
| `effective_max_open_positions` | Effective active max position cap. | Matches intended env. | Check `SWING_MAX_OPEN_POSITIONS` / `MAX_OPEN_POSITIONS`. |
| `base_max_open_positions` | Base configured max positions. | Intended swing cap. | Adjust only after risk validation. |
| `capacity_next_step` | Advisory next cap step. | Conservative staged guidance. | Do not blindly follow without risk validation. |
| `safe_to_12` | Advisory flag for 12 positions. | `TRUE` if sample supports 12. | If false, do not increase to 12. |
| `safe_to_15` | Advisory flag for 15 positions. | Usually false until stronger sample. | Avoid 15 until proven. |
| `capacity_note` | Human-readable capacity recommendation. | Clear guidance. | Follow conservative interpretation. |
| `portfolio_exposure_snapshot` | Snapshot exposure info. | Within caps. | Inspect if high. |
| `effective_portfolio_cap_pct` | Effective portfolio exposure cap. | Intended percentage. | If too high/low, review env. |
| `effective_symbol_cap_pct` | Per-symbol exposure cap. | Intended percentage. | If too high, concentration risk increases. |
| `portfolio_cap_mode` | Cap enforcement mode. | Expected strategy mode. | Check env if unexpected. |
| `Strategy symbols` | Current open symbols list. | Matches active positions. | Reconcile if inconsistent. |

---

# Daily Halt Truth

## Section purpose

Shows whether the system is halted for the day and why.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| Headline | Daily halt status. | `NO DAILY HALT`. | If halted, do not force new entries. |
| `daily_halt_active` | Boolean halt flag. | `FALSE`. | Investigate halt reason. |
| `daily_halt_reason` | Reason halt was triggered. | `none`. | Review losses, stops, or system state. |
| `account_daily_pnl_snapshot` | Last persisted Alpaca account equity delta from the worker/incident snapshot. | Matches broker daily change closely. | If missing or far from broker, open `/diagnostics/loss_control_incident`. |
| `breach_source` | Which P&L truth source proved the halt threshold. | `alpaca_account_daily_pnl` for account-level daily stop. | If estimate-based, reconcile broker orders before resetting. |
| `flatten_policy` | Daily-stop policy in force at the last incident. | `halt_only` for swing mode. | Do not use `flatten_all` unless intentional. |
| `bulk_flatten_allowed` | Whether explicit bulk liquidation permission was enabled. | `FALSE` for swing. | If true unexpectedly, disable before next session. |
| `can_flatten` | Whether all bulk-flatten guardrails were satisfied. | `FALSE` unless emergency liquidation intended. | If true unexpectedly, review daily-stop envs immediately. |
| `fully_flat` | Whether snapshot positions, active plans, and open orders were all empty after the incident. | `TRUE` after a verified liquidation/flat state. | If false, verify Alpaca positions/orders and reconcile. |
| `scanner_dashboard_ready` | Dashboard's scanner readiness state. | `TRUE`. | Check scanner if false. |
| `scanner_raw_healthy` | Raw scanner health flag. | `TRUE`. | Inspect scanner logs if false. |
| `scanner_status` | Scanner status string. | `ok` or success equivalent. | Errors need investigation. |
| `scanner_status_normalized` | Dashboard-normalized scanner health. | `healthy`. | If not healthy, inspect heartbeat. |
| `scanner_last_event` | Latest scanner event. | `heartbeat` or `scan_ok`. | Error event means investigate. |
| `scanner_in_flight` | Whether scanner is running. | `FALSE` unless actively scanning. | If stuck true, inspect scanner timeout. |
| `release_or_reconcile_blocked` | Whether release/reconcile blocks trading. | `FALSE`. | Do not trade if true. |
| `configured_daily_loss_limit` | Configured daily loss limit. | Intended env value. | Verify before live trading. |
| `configured_daily_stop_dollars` | Configured daily stop dollars. | Intended env value. | Verify before live trading. |

## Patch 217 loss-control guardrails

Daily stop should normally be treated as a **halt / no-new-risk** control for swing mode, not an automatic instruction to liquidate every swing position at the open. Use these controls when reviewing loss-control behavior:

| Control | Meaning | Recommended Swing Default | Operator Rule |
|---|---|---|---|
| `DAILY_STOP_ACTION` | Selects whether daily stop is `halt_only` or `flatten_all`. | `halt_only` or unset. | Use `flatten_all` only for explicitly approved emergency behavior. |
| `ALLOW_DAILY_STOP_BULK_FLATTEN` | Explicit second confirmation required before daily stop can close multiple positions. | `false`. | Keep false unless an operator intentionally wants account-level liquidation. |
| `DAILY_STOP_CONFIRMATION_SEC` | Minimum time a daily-stop breach must persist before any eligible bulk flatten. | `60`. | Prevents one opening print or stale account tick from liquidating the book. |
| `SWING_DAILY_STOP_OPEN_GRACE_MIN` | Market-open grace window during which swing daily-stop bulk flatten remains blocked. | `15`. | Lets the book absorb opening volatility and requires deliberate review. |

If Daily Halt Truth shows `daily_stop_hit`, confirm whether the worker returned `daily_stop_halt_only` or `daily_stop_bulk_flatten` in `/diagnostics/worker_exit_status`, then verify broker truth in `/diagnostics/reconcile`.

## Patch 219 session rollover note

Daily halt state is session-scoped. A halt from the prior NY trading day should reset on the next NY session and should not be re-armed from stale premarket or after-hours account P&L. If the dashboard still shows `daily_halt_active=TRUE` before the next open, check `/diagnostics/worker_exit_status` for `daily_stop_evaluation_allowed=false` and confirm the snapshot reason is outside market hours rather than a new same-session loss event.

---

# Today P&L Truth

## Section purpose

Shows today’s realized/unrealized accounting state.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `accounting_source` | Source used for today P&L. | `alpaca_orders` or `persisted_strategy_state`. | If no data, inspect performance state. |
| `account_daily_pnl` | Alpaca account equity minus prior close equity from the latest persisted truth snapshot. | Should match the broker daily change directionally. | Treat this as the loss-control source of truth when broker order P&L is incomplete. |
| `today_realized_pnl` | Realized P&L today. | Within daily limits. | Stop trading if limit breached. |
| `today_unrealized_pnl_snapshot` | Current unrealized P&L from snapshot. | Within tolerance. | Watch open-book drawdown. |
| `closed_trades_today` | Count of closed trades today. | Informational. | Validate if unexpected. |
| `broker_exit_fills_today` | Broker exit fills today. | Matches expected exits. | Reconcile if mismatch. |

---

# Loss Control Incident

## Section purpose

Summarizes the last daily-stop / daily-halt incident from persisted worker truth. This section is snapshot-only on the dashboard; use `/diagnostics/loss_control_incident` for live broker-account reconciliation.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `active` | Whether a daily halt is active. | `FALSE` before trading; `TRUE` after a loss-control halt. | If true, keep entries blocked until next session/reset procedure. |
| `recommended_action` | Primary operator action. | `monitor` when no incident; otherwise halt/reconcile guidance. | Follow this before touching trading envs. |
| `recommended_actions` | Full ordered action list. | Clear checklist. | Complete each item before resuming trading. |
| `snapshot_reason` | Snapshot reason that captured the incident. | `daily_stop_halt_only` for guarded swing stops. | If `daily_stop_bulk_flatten`, verify it was intentional. |
| `broker_positions_count` | Positions in the persisted broker-backed snapshot. | `0` when flat. | If nonzero unexpectedly, verify Alpaca positions. |
| `active_plan_count` | Active internal plans after stale-plan recovery. | `0` when flat. | If broker is flat but plans remain, run reconcile. |
| `open_order_count` | Open orders seen by incident diagnostics. | `0`. | Cancel/verify orders before any reset. |
| `flatten_decision_reasons` | Guardrail reasons explaining why bulk flatten was blocked or allowed. | `daily_stop_action_halt_only`, `bulk_flatten_not_explicitly_enabled` for swing. | Unexpected reasons indicate env drift. |

Diagnostic endpoint: `/diagnostics/loss_control_incident`.

---

# EOD Flatten Status

## Section purpose

Shows whether end-of-day flattening attempted, submitted, confirmed, or left residual positions.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `last_eod_attempt` | Last EOD flatten attempt timestamp. | Recent only when applicable. | If stale and intraday active, inspect worker. |
| `eod_flatten_time_ny` | Configured NY flatten time. | Blank for swing, configured for intraday. | Avoid accidental flatten of swing positions. |
| `fully_flat` | Whether account was flat after EOD cycle. | `TRUE` for intraday-only sessions. | If false, inspect residuals. |
| `attempted_count` | Number of close attempts. | Matches intended EOD action. | If zero unexpectedly, check config. |
| `submitted_count` | Submitted close orders. | Matches attempted closes. | If lower, inspect broker/order errors. |
| `confirmed_closed_count` | Confirmed closed symbols. | Should match submitted if successful. | Investigate residuals. |
| `residual_count` | Remaining positions. | `0` for intraday flatten. | Manually close or wait next regular session. |
| `residual_symbols` | Symbols still open. | `none` if flat. | Review each residual. |
| `pending_close_order_symbols` | Symbols with pending close orders. | `none` unless closing. | Avoid duplicate close orders. |
| `recommended_action` | Suggested next EOD action. | Clear next step. | Follow operationally. |

Diagnostic endpoint: `/diagnostics/eod_flatten_status`

---

# Performance Analytics

## Section purpose

High-level closed-trade performance summary.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `closed_trades` | Total closed trades in sample. | Higher sample = more confidence. | Avoid scaling on tiny sample. |
| `wins` | Winning closed trades. | Informational. | Use with win rate and avg R. |
| `losses` | Losing closed trades. | Informational. | Watch loss clusters. |
| `flat` | Break-even/flat exits. | Low or expected. | Many flat exits may indicate poor follow-through. |
| `win_rate` | Wins divided by closed trades. | Strong if above risk threshold. | Low win rate blocks risk scaling. |
| `gross_pnl` | Total closed-trade P&L. | Positive and growing. | Negative means strategy needs review. |
| `avg_r` | Average R multiple. | Higher is better; key risk-scaling input. | Low avg R blocks risk increases. |
| `sample_maturity` | Whether sample size is established. | `ESTABLISHED`. | Avoid big changes if immature. |

---

# Open Book Risk

## Section purpose

Summarizes current open-position risk and exposure.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `open_positions` | Number of open positions. | At/below max. | If high, expect fewer new entries. |
| `max_open_positions` | Effective max positions. | Intended cap. | Confirm env if unexpected. |
| `total_notional` | Total notional exposure. | Within account plan. | Review leverage/exposure if large. |
| `total_risk_to_stop` | Estimated risk to stops. | Within risk budget. | Reduce risk if too high. |
| `total_unrealized_pl` | Current unrealized P&L. | Within tolerance. | Watch drawdowns. |
| `portfolio_exposure_cap_pct` | Portfolio cap percentage. | Intended cap. | Adjust only deliberately. |
| `symbol_exposure_cap_pct` | Per-symbol cap percentage. | Intended cap. | Avoid concentration. |

---

# Intraday Launch Projection

## Section purpose

Shows whether switching to intraday would be ready and what env settings are recommended.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `launch_ready_now` | Whether intraday can launch now. | `TRUE` only when all launch gates pass. | Do not switch intraday if false. |
| `projection_capacity_ready` | Whether projected intraday slots meet launch minimum. | `TRUE`. | Raise intraday cap or free slots if false. |
| `projection_blockers` | Capacity/config blockers from projection. | `none`. | Resolve before intraday launch. |
| `launch_gate_actions` | Market/regime actions required. | `none` when ready. | Wait for market/regime if listed. |
| `required_actions` | Required setup actions. | Empty/none when ready. | Apply required env changes deliberately. |
| `optional_scaling_actions` | Recommended but not always blocking scaling actions. | Review before launch. | Apply if wanting more intraday capacity. |
| `active_strategy_mode` | Current strategy mode. | `swing` until deliberately switching. | Set `STRATEGY_MODE=intraday` only when ready. |
| `mode_switch_required` | Whether intraday launch requires mode switch. | Usually `TRUE` while in swing. | Do not switch accidentally. |
| `active_open_slots` | Current open slots under active mode. | Meets launch minimum. | Free slots or raise cap if low. |
| `launch_min_open_slots` | Minimum slots required to launch intraday. | Configured threshold. | Validate env. |
| `recommended_intraday_max_positions` | Suggested intraday max cap. | Use as launch checklist. | Set env before launch if needed. |
| `recommended_intraday_open_slots` | Suggested open slots after intraday cap. | Enough room for strategy. | Increase cap if too low. |
| `recommended_intraday_daily_stop` | Suggested intraday daily stop dollars. | Operational risk setting. | Confirm before live intraday. |
| `recommended_intraday_daily_loss` | Suggested intraday daily loss limit. | Operational loss cap. | Confirm before live intraday. |
| `projected_intraday_max_positions` | Max positions if intraday mode applied now. | Meets target. | Adjust env if too low. |
| `projected_intraday_open_slots` | Open slots if intraday mode applied now. | Meets launch minimum. | Free slots or raise cap if low. |
| `open_slots_gap_to_min` | Gap between projected slots and minimum. | `0`. | Resolve if positive. |
| `projected_daily_stop_dollars` | Daily stop after intraday env. | Intended value. | Adjust env. |
| `projected_daily_loss_limit` | Daily loss limit after intraday env. | Intended value. | Adjust env. |
| `projected_portfolio_cap_pct` | Intraday projected portfolio cap. | Intended value. | Confirm exposure risk. |
| `projected_symbol_cap_pct` | Intraday projected symbol cap. | Intended value. | Confirm concentration risk. |
| `june4_framework` | Margin framework note. | Informational. | N/A. |
| Launch env checklist | Copyable recommended env values. | Review before launch. | Apply deliberately, not blindly. |

Diagnostic endpoint: `/diagnostics/intraday_launch_readiness`

---

# Readiness Evidence

## Section purpose

Consolidated launch/readiness proof for system, market, scanner, and intraday.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `system_health_ok` | System health is acceptable. | `TRUE`. | Fix system issues first. |
| `market_tradable_now` | Market is tradable now. | `TRUE` for live entries. | Wait for market open. |
| `scanner_ready` | Scanner ready state. | `TRUE`. | Inspect scanner if false. |
| `component_ready` | Required components ready. | `TRUE`. | Inspect blockers. |
| `intraday_launch_ready` | Intraday launch readiness. | `TRUE` only for launch. | Keep intraday shadow if false. |
| `intraday_launch_blockers` | Count of launch blockers. | `0`. | Resolve listed blockers. |
| `launch_gate_blockers` | Specific launch gate blockers. | `none`. | Wait or fix config. |
| `same_session_proven` | Scanner/session evidence exists. | `TRUE`. | Let scanner run. |
| `trade_path_proven` | Trade path has historical evidence. | `TRUE`. | Continue proofing if false. |
| `entry_events` | Count of entry/dispatch events. | Informational. | Low count means little proof. |
| Assessment | Human-readable readiness state. | `READY` only when all pass. | Follow status. |

---

# Guarded Live Path

## Section purpose

Confirms release, live order permission, scanner readiness, and risk limits.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `release_stage` | Current release stage. | `live` only when intended. | Check env. |
| `live_orders_permitted` | Live orders allowed. | Matches operating intent. | Do not trade if false unexpectedly. |
| `scanner_ready` | Scanner readiness. | `TRUE`. | Check scanner if false. |
| `risk_limits_ok` | No daily halt/risk block. | `TRUE`. | Do not enter if false. |
| Current blockers | Current blocking reasons. | `none`. | Resolve before trading. |

---

# Swing Rollback / Entry Controls

## Section purpose

Shows active strategy mode, entry controls, dry-run state, and sleeve targets.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `strategy_mode` | Active mode: swing/intraday/etc. | `swing` for swing system. | Verify before market open. |
| `hybrid_mode` | Hybrid posture. | `shadow` unless deliberately paper/live. | Avoid accidental live hybrid. |
| `new_entries_enabled` | Global new-entry gate. | `TRUE` for live entries. | Set false for exits-only rollback. |
| `entry_control_status` | Human-readable entry status. | `enabled`. | Investigate if disabled. |
| `swing_live_enabled` | Swing live entries enabled. | `TRUE` if trading swing. | Check env if false. |
| `intraday_paper_enabled` | Intraday paper simulation enabled. | Usually false/controlled. | Confirm before paper pilot. |
| `intraday_live_enabled` | Intraday live entries enabled. | `FALSE` until launch. | If true unexpectedly, stop and inspect. |
| `scanner_allow_live` | Scanner allowed to submit live entries. | `TRUE` for live swing. | False means scanner will not submit. |
| `scanner_dry_run` | Scanner dry run flag. | `FALSE` for live swing. | True means simulated entries. |
| `effective_entry_dry_run` | Effective dry-run state after all gates. | `FALSE` for live entries. | If true, inspect gating. |
| `exits_still_permitted` | Exit path remains permitted. | `TRUE`. | Critical: exits should remain available. |
| `swing_sleeve_max_positions` | Swing sleeve capacity target. | Intended value. | Adjust only deliberately. |
| `intraday_sleeve_shadow_target` | Intraday sleeve target for future hybrid. | Informational while shadow. | Use for planning only. |

---

# Intraday Shadow Proof / Paper Pilot

## Section purpose

Shows intraday shadow/paper candidate proof while swing remains live.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `shadow_enabled` | Intraday shadow evaluation enabled. | `TRUE` if collecting proof. | Enable if proving intraday. |
| `shadow_status` | Latest shadow scan status. | `completed` or waiting state. | Errors require scanner/debug inspection. |
| `shadow_symbols_scanned` | Number of symbols shadow-scanned. | Matches configured scope. | If too low, inspect universe/scanner. |
| `shadow_signals` | Intraday signals found. | Informational. | Zero can be normal. |
| `shadow_would_trade` | Shadow candidates that would trade. | Informational. | Track over time. |
| `paper_pilot_eligible` | Whether paper pilot eligibility is met. | `TRUE` before paper pilot. | Keep shadow if false. |
| `paper_pilot_planned` | Planned paper candidates count. | Informational. | Validate before paper pilot. |
| `live_submission` | Whether live intraday orders were submitted. | `FALSE` while shadow/paper proofing. | If true unexpectedly, disable immediately. |
| `top_shadow_signals` | Recent top intraday shadow signals. | Informational. | Review signal quality. |

Diagnostic endpoints: `/diagnostics/intraday_shadow`, `/diagnostics/intraday_signal_debug`

---

# Hybrid Proof Ledger

## Section purpose

Persistent intraday proof ledger for shadow/paper readiness.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `ledger_count` | Number of proof rows. | Meets minimum sample. | Collect more if low. |
| `positive_rate` | Fraction of positive proof outcomes. | Higher is better. | Low rate blocks promotion. |
| `avg_r` | Average R of proof rows. | At/above configured minimum. | Keep shadow/paper if low. |
| `best_r` | Best proof R. | Informational. | N/A. |
| `worst_r` | Worst proof R. | Should not be too deep. | Review strategy if poor. |
| `sample_ready` | Ledger sample size ready. | `TRUE`. | Collect more data if false. |
| `avg_r_ready` | Avg R meets threshold. | `TRUE`. | Continue proofing if false. |
| `proof_ready_for_paper` | Ready to promote to paper pilot. | `TRUE` before paper. | Keep shadow if false. |
| `proof_ready_for_live` | Ready for live promotion. | Usually false until much later. | Do not enable live if false. |
| `recommended_action` | Suggested next proof action. | Clear next step. | Follow conservative action. |
| `recent_shadow_rows` | Recent proof entries. | Informational. | Review quality. |

Diagnostic endpoint: `/diagnostics/hybrid_proof`

---

# Hybrid Sleeve Readiness

## Section purpose

Shows future hybrid capacity plan and intraday EOD proof gate.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `swing_sleeve_max_positions` | Swing sleeve target. | Intended cap. | Confirm env. |
| `intraday_sleeve_max_positions` | Intraday sleeve target. | Intended future cap. | Planning only until live. |
| `intraday_paper_max_positions` | Paper pilot max positions. | Conservative. | Raise only after proof. |
| `combined_target_open_positions` | Swing + intraday target positions. | Informational. | Ensure account can support. |
| `sleeves_configured` | Whether sleeve targets are configured. | `TRUE`. | Set sleeve envs if planning hybrid. |
| `eod_flat_ok` | Intraday EOD proof gate. | `TRUE`. | Resolve intraday residuals if false. |
| `eod_gate_mode` | EOD gate mode. | Expected mode. | Verify config. |
| `eod_gate_note` | Human-readable EOD note. | Clear/acceptable. | Follow note. |
| `intraday_residuals` | Unresolved intraday residual symbols. | `none`. | Close/reconcile residuals. |
| `min_shadow_trades` | Minimum proof trades needed. | Config threshold. | Collect until met. |
| `min_avg_r` | Minimum avg R needed. | Config threshold. | Improve proof before promotion. |

---

# Swing Profit Acceleration

## Section purpose

Converts performance attribution into risk-scaling and tuning decisions.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `risk_current_dollars` | Current per-trade risk dollars. | Intended value. | Verify before live trading. |
| `risk_recommended_dollars` | Suggested risk if ready. | Higher only if ready. | Do not increase unless ready. |
| `risk_ready_for_40` | Whether $40 risk is currently supported. | `TRUE` before increasing. | Keep current risk if false. |
| `risk_ready_for_50` | Whether $50 risk is supported. | `TRUE` only with strong proof. | Do not jump to $50 if false. |
| `risk_blockers` | Reasons risk scaling is blocked. | `none`. | Address blockers first. |
| `worst_r_symbol` | Symbol causing worst-R blocker. | Informational. | Inspect worst trade contributor. |
| `worst_r_exit_reason` | Exit reason tied to worst-R blocker. | Informational. | Tune exit logic if repeated. |
| `worst_r_strategy` | Strategy tied to worst-R blocker. | Informational. | Tune/quarantine if weak. |
| `risk_scale_next_required_fix` | Next required fix before risk scale. | Clear actionable fix. | Follow before raising risk. |
| `quarantine_enforced` | Whether quarantine controls are live. | Usually false/read-only. | Enable only deliberately. |
| `promote` | Buckets with positive edge. | Review for scaling. | Promote carefully. |
| `reduce` | Weak buckets to reduce. | Review. | Consider filters. |
| `quarantine` | Strongly weak buckets. | Review before enforcement. | Enforce only after confirmation. |

Diagnostic endpoint: `/diagnostics/swing_performance_attribution`

---

# Top Symbol Attribution

## Section purpose

Ranks symbols by closed-trade performance.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `Symbol` | Ticker. | Informational. | Review weak symbols. |
| `Closed` | Closed trades for symbol. | Enough sample to judge. | Low sample = monitor only. |
| `Win rate` | Symbol-specific win rate. | High with positive avg R. | Weak win rate needs review. |
| `Gross P&L` | Symbol P&L contribution. | Positive for promoted symbols. | Negative symbols may need reduction. |
| `Avg R` | Symbol average R. | Positive and strong. | Low avg R may need filter/quarantine. |

---

# Swing Tuning Simulator

## Section purpose

Read-only what-if simulation that removes weak historical buckets to estimate performance improvement.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `mode` | Simulator mode. | `read_only_simulation`. | N/A. |
| `best_category` | Category of best simulated filter. | Informational. | Review before acting. |
| `best_name` | Name of best candidate bucket. | Informational. | Inspect details. |
| `best_trades_removed` | Number of trades removed in simulation. | Enough to matter, not too broad. | Avoid overfitting. |
| `best_gross_pnl_delta` | Simulated gross P&L improvement. | Positive. | If negative, do not filter. |
| `best_avg_r_delta` | Simulated avg R improvement. | Positive. | If small, collect more data. |
| `best_worst_r_after` | Simulated worst R after filter. | Better than current. | If still poor, not enough. |
| `best_ready_for_40_after` | Whether simulation would unlock $40 risk. | `TRUE` is promising. | Still requires operator review. |
| `recommended_action` | Suggested simulator action. | Review best candidate. | Do not blindly enforce. |

Diagnostic endpoint: `/diagnostics/swing_tuning_simulator`

---

# Top Tuning Scenarios

## Section purpose

Lists the best simulated tuning scenarios.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `Category` | Bucket type removed. | Symbol/exit/strategy/etc. | Understand before acting. |
| `Name` | Bucket name. | Specific and actionable. | Avoid broad vague filters. |
| `Removed` | Trades removed. | Enough sample. | Too few may be noise. |
| `P&L Δ` | Simulated P&L change. | Positive. | Negative means avoid. |
| `Avg R Δ` | Simulated avg R change. | Positive. | Small change may not justify action. |
| `Worst R After` | Worst R after simulated filter. | Improved. | If still deep, other issues remain. |
| `Ready $40` | Whether simulated filter unlocks $40. | `TRUE` is promising. | Still requires confirmation. |

---

# Stall Exit Drilldown

## Section purpose

Attribution focused specifically on `stall_exit` trades.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `stall_exit_count` | Number of stall exits. | Informational. | High count may indicate weak follow-through. |
| `stall_exit_gross_pnl` | P&L from stall exits. | Not deeply negative. | Tune stall exits if poor. |
| `stall_exit_avg_r` | Average R of stall exits. | Around/above 0R after tuning. | Negative means stalls are leaking risk. |
| `stall_exit_worst_r` | Worst stall-exit R. | Better than -1R. | Deep values block risk scaling. |
| `top_stall_symbols` | Symbols most tied to stall exits. | Informational. | Review repeated offenders. |
| `recommended_exit_tuning` | Best read-only tuning scenario. | Clear next candidate. | Validate before changing env. |
| `recommended_env_hint` | Suggested env direction. | Informational. | Apply only after review. |

Diagnostic endpoint: `/diagnostics/swing_stall_exit_drilldown`

---

# Exit Tightening Simulations

## Section purpose

Read-only simulations for exit tightening.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `Scenario` | Exit tuning scenario. | Specific and explainable. | Avoid overbroad changes. |
| `Removed` | Trades affected/removed. | Enough sample. | Too few = low confidence. |
| `P&L Δ` | Simulated P&L improvement. | Positive. | Negative means avoid. |
| `Avg R Δ` | Simulated avg R improvement. | Positive. | Small changes may not matter. |
| `Worst R After` | Simulated worst R after change. | Improved. | If still deep, more work needed. |
| `Env hint` | Env direction to consider. | Informational. | Apply deliberately. |

---

# Stall Tuning Monitor

## Section purpose

Confirms active stall tuning settings and measures stall exits after the configured tuning start timestamp.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `active_SWING_STALL_MIN_R` | Active stall minimum R threshold. | Intended value, e.g. `0.50`. | Verify env if unexpected. |
| `active_SWING_STALL_EXIT_DAYS` | Active stall exit day threshold. | Intended value. | Verify env. |
| `stall_tuning_active` | Whether tuning differs from baseline or start timestamp exists. | `TRUE` after tuning. | Set envs if false. |
| `stall_tuning_changed_from_default` | Whether min R differs from baseline. | `TRUE` if tuned. | Confirm if intended. |
| `stall_tuning_start_utc` | Timestamp for post-change measurement. | Set to deploy/change time. | Set this env if not set. |
| `stall_exits_since_tuning` | Stall exits after start timestamp. | Enough sample over time. | Wait if zero. |
| `stall_exit_avg_r_since_tuning` | Avg R of post-tuning stall exits. | >= 0R. | If negative, tuning not enough. |
| `stall_exit_worst_r_since_tuning` | Worst post-tuning stall R. | Better than -1R. | If deep, do not raise risk. |
| `negative_stall_exit_count_since_tuning` | Count of post-tuning negative stall exits. | 0 or low. | Review if rising. |
| `assessment` | Human-readable monitor state. | No deep stall issue. | Follow recommendation. |

Diagnostic endpoint: `/diagnostics/stall_exit_tuning_monitor`

---

# Post-Tuning Exit Validation

## Section purpose

Determines whether post-tuning evidence is strong enough to consider unlocking higher risk.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `post_tuning_closed_trades` | Closed trades after tuning timestamp. | 10+ before risk decision. | Collect more trades. |
| `post_tuning_stall_exits` | Stall exits after tuning timestamp. | At least 1 for stall validation. | Wait for sample. |
| `post_tuning_negative_stall_exits` | Negative stall exits after tuning. | 0. | Tune exits further if recurring. |
| `post_tuning_deep_stall_exits` | Deep stall exits after tuning. | 0. | Do not raise risk if > 0. |
| `post_tuning_validation_ready` | Whether validation sample is ready. | `TRUE`. | Keep collecting if false. |
| `ready_for_40_after_post_tuning` | Whether $40 risk is supported after tuning. | `TRUE` before risk increase. | Keep current risk if false. |
| `risk_scale_unlock_candidate` | Final read-only candidate flag for risk unlock. | `TRUE` before operator review. | Do not increase risk if false. |
| `risk_scale_unlock_blockers` | Reasons unlock is blocked. | `none`. | Address listed blockers. |
| `recommended_action` | Suggested next action. | Clear and conservative. | Follow it. |

Diagnostic endpoint: `/diagnostics/post_tuning_exit_validation`

---

# Worst Trade Contributors

## Section purpose

Shows the worst closed trades by R to explain tail-risk blockers.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `Symbol` | Ticker of worst trade. | Informational. | Watch repeated names. |
| `R` | Trade R result. | Not deeply negative. | Deep values block scaling. |
| `P&L` | Dollar result. | Informational. | Review large losses. |
| `Strategy` | Strategy that produced trade. | Expected strategy. | Tune if repeated weakness. |
| `Entry` | Entry type. | Expected entry type. | Review weak entry families. |
| `Exit` | Exit reason. | Expected exit. | Tune bad exits. |
| `Hold days` | Holding duration. | Within strategy expectations. | Review overstays. |
| `Regime` | Entry regime mode. | Favorable/trend preferred. | Avoid weak regime if harmful. |
| `Rank` | Entry rank score. | Higher is better. | Raise rank filter if low ranks lose. |

---

# Quarantine Controls

## Section purpose

Shows advisory/enforced quarantine lists.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `SWING_QUARANTINE_SYMBOLS` | Symbols to quarantine. | `none` unless intentionally set. | Add only after proof. |
| `SWING_QUARANTINE_STRATEGIES` | Strategies to quarantine. | `none` unless intentionally set. | Use carefully. |
| `SWING_QUARANTINE_ENTRY_TYPES` | Entry types to quarantine. | `none` unless intentionally set. | Use after attribution. |
| `SWING_QUARANTINE_ENFORCE` | Whether quarantine blocks entries. | `FALSE` unless deliberate. | Be careful enabling. |
| `mode` | Read-only or enforced. | `read_only` for analysis. | `enforced` means live behavior changes. |

---

# Strategy / Entry Attribution

## Section purpose

Breaks closed-trade performance down by strategy and entry type.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `Strategy` | Strategy name, e.g. daily breakout. | Positive avg R. | Reduce/tune weak strategies. |
| `Entry` | Entry type, e.g. worker exit/standard/adaptive. | Positive avg R. | Review weak entry types. |
| `Closed` | Closed-trade count. | Enough sample. | Low sample = monitor. |
| `Win rate` | Win percentage. | Strong enough for strategy. | Low win rate needs review. |
| `Gross P&L` | P&L contribution. | Positive. | Negative bucket may need filter. |
| `Avg R` | Average R contribution. | Positive and strong. | Low avg R blocks scaling. |

---

# Exit / Holding Attribution

## Section purpose

Breaks closed-trade performance down by exit reason and holding period.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `Exit` | Exit reason. | Targets/profit exits positive; stops controlled. | Tune negative exit categories. |
| `Hold` | Holding period bucket. | Positive avg R buckets. | Tune overstays/weak buckets. |
| `Closed` | Number of closed trades. | Enough sample. | Low sample = monitor. |
| `Win rate` | Win percentage. | Strong enough for bucket. | Low win rate needs review. |
| `Gross P&L` | P&L contribution. | Positive. | Negative bucket may need action. |
| `Avg R` | Average R contribution. | Positive. | Low avg R needs tuning. |

---

# Signal / Rejection Summary

## Section purpose

Summary-mode view of latest scanner/rejection state.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `latest_scan_ts` | Timestamp of latest scan. | Recent during market hours. | Check scanner if stale. |
| `actionable_state` | Current scanner actionability state. | Expected state. | Investigate if stuck. |
| `scanned` | Number of symbols scanned. | Matches universe/window. | If zero during market, inspect scanner. |
| `signals` | Number of signals found. | Can be zero. | Review blockers if always zero. |
| `top_reclaim_blockers` | Intraday reclaim blockers. | Informational. | Tune only after proof. |
| `top_continuation_blockers` | Intraday continuation blockers. | Informational. | Tune only after proof. |
| `dominant_recent_blocker` | Most frequent recent rejection blocker. | Expected candidate-quality blocker. | Investigate operational blockers. |
| `rejection_window` | Number of scans in rejection window. | Configured value. | N/A. |

---

# Top Candidate Rejections

## Section purpose

Shows top scanner candidate rows and why they did not become entries.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `Symbol` | Candidate ticker. | Informational. | Review repeated high-quality rejects. |
| `Rank` | Candidate rank score. | Higher = stronger candidate. | Low rank rejections are normal. |
| `Close` | Candidate close price. | Informational. | N/A. |
| `Breakout %` | Distance from breakout/trigger. | Near valid range. | Too far may be correct rejection. |
| `Reasons` | Rejection reasons. | Candidate-quality reasons are normal. | Operational reasons need review. |

---

# Recent Lifecycle / Dispatch

## Section purpose

Recent scanner/entry lifecycle activity.

| Column | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| `UTC` | Event timestamp. | Recent if system active. | Stale events may indicate worker issue. |
| `Symbol` | Event symbol. | Informational. | Review unexpected symbols. |
| `State/Event` | Lifecycle state/event. | Expected transitions. | Investigate failures. |
| `Reason` | Reason for event. | Informational. | Error reasons need investigation. |

---

# Current Blockers

## Section purpose

Plain-text list of active dashboard-visible blockers.

| Metric | Meaning | Good / Expected | Action if Bad |
|---|---|---|---|
| Current blockers text | Current blocking reasons. | `none`. | Resolve blockers before entries/scaling. |
| Heavy Detail Split note | Reminder that raw details live in diagnostics endpoints. | Informational. | Use diagnostics for deeper investigations. |

---

## 6. Full Diagnostic View Only Sections

The full diagnostic view includes additional heavy panels. These are intentionally skipped from the summary dashboard.

### Closed Trades by Rank Bucket

| Column | Meaning |
|---|---|
| `Rank` | Rank bucket. |
| `Closed` | Closed trades in bucket. |
| `Wins` | Wins in bucket. |
| `Losses` | Losses in bucket. |
| `Win rate` | Win percentage. |
| `Gross P&L` | P&L contribution. |
| `Avg R` | Average R for bucket. |

### Closed Trades by Holding Period

Same metrics as rank bucket, grouped by holding period.

### Closed Trades by Symbol

Same metrics as rank bucket, grouped by symbol.

### Strategy / Exit Attribution

Full diagnostic strategy and exit-reason tables with wins/losses included.

### Rejected Setup Follow-through Focus

| Column | Meaning |
|---|---|
| `Reason` | Rejection reason. |
| `Count` | Number of rejected rows. |
| `Symbols` | Affected symbols. |
| `With later scans` | Rejections that had later scan data. |
| `Avg best move` | Average best later move. |
| `Avg last move` | Average last later move. |
| `Positive rate` | Fraction with positive later movement. |
| `Top symbols` | Main symbols in that reason bucket. |

### Missing Historical Fields

Shows missing fields in historical records that may limit attribution accuracy.

### Intraday Signal Debug

Detailed persisted intraday scanner debug: enabled paths, score thresholds, blocker breakdowns, near misses, pre-ranked candidates, signals, rank-filtered rows, and would-submit rows.

### Rejection Totals

Counts all rejection reasons.

### Rejected by Reason

Counts rejection reasons over the configured recent scan window.

### Correlation Relaxation Impact

Shows whether correlation rules alone blocked candidates.

---

## 7. Diagnostics Endpoint Reference

| Endpoint | Purpose |
|---|---|
| `/diagnostics/eod_flatten_status` | EOD flatten attempts, submissions, residuals, and recommended action. |
| `/diagnostics/hybrid_proof` | Intraday shadow/paper proof ledger and readiness metrics. |
| `/diagnostics/intraday_shadow` | Latest intraday shadow scan payload. |
| `/diagnostics/intraday_signal_debug` | Intraday signal blockers, candidates, and debug details. |
| `/diagnostics/intraday_launch_readiness` | Full intraday launch readiness and blocker checklist. |
| `/diagnostics/swing_performance_attribution` | Swing symbol/strategy/entry/exit attribution and recommendations. |
| `/diagnostics/swing_tuning_simulator` | Read-only profit acceleration/tuning simulations. |
| `/diagnostics/swing_stall_exit_drilldown` | Stall-exit attribution and exit tightening simulations. |
| `/diagnostics/stall_exit_tuning_monitor` | Active stall tuning settings and post-tuning stall exits. |
| `/diagnostics/post_tuning_exit_validation` | Post-tuning validation and risk-scale unlock criteria. |
| `/diagnostics/scanner` | Scanner status, memory profile, and recent scan info. |
| `/diagnostics/candidates_full` | Full candidate payloads. |
| `/diagnostics/swing_full` | Full swing scanner diagnostics. |

---

## 8. Glossary

| Term | Meaning |
|---|---|
| R | Risk multiple. `+1R` means a gain equal to planned trade risk; `-1R` means planned risk loss. |
| Avg R | Average R across a group of trades. Key metric for risk scaling. |
| Stall exit | Exit caused by a trade failing to progress after the stall window. |
| Deep stall exit | Stall exit that still lost too much R, typically a risk-scaling blocker. |
| Snapshot | Persisted account/position state used by dashboard. |
| Snapshot freshness | Age/validity of persisted snapshot. |
| Reconcile | Process comparing broker-backed positions, active plans, and orders. |
| Daily halt | Daily risk stop that blocks new entries after a loss/risk condition. |
| Shadow proof | Non-order-submitting intraday signal evaluation. |
| Paper pilot | Simulated/paper intraday promotion stage before live hybrid trading. |
| Sleeve | Separate conceptual capacity allocation, e.g. swing sleeve and intraday sleeve. |
| Quarantine | Optional controls to exclude weak symbols/strategies/entry types. |
| Risk-scale unlock | Read-only recommendation that enough evidence exists to consider higher per-trade risk. |

---

## 9. Notion Maintenance Notes

When dashboard sections or metrics change:

1. Update this guide in the same patch.
2. Keep metric names exactly as shown on the dashboard.
3. Add any new diagnostics endpoint to the endpoint reference.
4. Add operator decision rules for any metric that can change trading behavior.
5. Keep the guide copy/paste friendly: headings, tables, bullets, and code blocks only.

### Patch 220 worker completion watchdog and position truth

Patch 220 adds a worker-exit completion watchdog and a broker/plan/snapshot truth view.  The dashboard remains snapshot-only, but the Workers and Reconcile cards now show whether a worker-exit heartbeat has been stuck in `started` longer than `WORKER_EXIT_STARTED_STALE_SEC`, plus a compact position-truth status and mismatch count.

Operational flow:

1. If `worker_exit_started_stale` is true, inspect `/diagnostics/worker_exit_status` before relying on a fresh exit cycle.
2. If `position_truth_status` is not `aligned`, open `/diagnostics/position_truth` to compare live broker positions, active internal plans, open orders, and the persisted dashboard snapshot.
3. Treat dashboard active positions as advisory whenever the snapshot is stale or position truth reports mismatches; reconcile against Alpaca before taking manual action.

New optional environment variables:

- `WORKER_EXIT_STARTED_STALE_SEC` — seconds a `started` worker-exit heartbeat may remain incomplete before it is flagged.  Defaults to the larger of 180 seconds or twice `READINESS_EXIT_MAX_AGE_SEC`.
- `POSITION_TRUTH_STALE_SEC` — freshness threshold for position-truth snapshot alignment.  Defaults to 180 seconds.


## Patch 221 stall-exit guardrails

Patch 221 keeps the next successful step focused on swing exit quality: it adds a configurable stall-loss guard that can close a stalled swing position before it becomes a deep loss-R outlier. This does **not** size up risk and does **not** enable intraday live orders.

| Setting | Meaning | Default | Operator note |
|---|---|---:|---|
| `SWING_STALL_LOSS_GUARD_ENABLED` | Enables the early stall-loss guard. | `true` | Keep enabled while reviewing stall exits. |
| `SWING_STALL_LOSS_GUARD_DAYS` | Minimum held days before the loss guard can trigger. | `1` | Avoids same-day churn while still acting before the classic stall window. |
| `SWING_STALL_MAX_LOSS_R` | Loss-R threshold that marks a stalled trade for exit. | `-0.60` | Tune conservatively; this is intended to prevent `stall_exit` losses from reaching deep-loss buckets. |

Review `/diagnostics/swing_stall_exit_drilldown` and `/diagnostics/stall_exit_tuning_monitor` after deployment. The dashboard now shows the active stall max-loss guard in the Stall Tuning Monitor panels.


## Patch 222 recovered-plan attribution backfill

Patch 222 keeps recovered broker-backed plans from polluting strategy attribution as `RECOVERED`. When reconcile has to rebuild an internal plan from an existing broker position or open order, the system now searches recent decision rows, the execution journal, lifecycle history, candidate history, and scan history for the original strategy identity.

Operator notes:

1. A recovered plan can still show `recovered=true` internally, but the dashboard Signal column should prefer the inferred strategy such as `daily_breakout` or `daily_mean_reversion`.
2. Closed-trade analytics keep both facts: the recovered provenance remains available, while strategy-level P&L buckets use the inferred strategy instead of a generic recovered bucket.
3. If no reliable match exists, the fallback remains the configured swing breakout strategy rather than `RECOVERED`, so attribution stays useful while preserving recovered metadata for audit.
4. Use `/diagnostics/reconcile`, `/diagnostics/position_truth`, and `/diagnostics/swing_performance_attribution` to verify recovered plans are broker-aligned and attributed to their trading strategy.