# Trading Webhook Codex Handoff

## Current goal

Analyze the July 2, 2026 swing loss cluster and propose the next patch.

The operator does **not** want to shut off trading. The goal is to diagnose whether the losses were caused by bad entries, late exits, open-gap behavior, stale/recovered attribution, duplicate accounting, or daily halt logic. We should preserve live exits and avoid enabling intraday live or sizing up.

## Repo context

Repo: trading-webhook

Recent deployed patch: patch-227-live-halt-layout

Important recent features:
- /dashboard/live broker-backed live operator dashboard
- /diagnostics/live_positions
- Loss Halt Checklist on live dashboard
- Active Positions Audit
- recovered-plan attribution backfill
- stall-exit guard / tuning monitor
- strategy performance attribution
- daily halt / EOD flatten safety controls

## Key concern

On July 2, several swing positions opened about two days earlier dumped near the market open. Operator estimates about $600 lost. Need to determine actual broker-realized loss vs internal strategy ledger accounting.

## First diagnostic already reviewed: live_positions

Important observations from live_positions:
- At 2026-07-02 12:01 ET, broker truth was aligned:
  - broker_positions_count = 2
  - active_plan_count = 2
  - open_order_count = 0
  - position_truth_status = aligned
- Remaining positions were IWM and PANW.
- Exits clustered near market open:
  - INTC exited around 09:31 ET
  - AMAT exited around 09:33 ET
  - UBER exited around 09:35 ET
  - MU exited around 09:39 ET
  - VRT exited around 09:44 ET
- Broker reconstructed realized P&L showed about -461.12.
- Internal strategy_state_today showed about -1021.79.
- The strategy state appears to double-count the same economic exits:
  - worker_exit / stall_exit row
  - broker_exit_fill / alpaca_orders_reconciled row
- UBER broker order reconstruction had missing entry basis:
  - entry_price = null
  - calc_ok = false
- Daily halt was not active because account_daily_pnl was about -42, while realized closed-trade P&L was much worse.
- This suggests the halt is account-equity based, not closed-trade-realized-loss based.

## Current hypothesis

There are two separate issues:

1. Trading issue:
   - daily_breakout swing positions were closed near market open by stall_exit.
   - Need to inspect whether the entry quality, holding period, stop levels, market regime, or open-gap handling was bad.

2. Analytics/accounting issue:
   - strategy_performance_state may be double-counting closed trades when both worker_exit and broker_exit_fill rows exist for the same economic exit.
   - Need to dedupe or clearly separate broker truth from internal lifecycle events.

## Files to inspect

Please inspect all files in this folder, especially:

1. strategy_performance
2. swing_stall_exit_drilldown
3. stall_exit_tuning_monitor
4. post_tuning_exit_validation
5. loss_control_incident
6. reconcile
7. position_truth
8. live_positions

## Desired output

Please produce:

1. A concise diagnosis of what happened on July 2.
2. Which metrics are trustworthy vs suspect.
3. Whether the actual loss was closer to broker realized P&L or strategy_state P&L.
4. Whether duplicate accounting exists.
5. Whether stall_exit is too late or too loose.
6. Whether daily halt logic should consider realized closed-trade P&L in addition to account equity.
7. A proposed Patch 228.

## Proposed Patch 228 direction

Patch 228 should likely be:

"Loss cluster forensic report and duplicate realized-exit audit"

Possible endpoint:
- /diagnostics/loss_cluster_report

It should show:
- broker realized exits today
- internal worker_exit rows today
- broker_exit_fill rows today
- duplicate pairs by symbol/time/qty/order id
- duplicate-adjusted realized P&L
- losses by symbol
- losses by strategy
- losses by exit reason
- losses by holding period
- losses by recovered vs attributed
- missing entry basis rows
- whether account daily halt differs from realized-trade-loss halt
- recommended actions

Important: do not shut off trading. Do not enable intraday live. Do not size up.