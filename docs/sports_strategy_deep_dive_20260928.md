# Sports strategy deep dive — September 28, 2026

## Evidence correction

The previously quoted four episodes and −$5.08 were not weekend results. They were cumulative trades from September 10–12 in the original research ledger. The experiment stopped accepting new games on September 18, but the daily review regenerated the lifetime totals without labeling their evidence period. The review now reports first sample, last sample, last trade action, age, and a prominent stale-evidence warning.

The newer frozen v3.5 ledger is the relevant performance record. Its 300- and 900-second accounts are alternative holding-period simulations of the same opportunities and must not be added into one portfolio.

| League | Horizon | Games traded | Entries | Gross before modeled costs | Fees + slippage | Net |
|---|---:|---:|---:|---:|---:|---:|
| NCAAF | 300s | 21 | 67 | −$2.26 | $30.97 | −$33.23 |
| NCAAF | 900s | 21 | 64 | −$10.21 | $30.05 | −$40.26 |
| NFL | 300s | 3 | 7 | $1.38 | $3.22 | −$1.84 |
| NFL | 900s | 3 | 7 | $1.44 | $3.22 | −$1.78 |
| MLS | 300s | 5 | 13 | $2.01 | $6.25 | −$4.24 |
| MLS | 900s | 5 | 12 | $1.67 | $5.84 | −$4.17 |
| EPL | 300s | 2 | 4 | $1.86 | $1.65 | $0.21 |
| EPL | 900s | 2 | 3 | $3.35 | $1.02 | $2.33 |

EPL is positive but has only two independent traded events, far below a defensible sample. NCAAF loses before costs at both horizons; its probability model is not ready for execution. NFL and MLS show positive price movement before costs, but the prior taker implementation gives all of it away and more.

## What caused the losses

1. The strategy crossed the spread to enter and crossed again to exit. It paid a fee and one cent per contract of simulated slippage on every transaction.
2. It allowed five entries per event with only a five-minute cooldown. Repeated observations of one game created repeated costs and correlated risk, not independent evidence.
3. Fixed 300- and 900-second holding limits forced marketable exits even when the thesis had not failed.
4. A persistent data-feed failure triggered a sale. Inability to calculate a fresh probability became a financial decision and crystallized losses at the bid.
5. The model could swing sharply between consecutive observations without a meaningful score change. A single unstable probability was enough to enter or exit.
6. Coverage was static and expired. Soccer lacked prospective pregame anchors, tennis mappings changed, and the whole experiment stopped on September 18. That explains the later absence of trades.

## Replacement challenger

`sports_persistent_passive_v1_shadow` observes all eligible NCAAF, NFL, MLB, EPL, MLS, ATP, and WTA events. It remains shadow-only until its preregistered gate is met.

- One contract and one signal per event.
- Post-only limit below the ask; no routine spread crossing.
- At least eight cents of edge after maker fees and a two-cent adverse-selection stress.
- The same side must qualify on two distinct fresh play states at least 15 seconds apart.
- A feed or mapping failure freezes new actions and starts reconciliation. It never causes a sale.
- Normal exit is settlement or a post-only exit after two fresh contradictory states.
- Every family is monitored. A family produces no execution candidate until its mapping, feed, calibration, liquidity, and cost gates pass.

The promotion gate is 30 independent events, 100 complete signals, complete 5/30/300-second markouts, positive cost-stressed net at every horizon, and a positive event-clustered 95% lower confidence bound. Only then may a separate one-contract Demo execution trial begin.

## Activity without forced losses

More observations come from continuous event discovery, early pregame registration, refreshed soccer anchors, and participant/rules validation for tennis. More trades come only from qualified independent events. Lowering the edge threshold or restoring repeated taker entries would increase activity by replaying the mechanism that already lost money.

No strategy can guarantee no losses. The operational way to avoid further losses now is to keep the challenger in shadow, collect broad prospective evidence, and permit Demo execution only after the frozen gate passes.
