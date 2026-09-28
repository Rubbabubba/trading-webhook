# Kalshi external strategy research and sleeve test plan

Date: 2026-09-28

## Objective

Add independent, prospective research sleeves without changing the running
Demo workers or pooling their evidence. Each sleeve must answer one fixed
hypothesis using event-time data, executable prices, actual fee metadata, and
event-clustered results. A favorable backtest is permission to run a shadow
test, not permission to trade production capital.

## What the external evidence supports

### 1. Structural and cross-contract consistency

Prediction contracts that describe mutually exclusive or nested outcomes have
hard probability constraints. A mutually exclusive and exhaustive partition
should sum to one; a higher threshold cannot be more likely than a lower
threshold under identical settlement rules. Research on combinatorial
prediction markets shows that logical relationships create arbitrage
constraints, and recent empirical work reports persistent arbitrage in
prediction markets. Kalshi's API exposes events, multivariate events, complete
rules, strikes, books, trades, and event fee overrides, so these relationships
can be checked directly.

Sources:

- [Arbitrage-Free Combinatorial Market Making via Integer Programming](https://arxiv.org/abs/1606.02825)
- [Unravelling the Probabilistic Forest: Arbitrage in Prediction Markets](https://arxiv.org/abs/2508.03474)
- [Kalshi multivariate events API](https://docs.kalshi.com/api-reference/events/get-multivariate-events)
- [Kalshi API documentation index](https://docs.kalshi.com/llms.txt)

### 2. Favorite-longshot bias

A 2025 Kalshi study using more than 300,000 contracts finds that low-price
contracts win too rarely to break even while high-price contracts earn small
positive returns, with different behavior for makers and takers. A much larger
2026 Polymarket study finds the same aggregate pattern but warns that results
change materially when observations are grouped by parent event and that the
pattern is absent in its sports subset. The hypothesis is credible, but it must
be tested on Kalshi by family, maker/taker role, time to expiry, and parent
event. It is not a license to buy every 90-cent contract.

Sources:

- [Makers and Takers: The Economics of the Kalshi Prediction Market](https://papers.ssrn.com/sol3/Delivery.cfm/SSRN_ID5502658_code4203760.pdf?abstractid=5502658&mirid=1)
- [The Favorite-Longshot Bias in Prediction Markets: Evidence from Polymarket](https://arxiv.org/abs/2609.12878)
- [Do Prediction Markets Produce Well-Calibrated Probability Forecasts?](https://scholars.duke.edu/publication/765630)

### 3. Post-news underreaction in live sports

A 2026 study matches 41.8 million Kalshi transactions from 1,496 NBA and NFL
games to play-by-play win probabilities. Prices underreact to scoring news over
short windows, but the authors' tested rule does not retain robust profit under
conservative costs. This supports a narrow score-shock experiment and argues
against assuming that any visible probability lag is tradable.

Source:

- [Underreaction, salience, and hot-hand beliefs in sports prediction markets](https://doi.org/10.1016/j.jbef.2026.101259)

### 4. Order-flow imbalance as an execution filter

Limit-order-book research finds that short-horizon price changes relate more
closely to order-flow imbalance than raw traded volume, with the effect scaled
by depth. This is most useful here as an adverse-selection veto and quote-price
input. The existing V10/V11 work already tests directional imbalance, so a new
standalone imbalance strategy would duplicate existing evidence.

Source:

- [The Price Impact of Order Book Events](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1712822)

### 5. Crypto threshold relative value

Research comparing prediction-market Bitcoin thresholds with listed crypto
options finds persistent option-implied pricing gaps and reports a
cost-stressed delta-hedged proxy with marginal statistical precision. Kalshi
provides real-time CF Benchmarks and Pyth reference feeds, so a Kalshi-only
shadow sleeve can compare internally observed spot/volatility distributions
with threshold prices before considering any external hedge.

Sources:

- [Do Prediction Markets Match Option Prices? Bitcoin Threshold Evidence](https://arxiv.org/abs/2606.19517)
- [Kalshi WebSocket and market-data documentation](https://docs.kalshi.com/llms.txt)

### 6. Liquidity incentives

Kalshi currently pays eligible participants for useful resting liquidity. Books
are sampled once per second at a random instant; scores depend on size and
distance from a reference price. Incentives can change the economics of market
making, but gross rewards are not an edge unless they exceed maker fees,
inventory losses, adverse selection, and opportunity cost.

Source:

- [Kalshi Liquidity Incentive Program](https://help.kalshi.com/en/articles/13823851-liquidity-incentive-program)

### 7. Weather forecasts

Public weather models are credible inputs, but a 1,506-event Kalshi study found
that three NWS-based strategies broke even or lost after fees and that Kalshi
incorporated NWS updates within roughly 10-30 minutes. A weather sleeve should
therefore test forecast-update latency and cross-strike coherence, not a naive
"forecast versus price" rule.

Source:

- [We Built a Kalshi Weather Model. The Market Won Anyway](https://oddsreference.com/predictions/research/kalshi-weather-model)

## Proposed sleeves

### Sleeve A — logical partition and monotonicity arbitrage

**Priority:** 1

**Hypothesis:** Some groups of contracts temporarily violate rules-implied
probability bounds by more than all fees, available-depth slippage, and
multi-leg execution risk.

**Universe:** Every open Kalshi event, including mutually exclusive events,
threshold ladders, interval buckets, and verified cross-event logical
relationships. Every relationship requires normalized settlement rules and a
stored rule hash; text similarity alone is insufficient.

**Signals:**

- Exhaustive partition: sum of immediately executable asks below 100 cents, or
  sum of executable bids above 100 cents, after all costs.
- Nested thresholds: higher-threshold YES price above lower-threshold YES price
  by more than costs.
- Interval/threshold replication: a bucket price inconsistent with differences
  between adjacent cumulative thresholds.
- Explicit multivariate leg price inconsistent with its verified component
  bounds.

**Simulation:** Use displayed depth on every leg, assume the worst fill order,
charge current event-specific fees, and stress each unfinished leg by two
additional cents. Record opportunity duration and whether simultaneous depth
existed. Do not label a price-screen violation an arbitrage unless every leg
was executable during the same snapshot.

**Gate:** At least 50 fully executable opportunities across 30 independent
events and five families; positive net after actual fees and the two-cent
unfinished-leg stress; no settlement-rule mismatch; positive event-clustered
95% lower confidence bound. Reject after 100 fully observed opportunities if
the stressed net is nonpositive.

### Sleeve B — Kalshi favorite/longshot calibration

**Priority:** 2

**Hypothesis:** Passive purchases of the favorite side at 90-98 cents outperform
symmetrically priced longshots after fees and capital-time cost in selected
non-sports Kalshi families.

**Universe:** Resolved and newly listed contracts, grouped first by parent
event. Sports remain a separate stratum because the cross-platform evidence
does not show the same bias there. Exclude ambiguous settlement, prices below
2 cents or above 98 cents, copied Demo artifacts, and events lacking fee
history.

**Design:** Pre-register four price bins (2-5, 5-10, 90-95, 95-98), maker and
taker roles, time-to-expiry bins, and market family before looking at results.
Use historical trades for calibration, then freeze the model and run a future
shadow cohort. Measure settlement return, markout, Brier score, capital-days,
fees, and event-weighted return.

**Gate:** Historical discovery needs at least 500 resolved parent events. The
prospective gate requires 300 new resolved parent events, at least five
families, positive fee-and-carry-stressed return, and a positive
event-clustered 95% lower bound. No subgroup promotion unless it was declared
before the prospective sample.

### Sleeve C — sports scoring-shock underreaction

**Priority:** 3

**Hypothesis:** After an authoritative scoring event, a passively priced order
in the direction of the updated win probability earns positive 10/30/60/120
game-clock-second markouts after fees and adverse-selection stress.

**Universe:** NFL and NBA first, because the cited evidence is specific to
those leagues. NCAAF, MLB, soccer, and tennis do not inherit the result. Exact
game identity and official play IDs are mandatory.

**Design:** Pair every shock with three outcomes: no trade, a hypothetical
taker, and a post-only quote. Freeze shock-size bins and the external
win-probability model. Require a new book after the play, stable game state,
and a second confirming observation. Capture missed fills and queue position;
do not assume a maker fill merely because the market later traded through the
quote.

**Gate:** 100 scoring shocks across at least 30 games, complete markouts at all
four horizons, positive stressed net for the passive arm, and a positive
game-clustered 95% lower bound. Automatically reject the taker arm if its
stressed net is nonpositive at 100 shocks, consistent with the published cost
warning.

### Sleeve D — incentive-adjusted passive liquidity

**Priority:** 4

**Hypothesis:** In selected incentive markets, expected program rewards plus
spread capture exceed actual maker fees, adverse selection, inventory risk,
and capital opportunity cost.

**Universe:** Only currently incentive-eligible markets whose schedule and
parameters are captured from the public API. Eligibility must be checked for
the account before any eventual execution trial.

**Design:** Shadow quotes at preregistered distances and sizes. Reconstruct the
one-second random-snapshot exposure conservatively, score actual time at risk,
and report economics both with and without incentives. Pair with an identical
unincentivized control group matched by spread, depth, duration, and family.

**Gate:** Fourteen calendar days, 30 independent markets, 10,000 eligible
quote-seconds, positive net with reward estimates haircut by 50%, positive net
without assuming a fill when queue evidence is missing, and positive
market-clustered lower confidence bound. Stop immediately if terms or account
eligibility cannot be verified.

### Sleeve E — crypto threshold distribution consistency

**Priority:** 5

**Hypothesis:** Kalshi crypto threshold prices sometimes diverge from a
pre-registered spot-and-volatility digital-option benchmark by more than fees,
spread, model uncertainty, and hedge cost.

**Universe:** Bitcoin and Ethereum threshold contracts with exact strike and
expiry mapping to Kalshi's CF Benchmarks or Pyth feeds. No cross-venue order is
authorized in the initial study.

**Design:** Save synchronized Kalshi books and reference values. Estimate
realized volatility using only past data, produce a full monotone distribution
across strikes, and compare both raw and isotonic-adjusted Kalshi probabilities.
Use walk-forward volatility estimation and block-bootstrap inference. Test a
Kalshi-only relative-value basket separately from any option-hedged proxy.

**Gate:** 200 synchronized hourly observations, 30 contract-days, at least ten
distinct expiries, positive cost-stressed markouts at 5/30/300 minutes, and a
positive expiry-clustered lower confidence bound. Reject if the result depends
on one strike, expiry, or volatility estimator.

### Sleeve F — forecast-update weather latency

**Priority:** 6

**Hypothesis:** A large, verified change in an official forecast distribution
occasionally reaches Kalshi slowly enough for a passive order to retain value
after fees.

**Universe:** Daily temperature events with exact settlement-station mapping,
official forecast issue times, and complete bucket definitions.

**Design:** Build city/station-specific walk-forward error distributions. A
signal requires a new official model issuance, a material probability change,
agreement across at least two independent model families, coherent adjacent
buckets, and an executable post-update Kalshi book. Compare entry delays of
0/5/10/20/30 minutes. The previously published null makes the 10-30 minute
window a likely stopping boundary rather than an assumed edge.

**Gate:** 300 city-days with at least 50 per city, two seasons where feasible,
positive fee-stressed return, improved calibration over the market, and a
positive city-day-clustered lower confidence bound. Reject any version selected
only after searching many thresholds.

## Shared test infrastructure

1. **Immutable capture.** Stream orderbook snapshots/deltas, public trades,
   market lifecycle, event metadata, rule text/hash, fee changes, incentives,
   and source timestamps. Kalshi documents real-time orderbook, public-trade,
   and lifecycle channels plus historical trades and one-minute candles.
2. **As-of joins.** Every decision sees only data available at that timestamp.
   Store provider publication time, receipt time, and decision time.
3. **One common cost engine.** Use executable depth, actual fee type and
   multiplier, conservative fee rounding, partial fills, queue ahead, cancel
   latency, and capital-days. Multi-leg sleeves also carry an unfinished-leg
   stress.
4. **Separate ledgers.** Each sleeve has its own registration, candidates,
   decisions, hypothetical orders, fill assumptions, markouts, settlements,
   and P&L. No result is pooled across correlated sleeves.
5. **Paired controls.** Evaluate challengers and their frozen baselines on the
   same events and timestamps. Missed data remain missing.
6. **Event-level inference.** Use event/parent-event clusters, bootstrap
   confidence intervals, family concentration, and leave-one-family-out tests.
7. **Automatic rejection.** Apply each sleeve's fixed sample and stop rule.
   Preserve failed experiments to prevent repeated retesting.
8. **Promotion sequence.** Historical replay, live shadow, one-contract Demo,
   then a separate production review. Passing one stage never activates the
   next automatically.

## Implementation order

### Days 1-2

- Extend the existing all-market collector with rule hashes, event fee history,
  incentive metadata, public trades, and synchronized multi-market snapshots.
- Build a relationship registry for partitions, nested thresholds, buckets,
  and multivariate legs.
- Add a shared cost and event-cluster evaluation library.
- Register Sleeves A and B before calculating results.

### Days 3-4

- Run historical calibration for Sleeve B and a historical opportunity census
  for Sleeve A.
- Start live shadow collection for A and B without changing existing workers.
- Implement paired maker/taker/no-trade observations for Sleeve C and add NBA
  discovery when games become available.

### Days 5-7

- Add the incentive metadata and quote-score reconstruction for Sleeve D.
- Add synchronized CF/Pyth crypto reference capture for Sleeve E.
- Produce one compact daily packet with counts, data completeness, cost-stressed
  results, concentration, and stop/gate state for every sleeve.

### Week 2 and later

- Add Sleeve F only after exact settlement-station mappings and archived
  official forecast issuances are verified.
- Review defects immediately, but do not tune economic parameters before the
  registered sample ends.
- Advance only a sleeve that passes every preregistered gate to a separate,
  one-contract Demo execution trial.

## Decision

Build A and B first. They apply across the broad Kalshi universe and rely on
exchange structure and directly observed Kalshi behavior. Add C as a narrow
NFL/NBA test, D as a potential economics overlay, and E as a specialized
relative-value study. Treat weather as a later, high-bar experiment because a
large published test already found that a naive forecast advantage did not
survive fees.
