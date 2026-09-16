# Kalshi portfolio liquidity momentum V4

## Decision

V3 is rejected as a production candidate and paused. It lost 237 cents over 35
completed paper episodes. Its registered future holdout lost 241 cents nominal
and 301 cents under the extra two-cent-per-episode stress over 30 episodes. The
weakness was structural: 21 stop exits lost 277 cents, repeated entries lost 101
cents, and both YES and NO directions were negative.

V4 is a new prospective paper challenger. It is not yet demonstrated profitable
and production execution remains disabled. Its parameters were frozen before its
first future observation at 2026-09-16T20:53:00Z.

## Original hypothesis

When a liquid Kalshi contract makes a material multi-minute move, continues in
the same direction in the most recent observations, and the displayed book has
supporting depth, the move is more likely to persist for the next five minutes
than reverse after spread and slippage. This directly tests the opposite of V3's
failed short-horizon reversal premise.

The strategy trades either economic outcome. Rising YES momentum buys YES;
falling YES momentum buys NO. It is not a settlement-only “buy the winner” rule.
It can scan sports, macro, weather, crypto, commodities, and other active binary
categories represented in the frozen universe.

## Frozen entry rules

- Compare the current executable price with an anchor observed four to seven
  minutes earlier. Require a move of 6–20 cents.
- Require at least two cents of confirmation in the same direction from an
  observation 30–120 seconds earlier.
- Require a 20–80 cent midpoint, spread no wider than four cents, and at least
  three contracts at the best displayed price on both sides.
- Require displayed depth on the momentum side to be at least 1.25 times the
  opposing ask depth.
- Require modeled debit below 90 cents and a distinct later book for simulated
  fill confirmation.
- Allow one filled entry for the lifetime of an event. No reentry and no side
  flip are allowed.

## Frozen risk and evaluation

- Take profit: 12 cents.
- Observed stop: 6 cents.
- Maximum hold: 300 seconds.
- Confirmation latency: 2 seconds.
- Modeled slippage: 1 cent.
- Portfolio loss stop: 500 cents.
- Release sample: at least 30 independent future events and 100 completed future
  episodes, disjoint from the 29 registered training events.
- Every release metric must pass: positive nominal net, positive net after two
  extra cents of cost per episode, and a positive event-cluster 95% lower bound.

This is deliberately harder than declaring success from a favorable backtest.
The current registered holdout is 0 events and 0 episodes, so no profitability
claim is supported yet.

## Current operation

The local paper worker is running with production execution disabled. Its latest
50-action health window is healthy: 21 waits, 16 rejected observations, 13
finalized markets without positions, a 32% rejection ratio, no open position,
and no pending entry. The worker stops at 2026-09-21T14:30:00Z unless extended by
a later reviewed registration.

Configuration and evidence:

- `configs/kalshi_portfolio_v4_20260916/manifest.json`
- `configs/kalshi_portfolio_v4_20260916/holdout_registration.json`
- `sports_paper/portfolio_v4_20260916/status.json`
- `sports_paper/portfolio_v4_20260916/health.json`
- `sports_paper/portfolio_v4_20260916/holdout_status.json`

## Why the design is plausible but unproven

Kalshi's V2 order model represents YES as a bid and NO as the economically
equivalent YES ask at the complement price, so the symmetric signal and execution
mapping are supported by the exchange interface. Kalshi also exposes incremental
order-book updates and price-time queue position, which are needed for a later
maker variant rather than pretending periodic snapshots know queue priority.

The external evidence does not guarantee this signal. Bartlett and O'Hara find
that one-sided flow can predict maker losses in toxic single-name markets. That
supports V4's liquidity and directional-depth filters and the separate maker
research gate, while also giving a reason to demand prospective evidence.

Sources:

- https://docs.kalshi.com/api-reference/orders/create-order-v2
- https://docs.kalshi.com/websockets/orderbook-updates
- https://docs.kalshi.com/api-reference/orders/get-order-queue-position
- https://ssrn.com/abstract=6615739
