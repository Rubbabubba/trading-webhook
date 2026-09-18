# Kalshi V5 demo maker experiment

V4's first six prospective paper entries all crossed the spread and stopped out,
losing 50 cents nominal and 62 cents under the registered extra-cost stress. V5
tests a different source of return: supplying liquidity in stable books and
measuring whether spread capture survives actual fills, fees, queue priority, and
post-fill adverse selection.

The hypothesis is motivated by Bartlett and O'Hara's 2026 Kalshi study, which
finds a maker surplus funded by systematic YES-side demand and also finds that
one-sided flow predicts maker losses. It is consistent with Kalshi's published
price-time queue mechanics. These sources motivate the test; they do not prove
that this implementation is profitable.

## Frozen rules

- Demo environment only; production transport is unavailable to the worker.
- One contract, one working order, and one open position maximum.
- Quote only when the YES midpoint has moved no more than two cents over 90–300
  seconds, midpoint is 20–80 cents, spread is 3–8 cents, both top levels contain
  at least three contracts, and the two top depths are within a 2:1 ratio.
- Alternate YES and NO attempts. Improve the selected best bid by one cent only
  when the order remains passive.
- Build a diverse scan cohort from MLB, NFL, college-football, EPL, and Federal
  Reserve series. Use at most one contract per event so a single event cannot
  crowd out the prospective sample.
- Use post-only GTC, record queue position, and cancel an unfilled order after 90
  seconds. A fill is held for the registered 5, 30, and 300 second markouts, with
  a 12-cent emergency loss exit and a 300-second maximum hold.
- One filled entry per event. Entry attempts that never fill do not consume the
  event lock.
- Capital limit 160 cents, order limit 110 cents, and daily loss stop 100 cents.
- Every mutation is durably journaled before submission. Unknown submissions stop
  the worker and are never retried blindly.

## Evidence gate

The experiment needs at least 30 terminal post-only attempts across five markets,
both YES and NO attempts, at least ten maker fills, a queue record for every
attempt, actual fee evidence for every fill, all 5/30/300-second markouts, no
unresolved order, and a flat ending position before profitability is analyzed.
Release still requires positive net results after costs and stress with a positive
event-cluster lower confidence bound. Demo results cannot activate production.

Sources:

- https://docs.kalshi.com/api-reference/orders/get-order-queue-position
- https://docs.kalshi.com/getting_started/orderbook_responses
- https://docs.kalshi.com/getting_started/fee_rounding
- https://ssrn.com/abstract=6615739
