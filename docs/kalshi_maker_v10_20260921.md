# Kalshi maker V10 prospective challenger

V10 is a shadow-only challenger attached to the hosted V9 demo worker. It does
not submit, cancel or modify an order. V9 remains the frozen one-contract
exchange-control strategy and production execution remains disabled.

V9 selected an outcome to balance YES and NO attempt counts. That is useful for
execution acceptance but supplies no directional fair-value estimate. V10
instead requires agreement between the median 60-300 second midpoint trend and
the current top-of-book microprice/depth imbalance. Its passive entry must retain
at least three cents to a fair-value estimate capped at two cents from midpoint.
Every signal is marked at 5, 30 and 300 seconds, both gross and after a fixed
two-cent round-trip stress.

The registration in `configs/kalshi_maker_v10_20260921/registration.json` is
frozen before hosted evidence. Shadow promotion requires 100 complete signals
from 30 independent events, positive stressed net at every horizon and a
positive event-cluster 95% lower bound. Those conditions permit a separate
one-contract demo execution trial; they do not enable production.

The module also freezes two execution rules for that later trial. A quote more
than five contracts back after 45 seconds is canceled; a quote still more than
one contract back with no queue improvement after 60 seconds is canceled. A
filled entry uses a passive exit that covers its basis, a one-cent exit-fee
reserve and a two-cent target. Aggressive exits are reserved for invalidation or
risk limits. These helpers are tested now but remain disconnected from writes.
