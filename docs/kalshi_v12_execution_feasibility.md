# V12 execution feasibility finding

The V12 microprice estimate is a weighted average of the best bid and ask.
Crossing the spread to buy at the ask therefore has nonpositive modeled gross
edge before fees. The worker publishes `v12_crossing_feasibility` for the
latest prospective signal. This rules out a taker-order version of the same
signal, but says nothing about other strategies.

The original 50-order Demo trial and the one-tick fillability trial (100
attempts) produced no fills. The current Demo order trial remains stopped by
its registered checkpoint. A new execution strategy needs a separate versioned
hypothesis, prospective evidence, bounded Demo trial, and actual fee-adjusted
fill results before a live promotion request.
