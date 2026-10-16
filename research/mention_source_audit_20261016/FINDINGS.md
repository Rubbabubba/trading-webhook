# Published mention NO strategy — source audit

Pinned public source: DanMcInerney/kalshi-analysis revision 9c9b285f81215bfc7a982dcbcf4559e0991d92d4. Twelve immutable public responses are hashed and retained; third-party code was read statically, never executed. The official current Mentions registry contains461 series. No underlying provider database is published in this repository.

## Concrete replication risks

1. The pinned fill predicate accepts YES-taker prints with NO price up to1c ABOVE our NO bid. Example: our NO bid35c is a YES ask65c. A YES taker print64c (NO36c) does not reach our65c ask, but the simulator counts it. A YES taker print66c (NO34c) does reach that price, but its predicate excludes it. Neither a price touch nor a trade-through alone proves our queue fill.
2. The runtime computes fractional maker fees without the benchmark per-fill rounding. A one-contract50c example charges0.4375c instead of a rounded1c under the stated standard maker coefficient. Exact historical series schedules/rounding remain unverified; this example does not establish the total return correction.
3. The entry query filters on final market volume, which was not necessarily known at entry. Its nearest trade in a symmetric timing window can occur after a fixed decision time. Price is a trade-derived proxy rather than a demonstrated executable ask.
4. Fill share/queue estimates are assumptions. Source parameter grids, skipped series/words and same-sample sizing make an independent untouched test necessary. Words from the same speech are correlated.

These are concrete reasons not to import the advertised return claim as evidence. They do not prove that every possible mention strategy loses. Our separate preregistered NO-taker screen uses actual candle bid/ask sides, rounded benchmark taker fees, no final-volume filter and one contract per event. Its result is retained in ../mention_no_taker_20261016/FINDINGS.md.
