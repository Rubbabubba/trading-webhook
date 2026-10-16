# Prospective source integrity and overlap audit

Read-only, preregistered inspection of the deployed separate continuing-capture SQLite archive. No source downloads, model calls, orders, fills or trading-rule changes.

At2026-10-16T03:53:07UTC, an independent standalone auditor verified205 compressed blobs (187182 raw bytes),686 quote records across110 events,82 poll/page receipts and37 public trades across7 events. All tested content hashes, receipt clocks, identities, normalized book domains, stored usability classifications and trade-to-raw-page economic fields were consistent.392 quotes met the archive's two-sided one-contract depth classification; that classification alone is not execution qualification.

**Coverage remains insufficient for a short-latency fillability claim:** none of37 trades had its poll-associated prior quote within2 or5 seconds. All37 were inside the recorded30-second window, with21 YES taker and16 NO taker flags, all explicitly non-block. Seven sampled events are not110 independent trading outcomes. Missing windows and quote-to-trade intervals must not be filled forward as executable depth.

One quote's receipt was after its metadata's returned close time. Its raw evidence is retained, and any strategy replay must apply the exact lifecycle cutoff rather than treating active-status/book-shape flags as sufficient. This is not a realized loss or proof a broker accepted a post-close order.

This audit verifies only the enumerated raw-record consistency. It does not verify publication completeness, continuous quotes, queue position, our activation/fill, historical fees, settlements or profit. The archive remains append-only on the existing Demo disk. No new trading authority follows. Future price replays must freeze their own rule/selection/cost protocols and preserve censoring/clock ambiguity.

## Genuine timing milestone

Fresh native signal70 (KXLIGAMXGAME-26OCT21TOLTIJ-TOL), paired with timely V12 source1285, completed5/30/300-second observations. Independent timestamp arithmetic gives lateness4.585443/4.897660/2.251738seconds, all within the unchanged5-second allowances. The worker revision remains bbbb166f49ec4433206e7d5416def1de04825ad1, Demo, productionfalse.

The timing-safe V12 ledger now has1complete signal/1event out of9signals. V15 has2complete/2events out of70signals, with68missing-horizon observations retained. This verifies a genuine complete measurement path; it does not establish reliable fleet-wide timing or strategy profitability. On this new path V15 stressed quote markouts are0c at every horizon, frozen V12 comparison2c, and V10 no-trade0c. These are passive quote comparisons, not realized returns or assumed broker fills. The candidate release dossier still blocks promotion.

A separate nearest-prior-quote query across all retained usable quotes (cutoff03:53:07.474572UTC) also finds0of37trades within2or5seconds, and37within30seconds. Thus the freshness gap is not merely an artifact of associating trades with their originally polled quote. This remains sparse source coverage, not proof of no market liquidity.
