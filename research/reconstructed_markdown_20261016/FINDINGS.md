# Reconstructed-feed ask markdown v2 — October16 UTC

## Decision

No profitable executable strategy established. The separately registered reconstructed-feed test completed its whole planned window but its fresh-event conditional entries lost65c **before** fees or slippage and101c under the frozen original reserve. Do not advance this tested version. The earlier failed momentum and ask-markdown studies remain unchanged; this is not their replacement or a parameter search.

## Registration and scope

Frozen at2026-10-16T00:27:12.752539Z before downloading the next six unused chronological depth files. Registration SHA256`c68a56bc8622f8dfd006a2bebeed9f205d1ed17c03cbf0e35549080571f5e5a8` remained unchanged through review. New files7–12 total402,469,794compressed bytes; all publishedLFS hashes verified. Six previously cached files are warmup only. All12files total762,754,077bytes. Raw files remain private outside Git.

The entry hypothesis and parameters are unchanged: ask markdown≥3c/10s, bid holds/strengthens, spread≤3c, ask10–90c,5sdecision grid, source quote age≤1s,1sdelayed entry with at most1s lateness, no chase, one earliest event attempt and hold to official settlement. Original costs remain5cfee+1cslippage per entry with extra2c/5c stresses. Thirty-three official events attempted by the previous two studies were excluded before outcomes, preventing recycled events from appearing independent.

## New source version and engineering evidence

The new feed rebuilds full positive ladders from raw snapshots/deltas, checks before/after arithmetic, and validates derived levels. It accepts the already diagnosed zero-level rendering truncation only when every displayed positive level matches the independently reconstructed prefix. Other mismatches invalidate the ticker until a new full snapshot. Explicit sequence1 snapshots infer a new channel epoch; old ticker state never carries forward. Within-channel clock reversal invalidates a channel. No source rows are sorted or moved backward in modeled availability.

For cross-channel interleaving, a row is usable no earlier than the maximum receipt timestamp seen at that point in serialized order. Original receipt clocks remain distinct and freshness checks use both. This is a modeled availability fence, not measured executable latency or a verified connection UUID.

The completed replay processed7,208,066records:544,082raw snapshots,6,663,981raw deltas and3universe rows. All7,208,063book rows passed the reconstructed-state checks. There were37inferred restarts,3,056global timestamp regressions and3,482,402zero-truncation rows rebuilt from raw data; maximum availability fence was7.615694ms. These counts include warmup. There were6,251,583usable noncombo quote rows; unusable/empty/crossed/sub-one-contract quote conditions were not manufactured into entries. No fatal source error truncated the registered window.

Decision window: July19,2026 approximately02:36:40.835996–02:48:19.457630UTC, about11.64minutes. This is one date, not multiple independent sessions.

## Reviewed fresh-event result

101preliminary ticker attempts. Official metadata rejected52attempts from prior experiment events,8outside documented trading windows and23duplicate fresh-event attempts. Remaining18official-event attempts comprise6conditional entries with numeric/label settlement agreement,9late/unfilled entries and3price-guard rejections. Failed earliest attempts were never replaced by later favorable entries.

| Conditional accounting | Result |
|---|---:|
| Completed conditional settlement entries |6|
| Gross payout less entry prices |−65c|
| Net after original fee/slippage reserves |−101c|
| Net with extra2c per entry |−113c|
| Net with extra5c per entry |−131c|
| Unknown settlements among observed conditional entries |0|
| Actual orders / broker fills |0 /0|

Three bounded official historical GETs returned all71uncached contracts; earlier metadata was reused. A separate direct accounting check re-read the official records and verified event deduplication, prior exclusions, open/close/occurrence guards, delayed-entry/price guards, settlement labels/numeric values and each scenario total. See`independent-accounting.json`. This is an accounting cross-check, not an independent broker execution record.

The normal official-event proxy lower bound is negative69.1973c at original reserves. It is not valid parent-event/session-cluster inference: related games and crypto thresholds can share outcomes. The proper session-cluster bound is unavailable from one date. Sample, event diversity, independent holdout, fee provenance, contemporaneous rule publication and actual execution gates remain unmet regardless of costs.

## Verification, limitations and next work

Ten synthetic tests passed, covering default strict audit behavior, raw reconstruction, positive-depth mismatch rejection, inferred restart isolation, cross-channel availability, within-channel invalidation and an end-to-end check that a later quote cannot leak into an earlier grid decision. Tests do not satisfy market or profit gates.

The [official July7fee schedule](https://kalshi.com/docs/kalshi-fee-schedule.pdf) was reviewed separately. Its general formula/rounding and series-specific applicability are recorded in`fee-source-review.json`; the frozen reserve was not reduced. This sample already loses before costs, so a cheaper fee assumption cannot rescue its observed gross result. Actual historical fills/fees are still unverified.

Retain all failure records and source limitations. Do not optimize/invert the inspected windows or rerun this entry family until a historical winner appears. Next work should test a distinct source-grounded economic or structural hypothesis on unused evidence with independently verified rule/data timing and sufficient independent dates. Two additional dataset leads were reviewed in`additional-source-review.json`; neither currently supplies a verified free multi-day executable-depth holdout. No signup, purchase or model call was made.

This work is offline only. Deployed worker remains the last verified8e6dd8d revision; no new deployment, service, schedule, risk change, Demo order or live activation was made. Full engineering and profitable-system objectives remain incomplete and active.
