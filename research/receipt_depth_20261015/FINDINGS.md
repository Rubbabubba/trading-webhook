# Receipt-timestamped depth research — October15,2026

## Decision and goal state

No profitable strategy has been established. The fixed depth/momentum hypothesis does not advance. The owner's search objective remains active; do not mark it complete because a test or deployment completed. No production, Demo or paper order was submitted by this research.

## New usable source

The [public Kalshi orderbook alpha dataset](https://huggingface.co/datasets/lerchen3/kalshi-orderbook-alpha) was accessible without signup. Four compressed source files totaling232,200,851bytes were downloaded into a private workspace cache and verified against source-catalogLFS SHA256 values. Three consecutive files constitute the initial sample; the fourth was separately preregistered for completion of already selected exits only. No new entries were generated from the recovery file.

The initial three files contain1,565,782records. No subscription sequence gap or uninitialized delta was observed. After combo and invalid/empty-book exclusions,1,162,445noncombo book updates were usable for preliminary decisions. The first file alone was a35.656second startup burst, not five full minutes despite its filename. Its175,132tickers reflect an expanded historical universe, not the provider's later200market policy. Receipt clocks are recorder clocks, not guaranteed exchange latency or our execution times.

Prices/quantities were parsed independently from the recorded top-three bid ladders. Asks derive from opposite outcome bids; fractional quantity below one contract, empty sides and crossed books are excluded. No provider code was executed. License is labeled`other`; no production feed/data redistribution grant is assumed. Raw source files were not committed or redistributed; retain source URLs, sizes and hashes in the receipts.

## Fixed hypothesis and execution model

Before price inspection, froze a five-second decision grid,10second2cent mid momentum,80%top-level imbalance in the same direction, spread≤3c and entry10–90c. Require unbroken snapshot-initialized subscriptions and decision quote age≤1s. Conditional entry crosses a valid one-contract displayed ask after1second, no later than2seconds. Conditional exit crosses displayed bid30seconds after entry, with at most5seconds lateness. Costs are5c fee reserve+1c slippage per fill, with separate additional2c/5c-per-fill stresses. These reserves are assumptions, not independently verified market-specific historical fees.

Official public historical metadata for all42preliminary tickers was recovered in two batchedGETs. Three records were outside documented trading windows and18were duplicate event attempts. Enforce one attempt per official event, including failures; do not replace unfilled entries with later favorable opportunities.

## Result after exit-only tail recovery

| Measure | Result |
|---|---:|
| Preliminary ticker signals |42|
| Eligible distinct-event attempts |21|
| Complete conditional round trips |15|
| Entry attempts not filled within timing window |2|
| Exit paths unresolved beyond original deadline |4|
| Complete-subset gross before fee/slippage reserves |−54c|
| Complete-subset net after original reserves |−234c =−$2.34|
| Complete-subset net with extra2c/fill |−294c|
| Complete-subset net with extra5c/fill |−384c|
| Original-reserve net with unresolved positions stressed to full principal loss |−513c|
| Approximate independent-event95%lower bound, mean net |−18.736c/event|
| Actual broker orders/fills |0 /0|

These are conditional historical simulations, not realized P&L. Four unresolved exits remain unknown: they are neither zero-profit trades nor silently dropped. The−513c figure is a full-principal-loss stress, not an observed outcome. No positive subset, parameter inversion or thresholds were selected after seeing losses. This short period from one date is insufficient independent validation regardless of totals.

The fourth-file recovery processed117,830records with no sequence defects. It completed four of the selected event exit paths. Earlier late exits were never repaired with quotes outside their original deadlines. Original reports with11completed round trips remain retained, superseded by the15-complete recovery report for the same frozen attempts.

## Implementation correction and tests

Initial preliminary replay retained an earlier valid decision quote after an invalid/empty book arrived. Fixed by invalidating that quote and resetting local momentum history, without changing any strategy parameter. Original`preliminary-v1.json` is retained alongside corrected`preliminary-v2-invalid-book-corrected.json` and exit-only`preliminary-v3-tail-recovery.json`. This implementation repair is not another independent experiment.

Seven targeted synthetic tests passed: complement asks and minimum depth, crossed books, momentum/imbalance agreement, delayed entry and full round-trip costs, unresolved late exits, late entry/subscription-change rejection, and official trading-window validation. They do not prove authentic execution.

## Evidence files and next work

`review-v2-tail-recovery.json` is the current reviewed result. Source acquisition/catalog receipts, original registrations, repair erratum, original reports, recovered official metadata and all failed paths are retained. Frozen V14/V15 protocols and prior stopped trials remain unchanged. Deployed worker revision remains unchanged; this is offline research only.

The next search should use new independently registered source-grounded hypotheses and unused windows, preserving this failure rather than fitting or inverting it. The public macro replication repository advertises orderbook data but its current tree does not expose the documented`data/orderbook_data`path; verify actual data availability before claiming that source usable. Trade-only archives and account-gated feeds are not substitutes for recorded executable depth. Continue feasible engineering/research without new schedules, paid feeds, application model calls, risk-limit changes or live activation.
