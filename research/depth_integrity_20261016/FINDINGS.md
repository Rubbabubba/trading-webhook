# Independent recorder integrity audit — October16 UTC

## What changed

Implemented an independent full-ladder auditor for the already cached sixth source file. This is source engineering and diagnosis, not a new trading test. Neither the frozen momentum nor ask-markdown result was replayed, overwritten or promoted. No prices were selected for entries, no outcomes optimized, and no orders submitted.

The fixed audit protocol was saved before its results. It reconstructs ladders from raw snapshots and deltas, checks reported before/after quantities and the derived top-three bid/complementary ask levels, and separates per-channel receipt ordering from global file ordering. Quantity tolerance is a fixed0.000001contract solely for serialized float arithmetic; prices compare exactly. Negative quantities fail. Invalid ticker state stays invalid until a new full snapshot.

## Authentic source results

Source SHA256`5b1605514724e7823152008cfd70f8c86b893d6778cf466314c9036c3d6a6d3c`,59,965,899compressed bytes;641,477records.

| Check | Observed |
|---|---:|
| First observed channels |19|
| Explicit snapshot/seq1 inferred restarts |18|
| Raw snapshots reconstructed |182,768|
| Raw deltas checked |100,223|
| Rows passing full independent reconstruction |282,929|
| Derived depth disagreements, rejected |62|
| Raw delta arithmetic disagreements in checked rows |0|
| Global file receipt-clock regressions |414|
| Within-channel receipt-clock regressions |0|
| Rows lacking verified channel history |305,647|
| Rows lacking current-epoch ticker snapshot |52,838|

The missing initialization rows are exclusions, not successful checks. Beginning mid-file with a delta does not establish history. A new snapshot with sequence1 after a prior sequence>1 starts an **inferred** channel epoch; an actual connection UUID is not present. Earlier ticker snapshots do not carry into that epoch. Other gaps or duplicate sequence1 invalidate the channel. This prevents both permanently rejecting a demonstrable snapshot restart and accidentally reusing stale ticker state.

## Concrete display defect

All62strict depth disagreements were classified as zero-quantity top-three truncation. In the first example, a raw delta removes the entire1000contracts at88c. The derived list still contains `[88,0]` among its top three, masking a positive87c level known from the raw ladder. The complementary ask similarly includes a zero-size level. Five bounded public-data examples are retained in`zero-level-diagnostic.json`.

The diagnostic requires every displayed positive level to match the independent ladder prefix and requires an explicit zero level; it cannot excuse different positive prices or quantities. It does **not** relax the strict audit or restore rejected rows. Earlier simulations already ignored zero quantities and used the remaining positive displayed levels; therefore this discovery is not proof that their selected entry prices were wrong. Their failures remain unchanged. It can understate available deeper depth and create false missing-book classifications; future research needs a separately registered reconstructed feed version.

Global reversals are cross-channel in this file. This supports an interleaving/scheduling explanation but does not prove exchange availability or our executable latency. Sorting retrospectively can introduce look-ahead and is not authorized by this finding. Any future source transform must explicitly retain receipt clocks, serialized order, inferred epoch and an availability rule; it must be tested on unused data before strategy interpretation. No historical execution gate is passed by these diagnostics.

## Verification and next work

Five synthetic tests pass: independent bid/ask reconstruction, raw arithmetic faults, sequence gaps/duplicates, restart isolation of ticker snapshots, timestamp fault visibility, and refusal to excuse incorrect positive prices as zero-level truncation. These tests do not prove profitability or broker execution.

Next feasible work is a separately registered feed version that rebuilds positive ladders from raw fields, retains uncertain connection/clock provenance, and preserves unavailable rows; then fixed hypothesis testing on the next unused window. Retain every earlier result. This offline audit changes no deployed worker, frozen registration, trial, risk limit, application model usage, service, schedule or execution authority. The profitable-system goal and the full engineering backlog remain incomplete.
