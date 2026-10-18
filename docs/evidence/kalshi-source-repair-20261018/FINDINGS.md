# Demo order-book and continuous-source repair, October 18

## Verified release

The main source/pipeline release `fe300d093a1a3d51438979f99e35213de349dec8` was Live on the existing Demo service, deployment `dep-dbaiqgk9v7es738mbgtg`. Life OS independently accepted it at 20:35:19 UTC. Timeout follow-through `e375ed9b481143185f3d155f2c6707a60e954fdb` was accepted at 20:44:19 UTC. Final reporting follow-through is `a85c0201d596190a6d88baab19e157394d68ce73`; its exact deployment receipt is retained separately. The final code passed 249 distinct synthetic regression cases. Earlier overlapping test totals are not added to this count.

Implemented and deployed:

- Optional scanner HTTP 500/502/503/504 isolation; source failures remain visible and do not become invented market frames. Broker reconciliation remains intact. A further concrete defect was fixed: an unavailable book no longer throws out of the working-order path before its original timeout cancellation. Ten actual worker-loop synthetic cases verify expired/unexpired orders across missing books and four server error classes. No new order authority, price fallback, timeout extension or changed exit threshold was introduced.
- Continuous official Demo public WebSocket collection in the existing worker, bounded immutable compressed storage, raw-message hashes, connection epochs, reconnects, disk/capacity stops and sanitized health.
- Separate classification of exact unavailable snapshots. Missing ladders are not zero-price books. Invalid sequences, identities, clocks and negative levels still fail closed. Original strict decoder defaults and old evidence remain intact.
- Sampling recovery uses distinct signed unavailable observations, not repeated reads of a cached failure. Original two-failure and full-window conditions remain.
- A separate prospective comparison ledger invokes unchanged native V10/V12/V15 formulas, preserves input provenance and exact timing gates, and never merges old REST observations or submits orders.
- Independent read-only Fraction reconstruction and comparison-input provenance audits.
- Quiet-market heartbeat flushes the last burst's on-disk status. Source timestamps and archive counts do not advance without a genuine message.

## Genuine observations

The earlier REST failures were reproduced on the documented single/batch endpoints, both supported Demo hosts, and an authenticated read. Listings and metadata succeeded. Their bounded body reported `internal_server_error`. We repaired worker behavior; we did not claim to repair Kalshi's server. After the final deployment, 13 natural REST reads succeeded with zero read failures in that process at 20:37:59 UTC.

The new stream acknowledged both public subscriptions, retained six usable books and two exact unavailable snapshots, then continued receiving deltas. The first independent audit verified 16 immutable messages and 12 reconstructed book states with consistent sequence and hashes. A separate audit verified all 11 then-accepted comparison inputs against their exact raw source hashes, epochs, SID/sequence, independently reconstructed Fraction ladders and one-second spacing. Neither audit writes the running source database.

The fresh monitor packet was 48,711 bytes, below the unchanged 64,000-byte limit, with no faults and production execution false. Life OS saved the engineering receipt at 20:37:32 UTC. Screenshots and original terminal receipts are retained here.

The recorder naturally renewed its connection without a deployment: a second audit verified 28 records/22 reconstructed states across two epochs. After the timeout deployment replaced the process, a third audit verified the entire 38-record/30-state prefix across three epochs, retaining the interrupted old connection explicitly. All eight selected books were usable in that new connection; the separate comparison ledger restored and reached 20 accepted inputs, zero signals. No position was opened for this restart observation.

Final release `a85c0201d596190a6d88baab19e157394d68ce73` is Live in deployment `dep-dbaj025g1s2s73c9plbg`. A fresh 20:47:27 UTC worker status had no errors; one additional zero-AI deterministic deployment check generated a 48,711-byte fault-free packet accepted by the configured Life OS publisher at 20:47:40 UTC. The signed-in Life OS page independently showed the exact revision and timestamp. The final source audit verified 48 immutable records/38 Fraction-reconstructed states across four epochs, and the input audit verified all 35 then-accepted comparison inputs. Source status during the quiet period advanced process health while retaining its actual earlier latest-message timestamp. Zero public trades and zero owned fills remain; these are not profit observations.

## Failures preserved

The initial deployed version correctly rejected an unavailable snapshot with an empty identity. Availability v2 then exposed a valid UUID with no ladder fields, and an overly broad error classifier called its source error storage capacity. Availability v3 is separately registered, preserves these failures, handles only the exact unavailable shape, and never treats a SourceFault as storage exhaustion. No prior database, stopped trial, loss, incomplete markout or failed historical study was reset.

## Completion boundary

The continuous-worker adapter and source/formula pipeline previously missing from the October 16 foundation are now implemented and deployed. This is source and measurement engineering, not executable-profit proof. Public trade overlap and owned queue/acknowledgement/fill/fee/exit evidence remain absent from this new ledger. Its cached event metadata is explicitly not independently current. Zero signals is not a zero-return trading record.

The current durable inventory still contains 14 queued strategy-specific capability packages. Their remaining work includes authoritative publication ingestion and rule matching (economic releases, Treasury, polls, NHC, final scores and external consensus), paired execution/capital accounting, and strategy-specific adapters. The generic recorder does not implement those requirements. Failed markdown/momentum studies and the algebraically infeasible ordinary same-contract complementary-ask proposal remain rejected evidence, not invitations to fit parameters. This release does not certify the whole research backlog complete or any live-money strategy.

No new service, schedule, application model call, paid feed, order, risk limit or live grant was introduced. Existing Demo/paper restrictions and strategy-specific owner first-live approval remain.
