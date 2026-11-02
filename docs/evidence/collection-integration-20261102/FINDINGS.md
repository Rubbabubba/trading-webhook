Collection integration release, November 2

The Demo maker attaches one bounded background metadata writer to its existing
metadata GETs. It performs no extra request, retry, fee assumption, broker action
or order. Exact decoded response entity bytes and actual local request/receipt
clocks are retained in a new separately registered archive. Missing HTTP Age is
explicit null/http_age_unknown; even a timely response is not authenticated
publication, historical availability, a fee quote, queue/fill or economic proof.

The single writer owns an OS lock before registration/SQLite; the trading thread
only queues at most eight bounded 64KiB entities without waiting for disk. The
immutable start survives boots. Admission ends at the original six-hour, 1000
receipt, 4MiB payload, 8MiB database/journal or 128MiB-free-floor boundary. Storage
stops are durable; no old collector is resumed or reset. Process-local rejection
and queue-drop counters are explicitly labelled, not claimed as full history.

The Life OS projection gives Hermes the count, timestamps, hashes and explicit
unknown/stopped/fault states, never response bodies or a new trading permission.
Empty paper observer summaries now reserve context for genuine cancel receipts.
Full accepted audits remain retained. The authentic read-only preview displayed
the AAPL cancel request20:55:11.871105UTC and first terminal observation
20:55:27.322448UTC (15.451343 seconds), exact broker ID
d77f582f-898a-4f46-8960-1db44f2a5bd2, canceled/unfilled. All three identities,
cash differences and account stops were unchanged; 6429 bytes under the existing
6500-byte diagnostic limit. This is a worker observation, not exchange atomicity
or a guarantee on later cancellations. Existing remains unreconciled/stopped2.23.

Final validation:926 full backend unittest executions,92 isolated installed-wheel
checks plus installed dashboard/hash smoke; real pinned Hermes lead and both
specialists completed synthetic Responses/history/collection/verifier workflow
within unchanged request caps and with explicit open obligations. Provider calls
were simulated, not application API calls.16 new collector tests include a real
background SQLite writer, physical/storage stops, lock contention, restart and
unknown-Age handling. Final Kalshi pytest:687 passed,1 deselected. The excluded
unchanged legacy test_quote_failure_rolls_back_and_evidence_persists fails because
its production-only kalshi_production_risk module is absent from this branch;
the initial broad result (686 passed/1 failed before the final added thread case)
is preserved. Neither the test nor the production-risk path was edited. The
existing maker, monitor, source, strategy, journal and reconciliation suites pass.

Initial restricted backend tests hit localhost WinError10013, then passed with
scoped local test-server access. Default pytest temporary-directory setup failed;
fresh bounded writable fixtures passed. First no-isolation wheel build lacked
setuptools; standard isolated build succeeded. A large synthetic history delta
exceeded its unchanged 3000-byte cap; removal of repeated descriptive scope and
empty cause arrays fixed it while retaining all cash/identity/guard/stop fields.
All initial failures are retained in the chat's release artifacts.

Release integration and synthetic checks do not reconstruct old missing marks,
exceptions, fee records or queue observations. Genuine new collector responses,
installed revisions and fresh private Life OS acceptance must be checked after
deployment. Existing's actual broker-charge evidence, original history limits,
executable economics and all frozen strategy/live gates remain open obligations.
