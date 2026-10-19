# Bounded comparison successor v2

Registered prospectively October 19, 2026 before deployment. Old v1 is retained read-only: 16,777,216 bytes, SHA256 1c0d1f1654930d6f1094d7cf708a36796dbcad9cc7ff4c1b9df9fd130c935bd3, 4,073 inputs, 17 signals, zero complete paths. No old observation is imported.

V2 uses its own compressed ledger with the same 16 MiB maximum and 128 MiB shared free-disk floor. Entry admission stops at 12 MiB, 10,000 inputs, 100 signals or 72 hours. It admits at most one pending 305-second signal window at a time. These sampling restrictions are a separate measurement protocol; frozen V10/V12/V15 formulas, costs and original gates are unchanged.

A separate existing-worker thread follows persisted signal deadlines through source reconnects, target rotation and process restoration. Only due public Demo order-book GETs are allowed: at most 900 total, three per horizon, one per two seconds, two-second timeout, HTTP Age and request duration at most two seconds. Marks require a response inside the unchanged 5/30/300-second windows plus at most five seconds. There is no retroactive fill, cached-price resampling or backfill. Raw response hashes/compressed bytes, attempt times, HTTP age and explicit missing terminal windows are retained. HTTP freshness is not a verified exchange timestamp.

Completed paths compare quote outcomes only. They are neither actual trades nor settlement observations and do not establish executable profit, queue position or promotion readiness. Cached entry metadata is explicitly not independently current. Old source archive and its limits remain unchanged; source capacity can stop new entries while the outcome thread finishes already admitted signals.

65 focused source/comparison/monitor regressions passed before the final receipt-detail addition. Genuine deployed outcomes and final exact revision will be recorded separately after deployment.
