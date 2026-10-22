# Settlement capital accounting foundation

Implemented a pure, read-only calculator for the queued settlement-duration capability. It records locked dollar-seconds, concurrent cash capacity, open locked capital and completed net cash by original entry. Explicit credit receipts, rather than outcome labels, release capital. Future receipts cannot contribute to the cutoff's delay summary. Lost principal is retained when a losing settlement releases zero cash.

Seven synthetic cases passed once: fees and receipt latency, future credit exclusion, future entry exclusion, losing settlement, overlapping capital, invalid identities/chronology/nonfinite numbers, and input-order independence. These fixtures establish implementation behavior only.

This is **partial engineering**, not a deployed collector, a new trading strategy, independently verified fills, a historical replay or profit evidence. No source data or prices were downloaded. Authentic attributable resolution/cash-credit provenance and a separately registered fixed portfolio comparison remain missing. Partial credits and event-level collateral netting are deliberately rejected by this version.

The higher-priority V12 execution package still lacks demonstrated positive executable edge; its frozen failed caps and the exhausted stream source/comparison evidence remain unchanged. Life OS owner UI redirected to sign-in during this run, so no owner-page engineering note is claimed. The current priority package and saved earlier receipt were read through the existing read-only backend inspection.
