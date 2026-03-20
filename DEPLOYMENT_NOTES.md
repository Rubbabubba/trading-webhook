# Patch 53 - Baseline Integrity + Trade Path Proof

Adds hard diagnostics for baseline trust and execution-path evidence:
- `/diagnostics/build` for patch/build/artifact fingerprint
- `/diagnostics/routes` for registered route manifest and missing-expected-route detection
- `/diagnostics/config_integrity` for allowed-symbol vs active scanner-universe mismatch detection and release-gate policy warnings
- `/diagnostics/trade_path` for selected -> entry -> exit proof coverage from lifecycle and decision evidence
- `/diagnostics/promotion_failures` for explicit why-no-promotion analysis from the latest scan
- scanner candidate selections now emit structured `CANDIDATE` decisions for auditability

This patch does **not** relax strategy quality or arm live trading. It is a baseline-integrity and execution-proof patch.

Patch 42 (drop-in)

Purpose
- Clarify readiness semantics when the system is healthy but market data is not currently tradable.
- Separate market-closed from quote/data-quality failures in readiness output and dashboard messaging.

What changed
- Added `_readiness_data_feed_state()` classification helper.
- `/diagnostics/readiness` now exposes:
  - `data_feed_reason`
  - `data_feed_label`
  - `data_feed_detail`
- Dashboard readiness assessment now distinguishes:
  - `HEALTHY / MARKET CLOSED`
  - `HEALTHY / DATA NOT TRADABLE`
  - `FULLY PROVEN`
  - `HEALTHY / PATH NOT PROVEN`
- Guarded Live Path table now includes `data_feed_reason`.
- Guarded Live Path blockers now use the specific readiness data-feed reason when market is open but quotes are not tradable.

Expected result
- During market hours with bad/stale quotes, the dashboard should no longer imply a generic non-tradable state; it should explicitly point to the quote/data issue.
- Outside market hours, the dashboard should explicitly show the market-closed condition instead of mixing it with quote tradability.


## Patch 45

Adds shadow regime analytics for the swing scanner.

Included changes:
- Identifies shadow candidates whose only blockers are market-gate reasons (`weak_tape`, `index_alignment_failed`).
- Persists shadow candidate details into scan summary and candidate history.
- Exposes shadow candidate data on `/dashboard` and `/diagnostics/candidates`.
- Keeps live release controls unchanged. This is analytics-only and does not relax any gate.


## Patch 51 Fixed
- Adds cohort evidence persistence backed by `/var/data/cohort_evidence_state.json`.
- Persists/restores `candidate_history` and `last_swing_candidates` inside `scan_state.json`.
- Adds `/diagnostics/cohort_evidence` for multi-scan cohort persistence evidence.
- Adds `/diagnostics/system_state` as a compatibility alias to `/diagnostics/state`.


## Patch 52
- Adds `/diagnostics/cohort_scorecard` with recency-weighted cohort ranking, bucketed watchlists, and promotion states.
- Adds `/diagnostics/promotion_watchlist` with top promotion candidates, separated into breakout, alternate-entry, and mixed-signal watchlists.
- Preserves all Patch 51 diagnostics and routes.
