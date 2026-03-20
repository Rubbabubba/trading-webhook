# Patch 54 - Promotion Failures Hotfix

Fixes a production regression in `/diagnostics/promotion_failures` and hardens scanner-universe integrity diagnostics.

What changed:
- fixed `NameError` in `/diagnostics/promotion_failures` by calling the correct nearest-pass builder
- hardened scanner universe runtime resolution so `/diagnostics/config_integrity` no longer reports a false zero-overlap state when the resolver returns empty during startup or fallback conditions
- bumped patch version to `patch-054-promotion-failures-hotfix`

Why this patch exists:
- Patch 53 introduced a broken function reference inside the promotion-failure endpoint
- The config integrity route was technically correct about the current runtime list being empty, but operationally misleading because the environment scanner universe was already configured

Expected result:
- `/diagnostics/promotion_failures` returns JSON instead of 500
- `/diagnostics/config_integrity` shows the effective scanner universe with real overlap against `ALLOWED_SYMBOLS`
- route set and all prior diagnostics remain intact

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


## Patch 55 - Scan Truth Alignment
- Trade path now prefers the latest completed scan with real candidate data instead of the most recent skipped scan.
- Promotion failure diagnostics now report scan_source and candidate_source and expose candidates outside the active runtime universe.
- Config integrity now validates latest completed scan evidence against the active runtime scanner universe.


## Patch 56 - Static Universe Fix
- Fixed `universe_symbols()` so `SCANNER_UNIVERSE_PROVIDER=static` honors `SCANNER_UNIVERSE_SYMBOLS` when configured instead of silently scanning the alphabetically first allowed symbols.
- Fixed runtime universe diagnostics to resolve from the same universe function used by the scanner.
- Added explicit `symbols`/`symbols_total` to swing scan summaries and trade-path snapshots.
- Added candidate-vs-scan universe mismatch reporting in trade path and promotion failure diagnostics.


## Patch 57 - Filter Pressure Lab
- Added /diagnostics/filter_pressure to show baseline eligibility, market-gate-free eligibility, relaxed threshold counterfactuals, and single-filter-removal pressure.
- Embedded filter_pressure inside /diagnostics/promotion_failures so latest non-promotion now includes counterfactual pass counts.
- Purpose: quantify whether the current strategy is blocked primarily by market gate, entry geometry, quality filters, or combined strictness.


## Patch 59 - Universe Shadow Lab
- Added /diagnostics/universe_shadow to compare current runtime universe vs expanded allowed-symbol universe under the same swing logic.
- Quantifies whether the narrow 9-symbol runtime universe is suppressing candidate discovery relative to the broader allowed list.
- Patch version: patch-059-universe-shadow-lab


## Patch 060 - policy shadow lab
- Added `/diagnostics/policy_shadow` to compare constraint pressure between runtime universe and expanded allowed universe.
- Refactored filter-pressure logic into reusable payload generation so policy analysis can be run on alternate universes.
- Exposes minimum unlock combo and best unlock candidates outside the runtime basket.


## Patch 61 - universe redesign advisor
- Added `/diagnostics/universe_recommendation`
- Converts policy-shadow evidence into an actionable runtime-universe redesign plan
- Recommends additions, removals, and a drop-in `SCANNER_UNIVERSE_SYMBOLS` env string based on lowest blocker count and highest rank score
- Purpose: decide the next runtime universe before touching production filter policy


## Patch 062 - Regime Engine Mode Switching
- Added regime mode switching with trend, neutral, and defensive operating modes.
- Added /diagnostics/regime_mode endpoint.
- Swing candidate evaluation now uses mode-aware thresholds for breakout distance, close-to-high, momentum, and trend requirements.
- Weak tape can route into defensive mode instead of always hard blocking entries when regime-mode switching is enabled.
- Scan summaries now include regime_mode and mode thresholds for diagnostics.
