# Patch 104 Notes

Patch marker: `patch-104-price-freshness-threshold-fix`

## Purpose
Fix the stale-price false negative that remained after Patch 103.

## Root cause
`get_latest_quote_snapshot()` compared quote age against `ENTRY_MAX_PRICE_AGE_SEC`, but the configured environment variable is `ENTRY_PRICE_MAX_AGE_SEC`. Because the wrong variable name was referenced inside a try/except, the exception path silently forced `fresh = False` on otherwise valid, recent quotes.

## Changes
- changed the freshness threshold reference from `ENTRY_MAX_PRICE_AGE_SEC` to `ENTRY_PRICE_MAX_AGE_SEC`
- added `freshness_reference` and `freshness_threshold_sec` into `quote_debug` for clearer diagnostics
- bumped patch marker to `patch-104-price-freshness-threshold-fix`

## Expected result
On the next fresh AMD-style event with a tight spread and recent quote, the order path should move past `price_stale` and either:
- build a dry-run payload cleanly,
- submit successfully if permitted, or
- fail with the next real broker-side or gating reason.
