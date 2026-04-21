# Patch 157 — Opening Window Freshness Alignment

## Purpose
Fix opening-window control-plane drift where the dashboard could show stale regime data and let scanner telemetry masquerade as a fresh in-session scan.

## Changes
- Refresh regime snapshot automatically when it is missing, incomplete, stale, or from a prior session during the opening window.
- Add opening-window detection to session boundaries.
- Prevent scanner telemetry fallback from being treated as a completed in-session market scan when the last scan reason is still `outside_market_hours`.
- Preserve existing trading, accounting, and lifecycle behavior.

## No-drift / no-regression scope
This patch does not change entry selection, exit logic, broker order submission, or accounting quarantine behavior.
