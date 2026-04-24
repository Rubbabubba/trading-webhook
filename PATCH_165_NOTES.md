# Patch 165 — Single Source of Truth Cap Gate

Built from Patch 164 baseline.

## Purpose
Eliminate duplicate portfolio exposure gate behavior by routing portfolio-cap admission through one canonical helper.

## Changes
- Added `_canonical_portfolio_cap_gate(...)` as the single source of truth for portfolio cap admission.
- Canonical gate blocks only when projected candidate notional is greater than remaining configured capacity.
- Removes stale legacy `portfolio_exposure_limit` reasons before applying the canonical decision.
- Applies canonical gate in both live scan finalization and current runtime preview paths.
- Dashboard sanitizes fallback/historical scan candidates through the same canonical gate before rendering, so stale fallback rows cannot keep showing false cap rejects.
- Rebuilds rejection counts from sanitized candidate rows for dashboard consistency.

## No changes
- No entry sizing logic changes.
- No ranking changes.
- No broker execution changes.
- No exit logic changes.
- No accounting behavior changes.

## Validation
- `py_compile` passed for `app.py`, `scanner.py`, and `worker.py`.

## Env changes
None.
