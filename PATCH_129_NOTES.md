# Patch 129 — Trade Quality Scoring + Smarter Candidate Selection Ordering

## Goal
Improve entry quality and tie-breaking without loosening core risk controls.

## What changed
- Added `selection_quality_score` for candidates.
- Breakout selection now prefers:
  - stronger rank score
  - stronger close-to-high behavior
  - stronger dollar volume
  - less extension beyond breakout
  - less correlation crowding
- Mean reversion candidates also receive a quality score, but no strategy rules changed.
- Approved candidate ordering now uses `selection_quality_score` first, then `rank_score`.
- Early-entry override candidate ordering now uses `selection_quality_score` first, then `rank_score`.

## What did not change
- lifecycle
- reconcile
- scanner scheduling
- release gating
- stop / target logic
- max open positions
- correlation group rules
- daily entry caps

## New envs
None required. Defaults are built in.

Optional tuning envs if needed later:
- `SWING_SELECTION_EXTENSION_PENALTY_WEIGHT`
- `SWING_SELECTION_RANK_WEIGHT`
- `SWING_SELECTION_CLOSE_TO_HIGH_WEIGHT`
- `SWING_SELECTION_VOLUME_WEIGHT`
- `SWING_SELECTION_CORRELATION_PENALTY_WEIGHT`
