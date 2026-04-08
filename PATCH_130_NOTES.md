# Patch 130 Notes

Patch version: `patch-130-extension-discipline-breakout-distance-control`

## Purpose
Tighten selection discipline for overextended breakout candidates without changing core lifecycle, reconcile, or release behavior.

## Changes
- Added nonlinear breakout extension penalty to selection quality scoring
- Added hard overextension penalty once breakout distance exceeds the effective breakout ceiling
- Preserved existing candidate generation, release gating, lifecycle, and risk controls

## New optional envs
- `SWING_SELECTION_EXTENSION_HARD_RATIO` (default `1.0`)
- `SWING_SELECTION_EXTENSION_HARD_PENALTY` (default `3.0`)

## Expected impact
- Cleaner names should beat highly extended breakouts more often
- Correlation blockers and daily entry caps remain unchanged
