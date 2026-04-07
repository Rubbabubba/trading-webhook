PATCH 121 NOTES

Goal:
Improve win rate without opening the floodgates.

Built directly on Patch 120 baseline.

Changes:
- Added breakout quality volume confirmation:
  - SWING_BREAKOUT_QUALITY_MIN_AVG_DOLLAR_VOLUME default = 30000000
  - rejects daily breakout candidates with rejection reason: low_volume
- Recalibrated defensive breakout rank floor default:
  - SWING_DEFENSIVE_BREAKOUT_MIN_RANK_SCORE default 90.0 -> 88.0
- Recalibrated defensive close-to-high default:
  - SWING_DEFENSIVE_MIN_CLOSE_TO_HIGH_PCT default 98.5% -> 98.25%

No changes:
- lifecycle
- reconcile
- startup restore
- recovered stop enforcement
- execution paths
- journal / release gating

Expected effect:
- keep weak low-quality names filtered out
- admit more borderline but structurally valid defensive-mode breakouts
- improve win-rate orientation without materially loosening the system
