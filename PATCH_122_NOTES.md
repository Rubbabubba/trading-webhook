PATCH 122 NOTES

Goal:
Improve exit quality without touching lifecycle, reconcile, startup restore, or recovered-stop enforcement.

Changes:
- Added optional partial profit-taking for swing positions at +1.0R by default.
- Added time-exit grace for stronger winners so they are not forced out immediately at max hold.
- Tightened stop to at least breakeven when partial profit or time-exit grace activates.
- Left hard stop / target / stall exits intact.

Defaults:
- SWING_PARTIAL_PROFIT_ENABLED=true
- SWING_PARTIAL_PROFIT_R=1.0
- SWING_PARTIAL_PROFIT_FRACTION=0.5
- SWING_PARTIAL_PROFIT_MIN_QTY=0.25
- SWING_TIME_EXIT_GRACE_R=0.75
- SWING_TIME_EXIT_GRACE_DAYS=1
