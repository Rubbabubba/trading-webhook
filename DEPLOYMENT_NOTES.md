Patch 53 – Promotion Probability Ladder + Scan-to-Scan Delta Diagnostics

What this patch adds
- /diagnostics/promotion_momentum
- /diagnostics/fastest_improvers
- Scan-to-scan delta tracking for cohort names already being tracked by the diagnostics layer
- Momentum classification: improving / flat / deteriorating
- False persistence detection for names that recur but are not actually tightening

What to test after deploy
- /diagnostics/promotion_momentum
- /diagnostics/fastest_improvers
- /diagnostics/cohort_scorecard
- /diagnostics/promotion_watchlist

Expected behavior
- Existing Patch 52 endpoints remain available
- New endpoints rank symbols by scan-to-scan improvement instead of recurrence alone
- False-persistence names should surface separately from fastest improvers
