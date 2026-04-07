PATCH 120 — Profitability Recalibration Phase A

Goal
- Improve trade selection quality and candidate flow without touching lifecycle, reconcile, execution, or recovered-stop logic.

Changes
- Patch version updated to patch-120-profitability-recalibration-phase-a.
- Added ATR14/ATR%% diagnostics to daily breakout candidates.
- Added regime-aware breakout minimum rank score floors:
  - trend: 85
  - neutral: 88
  - defensive: 90
- Added modest strong-ATR breakout-distance relaxation so volatile leaders can remain eligible without loosening the system broadly.
- Recalibrated default trend/neutral breakout distance and 20D return thresholds:
  - trend breakout max distance default: 2.0%%
  - neutral breakout max distance default: 3.0%%
  - trend min 20D return default: 2.0%%
  - neutral min 20D return default: 1.0%%

Non-goals
- No changes to startup restore, reconcile, journal, duplicate prevention, quote system, entry execution, exit handling, or release gating.

Expected effect
- Fewer weak low-rank breakout selections in defensive tape.
- Better visibility into breakout volatility quality.
- Slightly better opportunity flow in trend/neutral tape without broad loosening.
