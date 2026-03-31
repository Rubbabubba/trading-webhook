
Patch 097 - Regime B mean reversion

Changes:
- adds additive Regime B strategy `daily_mean_reversion` for unfavorable regimes
- breakout stays primary; mean reversion is only selected when breakout has no eligible entries
- adds env-driven Regime B thresholds, reduced risk sizing, reduced symbol exposure, and max hold days
- adds persisted strategy performance tracking and a mean-reversion kill switch
- adds diagnostics endpoints `/diagnostics/regime_b` and `/diagnostics/strategy_performance`
- updates patch marker to `patch-097-regime-b-mean-reversion`
