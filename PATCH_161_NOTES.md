# Patch 161 — Strategy Sleeve Transparency

## Scope
Visibility-only patch. No execution, sizing, ranking, or exit behavior changes.

## Added transparency
- Exposure & Capacity now shows:
  - strategy_portfolio_exposure
  - recovered_portfolio_exposure
  - unmanaged_portfolio_exposure
  - total_portfolio_exposure_remaining
  - strategy_sleeve_cap
  - strategy_sleeve_remaining
- portfolio_exposure_limit dashboard reason text now includes projected notional and remaining sleeve/cap context when available.

## Intent
Make `SWING_PORTFOLIO_CAP_BLOCK_MODE=strategy` operator-visible so cap rejections can be validated from the dashboard without guessing.
