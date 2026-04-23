Patch 158 — portfolio exposure truth alignment

What changed
- aligns candidate portfolio exposure rejection logic with the configured SWING_PORTFOLIO_CAP_BLOCK_MODE
- adds per-candidate portfolio exposure projection details:
  - current_total
  - current_strategy
  - projected_notional
  - projected_total
  - projected_strategy
  - remaining_capacity
  - blocking_basis
  - over_amount
- dashboard exposure panel now shows portfolio_exposure_remaining
- top candidate rejection display now annotates portfolio_exposure_limit with over-cap amount and basis when available

Safety
- no entry sizing changes
- no order submission changes
- no exit logic changes
- no accounting changes
