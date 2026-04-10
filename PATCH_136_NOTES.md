# Patch 136 — Global stop enforcement / profit-lock separation

## Purpose
Finalizes long-position stop enforcement so intentional break-even / partial-profit behavior uses `profit_lock_price` instead of overwriting `stop_price`.

## Changes
- `PATCH_VERSION` set to `patch-136-global-stop-enforcement-profit-lock-fix`
- break-even, partial-profit, and time-exit-grace now arm `profit_lock_price` at entry instead of pinning `stop_price` to entry
- normalization now migrates entry-pinned long stops back to `initial_stop_price` when a winner-lock is intentional and preserves the winner lock through `profit_lock_price`

## Guarantees
- protective long stop remains below entry unless an explicit strategy path intends otherwise
- winner protection can still lock break-even / profit using `profit_lock_price`
- migration is idempotent and safe across repeated exit-worker cycles
