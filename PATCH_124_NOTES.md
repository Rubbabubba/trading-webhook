PATCH 124 NOTES

Goal:
Clean up exit semantics after Patch 123 by separating protective stops from managed winner profit-lock logic.

Key changes:
- Introduced explicit long-plan normalization:
  - stop_price = protective stop
  - profit_lock_price = managed winner floor
  - take_price = final target
- Migrates legacy above-entry long stops into profit_lock_price.
- Prevents long protective stops from living above entry.
- Evaluates exit reasons separately:
  - stop
  - profit_lock_stop
  - target
- Preserves startup restore, reconcile, entry logic, and journal behavior.

Expected result:
- No repeated dry-run false stop churn caused by a winner stop being treated like a loss stop.
- AMD-like recovered winners can carry locked-profit semantics without corrupting stop logic.
