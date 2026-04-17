Patch 150 — Truth Alignment & Manual Exit Reconcile

What changed
- Manual broker-side exits are now reconciled into strategy performance when an active plan disappears from broker positions without a pending entry.
- Dashboard readiness wording now distinguishes between "no new entry this session" and truly unproven trade path.
- Top Candidate Rejections now annotate that they are sourced from the last scan and convert stale `daily_halt_active` display text to `halt_cleared_since_scan` when the halt has already cleared.
- Dashboard hero pill updated to Patch 150 labeling.

What did not change
- Entry logic
- Exit trigger logic
- Scanner selection logic
- Reconcile safety behavior
- Broker execution path other than truth/accounting alignment for manual exits
