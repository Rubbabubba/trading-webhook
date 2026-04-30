# Patch 171B — Dashboard Snapshot Fast Path NameError Hotfix

Built clean from the attached Patch 170 baseline.

Changes:
- Fixes dashboard 500 caused by direct undefined DAILY_LOSS_LIMIT reference.
- Uses safe daily_halt_truth_snapshot fields for daily stop/loss display.
- Makes /dashboard snapshot-first, using persisted position snapshot instead of live broker/reconcile calls during render.
- Merges cached broker positions with active plan risk fields for Qty, Entry, Stop, Target, Signal, Status.
- Adds short in-process dashboard HTML cache.

No changes:
- Entry logic
- Exit logic
- Scanner logic
- Risk logic
- Order placement
- Reconcile diagnostics endpoint
