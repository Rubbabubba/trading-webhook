PATCH 126 NOTES

Goal:
Fix restore classification leakage and stale exit-worker readiness blockers without touching entry, reconcile, or stop/target semantics.

Changes:
- Snapshot restore now only keeps recovered=true when the plan actually looks recovered (RECOVERED signal/strategy or explicit recovered classification).
- Normal strategy plans restored from snapshot are reclassified to strategy and have recovered flags removed.
- Startup recovered_from_snapshot_count now increments only for true recovered plans.
- Release workflow and release gate snapshots normalize worker-related unmet conditions against current worker heartbeat state.

No changes:
- Entry selection
- Scanner behavior
- Reconcile plumbing
- AMD stop/target semantics
