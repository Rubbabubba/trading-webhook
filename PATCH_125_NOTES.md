
# PATCH 125 NOTES

Goal:
- Isolate dry-run exits from lifecycle mutation
- Preserve startup restore classification for strategy plans vs recovered plans

Changes:
- Snapshot-restored plans no longer get force-labeled as recovered unless they were already recovered in snapshot
- Added startup_restore_classification metadata
- Dry-run full/partial exits stamp dry-run metadata only and defensively normalize any stale close_submitted state back to filled
- No changes to entry logic, reconcile behavior, or release gating
