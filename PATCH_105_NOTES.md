# Patch 105 – scan submit source fix

- Preserves scanner submit source when calling `execute_entry_signal()`.
- Fixes early override live path being unintentionally forced back to `worker_scan`, which re-enabled effective dry-run.
- No strategy, regime, sizing, or dashboard changes.
