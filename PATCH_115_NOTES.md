Patch 115 fixes the startup/runtime error introduced by Patch 114.

Changes:
- moves startup_restore_state() to run only after _plan_is_pending_entry and related helpers are defined
- preserves the broker-order adoption logic from Patch 114 without startup NameError
- updates patch marker to patch-115-startup-helper-order-adoption-fix
