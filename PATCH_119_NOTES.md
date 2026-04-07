# Patch 119 Notes

## Fixes
- Corrected `PATCH_VERSION` to Patch 119.
- Fixed startup restore ordering so `_restore_recovered_plan_protection` is defined before `startup_restore_state()` runs.
- Preserved Patch 118 recovered stop restoration logic.

## Expected outcome
- No startup `NameError` for `_restore_recovered_plan_protection`.
- Recovered long plans restore `stop_price` from `initial_stop_price` when stop was neutralized to entry.
