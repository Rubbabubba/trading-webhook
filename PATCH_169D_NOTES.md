# Patch 169D — Dashboard Format KeyError Clean Hotfix

Built cleanly from the attached Patch 169C baseline.

## Scope
- Fixes `/dashboard` 500 caused by Python `.format()` interpreting CSS braces such as `{color-scheme:dark}` as format fields.
- Preserves Patch 169C dashboard fast path and heavy diagnostics split.

## No behavior changes
- No entry logic changes.
- No exit logic changes.
- No scanner changes.
- No reconcile changes.
- No risk/accounting changes.

## Env changes
None.
