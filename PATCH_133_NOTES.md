Patch 133 — scan telemetry release fallback fix

What changed
- Reverts the bad Patch 132 runtime-preview release fallback that made endpoints slow and unstable.
- Uses recent scanner telemetry as a lightweight release-gate fallback when LAST_SCAN is stuck on scan_exception.
- Preserves Patch 130 extension discipline and Patch 129 selection ordering.
- Fixes artifact packaging: root folder is patch133_build and PATCH_133_NOTES.md is included.

What did not change
- Lifecycle, reconcile, exits, stops, targets, correlation rules, entry caps, or open-position limits.
