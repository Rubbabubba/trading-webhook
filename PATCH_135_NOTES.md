# Patch 135 Notes

- Fixes `/dashboard` crash caused by `aligned_last_scan` being referenced before definition.
- Defines `aligned_last_scan` in the dashboard route using freshness-aligned scan truth with safe fallback to `_aligned_last_scan_display(session)`.
- No changes to strategy logic, lifecycle, reconcile, selection ordering, extension discipline, or release behavior.
