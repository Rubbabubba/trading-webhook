# Patch 143 — Dashboard Performance Trim

## Goal
Reduce dashboard response weight and render cost without changing trading behavior.

## Changes
- Trim oversized raw JSON blocks in dashboard previews
- Keep full diagnostics available via existing links
- Remove duplicated near-miss section
- Preserve Patch 142 visual refresh, alerts, and exception center

## No Changes
- No trading logic changes
- No scanner / worker / reconcile behavior changes
- No env changes
