# Patch 141 — Dashboard Entry Funnel Helper Scope Fix

Built from Patch 139 baseline.

## Purpose
Provide a clean, deployable artifact after the Patch 140 dashboard regression.

## Changes
- PATCH_VERSION updated to patch-141-dashboard-entry-funnel-helper-scope-fix
- Preserves the stable Patch 139 dashboard and P&L layer
- No trading logic changes
- No env changes

## Safety
- Read-only dashboard only
- No lifecycle, reconcile, scanner, or exit behavior changed
