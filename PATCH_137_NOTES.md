# Patch 137 — Dashboard v2 Foundation

## What changed
- Added read-only Active Positions panel to the dashboard
- Added read-only Risk Integrity panel to the dashboard
- Added read-only Exposure & Capacity panel to the dashboard
- Updated PATCH_VERSION to `patch-137-dashboard-v2-foundation`

## Safety
- No trading logic changes
- No scanner changes
- No reconcile logic changes
- No lifecycle changes
- No exit logic changes
- No env changes required

## Validation
- `py_compile` passed for `app.py`, `scanner.py`, and `worker.py`
- Dashboard remains read-only and uses existing in-memory/reconcile data only
