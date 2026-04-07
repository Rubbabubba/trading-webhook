# PATCH 118 — Recovered Stop Enforcement Final Fix

## Objective
Fix recovered stop enforcement with no drift and no regression.

## What changed
- Expanded recovered protection restoration so it now applies to:
  - startup-restored plans
  - broker-backed recovered plans
  - active recovered plans during runtime normalization
- Startup snapshot restore now marks restored plans as broker-backed and immediately restores protective stops when the stop has been neutralized.
- Reconcile sync now enforces recovered stop restoration even when there is a live broker position but no fresh fill price/order-status rebuild path.
- Lifecycle normalization now includes a recovered-stop protection pass so restart/reconcile drift cannot leave a long position unprotected.
- Added audit fields on restoration:
  - `protection_restored_at`
  - `protection_restored_source`

## Invariant enforced
For recovered long positions, if:
- `initial_stop_price < entry_price`
- and current `stop_price` is invalid / neutralized (`>= entry_price` or missing)

then `stop_price` is authoritatively restored to `initial_stop_price`.

Short-side logic is included symmetrically.

## Files changed
- `app.py`
- `tests/test_patch_118.py`

## Test status
Focused patch tests passed:
- `tests/test_patch_118.py` → 3 passed

Note: legacy `tests/test_patch_098.py` is syntactically broken in the provided baseline and prevents a clean broader pytest sweep unrelated to this patch.
