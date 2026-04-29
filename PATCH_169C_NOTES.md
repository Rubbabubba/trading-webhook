# Patch 169C — Dashboard Fast Path + Heavy Diagnostics Split

Purpose: restore operator dashboard responsiveness while active positions are open.

Changes:
- Replaces `/dashboard` rendering with a lightweight fast-path view.
- Keeps only essential operator cards: release, freshness, reconcile, daily halt, today P&L, active positions summary.
- Removes heavy embedded candidate/blocker/lifecycle JSON from dashboard.
- Leaves full raw details available through diagnostics endpoints.
- No entry logic changes.
- No exit logic changes.
- No scanner logic changes.
- No reconcile/accounting behavior changes.

Validation:
- python compile passed for app.py, scanner.py, worker.py.

Env changes: None.
