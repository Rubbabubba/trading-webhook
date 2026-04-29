# Patch 169A — Worker Endpoint Threadpool Stability Hotfix

## Purpose
Emergency stability patch for API responsiveness while active positions are open.

## What changed
- Converted `/worker/exit` from an async route to a sync FastAPI route so heavy broker/price/reconcile work runs in FastAPI's threadpool instead of blocking the event loop.
- Converted `/worker/scan_entries` from an async route to a sync FastAPI route for the same reason.
- Preserved worker auth, body parsing, scanner source detection, exit logic, scan logic, trade logic, risk logic, accounting, and dashboard behavior.
- Fixed a syntax issue in `worker.py` f-string quoting so the artifact compiles cleanly.

## What did not change
- No entry logic changes.
- No exit logic changes.
- No risk threshold changes.
- No sizing changes.
- No ranking changes.
- No accounting changes.

## Validation
- `python3 -m py_compile app.py scanner.py worker.py` passed.

## Env changes
None required.
