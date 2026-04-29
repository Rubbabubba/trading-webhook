# Patch 169B — API Event Loop Rescue + Worker Timeout Stability

Purpose: stabilize the live API while active positions exist.

Changes:
- Converts `/worker/exit` from an async blocking route to a sync FastAPI route so heavy broker/exit work runs in FastAPI's threadpool instead of blocking the main event loop.
- Converts `/worker/scan_entries` the same way, so long scanner evaluations do not starve manual diagnostic/dashboard requests.
- Adds configurable background-worker HTTP timeout via `WORKER_HTTP_TIMEOUT_SEC` with a safer default of 45 seconds.
- Keeps all entry, exit, scanner, ranking, risk, and accounting logic unchanged.

Recommended env change for background worker only:
WORKER_HTTP_TIMEOUT_SEC=45

No main trading env changes required.
