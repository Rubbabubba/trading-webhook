# Patch 155

- Safely auto-clears stale daily halt when the system is idle (no positions, no active plans, no open orders) and no daily stop is currently hit.
- Normalizes readiness lifecycle counters to same-session, symbol-deduped counts.
- Does not change entry/exit strategy logic or accounting quarantine behavior.
