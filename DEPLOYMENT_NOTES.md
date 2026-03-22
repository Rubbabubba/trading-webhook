# Patch 082 – Live readiness gate and execution visibility

This patch builds on patch 081 and adds the operator-facing summaries needed to finish today’s visibility/readiness work without changing trading thresholds or widening live permissions.

What changed:
- Added `/diagnostics/execution_visibility` to join current execution proof rows with execution lifecycle state so each selected symbol shows plan/order/fill/exit progress in one place.
- Added `/diagnostics/live_readiness_gate` to summarize component readiness, proof status, release workflow state, live env arming, and current blockers in one response.
- Kept existing runtime truth, execution proof, readiness, and release gate logic intact; this patch only adds summaries on top of the existing baseline.

Safety:
- No trading thresholds were changed.
- No live execution permissions were widened.
- No existing diagnostics routes were removed or renamed.
