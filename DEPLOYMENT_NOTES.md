Patch 085 — proof truth and exit-arm hardening

Changes in this patch:
- fixes proof visibility so entry statuses planned/submitted no longer count as fills
- treats exit lifecycle status armed as a valid exit-proof state
- hardens reconcile recovery so broker-open positions rebuild a filled execution state and re-arm exit protection
- preserves paper-proof and guarded-live controls from patch 084-fixed

Post-deploy checks:
- /diagnostics/build
- /diagnostics/proof_capture_plan
- /diagnostics/release
- /diagnostics/live_readiness_gate
- /diagnostics/execution_lifecycle
