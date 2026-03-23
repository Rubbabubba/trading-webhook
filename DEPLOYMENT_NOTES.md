# Patch 083 – Proof capture plan

This patch builds on patch 082 and adds one operator-facing diagnostic to help capture paper execution proof and same-session proof during market hours without loosening live controls.

What changed:
- Added `/diagnostics/proof_capture_plan` to combine current runtime preview, execution visibility, readiness, and live readiness gate into one response.
- The new snapshot shows exact arming gaps, whether proof capture is possible right now, the next required step for each selected symbol, and the next-best runtime candidates if nothing is currently selected.

Safety:
- No trading thresholds were changed.
- No release-gate logic was loosened.
- No live permissions were widened.
