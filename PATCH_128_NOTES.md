PATCH 128 — scanner telemetry keepalive

Goal:
- Eliminate false/lingering late scanner worker warnings caused by long sleep windows and restored telemetry
- Preserve live trading behavior with no drift/regression

Changes:
- Scanner heartbeat endpoint now preserves boot_ts from worker details when telemetry is restored
- Scanner worker now sends periodic keepalive heartbeats during long sleep intervals
- Patch version bumped to patch-128-scanner-telemetry-keepalive

No behavior changes to:
- entry logic
- exit logic
- reconcile
- release gating
