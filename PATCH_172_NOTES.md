# Patch 172 — Rich Dashboard Snapshot Restore

Restores the useful dashboard layout and information density from the earlier operator dashboard while keeping the safe snapshot-only fast path.

Changes:
- Restores rich cards/sections: Operator Alerts, Exception Center, KPI cards, Active Positions, P&L Summary, Risk Integrity, Exposure & Capacity, Daily Halt Truth, Today P&L Truth, Performance Analytics, Readiness, Guarded Live Path, Candidate Rejections, Lifecycle/Dispatch.
- Dashboard reads persisted snapshot/state files only.
- Dashboard does not execute broker, live reconcile, scanner, or exit work while rendering.
- Active positions table merges broker-backed snapshot positions with active plan risk fields.
- Daily halt thresholds use safe environment lookups.

No trading, scanner, exit, risk, order, or reconcile behavior changed.
