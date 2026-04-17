Patch 151 — Realized P&L Broker Reconciliation

What changed:
- added broker-order-backed manual exit reconciliation helpers
- worker exit path now prefers recent Alpaca filled exit orders to compute realized exit price
- startup restore now reconciles stale snapshot positions as manual exits when broker position is already gone
- dedupe protection added to avoid double-counting recently recorded closed trades
- patch version updated to patch-151-realized-pnl-broker-reconciliation

What did not change:
- entry logic
- scanner logic
- release gating
- reconcile blocking rules
- broker submit path for entries/exits
