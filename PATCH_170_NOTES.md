# Patch 170 — Active Positions Table Truth Bind

## Purpose
Fix dashboard-only mismatch where `Active Position Count` correctly showed broker/reconcile-backed open positions while the `Active Positions` table could display `No active positions`.

## Scope
Dashboard display binding only.

## Changes
- Binds the fast-path dashboard Active Positions table to the same broker/reconcile truth used by the count.
- Uses broker symbols and latest broker position snapshot as the primary table source.
- Enriches rows with active plan stop/target/signal/status when available.
- Falls back cleanly for recovered broker-backed positions.

## Not changed
- Entry logic
- Exit logic
- Scanner behavior
- Reconcile behavior
- Risk controls
- Order placement
- Accounting

## Env changes
None.
