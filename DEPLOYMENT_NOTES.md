# Patch 008

- Adds `VWAP_PB_MIN_BARS_5M` as a real env-controlled setting with default `15`.
- Restores `/diagnostics/gatekeeper?symbol=SPY`.
- Adds `/diagnostics/strategy` and `/diagnostics/strategy?symbol=SPY`.
- Shows whether VWAP min-bars came from env or code default.

Render env add/change:

```
VWAP_PB_MIN_BARS_5M=15
```

# Patch 002 Deployment Notes

- Patch 002 adds a durable execution journal and position snapshot files on disk.
- New diagnostics endpoints: `/diagnostics/journal` and `/diagnostics/execution`.
- `/diagnostics/orders` now includes ENTRY, EXIT, and RECONCILE rows and can enrich recent rows with broker order status.
- Restart behavior improves because recent journal rows bootstrap the in-memory decision buffer.
- Keep `JOURNAL_ENABLED=true` on Render and make sure `/var/data` is writable.

# Render deployment: 3-service layout

This repo supports a 3-service deployment on Render:

1) **Main API Service** (FastAPI)
   - Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
   - Env: your existing main env (ALPACA keys, WEBHOOK_SECRET, WORKER_SECRET, flags, etc.)

2) **Exit Worker Service**
   - Start command (recommended): `python worker.py`
   - Env:
     - BASE_URL = https://<main-service>
     - WORKER_SECRET = <same as main>
     - WORKER_MODE = exit
     - EXIT_INTERVAL_SEC = 30 (or desired)
     - EXIT_PATH = /worker/exit (default)

3) **Scanner Worker Service**
   - Start command (recommended): `python scanner.py`
     - (Alternative: `python worker.py` with WORKER_MODE=scan)
   - Env:
     - BASE_URL = https://<main-service>
     - WORKER_SECRET = <same as main>
     - SCAN_INTERVAL_SEC = 60 (or desired)
     - SCAN_PATH = /worker/scan_entries (default)
     - Scanner flags live in the main service env (SCANNER_ENABLED, SCANNER_DRY_RUN, universe config)

**Safety defaults**
- Scanner is OFF by default unless SCANNER_ENABLED=true on the main service.
- Scanner will not place orders unless SCANNER_DRY_RUN=false AND any additional live-gates you enable in the main service.



## Patch 003
- Adds pre-trade quote freshness and spread gates.
- Rechecks broker position after symbol lock before entry submission.
- Syncs active plans against broker order status during exit cycles.
- Reconciles entry price to broker filled average price when available.
- Deactivates stale submitted plans and stale no-position plans.
- Adds /diagnostics/gatekeeper?symbol=SPY.


## Patch 005
- Tightens VWAP pullback signal quality with ATR/day-range regime filters, 1m micro-volume confirmation, and entry timing confirmation.
- Adds /diagnostics/strategy for strategy config and per-symbol live diag.


## Patch 006
- Adaptive risk sizing based on signal score
- 5m ATR-driven stop sizing
- ATR / R-multiple target sizing
- Notional cap per trade
- New endpoint: /diagnostics/risk_model


## Patch 007
- Adds outbound webhook alerts for entries, exits, rejections, daily halts, and readiness failures.
- Adds /diagnostics/alerts and /test/alert.
- Supports Slack incoming webhooks, Discord webhooks, and generic JSON webhooks.
- Alerts are deduplicated to reduce spam.


## Patch 010
- Adds rank-aware slot allocation so the scanner prefers the best candidates when position slots are limited.
- Adds fallback minimum thresholds and /diagnostics/ranking.


## Patch 011
- Added micro confirmation flexibility with soft confirmation modes.
- Added macro/micro blocker split in strategy diagnostics.
- Added pre-ranked near-miss candidates to scan summaries and diagnostics/ranking fallback view.
