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

