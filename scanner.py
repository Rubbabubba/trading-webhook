import os
import time
import json
import urllib.request
import urllib.error

# Scanner worker: periodically triggers the main service's /worker/scan_entries endpoint.
#
# Deploy as a separate Render service from the main FastAPI app and the exit worker.
# Required env:
#   BASE_URL        - https://<your-main-service>.onrender.com
#   WORKER_SECRET   - must match main service WORKER_SECRET
#
# Optional env:
#   SCAN_INTERVAL_SEC   - default 60
#   SCAN_PATH           - default /worker/scan_entries

BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

INTERVAL_SEC = int(os.getenv("SCAN_INTERVAL_SEC", "60"))
SCAN_PATH = os.getenv("SCAN_PATH", "/worker/scan_entries")

def _post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}

def main() -> None:
    if not BASE_URL:
        raise SystemExit("BASE_URL is not set")
    if not WORKER_SECRET:
        raise SystemExit("WORKER_SECRET is not set")

    url = f"{BASE_URL}{SCAN_PATH}"
    print(f"[scanner] starting loop: url={url} interval={INTERVAL_SEC}s", flush=True)

    while True:
        t0 = time.time()
        try:
            result = _post_json(url, {"worker_secret": WORKER_SECRET})
            # Keep logs concise but useful.
            scanned = (result.get("scanner") or {}).get("symbols_scanned") or result.get("scanned") or result.get("symbols_scanned")
            would = result.get("would_submit") or ((result.get("scanner") or {}).get("would_submit")) or ((result.get("scanner") or {}).get("would_trade"))
            mode = "DRY_RUN" if result.get("dry_run") else "LIVE" if result.get("live") else ""
            skipped = result.get("skipped")
            reason = result.get("reason")
            dur = (result.get("scanner") or {}).get("duration_ms")
            if skipped:
                print(f"[scanner] tick ok skipped=True reason={reason} dur_ms={dur}", flush=True)
            else:
                wt = result.get("would_submit")
                wt_n = len(wt) if isinstance(wt, list) else ((result.get("scanner") or {}).get("would_trade"))
                print(f"[scanner] tick ok scanned={scanned} would={wt_n} blocked={(result.get("scanner") or {}).get("blocked")} dur_ms={dur}", flush=True)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"[scanner] HTTPError {e.code}: {body}", flush=True)
        except Exception as e:
            print(f"[scanner] error: {e}", flush=True)

        elapsed = time.time() - t0
        sleep_for = max(1.0, INTERVAL_SEC - elapsed)
        time.sleep(sleep_for)

if __name__ == "__main__":
    main()
