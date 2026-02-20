import os
import time
import json
import urllib.request
from datetime import datetime, timezone


def utc_ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


# Target URL for scan requests (must point to the MAIN web service, not this scanner service).
# Provide one of: SCAN_ENTRIES_URL (full), SCANNER_URL (full), or MAIN_SERVICE_URL / WORKER_BASE_URL (base).
URL = (os.getenv("SCAN_ENTRIES_URL") or os.getenv("SCANNER_URL") or os.getenv("BASE_URL") or os.getenv("MAIN_SERVICE_URL") or os.getenv("WORKER_BASE_URL") or "").strip()
# If URL is a base (no /worker/scan_entries), append the path
if URL and "/worker/scan_entries" not in URL:
    URL = URL.rstrip("/") + "/worker/scan_entries"

    os.getenv("SCAN_ENTRIES_URL")
    or os.getenv("SCANNER_URL")
    or os.getenv("MAIN_SERVICE_URL")
    or os.getenv("WORKER_BASE_URL")
    or os.getenv("URL")  # legacy
    or ""
)

if not URL:
    raise SystemExit(
        "[scanner] ERROR: missing target URL. Set SCAN_ENTRIES_URL (recommended, full URL) "
        "or MAIN_SERVICE_URL/WORKER_BASE_URL (base URL of the main service)."
    )

# If a base URL was provided, append the endpoint path.
if "worker/scan_entries" not in URL:
    URL = URL.rstrip("/") + "/worker/scan_entries"
INTERVAL_SEC = getenv_int("SCANNER_INTERVAL_SEC", 60)
TIMEOUT_SEC = getenv_int("SCANNER_HTTP_TIMEOUT_SEC", getenv_int("SCANNER_TIMEOUT_SEC", 60))

SECRET = os.getenv("WORKER_SECRET") or os.getenv("SCANNER_SECRET") or ""


def post_json(url: str, payload: dict) -> dict:
    if SECRET:
        payload = dict(payload)
        payload.setdefault("worker_secret", SECRET)
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if SECRET:
        headers["X-Worker-Secret"] = SECRET

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8")
        if not raw:
            return {}
        return json.loads(raw)


def main() -> None:
    print(f"[scanner] starting loop: url={URL} interval={INTERVAL_SEC}s timeout={TIMEOUT_SEC}s", flush=True)

    while True:
        t0 = time.time()
        try:
            data = post_json(URL, {})
            status = data.get("status") or {}
            scanned = status.get("scanned") or data.get("scanned") or 0
            would = status.get("would_trade") or data.get("would_trade") or status.get("would") or data.get("would") or 0
            blocked = status.get("blocked") or data.get("blocked") or 0
            dur_ms = status.get("duration_ms") or data.get("duration_ms") or int((time.time() - t0) * 1000)

            print(f"[scanner] tick ok scanned={scanned} would={would} blocked={blocked} dur_ms={dur_ms}", flush=True)
        except Exception as e:
            print(f"[scanner] error: {e}", flush=True)

        # sleep remaining interval
        elapsed = time.time() - t0
        sleep_for = max(0.0, INTERVAL_SEC - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()