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


URL = os.getenv("SCANNER_URL") or os.getenv("URL") or ""
if not URL:
    # Backward compatible default if user forgot to set
    URL = "http://localhost:10000/worker/scan_entries"
INTERVAL_SEC = getenv_int("SCANNER_INTERVAL_SEC", 60)
TIMEOUT_SEC = getenv_int("SCANNER_HTTP_TIMEOUT_SEC", getenv_int("SCANNER_TIMEOUT_SEC", 60))

SECRET = os.getenv("WORKER_SECRET") or os.getenv("SCANNER_SECRET") or ""


def post_json(url: str, payload: dict) -> dict:
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
