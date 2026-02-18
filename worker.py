import os
import time
import json
import urllib.request
import urllib.error

# Endpoint to ping (set this in Render env as BASE_URL)
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

# Interval (seconds)
WORKER_MODE = os.getenv("WORKER_MODE", "exit").strip().lower()

# Interval (seconds)
INTERVAL_SEC = int(os.getenv("EXIT_INTERVAL_SEC" if WORKER_MODE=="exit" else "SCAN_INTERVAL_SEC", "30" if WORKER_MODE=="exit" else "60"))

# Endpoint path
EXIT_PATH = os.getenv("EXIT_PATH", "/worker/exit")
SCAN_PATH = os.getenv("SCAN_PATH", "/worker/scan_entries")

def _target_path() -> str:
    return EXIT_PATH if WORKER_MODE=="exit" else SCAN_PATH

# Simple stdout logger
def log(msg: str):
    print(msg, flush=True)

def post_json(url: str, payload: dict, timeout: int = 15) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return resp.getcode(), body

def main():
    if not BASE_URL:
        raise RuntimeError("BASE_URL env var is required, e.g. https://trading-webhook-q4d5.onrender.com")

    url = f"{BASE_URL}{_target_path()}"

    payload = {}
    if WORKER_SECRET:
        payload["worker_secret"] = WORKER_SECRET

    log(f"[worker] starting exit loop: url={url} interval={INTERVAL_SEC}s secret={'set' if WORKER_SECRET else 'not_set'}")

    while True:
        start = time.time()
        try:
            code, body = post_json(url, payload)
            log(f"[worker] {code} {body[:500]}")
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = str(e)
            log(f"[worker] HTTPError {e.code}: {body[:500]}")
        except Exception as e:
            log(f"[worker] ERROR: {e}")

        # maintain cadence
        elapsed = time.time() - start
        sleep_for = max(0.0, INTERVAL_SEC - elapsed)
        time.sleep(sleep_for)

if __name__ == "__main__":
    main()
