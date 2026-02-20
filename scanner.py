import os
import time
import json
import urllib.request

SCAN_URL = os.getenv("SCAN_ENTRIES_URL")
API_KEY = os.getenv("INTERNAL_API_KEY")

if not SCAN_URL:
    raise RuntimeError("SCAN_ENTRIES_URL is required")

if not API_KEY:
    raise RuntimeError("INTERNAL_API_KEY is required")

INTERVAL = int(os.getenv("SCAN_INTERVAL_SEC", "60"))
TIMEOUT = int(os.getenv("SCAN_TIMEOUT_SEC", "60"))

print(f"[scanner] starting loop: url={SCAN_URL} interval={INTERVAL}s timeout={TIMEOUT}s")

def post():
    req = urllib.request.Request(
        SCAN_URL,
        data=b"{}",
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",   # ← THIS is what your API expects
        },
    )

    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        body = resp.read().decode()
        print(f"[scanner] {resp.status} {body}")

while True:
    try:
        post()
    except Exception as e:
        print(f"[scanner] error: {e}")

    time.sleep(INTERVAL)
