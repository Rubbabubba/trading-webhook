"""Dedicated scheduler for the regime-intraday paper system.

This process calls only regime-intraday endpoints. It cannot invoke legacy
swing scanning, swing exits, or a live-order endpoint.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


WORKER_VERSION = "regime-intraday-worker-v1"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _log(message: str) -> None:
    stamp = datetime.now(timezone.utc).isoformat()
    print(f"{stamp} [regime-intraday-worker] {message}", flush=True)


def _post(url: str, payload: dict, timeout: int) -> tuple[int, dict]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json", "User-Agent": WORKER_VERSION},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8", errors="replace")
        return response.status, json.loads(body or "{}")


def main() -> None:
    base_url = (os.getenv("MAIN_SERVICE_URL") or os.getenv("BASE_URL") or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError("MAIN_SERVICE_URL is required")
    secret = (os.getenv("WORKER_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("WORKER_SECRET is required")

    interval = max(30, _env_int("REGIME_INTRADAY_SCAN_INTERVAL_SEC", 300))
    timeout = max(10, _env_int("REGIME_INTRADAY_WORKER_TIMEOUT_SEC", 60))
    scan_url = (os.getenv("REGIME_INTRADAY_SCAN_URL") or f"{base_url}/worker/regime_intraday_scan").strip()
    reconcile_url = (os.getenv("REGIME_INTRADAY_RECONCILE_URL") or f"{base_url}/worker/regime_intraday_paper_reconcile").strip()
    payload = {"worker_secret": secret}

    _log(f"boot version={WORKER_VERSION} interval_sec={interval} timeout_sec={timeout}")
    while True:
        started = time.monotonic()
        for action, url in (("scan", scan_url), ("reconcile", reconcile_url)):
            try:
                status, response = _post(url, payload, timeout)
                _log(f"{action}_ok http={status} status={response.get('status', 'ok')} live_submission={response.get('live_submission', False)}")
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace")[:300]
                _log(f"{action}_http_error http={error.code} detail={detail}")
            except Exception as error:
                _log(f"{action}_error kind={type(error).__name__} detail={str(error)[:300]}")
        time.sleep(max(0.0, interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
