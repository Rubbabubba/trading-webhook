"""Small polling worker for the isolated SPY/QQQ regime engine."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def _truthy(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _endpoint() -> str:
    explicit = str(os.getenv("REGIME_INTRADAY_SCAN_URL", "")).strip()
    if explicit:
        return explicit
    base = str(os.getenv("MAIN_SERVICE_URL", os.getenv("WORKER_BASE_URL", ""))).strip().rstrip("/")
    return f"{base}/worker/regime_intraday_scan" if base else ""


def _post(url: str, payload: dict, timeout: int) -> tuple[int, str]:
    request = Request(url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json", "User-Agent": "regime-intraday-scanner/1.0"}, method="POST")
    with urlopen(request, timeout=timeout) as response:
        return int(response.status), response.read().decode("utf-8", errors="replace")


def main() -> int:
    url = _endpoint()
    if not url:
        print("missing REGIME_INTRADAY_SCAN_URL or MAIN_SERVICE_URL", flush=True)
        return 2
    interval = max(60, int(os.getenv("REGIME_INTRADAY_SCAN_INTERVAL_SEC", "180")))
    timeout = max(10, int(os.getenv("REGIME_INTRADAY_SCAN_TIMEOUT_SEC", "45")))
    payload = {}
    secret = str(os.getenv("WORKER_SECRET", "")).strip()
    if secret:
        payload["worker_secret"] = secret
    run_outside = _truthy("REGIME_INTRADAY_RUN_OUTSIDE_MARKET_HOURS")
    print(f"regime intraday worker started url={url} interval_sec={interval}", flush=True)
    while True:
        now = datetime.now(timezone.utc).isoformat()
        try:
            status, body = _post(url, payload, timeout)
            parsed = json.loads(body or "{}")
            print(f"{now} status={status} scan_status={parsed.get('status')} regime={(parsed.get('regime') or {}).get('name')} signals={parsed.get('signal_count', 0)}", flush=True)
        except HTTPError as exc:
            print(f"{now} http_error={exc.code} reason={exc.reason}", flush=True)
        except Exception as exc:
            print(f"{now} error={exc!r}", flush=True)
        if not run_outside:
            # The API itself owns the authoritative market-hours decision. This
            # worker stays simple so holidays and early closes cannot diverge.
            pass
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
