import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
import random
from datetime import datetime, timezone

try:
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)
except Exception:
    pass


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(msg: str) -> None:
    print(f"[scanner] {_ts()} {msg}", flush=True)


def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def resolve_scan_url() -> str | None:
    url = os.getenv("SCAN_ENTRIES_URL") or os.getenv("SWING_SCAN_ENTRIES_URL")
    if url:
        return url.strip()
    base = os.getenv("MAIN_SERVICE_URL") or os.getenv("WORKER_BASE_URL") or os.getenv("BASE_URL")
    if base:
        base = base.strip().rstrip("/")
        return f"{base}/worker/scan_entries"
    return None


def post_json(url: str, payload: dict, timeout: int) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "equities-scanner/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def preflight(url: str, timeout: int) -> None:
    parsed = urllib.parse.urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    test_urls = [f"{base}/diagnostics/swing", f"{base}/diagnostics/runtime", base + "/"]
    for test_url in test_urls:
        try:
            req = urllib.request.Request(test_url, method="GET", headers={"User-Agent": "equities-scanner/1.0"})
            with urllib.request.urlopen(req, timeout=min(timeout, 20)) as resp:
                body = resp.read(200).decode("utf-8", errors="replace")
                log(f"preflight_ok url={test_url} status={resp.status} body_prefix={body[:120]!r}")
                return
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            log(f"preflight_http_error url={test_url} status={e.code} reason={e.reason} body_prefix={body[:120]!r}")
            if e.code < 500:
                return
        except Exception as e:
            log(f"preflight_error url={test_url} err={e!r}")
    log("preflight_complete")


def main() -> None:
    url = resolve_scan_url()
    if not url:
        log("fatal missing target URL. Set SCAN_ENTRIES_URL or MAIN_SERVICE_URL/WORKER_BASE_URL/BASE_URL.")
        raise SystemExit(2)

    interval = getenv_int("SCAN_INTERVAL_SEC", getenv_int("SWING_SCAN_INTERVAL_SEC", 3600))
    timeout = getenv_int("SCAN_TIMEOUT_SEC", 60)
    run_on_start = getenv_bool("SCAN_RUN_ON_START", True)
    jitter_sec = max(0, getenv_int("SCAN_JITTER_SEC", 0))
    startup_retries = max(1, getenv_int("SCAN_STARTUP_RETRIES", 3))
    startup_retry_delay = max(1, getenv_int("SCAN_STARTUP_RETRY_DELAY_SEC", 10))

    worker_secret = (os.getenv("WORKER_SECRET") or "").strip()
    if not worker_secret:
        worker_secret = (os.getenv("INTERNAL_API_KEY") or "").strip()

    payload: dict = {}
    if worker_secret:
        payload["worker_secret"] = worker_secret
    for k in ["mode", "provider", "symbols_provider", "symbols", "max_symbols"]:
        envk = f"SCAN_{k.upper()}"
        v = os.getenv(envk)
        if v is not None and v != "":
            payload[k] = v

    safe_url = url
    log(
        f"starting loop url={safe_url} interval={interval}s timeout={timeout}s has_worker_secret={bool(worker_secret)} strategy_mode={os.getenv('STRATEGY_MODE', 'intraday')} run_on_start={run_on_start} jitter_sec={jitter_sec} startup_retries={startup_retries}"
    )
    preflight(url, timeout)

    first = True
    loop_idx = 0
    while True:
        loop_idx += 1
        if not first or run_on_start:
            attempts = startup_retries if first else 1
            for attempt in range(1, attempts + 1):
                try:
                    payload["reason"] = "startup" if first else "scheduled"
                    log(f"scan_attempt loop={loop_idx} attempt={attempt}/{attempts} reason={payload['reason']} url={safe_url}")
                    status, body = post_json(url, payload, timeout=timeout)
                    body_prefix = body[:500].replace("
", " ")
                    log(f"scan_ok loop={loop_idx} attempt={attempt}/{attempts} status={status} bytes={len(body)} body_prefix={body_prefix!r}")
                    break
                except urllib.error.HTTPError as e:
                    try:
                        err_body = e.read().decode("utf-8", errors="replace")
                    except Exception:
                        err_body = ""
                    log(f"scan_http_error loop={loop_idx} attempt={attempt}/{attempts} status={e.code} reason={e.reason} body_prefix={err_body[:500]!r}")
                except Exception as e:
                    log(f"scan_error loop={loop_idx} attempt={attempt}/{attempts} err={e!r}")
                if attempt < attempts:
                    time.sleep(startup_retry_delay)
        else:
            log(f"startup_scan_skipped run_on_start={run_on_start}")
        first = False
        sleep_for = interval + (random.randint(0, jitter_sec) if jitter_sec > 0 else 0)
        log(f"sleeping seconds={sleep_for}")
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
