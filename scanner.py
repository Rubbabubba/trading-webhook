import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
import random
from datetime import datetime, timezone


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(msg: str) -> None:
    print(f"{_ts()} [scanner] {msg}", flush=True)


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
    base = os.getenv("MAIN_SERVICE_URL") or os.getenv("WORKER_BASE_URL")
    if base:
        return base.strip().rstrip("/") + "/worker/scan_entries"
    return None


def resolve_base_url(scan_url: str) -> str:
    parsed = urllib.parse.urlparse(scan_url)
    return f"{parsed.scheme}://{parsed.netloc}"


def read_body(resp) -> str:
    return resp.read().decode("utf-8", errors="replace")


def shorten_body(body: str, limit: int = 500) -> str:
    return body[:limit].replace("\n", "\\n").replace("\r", "\\r")


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
        return resp.status, read_body(resp)


def preflight(base_url: str, timeout: int) -> tuple[bool, str]:
    req = urllib.request.Request(
        base_url,
        method="GET",
        headers={"User-Agent": "equities-scanner/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = read_body(resp)
            return True, f"status={resp.status} body={shorten_body(body, 200)}"
    except urllib.error.HTTPError as e:
        try:
            body = read_body(e)
        except Exception:
            body = ""
        return True, f"http_error={e.code} reason={e.reason} body={shorten_body(body, 200)}"
    except Exception as e:
        return False, repr(e)


def main() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True, write_through=True)
        sys.stderr.reconfigure(line_buffering=True, write_through=True)
    except Exception:
        pass

    url = resolve_scan_url()
    if not url:
        log("ERROR missing target URL; set SCAN_ENTRIES_URL or MAIN_SERVICE_URL/WORKER_BASE_URL")
        raise SystemExit(2)

    interval = getenv_int("SCAN_INTERVAL_SEC", getenv_int("SWING_SCAN_INTERVAL_SEC", 3600))
    timeout = getenv_int("SCAN_TIMEOUT_SEC", 60)
    run_on_start = getenv_bool("SCAN_RUN_ON_START", True)
    jitter_sec = max(0, getenv_int("SCAN_JITTER_SEC", 0))
    startup_retries = max(1, getenv_int("SCAN_STARTUP_RETRIES", 3))
    startup_retry_delay = max(1, getenv_int("SCAN_STARTUP_RETRY_DELAY_SEC", 10))
    strategy_mode = os.getenv("STRATEGY_MODE", "intraday")

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

    base_url = resolve_base_url(url)
    log(
        f"boot url={url} base_url={base_url} interval_sec={interval} timeout_sec={timeout} "
        f"run_on_start={run_on_start} jitter_sec={jitter_sec} startup_retries={startup_retries} "
        f"startup_retry_delay_sec={startup_retry_delay} has_worker_secret={bool(worker_secret)} strategy_mode={strategy_mode}"
    )

    ok, detail = preflight(base_url, timeout=min(timeout, 15))
    if ok:
        log(f"preflight_ok {detail}")
    else:
        log(f"preflight_error {detail}")

    loop_num = 0
    first = True
    while True:
        loop_num += 1
        should_run = (not first) or run_on_start
        if should_run:
            reason = "startup" if first else "scheduled"
            attempts = startup_retries if first else 1
            for attempt in range(1, attempts + 1):
                req_payload = dict(payload)
                req_payload["reason"] = reason
                try:
                    log(f"scan_attempt loop={loop_num} attempt={attempt}/{attempts} reason={reason} target={url}")
                    status, body = post_json(url, req_payload, timeout=timeout)
                    log(f"scan_ok loop={loop_num} attempt={attempt}/{attempts} reason={reason} status={status} body={shorten_body(body)}")
                    break
                except urllib.error.HTTPError as e:
                    try:
                        err_body = read_body(e)
                    except Exception:
                        err_body = ""
                    log(f"scan_http_error loop={loop_num} attempt={attempt}/{attempts} reason={reason} status={e.code} error={e.reason} body={shorten_body(err_body)}")
                except Exception as e:
                    log(f"scan_error loop={loop_num} attempt={attempt}/{attempts} reason={reason} err={e!r}")
                if attempt < attempts:
                    log(f"startup_retry_sleep sec={startup_retry_delay}")
                    time.sleep(startup_retry_delay)
        else:
            log(f"startup_scan_skipped run_on_start={run_on_start}")

        first = False
        sleep_for = interval + (random.randint(0, jitter_sec) if jitter_sec > 0 else 0)
        log(f"sleep sec={sleep_for}")
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
