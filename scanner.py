import json
import os
import time
import urllib.request
import urllib.error
import random
from datetime import datetime, timezone

def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default

def resolve_scan_url() -> str | None:
    url = os.getenv("SCAN_ENTRIES_URL") or os.getenv("SWING_SCAN_ENTRIES_URL")
    if url:
        return url.strip()
    base = os.getenv("MAIN_SERVICE_URL") or os.getenv("WORKER_BASE_URL")
    if base:
        base = base.strip().rstrip("/")
        return f"{base}/worker/scan_entries"
    return None

def resolve_base_url(scan_url: str) -> str:
    if scan_url.endswith("/worker/scan_entries"):
        return scan_url[:-len("/worker/scan_entries")].rstrip("/")
    return scan_url.rsplit("/", 1)[0].rstrip("/")

def post_json(url: str, payload: dict, timeout: int, user_agent: str = "equities-scanner/1.0") -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json", "User-Agent": user_agent, "X-Scanner-Source": "worker"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")

def get_text(url: str, timeout: int, user_agent: str = "equities-scanner/1.0") -> tuple[int, str]:
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")

def getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def ts_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def log(msg: str) -> None:
    print(f"{ts_utc()} [scanner] {msg}", flush=True)

def main() -> None:
    url = resolve_scan_url()
    if not url:
        log("ERROR missing target URL. Set SCAN_ENTRIES_URL or MAIN_SERVICE_URL/WORKER_BASE_URL.")
        raise SystemExit(2)
    base_url = resolve_base_url(url)
    heartbeat_url = f"{base_url}/worker/scanner_heartbeat"
    health_url = f"{base_url}/"
    interval = getenv_int("SCAN_INTERVAL_SEC", getenv_int("SWING_SCAN_INTERVAL_SEC", 3600))
    timeout = getenv_int("SCAN_TIMEOUT_SEC", 60)
    run_on_start = getenv_bool("SCAN_RUN_ON_START", True)
    jitter_sec = max(0, getenv_int("SCAN_JITTER_SEC", 0))
    startup_retries = max(1, getenv_int("SCAN_STARTUP_RETRIES", 3))
    startup_retry_delay_sec = max(1, getenv_int("SCAN_STARTUP_RETRY_DELAY_SEC", 10))
    worker_secret = (os.getenv("WORKER_SECRET") or os.getenv("INTERNAL_API_KEY") or "").strip()
    scan_payload: dict = {}
    if worker_secret:
        scan_payload["worker_secret"] = worker_secret
    for k in ["mode", "provider", "symbols_provider", "symbols", "max_symbols"]:
        envk = f"SCAN_{k.upper()}"
        v = os.getenv(envk)
        if v is not None and v != "":
            scan_payload[k] = v
    state = {"boot_ts_utc": ts_utc(), "attempts_total": 0, "success_total": 0, "failure_total": 0, "attempts_today": 0, "success_today": 0, "failure_today": 0, "consecutive_failures": 0, "last_attempt_utc": None, "last_success_utc": None, "last_failure_utc": None, "last_error": "", "pid": os.getpid(), "interval_sec": interval, "timeout_sec": timeout, "run_on_start": run_on_start, "jitter_sec": jitter_sec}
    def heartbeat(event: str, status: str = "ok", details: dict | None = None) -> None:
        payload = {"worker_secret": worker_secret, "event": event, "status": status, "details": {**state, **(details or {})}}
        try:
            post_json(heartbeat_url, payload, timeout=min(timeout, 15))
        except Exception as e:
            log(f"heartbeat_error event={event} err={e!r}")
    log(f"boot url={url} base_url={base_url} interval_sec={interval} timeout_sec={timeout} run_on_start={run_on_start} jitter_sec={jitter_sec} startup_retries={startup_retries} startup_retry_delay_sec={startup_retry_delay_sec} has_worker_secret={bool(worker_secret)} strategy_mode={os.getenv('STRATEGY_MODE', 'intraday')}")
    heartbeat("boot", "ok", {"health_url": health_url})
    try:
        status, body = get_text(health_url, timeout=min(timeout, 15))
        body_prefix = body[:500].replace("\n", " ")
        log(f"preflight_ok status={status} body={body_prefix}")
        heartbeat("preflight_ok", "success", {"status": status, "body_prefix": body_prefix})
    except Exception as e:
        log(f"preflight_error err={e!r}")
        heartbeat("preflight_error", "error", {"error": repr(e)})
    first = True
    loop_n = 0
    while True:
        loop_n += 1
        if (not first) or run_on_start:
            retries = startup_retries if first else 1
            for attempt in range(1, retries + 1):
                reason = "startup" if first else "scheduled"
                state["attempts_total"] += 1
                state["attempts_today"] += 1
                state["last_attempt_utc"] = ts_utc()
                log(f"scan_attempt loop={loop_n} attempt={attempt}/{retries} reason={reason} target={url}")
                heartbeat("scan_attempt", "attempt", {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "target": url})
                try:
                    payload = dict(scan_payload)
                    payload["reason"] = reason
                    status, body = post_json(url, payload, timeout=timeout)
                    body_prefix = body[:1000].replace("\n", " ")
                    state["success_total"] += 1
                    state["success_today"] += 1
                    state["consecutive_failures"] = 0
                    state["last_success_utc"] = ts_utc()
                    state["last_error"] = ""
                    log(f"scan_ok loop={loop_n} attempt={attempt}/{retries} reason={reason} status={status} body={body_prefix}")
                    heartbeat("scan_dispatch_ok", "success", {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "status": status, "body_prefix": body_prefix})
                    break
                except urllib.error.HTTPError as e:
                    try:
                        err_body = e.read().decode("utf-8", errors="replace")
                    except Exception:
                        err_body = ""
                    body_prefix = err_body[:1000].replace("\n", " ")
                    state["failure_total"] += 1
                    state["failure_today"] += 1
                    state["consecutive_failures"] += 1
                    state["last_failure_utc"] = ts_utc()
                    state["last_error"] = f"HTTP {e.code} {e.reason}"
                    log(f"scan_http_error loop={loop_n} attempt={attempt}/{retries} reason={reason} status={e.code} err={e.reason} body={body_prefix}")
                    heartbeat("scan_dispatch_http_error", "http_error", {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "status": e.code, "error": f"{e.reason}", "body_prefix": body_prefix})
                except Exception as e:
                    state["failure_total"] += 1
                    state["failure_today"] += 1
                    state["consecutive_failures"] += 1
                    state["last_failure_utc"] = ts_utc()
                    state["last_error"] = repr(e)
                    log(f"scan_error loop={loop_n} attempt={attempt}/{retries} reason={reason} err={e!r}")
                    heartbeat("scan_dispatch_error", "exception", {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "error": repr(e)})
                if attempt < retries:
                    time.sleep(startup_retry_delay_sec)
        first = False
        sleep_for = interval + (random.randint(0, jitter_sec) if jitter_sec > 0 else 0)
        next_run_iso = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + sleep_for, tz=timezone.utc).isoformat()
        log(f"sleep sec={sleep_for}")
        heartbeat("sleep", "ok", {"sleep_sec": sleep_for, "next_run_estimate_utc": next_run_iso})
        time.sleep(sleep_for)

if __name__ == "__main__":
    main()
