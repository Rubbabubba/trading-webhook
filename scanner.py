import json
import os
import time
import urllib.request
import urllib.error
import random
import uuid
from datetime import datetime, timezone, timedelta

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

def transient_main_web_status(err: Exception) -> int | None:
    if isinstance(err, urllib.error.HTTPError) and int(err.code or 0) in {502, 503, 504}:
        return int(err.code)
    return None


def brief_body(text: str, limit: int = 240) -> str:
    return (text or "")[: max(0, int(limit or 0))].replace("\n", " ").replace("\r", " ")


def wait_for_main_web_ready(health_url: str, timeout: int, grace_sec: int, poll_sec: int) -> dict:
    grace_sec = max(0, int(grace_sec or 0))
    poll_sec = max(1, int(poll_sec or 1))
    deadline = time.monotonic() + grace_sec
    attempt = 0
    last_status = None
    last_kind = ""
    last_detail = ""

    while True:
        attempt += 1
        try:
            status, body = get_text(health_url, timeout=min(timeout, 15))
            return {
                "ready": True,
                "attempts": attempt,
                "status": status,
                "body_prefix": brief_body(body, 500),
                "waited_sec": max(0, int(grace_sec - max(0, deadline - time.monotonic()))),
            }
        except Exception as e:
            transient_status = transient_main_web_status(e)
            last_status = transient_status
            last_kind = type(e).__name__
            last_detail = f"HTTP {transient_status}" if transient_status else repr(e)

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {
                    "ready": False,
                    "attempts": attempt,
                    "status": last_status,
                    "error_kind": last_kind,
                    "detail": last_detail,
                    "waited_sec": grace_sec,
                }

            if transient_status:
                log(f"main_web_not_ready attempt={attempt} status={transient_status} retry_in_sec={min(poll_sec, int(max(1, remaining)))}")
            else:
                log(f"main_web_readiness_wait attempt={attempt} kind={last_kind} retry_in_sec={min(poll_sec, int(max(1, remaining)))}")

            time.sleep(min(poll_sec, max(1, remaining)))

def is_timeout_error(err: Exception) -> bool:
    text = repr(err).lower()
    return "timed out" in text or "timeout" in text
    
def getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default

def fast_no_trade_recheck_sleep_sec(body_text: str, default_sleep_sec: int) -> int | None:
    if not getenv_bool("SCAN_FAST_NO_TRADE_RECHECK_ENABLED", True):
        return None
    try:
        payload = json.loads(body_text or "{}")
    except Exception:
        payload = {}

    summary = {}
    if isinstance(payload.get("scanner"), dict):
        summary = dict((payload.get("scanner") or {}).get("summary") or {})
    if not summary and isinstance(payload.get("summary"), dict):
        summary = dict(payload.get("summary") or {})

    hint = dict(summary.get("fast_no_trade_recheck") or {})
    if not bool(hint.get("apply")):
        return None

    try:
        requested = int(hint.get("sleep_sec") or getenv_int("SCAN_FAST_NO_TRADE_RECHECK_SEC", 300))
    except Exception:
        requested = getenv_int("SCAN_FAST_NO_TRADE_RECHECK_SEC", 300)

    floor_sec = max(30, getenv_int("SCAN_FAST_NO_TRADE_RECHECK_MIN_SEC", 120))
    return max(floor_sec, min(int(default_sleep_sec), int(requested)))

def market_open_catchup_sleep_sec(body_text: str, default_sleep_sec: int) -> int | None:
    if not getenv_bool("SCAN_MARKET_OPEN_CATCHUP_ENABLED", True):
        return None
    try:
        payload = json.loads(body_text or "{}")
    except Exception:
        payload = {}
    reason = str(payload.get("reason") or "").strip()
    if reason != "outside_market_hours":
        return None

    now = datetime.now(timezone.utc)
    open_hour = getenv_int("SCAN_MARKET_OPEN_UTC_HOUR", 13)
    open_minute = getenv_int("SCAN_MARKET_OPEN_UTC_MINUTE", 30)
    after_open_grace_sec = max(0, getenv_int("SCAN_MARKET_OPEN_CATCHUP_AFTER_OPEN_GRACE_SEC", 900))
    before_open_window_sec = max(0, getenv_int("SCAN_MARKET_OPEN_CATCHUP_BEFORE_OPEN_WINDOW_SEC", 1800))
    buffer_sec = max(1, getenv_int("SCAN_MARKET_OPEN_CATCHUP_BUFFER_SEC", 20))
    fallback_sec = max(15, getenv_int("SCAN_MARKET_OPEN_CATCHUP_SEC", 60))

    open_dt = now.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
    if now > open_dt + timedelta(seconds=after_open_grace_sec):
        return None
    if now < open_dt - timedelta(seconds=before_open_window_sec):
        return None

    if now < open_dt:
        return max(15, int((open_dt - now).total_seconds()) + buffer_sec)
    return min(default_sleep_sec, fallback_sec)

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
    sleep_heartbeat_sec = max(15, getenv_int("SCAN_SLEEP_HEARTBEAT_SEC", 60))
    background_accepted_recheck_sec = max(60, getenv_int("SCAN_BACKGROUND_ACCEPTED_RECHECK_SEC", 240))
    main_ready_grace_sec = max(0, getenv_int("SCAN_MAIN_READY_GRACE_SEC", 180))
    main_ready_poll_sec = max(1, getenv_int("SCAN_MAIN_READY_POLL_SEC", 10))
    worker_secret = (os.getenv("WORKER_SECRET") or os.getenv("INTERNAL_API_KEY") or "").strip()
    scan_payload: dict = {}
    if worker_secret:
        scan_payload["worker_secret"] = worker_secret
    for k in ["mode", "provider", "symbols_provider", "symbols", "max_symbols"]:
        envk = f"SCAN_{k.upper()}"
        v = os.getenv(envk)
        if v is not None and v != "":
            scan_payload[k] = v

    # Patch 455: do not silently force the main webhook back to 25 symbols.
    # Prefer explicit scanner envs, then the shared scanner cap, then the swing
    # runtime cap. This keeps the worker aligned with the current 41-symbol setup.
    default_max_symbols = (
        os.getenv("SCAN_MAX_SYMBOLS")
        or os.getenv("SCANNER_MAX_SYMBOLS_PER_CYCLE")
        or os.getenv("SWING_RUNTIME_SLIM_MAX_SYMBOLS")
        or "50"
    )
    scan_payload.setdefault("max_symbols", default_max_symbols)
    scan_payload.setdefault("runtime_slim", os.getenv("SCAN_RUNTIME_SLIM", "true"))
    boot_id = str(uuid.uuid4())
    transient_backoff_sec = max(30, min(interval, getenv_int("SCAN_TRANSIENT_MAIN_BACKOFF_SEC", 60)))
    transient_main_web = {"suppress_until": 0.0, "status": None, "last_utc": None}
    state = {"boot_id": boot_id, "boot_ts_utc": ts_utc(), "attempts_total": 0, "success_total": 0, "failure_total": 0, "attempts_today": 0, "success_today": 0, "failure_today": 0, "main_unavailable_total": 0, "main_unavailable_today": 0, "consecutive_failures": 0, "last_attempt_utc": None, "last_success_utc": None, "last_failure_utc": None, "last_main_unavailable_utc": None, "last_error": "", "pid": os.getpid(), "interval_sec": interval, "timeout_sec": timeout, "run_on_start": run_on_start, "jitter_sec": jitter_sec, "sleep_heartbeat_sec": sleep_heartbeat_sec, "main_ready_grace_sec": main_ready_grace_sec, "main_ready_poll_sec": main_ready_poll_sec, "transient_main_backoff_sec": transient_backoff_sec, "runtime_slim": scan_payload.get("runtime_slim"), "max_symbols": scan_payload.get("max_symbols")}
    def heartbeat(event: str, status: str = "ok", details: dict | None = None) -> None:
        if time.monotonic() < float(transient_main_web.get("suppress_until") or 0.0):
            return
        payload = {"worker_secret": worker_secret, "event": event, "status": status, "details": {**state, **(details or {})}}
        try:
            post_json(heartbeat_url, payload, timeout=min(timeout, 15))
        except Exception as e:
            transient_status = transient_main_web_status(e)
            if transient_status:
                transient_main_web["suppress_until"] = time.monotonic() + transient_backoff_sec
                transient_main_web["status"] = transient_status
                transient_main_web["last_utc"] = ts_utc()
                return
            log(f"heartbeat_post_failed event={event} kind={type(e).__name__} detail={brief_body(repr(e), 180)}")
    log(f"boot url={url} base_url={base_url} interval_sec={interval} timeout_sec={timeout} run_on_start={run_on_start} jitter_sec={jitter_sec} startup_retries={startup_retries} startup_retry_delay_sec={startup_retry_delay_sec} sleep_heartbeat_sec={sleep_heartbeat_sec} main_ready_grace_sec={main_ready_grace_sec} main_ready_poll_sec={main_ready_poll_sec} has_worker_secret={bool(worker_secret)} strategy_mode={os.getenv('STRATEGY_MODE', 'intraday')}")

    readiness = wait_for_main_web_ready(
        health_url=health_url,
        timeout=timeout,
        grace_sec=main_ready_grace_sec,
        poll_sec=main_ready_poll_sec,
    )
    state["main_ready"] = bool(readiness.get("ready"))
    state["main_ready_attempts"] = readiness.get("attempts")
    state["main_ready_waited_sec"] = readiness.get("waited_sec")

    if readiness.get("ready"):
        log(f"main_web_ready status={readiness.get('status')} attempts={readiness.get('attempts')} waited_sec={readiness.get('waited_sec')}")
        heartbeat("boot", "ok", {"health_url": health_url, "main_web_readiness": readiness})
        heartbeat("preflight_ok", "success", {"status": readiness.get("status"), "body_prefix": readiness.get("body_prefix"), "main_web_readiness": readiness})
    else:
        transient_main_web["suppress_until"] = time.monotonic() + transient_backoff_sec
        transient_main_web["status"] = readiness.get("status")
        transient_main_web["last_utc"] = ts_utc()
        state["last_main_unavailable_utc"] = transient_main_web["last_utc"]
        log(f"main_web_readiness_deferred attempts={readiness.get('attempts')} waited_sec={readiness.get('waited_sec')} detail={readiness.get('detail')}")
        heartbeat("boot", "main_web_not_ready", {"health_url": health_url, "main_web_readiness": readiness})
        heartbeat("preflight_deferred", "main_web_unavailable", {"detail": readiness.get("detail"), "main_web_readiness": readiness})
    first = True
    loop_n = 0
    while True:
        loop_n += 1
        if (not first) or run_on_start:
            retries = startup_retries if first else 1
            reason = "startup" if first else "scheduled"
            scan_attempt_id = f"{boot_id}:{loop_n}:{reason}"
            for attempt in range(1, retries + 1):
                state["attempts_total"] += 1
                state["attempts_today"] += 1
                state["last_attempt_utc"] = ts_utc()
                log(f"scan_attempt loop={loop_n} attempt={attempt}/{retries} reason={reason} target={url}")
                heartbeat("scan_attempt", "attempt", {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "target": url, "scan_attempt_id": scan_attempt_id})
                try:
                    payload = dict(scan_payload)
                    payload["reason"] = reason
                    payload["scan_attempt_id"] = scan_attempt_id
                    payload["timeout_sec"] = timeout
                    payload["fast_response"] = True
                    status, body = post_json(url, payload, timeout=timeout)
                    body_prefix = brief_body(body, 1000)
                    catchup_sleep_sec = market_open_catchup_sleep_sec(body, interval)
                    fast_recheck_sleep_sec = fast_no_trade_recheck_sleep_sec(body, interval)
                    if fast_recheck_sleep_sec is not None:
                        catchup_sleep_sec = fast_recheck_sleep_sec
                    accepted_not_completed = False
                    try:
                        response_payload = json.loads(body or "{}")
                        accepted_not_completed = (
                            status == 202
                            or str(response_payload.get("scan_contract") or "").strip().lower() == "accepted_not_completed"
                            or str(response_payload.get("reason") or "").strip().lower() == "swing_scan_background_accepted"
                            or str(response_payload.get("status") or "").strip().lower() == "accepted"
                            or bool(response_payload.get("accepted") and response_payload.get("background_completion"))
                        )
                    except Exception:
                        accepted_not_completed = status == 202
                    if accepted_not_completed:
                        catchup_sleep_sec = background_accepted_recheck_sec
                    state["success_total"] += 1
                    state["success_today"] += 1
                    state["consecutive_failures"] = 0
                    state["last_success_utc"] = ts_utc()
                    state["last_error"] = ""
                    event_name = "scan_dispatch_accepted" if accepted_not_completed else "scan_dispatch_ok"
                    event_status = "accepted" if accepted_not_completed else "success"
                    log(f"{event_name} loop={loop_n} attempt={attempt}/{retries} reason={reason} status={status} body={body_prefix}")
                    heartbeat(event_name, event_status, {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "status": status, "body_prefix": body_prefix, "scan_attempt_id": scan_attempt_id, "catchup_sleep_sec": catchup_sleep_sec, "fast_no_trade_recheck": fast_recheck_sleep_sec is not None, "accepted_not_completed": accepted_not_completed})
                    if catchup_sleep_sec is not None:
                        state["market_open_catchup_sleep_sec"] = catchup_sleep_sec
                    break
                except urllib.error.HTTPError as e:
                    try:
                        err_body = e.read().decode("utf-8", errors="replace")
                    except Exception:
                        err_body = ""
                    body_prefix = brief_body(err_body, 240)
                    transient_status = transient_main_web_status(e)
                    if transient_status:
                        state["main_unavailable_total"] += 1
                        state["main_unavailable_today"] += 1
                        state["last_main_unavailable_utc"] = ts_utc()
                        state["last_error"] = ""
                        transient_main_web["suppress_until"] = time.monotonic() + transient_backoff_sec
                        transient_main_web["status"] = transient_status
                        transient_main_web["last_utc"] = state["last_main_unavailable_utc"]
                        state["market_open_catchup_sleep_sec"] = transient_backoff_sec
                        log(f"scan_dispatch_deferred loop={loop_n} attempt={attempt}/{retries} reason={reason} status={e.code} retry_in_sec={transient_backoff_sec}")
                        if attempt >= retries:
                            break
                        time.sleep(min(startup_retry_delay_sec, transient_backoff_sec))
                        continue
                    state["failure_total"] += 1
                    state["failure_today"] += 1
                    state["consecutive_failures"] += 1
                    state["last_failure_utc"] = ts_utc()
                    state["last_error"] = f"HTTP {e.code} {e.reason}"
                    log(f"scan_http_failure loop={loop_n} attempt={attempt}/{retries} reason={reason} status={e.code} detail={e.reason} body={body_prefix}")
                    heartbeat("scan_dispatch_http_error", "http_error", {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "status": e.code, "error": f"{e.reason}", "body_prefix": body_prefix, "scan_attempt_id": scan_attempt_id})
                except Exception as e:
                    state["failure_total"] += 1
                    state["failure_today"] += 1
                    state["consecutive_failures"] += 1
                    state["last_failure_utc"] = ts_utc()
                    state["last_error"] = repr(e)
                    timeout_failure = is_timeout_error(e)
                    failure_status = "timeout_failure" if timeout_failure else "exception"
                    log(f"scan_error loop={loop_n} attempt={attempt}/{retries} reason={reason} status={failure_status} err={e!r}")
                    heartbeat("scan_dispatch_error", failure_status, {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "error": repr(e), "timeout_failure": timeout_failure, "scan_attempt_id": scan_attempt_id})
                    heartbeat("scan_fail", failure_status, {"loop": loop_n, "attempt": attempt, "retries": retries, "reason": reason, "error": repr(e), "timeout_failure": timeout_failure, "scan_attempt_id": scan_attempt_id})
                if attempt < retries:
                    time.sleep(startup_retry_delay_sec)
        first = False
        catchup_sleep_sec = state.pop("market_open_catchup_sleep_sec", None)
        if catchup_sleep_sec is not None:
            sleep_for = int(catchup_sleep_sec)
        else:
            sleep_for = interval + (random.randint(0, jitter_sec) if jitter_sec > 0 else 0)
        next_run_iso = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + sleep_for, tz=timezone.utc).isoformat()
        log(f"sleep sec={sleep_for} market_open_catchup={catchup_sleep_sec is not None}")
        heartbeat("sleep", "ok", {"sleep_sec": sleep_for, "next_run_estimate_utc": next_run_iso, "market_open_catchup": catchup_sleep_sec is not None})
        remaining_sleep = sleep_for
        while remaining_sleep > 0:
            chunk = min(remaining_sleep, sleep_heartbeat_sec)
            time.sleep(chunk)
            remaining_sleep -= chunk
            if remaining_sleep > 0:
                heartbeat("heartbeat", "ok", {"sleep_sec": sleep_for, "sleep_remaining_sec": remaining_sleep, "next_run_estimate_utc": next_run_iso})

if __name__ == "__main__":
    main()
