import json
import os
import time
import urllib.request
import urllib.error
import random

def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default

def resolve_scan_url() -> str | None:
    # Preferred: full URL
    url = os.getenv("SCAN_ENTRIES_URL") or os.getenv("SWING_SCAN_ENTRIES_URL")
    if url:
        return url.strip()

    # Back-compat: base URL(s)
    base = os.getenv("MAIN_SERVICE_URL") or os.getenv("WORKER_BASE_URL")
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

def getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in {"1","true","yes","y","on"}


def main() -> None:
    url = resolve_scan_url()
    if not url:
        print("[scanner] ERROR: missing target URL. Set SCAN_ENTRIES_URL (recommended, full URL) or MAIN_SERVICE_URL/WORKER_BASE_URL (base URL of the main service).")
        raise SystemExit(2)

    interval = getenv_int("SCAN_INTERVAL_SEC", getenv_int("SWING_SCAN_INTERVAL_SEC", 3600))
    timeout = getenv_int("SCAN_TIMEOUT_SEC", 60)
    run_on_start = getenv_bool("SCAN_RUN_ON_START", True)
    jitter_sec = max(0, getenv_int("SCAN_JITTER_SEC", 0))

    # IMPORTANT: main service expects worker_secret in JSON body when WORKER_SECRET is set.
    worker_secret = (os.getenv("WORKER_SECRET") or "").strip()
    if not worker_secret:
        # Fallback for older env naming (not recommended)
        worker_secret = (os.getenv("INTERNAL_API_KEY") or "").strip()

    payload: dict = {}
    if worker_secret:
        payload["worker_secret"] = worker_secret

    # Optional knobs passed through to the server (only if set)
    for k in ["mode", "provider", "symbols_provider", "symbols", "max_symbols"]:
        envk = f"SCAN_{k.upper()}"
        v = os.getenv(envk)
        if v is not None and v != "":
            payload[k] = v

    print(f"[scanner] starting loop: url={url} interval={interval}s timeout={timeout}s has_worker_secret={bool(worker_secret)} strategy_mode={os.getenv("STRATEGY_MODE", "intraday")} run_on_start={run_on_start} jitter_sec={jitter_sec}")

    first = True
    while True:
        if not first or run_on_start:
            try:
                payload["reason"] = "startup" if first else "scheduled"
                status, body = post_json(url, payload, timeout=timeout)
                print(f"[scanner] ok: status={status} bytes={len(body)}")
            except urllib.error.HTTPError as e:
                try:
                    err_body = e.read().decode("utf-8", errors="replace")
                except Exception:
                    err_body = ""
                print(f"[scanner] error: HTTP {e.code} {e.reason} body={err_body[:500]}")
            except Exception as e:
                print(f"[scanner] error: {e!r}")
        first = False
        sleep_for = interval + (random.randint(0, jitter_sec) if jitter_sec > 0 else 0)
        time.sleep(sleep_for)

if __name__ == "__main__":
    main()
