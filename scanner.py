import json
import os
import time
import urllib.request
import urllib.error

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
    url = os.getenv("SCAN_ENTRIES_URL")
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

def main() -> None:
    url = resolve_scan_url()
    if not url:
        print("[scanner] ERROR: missing target URL. Set SCAN_ENTRIES_URL (recommended, full URL) or MAIN_SERVICE_URL/WORKER_BASE_URL (base URL of the main service).")
        raise SystemExit(2)

    interval = getenv_int("SCAN_INTERVAL_SEC", 60)
    timeout = getenv_int("SCAN_TIMEOUT_SEC", 60)

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

    print(f"[scanner] starting loop: url={url} interval={interval}s timeout={timeout}s has_worker_secret={bool(worker_secret)}")

    while True:
        try:
            status, body = post_json(url, payload, timeout=timeout)
            print(f"[scanner] ok: status={status} bytes={len(body)}")
        except urllib.error.HTTPError as e:
            # HTTPError is also a valid response object, but treat as error with body
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            print(f"[scanner] error: HTTP {e.code} {e.reason} body={err_body[:500]}")
        except Exception as e:
            print(f"[scanner] error: {e!r}")
        time.sleep(interval)

if __name__ == "__main__":
    main()
