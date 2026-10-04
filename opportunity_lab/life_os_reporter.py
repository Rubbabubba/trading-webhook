"""Optional asynchronous export of the Kalshi demo monitor packet to Life OS."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen


_lock = threading.Lock()
_last_scheduled = 0.0
_inflight = False


def publish(packet: dict, *, url: str | None = None, token: str | None = None) -> bool:
    target = url if url is not None else os.getenv("LIFE_OS_KALSHI_INGEST_URL", "")
    credential = token if token is not None else os.getenv("LIFE_OS_KALSHI_INGEST_TOKEN", "")
    if not target and not credential:
        return False
    parsed = urlparse(target)
    if not credential or parsed.scheme != "https" or parsed.path != "/ingest/kalshi" or parsed.username or parsed.password:
        raise ValueError("Life OS Kalshi report destination must be a configured HTTPS intake")
    worker = packet.get("worker") or {}
    if worker.get("environment") != "demo" or worker.get("production_execution_enabled") is not False:
        raise ValueError("Only Kalshi demo reports may be sent to Life OS")
    body = json.dumps(packet, separators=(",", ":")).encode("utf-8")
    if len(body) > 200_000:
        raise ValueError("Life OS Kalshi report exceeds the intake limit")
    request = Request(target, body, {"Authorization": f"Bearer {credential}", "Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=5) as response:
        if response.status != 200:
            raise RuntimeError("Life OS Kalshi intake did not accept the report")
    return True


def fetch_ideas(destination: str | Path, *, url: str | None = None,
                token: str | None = None) -> int:
    """Fetch owner-system research specifications, never code or orders."""
    target = url if url is not None else os.getenv("LIFE_OS_KALSHI_INGEST_URL", "")
    credential = token if token is not None else os.getenv("LIFE_OS_KALSHI_INGEST_TOKEN", "")
    parsed = urlparse(target)
    if (not credential or parsed.scheme != "https" or parsed.path != "/ingest/kalshi"
            or parsed.username or parsed.password):
        raise ValueError("Life OS Kalshi destination is invalid")
    endpoint = parsed._replace(path="/research/kalshi/ideas", query="", fragment="").geturl()
    request = Request(endpoint, headers={"Authorization": f"Bearer {credential}"}, method="GET")
    with urlopen(request, timeout=5) as response:
        data = response.read(32769)
    if len(data) > 32768:
        raise ValueError("Life OS idea response exceeds limit")
    ideas = json.loads(data)
    if (not isinstance(ideas, dict) or ideas.get("schema") != "kalshi_research_ideas_v1"
            or not isinstance(ideas.get("ideas"), list) or len(ideas["ideas"]) > 32):
        raise ValueError("Life OS idea response invalid")
    path = Path(destination)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(ideas, sort_keys=True), encoding="utf-8")
    temporary.replace(path)
    return len(ideas["ideas"])


def schedule(packet_path: str | Path, *, urgent: bool = False, interval_seconds: int = 900) -> bool:
    """Never make the worker's trading loop wait for Life OS or its network."""
    global _last_scheduled, _inflight
    if not os.getenv("LIFE_OS_KALSHI_INGEST_URL"):
        return False
    with _lock:
        now = time.monotonic()
        if _inflight or (not urgent and now - _last_scheduled < interval_seconds):
            return False
        _last_scheduled, _inflight = now, True

    def send() -> None:
        global _inflight
        try:
            packet = json.loads(Path(packet_path).read_text(encoding="utf-8"))
            if publish(packet):
                fetch_ideas(Path(packet_path).parent.parent / "life_os_strategy_ideas.json")
        except Exception as exc:
            print(f"Life OS Kalshi report export failed: {type(exc).__name__}", flush=True)
        finally:
            with _lock:
                _inflight = False

    threading.Thread(target=send, daemon=True, name="life-os-kalshi-report").start()
    return True
