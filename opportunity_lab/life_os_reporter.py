"""Optional asynchronous export of the Kalshi demo monitor packet to Life OS."""

from __future__ import annotations

import json
import os
import threading
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen


_lock = threading.Lock()
_last_scheduled = 0.0
_inflight = False


def wallet_copy_status(path=Path('/var/data/kalshi-demo-v9/ranked-wallet-copy-v1/status.json'), *, now=None):
    """Bounded optional read-only health; absent inventory stays unknown."""
    now = now or datetime.now(timezone.utc)
    invalid = {'state': 'invalid', 'at': now.isoformat()}
    try:
        with Path(path).open('rb') as stream:
            raw = stream.read(32769)
        if len(raw) > 32768:
            return invalid
        data = json.loads(raw)
        if not isinstance(data, dict) or data.get('environment') != 'demo':
            return invalid
        at = datetime.fromisoformat(data['at'].replace('Z', '+00:00'))
        age = (now - at).total_seconds()
        if age < -5:
            return invalid
        result = dict(state='stale' if age > 180 else 'observed', at=at.isoformat(), environment='demo')
        def count(value):
            return value if type(value) is int and 0 <= value <= 1000000000 else None
        for key in ('ranked_wallets', 'wallets_polled', 'open_positions'):
            result[key] = count(data.get(key))
        signals = data.get('signals')
        result['signal_counts_lifetime'] = {key: count(value) for key, value in list(signals.items())[:64] if isinstance(key, str) and re.fullmatch(r'(buy|sell):[a-z_]{1,64}', key) and count(value) is not None} if isinstance(signals, dict) else {}
        def code(value):
            return value if isinstance(value, str) and re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{0,79}', value) else None
        source_errors = data.get('source_errors')
        result['source_error_count'] = len(source_errors) if isinstance(source_errors, list) else None
        result['current_error_present'] = bool(data.get('error') or data.get('execution_error'))
        result['current_error'] = {'code':code(data.get('error') or data.get('execution_error')), 'at':at.isoformat()}
        if 'lifecycle' in data:
            from .kalshi_wallet_lifecycle import normalize
            result['lifecycle']=normalize(data['lifecycle'])
        failure = data.get('last_submission_failure')
        if isinstance(failure, dict):
            try:
                stamp = failure.get('observed_at')
                stamp = datetime.fromtimestamp(stamp, timezone.utc) if type(stamp) in (int, float) else datetime.fromisoformat(stamp.replace('Z', '+00:00'))
                if stamp.tzinfo is not None and (now - stamp).total_seconds() >= -5:
                    result['latest_submission_error'] = {'code':code(failure.get('error')), 'at':stamp.isoformat()}
            except (ValueError, TypeError, AttributeError, OverflowError, OSError):
                # Invalid optional errors must not discard valid health coverage.
                pass
        return result
    except FileNotFoundError:
        return {'state':'missing', 'at':now.isoformat()}
    except Exception:
        return invalid


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
    from .kalshi_historical_diagnostics import collect
    try:
        history = collect(Path("/var/data/kalshi-demo-v9"))
    except Exception as error:
        # Optional historical recovery cannot stop the original monitor export.
        history = {"schema":"kalshi_historical_diagnostics_v1_20261101",
                   "coverage_error":type(error).__name__}
    try:
        copier = wallet_copy_status()
    except Exception:
        copier = {'state':'invalid', 'at':datetime.now(timezone.utc).isoformat()}
    try:
        from .kalshi_wallet_profitability import read_report,DIRECTORY as research_directory
        copier["profitability"]=read_report(Path("/var/data/kalshi-demo-v9")/research_directory/"status.json")
    except Exception:
        copier["profitability"]=None
    from .kalshi_wallet_cap_shadow import read_status as read_cap_shadow, DIRECTORY as cap_shadow
    observation = read_cap_shadow(Path('/var/data/kalshi-demo-v9') / cap_shadow / 'status.json')
    if observation is not None:
        copier['cap_shadow'] = observation
    from .kalshi_wallet_matched_demo import read_status, DIRECTORY as matched_directory
    from .kalshi_wallet_targeted_control import DIRECTORY as targeted_directory, HASH as targeted_hash
    targeted = read_status(Path('/var/data/kalshi-demo-v9') / targeted_directory / 'status.json')
    if targeted is not None and targeted.get('registration_sha256') == targeted_hash:
        copier['targeted_demo'] = targeted
    matched = read_status(Path('/var/data/kalshi-demo-v9') / matched_directory / 'status.json')
    if matched is not None and copier['profitability'] is not None:
        from copy import deepcopy
        from .wallet_pool_audit import bound_report
        expanded = deepcopy(copier['profitability'])
        expanded['matched_demo'] = matched
        try:
            bound_report(expanded, time.time())
            copier['profitability'] = expanded
        except ValueError:
            pass
    packet = dict(packet, historical_diagnostics=history, wallet_copy=copier)
    revision=os.getenv('RENDER_GIT_COMMIT','')
    service=os.getenv('RENDER_SERVICE_ID','')
    if re.fullmatch('[0-9a-f]{40}',revision) and service=='srv-dalhjqv40ujc73e2km1g':
        packet['workforce_runtime']={'revision':revision,'service_id':service,
                                     'reported_at':datetime.now(timezone.utc).isoformat()}
    from .map_capacity import sample
    body = json.dumps(dict(packet, resource_capacity=sample()), separators=(",", ":")).encode("utf-8")
    if len(body) > 200_000:
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


def schedule(packet_path: str | Path, *, urgent: bool = False, interval_seconds: int = 60) -> bool:
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
