"""Bounded public Demo stream, isolated from trading and REST decisions."""
import asyncio
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import time

from .kalshi_demo_stream_archive import Archive
from .kalshi_demo_stream_source import Books, SourceFault

URL = 'wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2'
SESSION_SECONDS = 300
RECONNECT_SECONDS = 60
SUBSCRIPTION_SECONDS = 10
PROTOCOL = 'demo_continuous_stream_availability_v2_20261018'
ARCHIVE_PROTOCOL = 'demo_stream_archive_availability_v2_20261018'
ARCHIVE_NAME = 'stream-source-v2.sqlite3'
STATUS_NAME = 'stream-source-v2-status.json'
REGISTRATION = Path(__file__).resolve().parents[1] / 'configs/kalshi_demo_continuous_stream_20261018/availability-v2.json'


def compact_status(value):
    if not isinstance(value,dict):return None
    fields=('protocol','state','registration_sha256','observed_at','records','compressed_bytes',
            'latest_received_at','acknowledged_channels','target_count','snapshots','deltas',
            'public_trades','trades_with_prior_book','source_fault_code','error_class',
            'execution_enabled','fill_assumed','profitability_proven')
    result={key:value[key] for key in fields if key in value}
    result['unavailable_symbol_count']=len(value.get('unavailable_symbols',[]))
    result['usable_symbol_count']=len(value.get('usable_symbols',[]))
    result['full_status_reference']=STATUS_NAME
    return result


def targets(path):
    with sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=1) as db:
        row = db.execute("SELECT detail FROM settings WHERE name='sampling_window'").fetchone()
    values = json.loads(row[0]).get('tickers') if row else None
    if (not isinstance(values, list) or not 1 <= len(values) <= 8
            or any(not isinstance(t, str) or not re.fullmatch(r'[A-Z0-9_.-]{1,160}', t) for t in values)
            or len(set(values)) != len(values)):
        raise ValueError('stream_targets_unavailable')
    return values


def signed_headers():
    # The signature is used only in the literal Demo WebSocket handshake.
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    key_id = os.environ.get('KALSHI_DEMO_API_KEY_ID')
    key_path = os.environ.get('KALSHI_DEMO_PRIVATE_KEY_PATH')
    if not key_id or not key_path:
        raise ValueError('stream_demo_credentials_missing')
    key = serialization.load_pem_private_key(Path(key_path).read_bytes(), password=None)
    stamp = str(int(time.time() * 1000))
    signature = key.sign((stamp + 'GET/trade-api/ws/v2').encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return {'KALSHI-ACCESS-KEY': key_id, 'KALSHI-ACCESS-TIMESTAMP': stamp,
            'KALSHI-ACCESS-SIGNATURE': base64.b64encode(signature).decode()}


def connector(*args, **kwargs):
    if not args or args[0] != URL:
        raise ValueError('stream_demo_endpoint_required')
    from websockets.asyncio.client import connect
    class NoRedirect(connect):
        def process_redirect(self, exc):
            return exc
    return NoRedirect(*args, **kwargs)


async def session(archive, tickers, stop, publish, *, connect=connector,
                  headers=signed_headers, wall=time.time, mono=time.monotonic):
    """One connection epoch; public data only. Exceptions never reach maker."""
    acknowledged = set()
    counts = {'snapshots': 0, 'deltas': 0, 'public_trades': 0, 'trades_with_prior_book': 0}
    unavailable = set()
    usable_symbols = set()
    async with connect(URL, additional_headers=headers(), open_timeout=10,
                       close_timeout=2, ping_interval=20, ping_timeout=10,
                       max_size=262144, max_queue=32, compression=None) as ws:
        archive.begin(tickers, wall())
        start = mono()
        try:
            await ws.send(json.dumps({'id': 1, 'cmd': 'subscribe', 'params': {
                'channels': ['orderbook_delta', 'trade'], 'market_tickers': tickers}}))
            while not stop.is_set() and mono() - start < SESSION_SECONDS:
                if mono() - start >= SUBSCRIPTION_SECONDS and len(acknowledged) != 2:
                    raise ValueError('stream_subscription_timeout')
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1)
                except asyncio.TimeoutError:
                    continue
                at = wall()
                if not isinstance(raw, str) or len(raw.encode()) > 262144:
                    raise ValueError('stream_invalid_message_size')
                result = archive.append(json.loads(raw), at)
                kind = result['kind']
                if kind == 'subscribed':
                    acknowledged.add(result['channel'])
                elif kind == 'orderbook_snapshot':
                    counts['snapshots'] += 1
                    unavailable.discard(result['ticker'])
                elif kind == 'orderbook_delta':
                    counts['deltas'] += 1
                elif kind == 'trade':
                    counts['public_trades'] += 1
                    counts['trades_with_prior_book'] += bool(result['book_available'])
                elif kind == 'market_unavailable':
                    unavailable.add(result['ticker'])
                if kind in {'orderbook_snapshot','orderbook_delta','market_unavailable'}:
                    if result['usable']:usable_symbols.add(result['ticker'])
                    else:usable_symbols.discard(result['ticker'])
                publish({'state': 'collecting' if len(acknowledged) == 2 else 'subscribing',
                         'acknowledged_channels': len(acknowledged),
                         'unavailable_symbols': sorted(unavailable),
                         'usable_symbols': sorted(usable_symbols),
                         'source_coverage': 'partial' if unavailable else 'selected_targets_only',
                         'counter_scope': 'records_cumulative; book/trade_counts_current_epoch', **counts})
        except BaseException:
            archive.end(wall(), 'source_or_storage_fault')
            raise
        finally:
            if archive.connection is not None:
                archive.end(wall(), 'shutdown' if stop.is_set() else 'normal_stop')
    return counts


class Service:
    """One existing-worker thread and one separately bounded source database."""
    def __init__(self, root):
        self.root = Path(root)
        self.stop_event = threading.Event()
        self.thread = None
        self.lock = threading.Lock()
        self.last_disk_at = 0
        self.last_disk_state = None
        self.value = dict(protocol=PROTOCOL, state='starting', execution_enabled=False,
                          fill_assumed=False, profitability_proven=False)

    def status(self):
        with self.lock:
            return dict(self.value)

    def publish(self, values):
        with self.lock:
            self.value.update(values, protocol=PROTOCOL, observed_at=time.time(), execution_enabled=False,
                              fill_assumed=False, profitability_proven=False)
            value = dict(self.value)
        # Atomic local status is also available if REST is down or maker cannot publish.
        path = self.root / STATUS_NAME
        if value['state'] == self.last_disk_state and time.monotonic() - self.last_disk_at < 1:
            return
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(value, separators=(',', ':'), allow_nan=False))
        temporary.replace(path)
        self.last_disk_at, self.last_disk_state = time.monotonic(), value['state']

    def start(self):
        if self.thread is not None:
            raise ValueError('stream_service_already_started')
        self.thread = threading.Thread(target=self.run, name='kalshi-demo-public-stream', daemon=True)
        self.thread.start()
        return self

    def close(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=15)
        return self.thread is None or not self.thread.is_alive()

    def run(self):
        archive = None
        try:
            spec = json.loads(REGISTRATION.read_text())
            if (spec['protocol'] != PROTOCOL or spec['endpoint'] != URL
                    or spec['execution_enabled'] is not False or spec['max_messages'] != 100000
                    or spec['session_seconds'] != SESSION_SECONDS):
                raise ValueError('stream_registration_mismatch')
            self.publish({'registration_sha256': hashlib.sha256(REGISTRATION.read_bytes()).hexdigest()})
            archive = Archive(self.root / ARCHIVE_NAME, protocol=ARCHIVE_PROTOCOL,
                              decoder_factory=lambda tickers:Books(tickers,allow_unavailable=True))
            while not self.stop_event.is_set():
                try:
                    tickers = targets(self.root / 'worker.sqlite3')
                    self.publish({'state': 'connecting', 'target_count': len(tickers),
                                  'error_class':None,'source_fault_code':None})
                    def progress(values):
                        self.publish({**archive.status(), **values})
                    asyncio.run(session(archive, tickers, self.stop_event, progress))
                    self.publish({**archive.status(), 'state': 'between_epochs'})
                except Exception as exc:
                    # No raw error bodies, request headers or credentials in status.
                    capacity = isinstance(exc, (ValueError, sqlite3.OperationalError)) and any(
                        word in str(exc) for word in ('capacity', 'low_disk', 'database or disk is full'))
                    self.publish({'state': 'capacity_stop' if capacity else 'source_or_transport_fault',
                                  'error_class': type(exc).__name__, **archive.status(),
                                  'source_fault_code': str(exc) if isinstance(exc,SourceFault) and re.fullmatch(r'[a-z_]{1,80}',str(exc)) else None})
                    if capacity:
                        break
                self.stop_event.wait(RECONNECT_SECONDS)
        except Exception as exc:
            self.publish({'state': 'initialization_fault', 'error_class': type(exc).__name__})
        finally:
            if archive is not None:
                archive.close()
            if self.stop_event.is_set():
                self.publish({'state': 'stopped'})
