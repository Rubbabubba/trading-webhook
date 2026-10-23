"""Bounded public Demo stream, isolated from trading and REST decisions."""
import asyncio
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import shutil
import threading
import time

from .kalshi_collection_recovery import RecoveryArchive as Archive, retained_evidence, lifetime_start
from .kalshi_demo_stream_source import SourceFault
from .kalshi_stream_availability_v5 import AvailabilityBooks as Books

URL = 'wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2'
SESSION_SECONDS = 300
RECONNECT_SECONDS = 60
SUBSCRIPTION_SECONDS = 10
PROTOCOL = 'demo_continuous_stream_availability_v5_20261023'
ARCHIVE_PROTOCOL = 'demo_stream_archive_availability_v5_20261023'
ARCHIVE_NAME = 'stream-source-v5.sqlite3'
STATUS_NAME = 'stream-source-v5-status.json'
REGISTRATION = Path(__file__).resolve().parents[1] / 'configs/kalshi_collection_recovery_20261023/availability-v5.json'


def compact_status(value):
    if not isinstance(value,dict):return None
    fields=('protocol','state','registration_sha256','observed_at','records','compressed_bytes',
            'latest_received_at','acknowledged_channels','target_count','snapshots','deltas',
            'public_trades','trades_with_prior_book','source_fault_code','error_class','comparison_error_class',
            'execution_enabled','fill_assumed','profitability_proven','storage_stop_reason','retained_evidence')
    result={key:value[key] for key in fields if key in value}
    result['unavailable_symbol_count']=len(value.get('unavailable_symbols',[]))
    result['usable_symbol_count']=len(value.get('usable_symbols',[]))
    result['full_status_reference']=STATUS_NAME
    # Ledger admission and raw-source capture are different states. Retained
    # usable ladders/counts must not imply new capture after a source cap/stop.
    source_active = (value.get('state') == 'collecting'
                     and value.get('acknowledged_channels') == 2
                     and bool(value.get('usable_symbols')))
    result['new_source_inputs_active'] = source_active
    result['retained_book_counts_only'] = not source_active
    comparison=value.get('formula_comparison')
    if isinstance(comparison,dict):
        result['formula_comparison']={key:comparison.get(key) for key in (
            'protocol','state','accepted_inputs','signals','complete_signals','complete_events','execution_enabled',
            'stop_reason','sqlite_page_cap','sqlite_pages')}
        result['formula_comparison']['outcome_coverage']=comparison.get('outcome_coverage')
        result['formula_comparison']['source_input_state'] = (
            'source_active' if source_active else 'source_' + str(value.get('state', 'unknown')))
        # Keep the immutable comparison ledger state, caps and missed outcomes.
        # Outcome collection can finish even while new source input is stopped.

    for key in ('prior_formula_comparison','comparison_outcomes'):
        if key in value:result[key]=value[key]
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
                  headers=signed_headers, wall=time.time, mono=time.monotonic,on_book=None):
    """One connection epoch; public data only. Exceptions never reach maker."""
    acknowledged = set()
    counts = {'snapshots': 0, 'deltas': 0, 'public_trades': 0, 'trades_with_prior_book': 0}
    unavailable = set()
    unavailable_observations = {}
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
                    # Flush the last burst's throttled status even when the market
                    # goes quiet. This is process health, never a new source tick:
                    # archive counts and latest_received_at remain unchanged.
                    publish({'state': 'collecting' if len(acknowledged) == 2 else 'subscribing',
                             'acknowledged_channels': len(acknowledged)})
                    continue
                at = wall()
                if not isinstance(raw, str) or len(raw.encode()) > 262144:
                    raise ValueError('stream_invalid_message_size')
                payload=json.loads(raw)
                result = archive.append(payload, at)
                kind = result['kind']
                if kind == 'subscribed':
                    acknowledged.add(result['channel'])
                elif kind == 'orderbook_snapshot':
                    counts['snapshots'] += 1
                    unavailable.discard(result['ticker'])
                    unavailable_observations.pop(result['ticker'],None)
                elif kind == 'orderbook_delta':
                    counts['deltas'] += 1
                elif kind == 'trade':
                    counts['public_trades'] += 1
                    counts['trades_with_prior_book'] += bool(result['book_available'])
                elif kind == 'market_unavailable':
                    unavailable.add(result['ticker'])
                    fingerprint=hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
                    unavailable_observations[result['ticker']]={'observed_at':at,
                        'source_id':hashlib.sha256((archive.connection+fingerprint).encode()).hexdigest()}
                if kind in {'orderbook_snapshot','orderbook_delta','market_unavailable'}:
                    if result['usable']:usable_symbols.add(result['ticker'])
                    else:usable_symbols.discard(result['ticker'])
                    if on_book is not None:
                        try:
                            on_book(archive,result,at)
                            publish({'comparison_error_class':None})
                        except Exception as exc:publish({'comparison_error_class':type(exc).__name__})
                publish({'state': 'collecting' if len(acknowledged) == 2 else 'subscribing',
                         'acknowledged_channels': len(acknowledged),
                         'unavailable_symbols': sorted(unavailable),
                         'unavailable_observations': dict(unavailable_observations),
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
        self.outcome_thread = None
        self.lock = threading.Lock()
        self.publish_lock = threading.Lock()
        self.last_disk_at = 0
        self.last_disk_state = None
        self.value = dict(protocol=PROTOCOL, state='starting', execution_enabled=False,
                          fill_assumed=False, profitability_proven=False)

    def status(self):
        with self.lock:
            return dict(self.value)

    def unavailable_observation(self,ticker,now):
        value=self.status()
        item=(value.get('unavailable_observations') or {}).get(ticker)
        if (value.get('state')!='collecting' or value.get('acknowledged_channels')!=2
                or not isinstance(item,dict) or not 0<=now-item['observed_at']<=SESSION_SECONDS):
            return None
        return dict(item)

    def publish(self, values):
        # Serialize the snapshot, throttle and atomic replace across collectors.
        with self.publish_lock:
            self._publish(values)

    def _publish(self, values):
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
        if self.outcome_thread is not None:
            self.outcome_thread.join(timeout=5)
        return self.thread is None or not self.thread.is_alive()

    def run(self):
        archive = None
        comparison = None
        try:
            spec = json.loads(REGISTRATION.read_text())
            if (spec['protocol'] != PROTOCOL or spec['endpoint'] != URL
                    or spec['execution_enabled'] is not False or spec['max_messages'] != 100000
                    or spec['session_seconds'] != SESSION_SECONDS):
                raise ValueError('stream_registration_mismatch')
            self.publish({'registration_sha256': hashlib.sha256(REGISTRATION.read_bytes()).hexdigest(), 'retained_evidence':retained_evidence(self.root)})
            started=lifetime_start(self.root / 'stream-source-v5-start.json',protocol=PROTOCOL)
            while shutil.disk_usage(self.root).free < 128*1024*1024:
                self.publish({'state':'capacity_stop','storage_stop_reason':'archive_low_disk'})
                if self.stop_event.wait(RECONNECT_SECONDS):return
            archive = Archive(self.root / ARCHIVE_NAME, protocol=ARCHIVE_PROTOCOL,
                              decoder_factory=lambda tickers:Books(tickers,allow_unavailable=True,allow_missing_book=True))
            from .kalshi_stream_formula_comparison_v4 import Comparison, metadata_for
            try:
                old=self.root/'stream-shadow-comparison-v1.sqlite3'
                if old.exists():
                    with sqlite3.connect(old.resolve().as_uri()+'?mode=ro',uri=True) as prior:
                        self.publish({'prior_formula_comparison':{'protocol':'stream_native_formula_comparison_v1_20261018',
                            'state':'retained_frozen','accepted_inputs':prior.execute('SELECT count(*) FROM comparison_inputs').fetchone()[0],
                            'signals':prior.execute('SELECT count(*) FROM high_edge_v15_signals').fetchone()[0]}})
                path=self.root/'stream-shadow-comparison-v4.sqlite3'
                comparison=Comparison(path)
                from .kalshi_comparison_deadlines import run as run_outcomes
                self.outcome_thread=threading.Thread(target=run_outcomes,args=(path,self.stop_event,self.publish),
                    name='kalshi-demo-comparison-outcomes',daemon=True)
                self.outcome_thread.start()
            except Exception as exc:
                self.publish({'formula_comparison':{'state':'initialization_fault','error_class':type(exc).__name__,'execution_enabled':False}})
            while not self.stop_event.is_set():
                try:
                    if time.time()-started>spec['maximum_elapsed_seconds']:
                        self.publish({**archive.status(),'state':'capacity_stop','storage_stop_reason':'lifetime_time_cap'})
                        break
                    tickers = targets(self.root / 'worker.sqlite3')
                    self.publish({'state': 'connecting', 'target_count': len(tickers),
                                  'error_class':None,'source_fault_code':None,'storage_stop_reason':None})
                    def progress(values):
                        self.publish({**archive.status(), **values})
                    metadata=metadata_for(self.root/'worker.sqlite3',tickers) if comparison is not None else {}
                    last_comparison_status=[0]
                    def on_book(source,result,at):
                        if comparison is None:return
                        if comparison.epoch!=source.connection:comparison.begin_epoch(source.connection)
                        ticker=result['ticker']
                        if result['kind']=='market_unavailable':comparison.invalidate(ticker)
                        elif result['usable']:
                            raw_sha=source.db.execute('SELECT raw_sha FROM messages ORDER BY id DESC LIMIT 1').fetchone()[0]
                            comparison.observe(ticker,source.decoder.books[ticker],metadata.get(ticker),raw_sha)
                        if at-last_comparison_status[0]>=5:
                            self.publish({'formula_comparison':comparison.status()});last_comparison_status[0]=at
                    asyncio.run(session(archive, tickers, self.stop_event, progress,on_book=on_book))
                    self.publish({**archive.status(), 'state': 'between_epochs'})
                except Exception as exc:
                    # No raw error bodies, request headers or credentials in status.
                    capacity = (not isinstance(exc,SourceFault)
                        and isinstance(exc,(ValueError,sqlite3.OperationalError))
                        and str(exc) in {'archive_capacity','archive_low_disk',
                                         'archive_connection_capacity','database or disk is full'})
                    self.publish({'state': 'capacity_stop' if capacity else 'source_or_transport_fault',
                                  'error_class': type(exc).__name__, **archive.status(),
                                  'storage_stop_reason':str(exc) if capacity else None,
                                  'source_fault_code': str(exc) if isinstance(exc,SourceFault) and re.fullmatch(r'[a-z_]{1,80}',str(exc)) else None})
                    if capacity:
                        if str(exc)=='archive_low_disk':
                            self.stop_event.wait(RECONNECT_SECONDS)
                            continue
                        break
                self.stop_event.wait(RECONNECT_SECONDS)
        except Exception as exc:
            self.publish({'state': 'initialization_fault', 'error_class': type(exc).__name__})
        finally:
            if comparison is not None:comparison.close()
            if archive is not None:
                archive.close()
            if self.stop_event.is_set():
                self.publish({'state': 'stopped'})
