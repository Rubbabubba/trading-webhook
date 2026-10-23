import asyncio
import json
import sqlite3
import threading

import pytest
from opportunity_lab import kalshi_demo_stream_worker_v4 as worker
from opportunity_lab.kalshi_demo_stream_archive import Archive
from opportunity_lab.kalshi_demo_stream_source import SourceFault


def messages():
    return [
        {'type': 'subscribed', 'sending_ts_ms': 1000000, 'msg': {'channel': 'orderbook_delta', 'sid': 1}},
        {'type': 'subscribed', 'sending_ts_ms': 1000000, 'msg': {'channel': 'trade', 'sid': 2}},
        {'type': 'orderbook_snapshot', 'sending_ts_ms': 1000000, 'sid': 1, 'seq': 1, 'msg': {
            'market_ticker': 'A', 'market_id': '01a1190b-2c97-79db-8080-d1c508c39855',
            'yes_dollars_fp': [['.40', '2']], 'no_dollars_fp': [['.50', '3']]}},
        {'type': 'trade', 'sending_ts_ms': 1000100, 'sid': 2, 'seq': 1, 'msg': {
            'market_ticker': 'A', 'yes_price_dollars': '.4', 'no_price_dollars': '.6',
            'count_fp': '1', 'taker_side': 'yes', 'ts_ms': 1000050}},
    ]


class Socket:
    def __init__(self, payloads, stop):
        self.payloads = list(payloads)
        self.stop = stop
        self.sent = []
        self.closed = False
        self.at = 0

    async def __aenter__(self):return self
    async def __aexit__(self, *args):self.closed = True
    async def send(self, value):self.sent.append(json.loads(value))
    async def recv(self):
        self.at += 1
        if not self.payloads:
            self.stop.set()
            raise asyncio.TimeoutError()
        value = self.payloads.pop(0)
        return value if isinstance(value, bytes) else json.dumps(value)


def exercise(tmp_path, payloads, monkeypatch=None, *, subscription=10):
    a = Archive(tmp_path/'source.sqlite3', free_bytes=lambda: 10**9)
    stop = threading.Event()
    ws = Socket(payloads, stop)
    received = []
    calls = []
    def connect(url, **kwargs):
        calls.append((url, kwargs))
        return ws
    if monkeypatch:
        monkeypatch.setattr(worker, 'SUBSCRIPTION_SECONDS', subscription)
    coro = worker.session(a, ['A'], stop, received.append, connect=connect,
        headers=lambda: {'KALSHI-ACCESS-KEY': 'PRIVATE_TEST_KEY'},
        wall=lambda: 1000 + ws.at / 100, mono=lambda: ws.at)
    return a, stop, ws, received, calls, coro


def test_both_acks_real_book_trade_and_shutdown_are_distinct_from_owned_fills(tmp_path):
    a, stop, ws, status, calls, coro = exercise(tmp_path, messages())
    counts = asyncio.run(coro)
    assert counts == {'snapshots': 1, 'deltas': 0, 'public_trades': 1, 'trades_with_prior_book': 1}
    assert status[-1]['state'] == 'collecting' and status[-1]['acknowledged_channels'] == 2
    assert calls[0][0] == worker.URL and calls[0][1]['max_size'] == 262144
    assert ws.sent == [{'id': 1, 'cmd': 'subscribe', 'params': {
        'channels': ['orderbook_delta', 'trade'], 'market_tickers': ['A']}}]
    assert ws.closed and not a.status()['active'] and a.status()['fill_assumed'] is False
    assert a.db.execute('select reason from connections').fetchone()[0] == 'shutdown'
    assert 'PRIVATE_TEST_KEY' not in str(a.db.execute('select * from messages').fetchall())
    a.verify();a.close()


def test_missing_second_ack_times_out_and_seals_valid_prefix(tmp_path, monkeypatch):
    a, stop, ws, status, calls, coro = exercise(tmp_path, messages()[:1], monkeypatch, subscription=1)
    with pytest.raises(ValueError, match='subscription_timeout'):asyncio.run(coro)
    assert ws.closed and a.status()['records'] == 1 and not a.status()['active']
    assert a.db.execute('select reason from connections').fetchone()[0] == 'source_or_storage_fault'
    a.close()


def test_quiet_socket_flushes_status_without_fabricating_source_records(tmp_path):
    a,stop,ws,status,calls,coro=exercise(tmp_path,messages())
    asyncio.run(coro)
    assert status[-1]=={'state':'collecting','acknowledged_channels':2}
    assert len(status)==5 and a.status()['records']==4
    assert a.db.execute('SELECT max(received) FROM messages').fetchone()[0]==1000.04
    a.close()


def test_sequence_gap_closes_socket_and_invalidates_connection(tmp_path):
    payloads = messages()[:3]
    payloads.append({**payloads[-1], 'seq': 3})
    a, stop, ws, status, calls, coro = exercise(tmp_path, payloads)
    with pytest.raises(SourceFault, match='gap'):asyncio.run(coro)
    assert ws.closed and a.status()['records'] == 3 and not a.status()['active']
    a.verify();a.close()


@pytest.mark.parametrize('payload', [b'private bytes', {'type': 'fill', 'msg': {'account_id': 'private'}}])
def test_unexpected_binary_or_private_channel_never_archived(tmp_path, payload):
    a, stop, ws, status, calls, coro = exercise(tmp_path, [payload])
    with pytest.raises(ValueError):asyncio.run(coro)
    assert ws.closed and a.status()['records'] == 0
    a.close()


def test_capacity_is_terminal_and_never_rotates_or_erases(tmp_path, monkeypatch):
    from opportunity_lab import kalshi_demo_stream_archive as archive_module
    monkeypatch.setattr(archive_module, 'MAX_RECORDS', 1)
    a, stop, ws, status, calls, coro = exercise(tmp_path, messages())
    with pytest.raises(ValueError, match='capacity'):asyncio.run(coro)
    assert ws.closed and a.status()['records'] == 1
    a.close()


def test_targets_use_only_existing_exact_window_with_no_db_mutation(tmp_path):
    path = tmp_path/'worker.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('create table settings(name,detail)')
        db.execute('insert into settings values(?,?)', ('sampling_window', json.dumps({'tickers': ['A', 'B']})))
    original = path.read_bytes()
    assert worker.targets(path) == ['A', 'B']
    assert path.read_bytes() == original
    with sqlite3.connect(path) as db:db.execute('update settings set detail=?', (json.dumps({'tickers': ['A', 'A']}),))
    with pytest.raises(ValueError, match='unavailable'):worker.targets(path)


def test_background_lifecycle_closes_archive_and_publishes_no_authority(tmp_path, monkeypatch):
    monkeypatch.setattr(worker, 'targets', lambda path: ['A'])
    async def fake_session(a, tickers, stop, publish,**kwargs):
        a.begin(tickers, 1000)
        a.append(messages()[0], 1000)
        publish({'state': 'subscribing'})
        stop.set()
    monkeypatch.setattr(worker, 'session', fake_session)
    service = worker.Service(tmp_path).start()
    service.thread.join(5)
    assert service.close()
    value = json.loads((tmp_path/worker.STATUS_NAME).read_text())
    assert value['state'] == 'stopped' and value['execution_enabled'] is False
    assert value['fill_assumed'] is False and value['profitability_proven'] is False
    with sqlite3.connect(tmp_path/worker.ARCHIVE_NAME) as db:
        assert db.execute('select reason from connections').fetchone()[0] == 'shutdown'


def test_source_error_text_does_not_escape_to_status(tmp_path, monkeypatch):
    monkeypatch.setattr(worker, 'targets', lambda path: ['A'])
    async def fake_session(a, tickers, stop, publish,**kwargs):
        stop.set()
        raise RuntimeError('PRIVATE_SECRET_RESPONSE')
    monkeypatch.setattr(worker, 'session', fake_session)
    service = worker.Service(tmp_path).start()
    service.thread.join(5)
    assert service.close()
    assert 'PRIVATE_SECRET_RESPONSE' not in (tmp_path/worker.STATUS_NAME).read_text()
    assert service.status()['error_class'] == 'RuntimeError'


def test_cache_capacity_accounting_survives_reopen(tmp_path):
    a = Archive(tmp_path/'source.sqlite3', free_bytes=lambda:10**9)
    a.begin(['A'], 1000);a.append(messages()[0],1000)
    before=a.status();a.close()
    b=Archive(tmp_path/'source.sqlite3', free_bytes=lambda:10**9)
    assert b.status()['records']==before['records']==1
    assert b.status()['compressed_bytes']==before['compressed_bytes']
    b.verify();b.close()


def test_compact_receipt_preserves_identity_faults_and_source_counts():
    raw={'protocol':worker.PROTOCOL,'state':'collecting','registration_sha256':'a'*64,
         'records':50,'execution_enabled':False,'fill_assumed':False,'profitability_proven':False,
         'unavailable_symbols':['A'],'usable_symbols':['B','C'],'extra_raw':'x'*10000}
    compact=worker.compact_status(raw)
    assert len(json.dumps(compact))<700
    assert compact['unavailable_symbol_count']==1 and compact['usable_symbol_count']==2
    assert compact['registration_sha256']==raw['registration_sha256']
    assert compact['execution_enabled'] is False and compact['full_status_reference']==worker.STATUS_NAME
    assert 'extra_raw' not in compact


def test_source_level_bound_is_not_misclassified_as_global_storage_exhaustion(tmp_path,monkeypatch):
    monkeypatch.setattr(worker,'targets',lambda path:['A'])
    async def bad(a,ts,stop,publish,**kwargs):
        stop.set()
        raise SourceFault('book_level_capacity')
    monkeypatch.setattr(worker,'session',bad)
    states=[]
    original=worker.Service.publish
    def recorded(self,value):
        states.append(value.get('state'));original(self,value)
    monkeypatch.setattr(worker.Service,'publish',recorded)
    service=worker.Service(tmp_path).start();service.thread.join(5)
    assert service.close()
    assert 'source_or_transport_fault' in states and 'capacity_stop' not in states
    assert service.status()['source_fault_code']=='book_level_capacity'


def test_unavailable_read_requires_current_acknowledged_epoch_and_unexpired_receipt(tmp_path):
    service=worker.Service(tmp_path)
    service.value.update(state='collecting',acknowledged_channels=2,
                         unavailable_observations={'A':{'observed_at':1000,'source_id':'a'*64}})
    assert service.unavailable_observation('A',1001)['source_id']=='a'*64
    assert service.unavailable_observation('A',1301) is None
    assert service.unavailable_observation('B',1001) is None
    service.value['state']='between_epochs'
    assert service.unavailable_observation('A',1001) is None


def test_formula_failure_does_not_drop_source_messages_or_disclose_error_body(tmp_path):
    a=Archive(tmp_path/'source.sqlite3',free_bytes=lambda:10**9)
    stop=threading.Event();ws=Socket(messages(),stop);status=[]
    def bad(*args):raise RuntimeError('PRIVATE_ERROR_BODY')
    asyncio.run(worker.session(a,['A'],stop,status.append,connect=lambda *a,**k:ws,
        headers=lambda:{},wall=lambda:1000+ws.at/100,mono=lambda:ws.at,on_book=bad))
    assert a.status()['records']==4 and ws.closed
    assert {'comparison_error_class':'RuntimeError'} in status
    assert 'PRIVATE_ERROR_BODY' not in json.dumps(status)
    a.verify();a.close()


def test_two_collectors_serialize_shared_status_file(tmp_path,monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from pathlib import Path
    import time
    original=Path.replace
    def slow_replace(path,target):
        time.sleep(.003)
        return original(path,target)
    monkeypatch.setattr(Path,'replace',slow_replace)
    service=worker.Service(tmp_path)
    def publish(i):service.publish({'state':'observed_'+str(i)})
    with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(publish,range(40)))
    value=json.loads((tmp_path/worker.STATUS_NAME).read_text())
    assert value['state'].startswith('observed_')
    assert not (tmp_path/worker.STATUS_NAME).with_suffix('.tmp').exists()
