import io
import json
from types import SimpleNamespace
from urllib.error import HTTPError
from unittest.mock import patch

import pytest
from opportunity_lab import kalshi_demo_v5_maker_worker as worker
from opportunity_lab.kalshi_demo_market_data import DemoMarkets


class Response(io.BytesIO):
    headers = {'Age': '0'}
    def __enter__(self):return self
    def __exit__(self, *args):self.close()


def test_source_health_retains_failure_until_a_real_book_read_succeeds():
    reader=DemoMarkets()
    failure=HTTPError('https://demo-api.kalshi.co',500,'PRIVATE_ERROR',{},io.BytesIO(b'private'))
    with patch('opportunity_lab.kalshi_demo_market_data.time.sleep'), patch.object(reader.opener,'open',side_effect=failure):
        with pytest.raises(ValueError,match='demo_market_http_500'):reader.get('A',book=True)
    assert reader.transport_status()['state']=='source_fault'
    assert reader.transport_status()['failed_reads']==1
    with patch('opportunity_lab.kalshi_demo_market_data.time.sleep'), patch.object(reader.opener,'open',return_value=Response(b'{"markets":[]}')):
        reader.get(params={'limit':1})
    assert reader.transport_status()['state']=='source_fault' # A listing is not a book recovery.
    with patch('opportunity_lab.kalshi_demo_market_data.time.sleep'), patch.object(reader.opener,'open',return_value=Response(b'{"orderbook_fp":{}}')):
        reader.get('A',book=True)
    assert reader.transport_status()['state']=='observed'
    assert reader.transport_status()['successful_reads']==1
    assert reader.transport_status()['substitute_quotes_used'] is False
    assert 'PRIVATE' not in json.dumps(reader.transport_status())


@pytest.mark.parametrize('code',[500,502,503,504])
def test_optional_scan_failure_never_creates_a_signal_or_lifecycle_exclusion(tmp_path,code):
    state=worker.MakerState(tmp_path/'worker.sqlite3')
    market=SimpleNamespace(quote=lambda _: (_ for _ in ()).throw(ValueError('demo_market_http_'+str(code))))
    assert worker.scan_market_candidate(state,object(),market,{'ticker':'A'})==(None,None)
    assert state.db.execute('select count(*) from history').fetchone()[0]==0
    assert state.load('sampling_window_book_health') is None
    detail=json.loads(state.db.execute('select detail from actions order by rowid desc limit 1').fetchone()[0])
    assert detail=={'action':'scan_source_unavailable','reason':'demo_market_http_'+str(code)}
    state.close()


def test_real_loop_keeps_reconciliation_and_other_collectors_after_optional_500(tmp_path,monkeypatch):
    calls=[]
    monkeypatch.setenv('KALSHI_DEMO_API_KEY_ID','synthetic')
    monkeypatch.setenv('KALSHI_DEMO_PRIVATE_KEY_PATH','synthetic')
    monkeypatch.setattr(worker,'DemoClient',lambda *a:object())
    monkeypatch.setattr(worker,'release_legacy_uncertainty_stop',lambda *a:None)
    monkeypatch.setattr(worker,'check_exchange',lambda *a:None)
    monkeypatch.setattr(worker,'recover_or_report',lambda *a:True)
    monkeypatch.setattr(worker,'log_event',lambda *a,**k:None)
    monkeypatch.setattr(worker,'measurement_optional',lambda *a:True)
    monkeypatch.setattr(worker,'factory_eligible_candidate',lambda *a,**k:None)
    monkeypatch.setattr(worker,'refresh_cohort',lambda *a,**k:([{'ticker':'A','event_ticker':'E'}],0))
    def quote(_):
        calls.append('book500')
        raise ValueError('demo_market_http_500')
    monkeypatch.setattr(worker,'DemoMarkets',lambda:SimpleNamespace(quote=quote))
    def snapshot():
        calls.append('broker_snapshot')
        return {'balance':{'balance':50000}}
    monkeypatch.setattr(worker,'BinaryDemoBroker',lambda *a:SimpleNamespace(snapshot=snapshot))
    monkeypatch.setattr(worker,'ContinuingCapture',lambda *a:SimpleNamespace(close=lambda:None,poll=lambda *a,**k:calls.append('continuing')))
    for name in ('poll_activity_trade_capture','poll_postquote_capture','poll_trade_probe','poll_targeted_trade_probe','poll_depth_trade_probe'):
        monkeypatch.setattr(worker,name,lambda *a,**k:calls.append('independent_collector'))
    monkeypatch.setattr(worker,'write_status',lambda *a,**k:calls.append(('status',k)))
    monkeypatch.setattr(worker,'submit',lambda *a,**k:pytest.fail('No order may follow failed quote'))
    worker.run(tmp_path,cycles=1)
    assert 'book500' in calls and 'broker_snapshot' in calls and 'independent_collector' in calls
    assert calls.index('broker_snapshot',calls.index('book500'))>calls.index('book500')
    assert calls[-1][0]=='status' and calls[-1][1]['phase']=='running'
    with worker.sqlite3.connect(tmp_path/'worker.sqlite3') as db:
        assert db.execute('select count(*) from history').fetchone()[0]==0


def test_stream_connector_rejects_production_before_import_or_auth():
    from opportunity_lab.kalshi_demo_stream_worker import connector
    with pytest.raises(ValueError,match='demo_endpoint_required'):
        connector('wss://external-api-ws.kalshi.com/trade-api/ws/v2')
