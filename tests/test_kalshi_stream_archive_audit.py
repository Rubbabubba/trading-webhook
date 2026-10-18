import json
import sqlite3
import pytest
from opportunity_lab.kalshi_demo_stream_archive import Archive
from research.stream_integration_20261018.audit import audit


def test_independent_readonly_audit_does_not_interrupt_active_epoch(tmp_path):
    path=tmp_path/'source.sqlite3';a=Archive(path,free_bytes=lambda:10**9)
    a.begin(['A'],1000)
    a.append(dict(type='subscribed',sending_ts_ms=1000000,msg=dict(channel='orderbook_delta',sid=1)),1000)
    snap=dict(type='orderbook_snapshot',sid=1,seq=1,sending_ts_ms=1000000,msg=dict(market_ticker='A',market_id='01a1190b-2c97-79db-8080-d1c508c39855',yes_dollars_fp=[['.4','2']],no_dollars_fp=[['.5','3']]))
    a.append(snap,1000)
    result=audit(path)
    assert result['records']==2 and result['reconstructed_states']==1
    assert a.db.execute('select reason,ended from connections').fetchone()==(None,None)
    assert a.status()['active']
    a.append({**snap,'seq':2},1000.1)
    assert audit(path)['records']==3
    a.close()


def test_audit_rejects_tampered_blob_without_repairing_it(tmp_path):
    path=tmp_path/'source.sqlite3';a=Archive(path,free_bytes=lambda:10**9);a.begin(['A'],1000)
    a.append(dict(type='subscribed',sending_ts_ms=1000000,msg=dict(channel='orderbook_delta',sid=1)),1000);a.close()
    with sqlite3.connect(path) as db:
        db.execute('drop trigger messages_immutable_update')
        db.execute("update messages set compressed_sha='wrong'")
    with pytest.raises(AssertionError):audit(path)
    with sqlite3.connect(path) as db:assert db.execute('select compressed_sha from messages').fetchone()[0]=='wrong'
