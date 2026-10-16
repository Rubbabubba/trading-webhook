import gzip
import json
import sqlite3
from unittest.mock import patch
import pytest
from opportunity_lab.kalshi_demo_stream_archive import Archive
from opportunity_lab.kalshi_demo_stream_source import SourceFault

def ack():
    return dict(type='subscribed',sending_ts_ms=1000000,msg=dict(channel='orderbook_delta',sid=1))

def snapshot(seq=1):
    return dict(type='orderbook_snapshot',sid=1,seq=seq,sending_ts_ms=1000000,msg=dict(market_ticker='A',market_id='01a1190b-2c97-79db-8080-d1c508c39855',yes_dollars_fp=[['.4','2']],no_dollars_fp=[['.5','3']]))

def archive(tmp_path):
    return Archive(tmp_path/'evidence.sqlite3',free_bytes=lambda:10**9)

def test_durable_restart_separates_connection_epochs(tmp_path):
    a=archive(tmp_path); first=a.begin(['A'],1000);a.append(ack(),1000);a.append(snapshot(),1000)
    a.db.close() # Simulate abrupt process death without graceful close.
    b=archive(tmp_path)
    assert b.status()['interrupted_connections']==1
    assert b.status()['records']==2
    second=b.begin(['A'],1000)
    assert first!=second
    with pytest.raises(SourceFault,match='unacknowledged'):
        b.append(snapshot(),1000)
    assert b.status()['records']==2 and not b.status()['active']
    b.begin(['A'],1000);b.append(ack(),1000);b.append(snapshot(),1000)
    assert b.status()['records']==4;b.verify();b.close()

def test_gap_closes_epoch_preserves_valid_prefix(tmp_path):
    a=archive(tmp_path);a.begin(['A'],1000);a.append(ack(),1000);a.append(snapshot(),1000)
    with pytest.raises(SourceFault,match='gap'):a.append(snapshot(3),1000)
    assert a.status()['records']==2 and not a.status()['active']
    with pytest.raises(ValueError,match='no_active'):a.append(snapshot(4),1000)
    assert a.db.execute('SELECT reason FROM connections').fetchone()[0]=='source_or_storage_fault'
    a.close()

def test_messages_cannot_be_mutated_or_deleted(tmp_path):
    a=archive(tmp_path);a.begin(['A'],1000);a.append(ack(),1000)
    for sql in ['DELETE FROM messages','UPDATE messages SET received=2']:
        with pytest.raises(sqlite3.IntegrityError,match='immutable'):a.db.execute(sql)
    a.verify();a.close()

@pytest.mark.parametrize('field',['authorization','headers','api_key'])
def test_unexpected_secrets_never_archived(tmp_path,field):
    a=archive(tmp_path);a.begin(['A'],1000);p=ack();p[field]='DO_NOT_STORE'
    with pytest.raises(ValueError,match='unexpected'):a.append(p,1000)
    assert a.status()['records']==0
    assert 'DO_NOT_STORE' not in str(a.db.execute('SELECT * FROM connections').fetchall())
    a.close()

def test_capacity_stop_keeps_prior_messages(tmp_path):
    a=archive(tmp_path);a.begin(['A'],1000);a.append(ack(),1000)
    with patch('opportunity_lab.kalshi_demo_stream_archive.MAX_RECORDS',1):
        with pytest.raises(ValueError,match='capacity'):a.append(snapshot(),1000)
    assert a.status()['records']==1 and not a.status()['active'];a.verify();a.close()

def test_low_disk_fails_without_creating_database(tmp_path):
    with pytest.raises(ValueError,match='low_disk'):Archive(tmp_path/'low.sqlite3',free_bytes=lambda:0)
    assert not (tmp_path/'low.sqlite3').exists()

def test_receipt_before_connection_invalidates_without_raw_write(tmp_path):
    a=archive(tmp_path);a.begin(['A'],1001)
    with pytest.raises(ValueError,match='before_connection'):a.append(ack(),1000)
    assert not a.status()['active'] and a.status()['records']==0;a.close()

def test_public_trade_never_claims_owned_fill(tmp_path):
    a=archive(tmp_path);a.begin(['A'],1000);a.append(ack(),1000);a.append(snapshot(),1000)
    a.append(dict(type='subscribed',sending_ts_ms=1000000,msg=dict(channel='trade',sid=2)),1000)
    p=dict(type='trade',sid=2,seq=1,sending_ts_ms=1000100,msg=dict(market_ticker='A',yes_price_dollars='.4',no_price_dollars='.6',count_fp='1',taker_side='yes',ts_ms=1000050))
    assert a.append(p,1000.2)['actual_fill_proven'] is False
    assert a.status()['fill_assumed'] is False;a.close()

def test_payload_roundtrip_and_chain_independent_hashes(tmp_path):
    import hashlib
    a=archive(tmp_path);conn=a.begin(['A'],1000);a.append(ack(),1000);a.append(snapshot(),1000)
    prev='0'*64
    for at,rawsha,packedsha,previous,chain,data in a.db.execute('SELECT received,raw_sha,compressed_sha,previous_sha,chain_sha,data FROM messages ORDER BY id'):
        raw=gzip.decompress(data)
        assert hashlib.sha256(raw).hexdigest()==rawsha
        assert hashlib.sha256(data).hexdigest()==packedsha
        assert previous==prev
        expected=json.dumps([prev,conn,at,rawsha],sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        assert hashlib.sha256(expected).hexdigest()==chain
        prev=chain
    assert a.verify()==prev;a.close()
