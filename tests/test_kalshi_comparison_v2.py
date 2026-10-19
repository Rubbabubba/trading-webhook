from datetime import datetime,timezone
from decimal import Decimal
import hashlib
import json
import sqlite3
import zlib

import pytest
from opportunity_lab.kalshi_stream_formula_comparison_v2 import Comparison
from opportunity_lab.kalshi_comparison_deadlines import Deadlines,outcome_status


def setup(tmp_path):
    now=[1000.0];path=tmp_path/'v2.sqlite3'
    study=Comparison(path,clock=lambda:now[0],free_bytes=lambda:10**9)
    study.begin_epoch('new-epoch')
    metadata={'ticker':'A','event_ticker':'EVENT','market_type':'binary','status':'active','id':'id',
        'close_time':datetime.fromtimestamp(100000,tz=timezone.utc).isoformat()}
    book={'market_id':'id','received_at':1000.,'sending_ts_ms':1000000,'sid':1,'seq':1,
        'yes':{Decimal('.40'):Decimal(9)},'no':{Decimal('.50'):Decimal(3)}}
    assert study.observe('A',book,metadata,'a'*64)
    raw=json.dumps({'orderbook_fp':{'yes_dollars':[['.40','9']],'no_dollars':[['.50','3']]}}).encode()
    def reader(ticker):
        assert ticker=='A'
        return raw,now[0],now[0],0
    runner=Deadlines(path,clock=lambda:now[0],reader=reader,free_bytes=lambda:10**9)
    return study,runner,now,raw,metadata,book


def test_complete_fresh_outcomes_without_any_more_stream_ticks_and_across_restart(tmp_path):
    old=tmp_path/'stream-shadow-comparison-v1.sqlite3';old.write_bytes(b'frozen old losing evidence')
    study,runner,now,raw,_,_=setup(tmp_path)
    for h in (5,30,300):
        now[0]=1000+h;runner.tick()
        if h==30:
            runner.close()
            runner=Deadlines(study.path,clock=lambda:now[0],reader=lambda t:(raw,now[0],now[0],0),free_bytes=lambda:10**9)
    value=study.status()
    assert value['complete_signals']==value['complete_events']==1
    assert value['outcome_coverage']['complete']==3
    assert value['native_quote_evidence']['stressed_quote_net_cents']=={'5':1.,'30':1.,'300':1.}
    assert not value['fill_assumed'] and not value['profitability_proven']
    assert old.read_bytes()==b'frozen old losing evidence'
    for digest,blob in runner.db.execute('SELECT raw_sha,raw_z FROM outcome_receipts'):
        assert zlib.decompress(blob)==raw and hashlib.sha256(raw).hexdigest()==digest
    blob,digest=study.db.execute('SELECT input_json,input_sha FROM comparison_inputs').fetchone()
    decoded=zlib.decompress(blob)
    assert hashlib.sha256(decoded).hexdigest()==digest
    assert json.loads(decoded)['source_message_sha256']=='a'*64
    runner.close();study.close()


@pytest.mark.parametrize('fault',['stale','late','crossed','nan','transport'])
def test_invalid_outcomes_never_become_complete_or_cached_marks(tmp_path,fault):
    study,runner,now,raw,_,_=setup(tmp_path)
    now[0]=1005
    def reader(t):
        if fault=='transport':raise OSError('no source')
        if fault=='crossed':return b'{"orderbook_fp":{"yes_dollars":[[".8","1"]],"no_dollars":[[".8","1"]]}}',1005,1005,0
        if fault=='nan':return raw,1005,1005,float('nan')
        if fault=='late':
            now[0]=1011;return raw,1005,1011,0
        return raw,1005,1005,3
    runner.reader=reader;runner.tick();now[0]=1011;runner.tick()
    assert outcome_status(runner.db,now[0])['missed']==1
    assert not runner.db.execute('SELECT * FROM high_edge_v15_marks').fetchall()
    assert runner.db.execute('SELECT state FROM outcome_terminal WHERE horizon=5').fetchone()[0]=='missed_window'
    runner.close();study.close()


def test_admission_and_storage_reserve_do_not_extend_old_limits(tmp_path):
    study,runner,now,_,metadata,book=setup(tmp_path)
    now[0]=1001;book.update(received_at=1001,sending_ts_ms=1001000)
    assert not study.observe('A',book,metadata,'b'*64)
    assert study.db.execute('PRAGMA max_page_count').fetchone()[0]==4096
    assert study.status()['signals']==1
    runner.close();study.close()


def test_request_attempts_survive_failed_source_and_are_bounded(tmp_path):
    study,runner,now,_,_,_=setup(tmp_path)
    def fail(t):raise OSError('failed')
    runner.reader=fail
    for t in (1005,1005.5,1007,1009,1010):now[0]=t;runner.tick()
    assert runner.db.execute('SELECT count(*) FROM outcome_attempts').fetchone()[0]==3
    assert runner.db.execute('SELECT count(*) FROM outcome_receipts').fetchone()[0]==3
    runner.close();study.close()


def test_missed_window_cannot_be_backfilled_after_clock_reversal(tmp_path):
    study,runner,now,raw,_,_=setup(tmp_path)
    now[0]=1011;runner.tick()
    now[0]=1005;runner.tick()
    assert not runner.db.execute('SELECT * FROM high_edge_v15_marks').fetchall()
    runner.close();study.close()
