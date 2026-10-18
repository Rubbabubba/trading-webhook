from datetime import datetime,timezone
from decimal import Decimal
import hashlib
import json
import sqlite3

import pytest
from opportunity_lab.kalshi_stream_formula_comparison import Comparison

MID='01a1190b-2c97-79db-8080-d1c508c39855'

def context():
    return {'ticker':'A','event_ticker':'EVENT','market_type':'binary','status':'active','id':MID,
            'close_time':datetime.fromtimestamp(100000,tz=timezone.utc).isoformat()}

def book(at=1000):
    return {'market_id':MID,'received_at':at,'sending_ts_ms':int(at*1000),'sid':1,'seq':int(at),
            'yes':{Decimal('.40'):Decimal('9')},'no':{Decimal('.50'):Decimal('3')}}

def setup(tmp_path):
    now=[1000]
    study=Comparison(tmp_path/'comparison.sqlite3',clock=lambda:now[0],free_bytes=lambda:10**9)
    study.begin_epoch('epoch-one')
    return study,now

def test_native_formulas_match_and_no_legacy_or_fill_evidence_is_reused(tmp_path):
    old=tmp_path/'worker.sqlite3';old.write_bytes(b'OLD_REST_EVIDENCE')
    study,now=setup(tmp_path)
    assert study.observe('A',book(),context(),'a'*64)
    detail=json.loads(study.db.execute('select detail from high_edge_v15_signals').fetchone()[0])
    assert detail['challenger']['price_cents']==42 and detail['frozen_v12']['price_cents']==40
    assert detail['frozen_v10'] is None
    value=study.status()
    assert value['signals']==1 and value['accepted_inputs']==1
    assert value['execution_enabled'] is value['fill_assumed'] is value['promotion_ready'] is False
    assert value['metadata_independently_current'] is False and value['original_rest_evidence_reused'] is False
    assert old.read_bytes()==b'OLD_REST_EVIDENCE'
    for raw,digest in study.db.execute('select input_json,input_sha from comparison_inputs'):
        assert hashlib.sha256(raw.encode()).hexdigest()==digest
    study.close()

def test_punctual_marks_and_missing_horizons_are_preserved_exactly(tmp_path):
    study,now=setup(tmp_path);study.observe('A',book(),context(),'a'*64)
    for at in [1011,1030,1300]:
        now[0]=at;study.observe('A',book(at),context(),'b'*64)
    assert [r[0] for r in study.db.execute('select horizon from high_edge_v15_marks order by horizon')]==[30,300]
    assert study.status()['complete_signals']==0
    study.close()

def test_complete_quote_path_keeps_actual_execution_false(tmp_path):
    study,now=setup(tmp_path);study.observe('A',book(),context(),'a'*64)
    for at in [1005,1030,1300]:
        now[0]=at;study.observe('A',book(at),context(),'b'*64)
    value=study.status()
    assert value['complete_signals']==value['complete_events']==1
    assert value['native_quote_evidence']['stressed_quote_net_cents']=={'5':1.0,'30':1.0,'300':1.0}
    assert value['profitability_proven'] is False and value['promotion_ready'] is False
    study.close()

@pytest.mark.parametrize('reason',['metadata','closed','stale','identity','crossed'])
def test_bad_context_cannot_generate_prices_or_markouts(tmp_path,reason):
    study,now=setup(tmp_path);b=book();m=context()
    if reason=='metadata':m.pop('event_ticker')
    if reason=='closed':m['close_time']=datetime.fromtimestamp(1100,tz=timezone.utc).isoformat()
    if reason=='stale':now[0]=1003
    if reason=='identity':m['id']='wrong'
    if reason=='crossed':b['no']={Decimal('.70'):Decimal('3')}
    assert study.observe('A',b,m,'a'*64) is False
    assert study.status()['accepted_inputs']==study.status()['signals']==0
    study.close()

def test_epoch_change_and_unavailability_clear_trend_history_without_erasing_signals(tmp_path):
    study,now=setup(tmp_path);study.observe('A',book(),context(),'a'*64)
    assert study.history['A']
    study.invalidate('A');assert 'A' not in study.history
    study.begin_epoch('epoch-two');assert not study.history
    assert study.status()['signals']==1
    study.close()

def test_capacity_stops_analysis_without_erasing_inputs(tmp_path,monkeypatch):
    from opportunity_lab import kalshi_stream_formula_comparison as module
    study,now=setup(tmp_path);study.observe('A',book(),context(),'a'*64)
    monkeypatch.setattr(module,'MAX_FRAMES',1);now[0]=1005
    assert not study.observe('A',book(1005),context(),'b'*64)
    assert study.status()['state']=='capacity_stop' and study.status()['accepted_inputs']==1
    with pytest.raises(sqlite3.IntegrityError):study.db.execute('delete from comparison_inputs')
    study.close()
