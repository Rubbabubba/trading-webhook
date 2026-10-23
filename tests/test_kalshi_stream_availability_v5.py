import copy
import pytest
from opportunity_lab.kalshi_stream_availability_v5 import AvailabilityBooks
from opportunity_lab.kalshi_demo_stream_source import SourceFault

ID='01a1190b-2c97-79db-8080-d1c508c39855'
def init():
    b=AvailabilityBooks(['A','B'],allow_unavailable=True,allow_missing_book=True)
    b.apply({'type':'subscribed','sending_ts_ms':1000000,'msg':{'channel':'orderbook_delta','sid':1}},1000)
    return b
def snap(seq=1,ticker='A'):
    return {'type':'orderbook_snapshot','sid':1,'seq':seq,'sending_ts_ms':1000000,
            'msg':{'market_ticker':ticker,'market_id':ID,'yes_dollars_fp':[['.4','2']],'no_dollars_fp':[['.5','3']]}}
def delta(seq=2):
    return {'type':'orderbook_delta','sid':1,'seq':seq,'sending_ts_ms':1000000,
            'msg':{'market_ticker':'A','market_id':ID,'side':'yes','price_dollars':'.4','delta_fp':'1'}}

@pytest.mark.parametrize('missing',['yes_dollars_fp','no_dollars_fp'])
def test_one_sided_snapshot_and_deltas_stay_unavailable_other_symbols_continue(missing):
    b=init();p=snap();p['msg'].pop(missing);original=copy.deepcopy(p)
    assert b.apply(p,1000)['kind']=='market_unavailable'
    assert p==original and 'A' not in b.books
    assert b.apply(delta(),1000)['usable'] is False
    assert 'A' not in b.books
    assert b.apply(snap(3,'B'),1000)['usable'] is True
    assert b.apply(snap(4),1000)['usable'] is True
    assert b.apply(delta(5),1000)['usable'] is True

@pytest.mark.parametrize('change',['gap','null','negative','wrongid','invalidid'])
def test_partial_availability_does_not_bypass_source_integrity(change):
    b=init();p=snap();p['msg'].pop('yes_dollars_fp')
    if change=='null':p['msg']['no_dollars_fp']=None
    if change=='negative':p['msg']['no_dollars_fp']=[['.5','-1']]
    if change=='invalidid':p['msg']['market_id']='wrong'
    if change in {'null','negative','invalidid'}:
        with pytest.raises(SourceFault):b.apply(p,1000)
    else:
        b.apply(p,1000);d=delta(3 if change=='gap' else 2)
        if change=='wrongid':d['msg']['market_id']='21a1190b-2c97-79db-8080-d1c508c39855'
        with pytest.raises(SourceFault):b.apply(d,1000)
    assert b.poisoned and b.books=={}
