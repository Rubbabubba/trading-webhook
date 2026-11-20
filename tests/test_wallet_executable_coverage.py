"""Real shard/fee shapes and cadence regressions; all fills here are fixtures."""
from contextlib import nullcontext
from datetime import datetime,timezone
import copy,json
import pytest
from opportunity_lab.kalshi_wallet_monitor import read_fee
from opportunity_lab.kalshi_factory_fee_probe import fee_basis
from opportunity_lab import kalshi_wallet_profitability as research
from test_kalshi_wallet_production import setup,signal,production_quote


@pytest.mark.parametrize('kind',['quadratic','quadratic_with_maker_fees','quadratic_with_combo_maker_fees'])
def test_wallet_taker_joins_real_shard_metadata_without_changing_factory_scope(kind):
    now=datetime.now(timezone.utc).timestamp()
    quote={'side':'yes','observed_at':now,'market':{
        'ticker':'GAME','event_ticker':'EVENT','exchange_index':3},
        'orderbook_fp':{'yes_dollars':[['.44','2']],'no_dollars':[['.50','2']]}}
    event={'event_ticker':'EVENT','series_ticker':'SERIES','exchange_index':3}
    series={'ticker':'SERIES','exchange_index':3,'fee_type':kind,'fee_multiplier':1}
    def read(url):return ({'event':event} if '/events/' in url else {'series':series},0,now+1,None,None)
    proof=read_fee(quote,'https://example.test',read)
    assert proof['fee_type']==kind and proof['exchange_index']==3
    assert proof['liquidity']=='taker_ioc' and proof['model_fee_upper_bound_cents']==2
    row=dict(market_type='binary',exchange_index=3,event_id='EVENT',price_cents=50,
             observed_at=datetime.fromtimestamp(now,timezone.utc).isoformat())
    with pytest.raises(ValueError,match='identity'):fee_basis(row,event,series,fetched_at=now+1)
    for field,value in [('exchange_index',2),('exchange_index',True),('ticker','OTHER')]:
        before=copy.deepcopy(series);series[field]=value
        with pytest.raises(ValueError):read_fee(quote,'https://example.test',read)
        series.clear();series.update(before)
    series['fee_type']='flat'
    with pytest.raises(ValueError,match='unsupported_fee_type'):read_fee(quote,'https://example.test',read)


@pytest.mark.parametrize('kind',['quadratic_with_maker_fees','quadratic_with_combo_maker_fees'])
def test_production_ioc_retains_shard_fee_and_owner_guards(kind,tmp_path):
    now,db,j,x,b,w,approved=setup(tmp_path)
    key,cid=signal(db,now,'BUY')
    trade,mapping=db.execute('SELECT trade,mapping FROM signals WHERE id=?',(key,)).fetchone()
    mapping=json.loads(mapping);mapping['exchange_index']=3
    db.execute('UPDATE signals SET mapping=? WHERE id=?',(json.dumps(mapping),key))
    j.reserve(cid,'GAME',1,50,2,outcome='yes',action='buy',account_snapshot=b.snapshot())
    q=production_quote(j,cid,key);q['market']['exchange_index']=3
    q['fee_basis'].update(fee_type=kind,exchange_index=3,liquidity='taker_ioc')
    bad=copy.deepcopy(q);bad['fee_basis']['exchange_index']=2
    with pytest.raises(ValueError,match='fee_identity'):b.submit(cid,quote_snapshot=bad)
    assert x.posts==0
    assert b.submit(cid,quote_snapshot=q)['filled']==1
    j.close();db.close()


@pytest.mark.parametrize('work_seconds,expected_pause',[(0,30),(25,5),(45,5)])
def test_loop_cadence_includes_work_and_never_catches_up_in_a_burst(work_seconds,expected_pause,tmp_path,monkeypatch):
    elapsed=[0];pauses=[];closed=[]
    class DB:
        def close(self):closed.append(True)
    class Worker:
        def __init__(self,*a):self.db=DB()
        def tick(self):elapsed[0]+=work_seconds
    def sleep(seconds):pauses.append(seconds);raise KeyboardInterrupt
    monkeypatch.setattr(research,'Research',Worker)
    monkeypatch.setattr(research.time,'monotonic',lambda:elapsed[0])
    monkeypatch.setattr(research.time,'sleep',sleep)
    monkeypatch.setattr('opportunity_lab.kalshi_process_lock.acquire',lambda *a:nullcontext())
    with pytest.raises(KeyboardInterrupt):research.run(tmp_path)
    assert pauses==[expected_pause] and closed==[True]
