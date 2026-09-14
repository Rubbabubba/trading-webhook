from copy import deepcopy
from datetime import datetime,timezone,timedelta
from opportunity_lab.sports_churn_paper import prepare
from opportunity_lab.sports_live_execution import initial,decide


def fixture():
    now=datetime(2026,9,13,tzinfo=timezone.utc)
    obs={'at':now.isoformat(),'snapshot':'1','latency_seconds':.25,'admission_ok':True,
         'model':{'probabilities':{'home':.85},'blockers':[],'completed':False},
         'markets':{'home':{'ticker':'T','valid':True,'quote':{'bid':.49,'ask':.50,'bid_size':10,'ask_size':10},'book_id':1,'book_received_at':now.isoformat(),'fee_coefficient':.07,'settlement_ok':False}}}
    return now,obs


def test_arms_identical_first_entry_and_cap_blocks_reentry():
    now,obs=fixture();a=initial();b=initial()
    assert decide(a,prepare(obs,now,0,5),now,300,.04)==decide(b,prepare(obs,now,0,1),now,300,.04)
    assert a==b
    assert prepare(obs,now,1,5)['admission_ok']
    assert not prepare(obs,now,1,1)['admission_ok']
    assert obs['admission_ok']


def test_stale_source_invalidates_quotes_and_never_backfills():
    now,obs=fixture();p=prepare(obs,now+timedelta(seconds=10),0,1)
    assert not p['markets']['home']['valid']
    assert 'shadow_source_stale' in p['model']['blockers']
    assert not p['admission_ok']


def test_cap_does_not_change_open_position_exit():
    now,obs=fixture();s=initial();decide(s,obs,now,300,.04)
    later=now+timedelta(seconds=1);obs['snapshot']='2';obs['markets']['home'].update(book_id=2,book_received_at=later.isoformat());obs['at']=later.isoformat()
    decide(s,obs,later,300,.04);assert s['position']
    a=deepcopy(s);b=deepcopy(s);end=now+timedelta(seconds=302);obs['at']=end.isoformat();obs['snapshot']='3';obs['markets']['home'].update(book_id=3,book_received_at=end.isoformat())
    assert decide(a,prepare(obs,end,1,5),end,300,.04)==decide(b,prepare(obs,end,1,1),end,300,.04)
    assert a['pending']['action']=='sell' and a==b
