from datetime import timedelta
from opportunity_lab.kalshi_broad_paper_v11 import eligible,reversal,settlement,utcnow

def market():
    return dict(ticker='X-A',event_ticker='X',rules_primary='Exact rule',rules_secondary='',market_type='binary',status='active',close_time=(utcnow()+timedelta(days=1)).isoformat())

def test_admission_rejects_missing_rules_and_near_close():
    m=market();now=utcnow();end=now+timedelta(days=7)
    assert eligible(m,now,end)
    assert not eligible(dict(m,rules_primary=''),now,end)
    assert not eligible(dict(m,close_time=(now+timedelta(minutes=10)).isoformat()),now,end)

def test_reversal_requires_history_and_cost_adjusted_move():
    q=dict(ask=.45,bid=.44,ask_size=10,bid_size=10)
    assert not reversal([],q,.07,1000)
    assert not reversal([(700,.46)],q,.07,1000)
    assert reversal([(700,.60)],q,.07,1000)
    assert not reversal([(0,.60)],q,.07,1000)

def test_settlement_requires_exchange_result_and_same_rules():
    m=market();p=dict(state='open',side='yes',entry_cost={'total':47})
    assert settlement(p,dict(m,result='yes'),m)==p
    assert settlement(p,dict(m,status='settled',result='yes',rules_primary='Changed'),m)==p
    x=settlement(p,dict(m,status='settled',result='yes'),m)
    assert x['net_cents']==53 and x['state']=='closed'
    assert settlement(p,dict(m,status='settled',result='no'),m)['net_cents']==-47
