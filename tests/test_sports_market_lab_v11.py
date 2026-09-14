from datetime import datetime, timedelta, timezone
from opportunity_lab.sports_market_lab_v11 import advance, opposite, suitable, game_key


def test_distinct_quotes_delays_and_conservative_costs():
    now = datetime(2026,9,10,tzinfo=timezone.utc)
    q = dict(ask=.51,bid=.49,ask_size=10,bid_size=10)
    p = dict(state='staged',stage_at=now.isoformat(),ask=.50,book_id=1,horizon=300)
    assert advance(p,q,now+timedelta(seconds=20),1,.035)['state']=='staged'
    assert advance(p,q,now+timedelta(seconds=5),2,.035)['state']=='staged'
    p=advance(p,q,now+timedelta(seconds=20),2,.035)
    assert p['entry_price']==.51 and p['entry_cost']['total']==53
    p=advance(p,q,now+timedelta(seconds=321),3,.035)
    assert p['state']=='exit_staged'
    p=advance(p,dict(q,bid=.48),now+timedelta(seconds=341),4,.035)
    assert p['state']=='closed' and p['net_cents']==-7


def test_missing_liquidity_never_invents_exit_and_expired_entry_is_not_filled():
    now=datetime(2026,9,10,tzinfo=timezone.utc)
    p=dict(state='open',stage_at=now.isoformat(),entry_at=now.isoformat(),horizon=300)
    assert advance(p,None,now+timedelta(seconds=500),2,.035)['state']=='open'
    p=dict(state='staged',stage_at=now.isoformat(),ask=.5,book_id=1,horizon=300)
    assert advance(p,None,now+timedelta(seconds=181),2,.035)['state']=='entry_expired'


def test_identity_and_cost_guards():
    assert game_key('KXNFLPASSYDS-26SEP13NODET-PLAYER-250')=='26SEP13NODET'
    assert game_key('KXNFLTOTAL-26SEP13NODET-40')=='26SEP13NODET'
    q=dict(ask=.51,bid=.49,ask_size=10,bid_size=10)
    assert suitable(q,.035)
    assert not suitable(dict(q,ask=.6),.035)
    assert not suitable(dict(q,bid_size=0),.035)
    assert not suitable(dict(q,ask=.05,bid=.03),.035)
    assert abs(opposite(q)['ask']-.51)<1e-9


def test_pending_probe_confirmation_has_priority_over_new_markets():
    from opportunity_lab.sports_market_lab_v11 import select_batch
    ordered=[str(i) for i in range(1000)]
    chosen=select_batch(ordered,{"999","998"})
    assert chosen[:2]==["998","999"]
    assert len(chosen)==200 and len(set(chosen))==200
