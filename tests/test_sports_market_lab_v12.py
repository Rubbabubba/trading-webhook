from datetime import datetime, timedelta, timezone
from opportunity_lab.sports_market_lab_v12 import advance, opposite, suitable, game_key


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


def test_busy_pending_queue_cannot_starve_new_games():
    from opportunity_lab.sports_market_lab_v12 import select_batch
    ordered = [str(i) for i in range(1000)]
    cache = {t: {'game': {'league': 'ncaaf', 'event_id': 'old' if int(t)<900 else str(int(t)%10)}, 'family': str(int(t)%3), 'market': {'status': 'active'}} for t in ordered}
    chosen = select_batch(ordered, set(ordered[:900]), cache)
    assert len(chosen)==200 and len(set(chosen))==200
    assert sum(int(t)>=900 for t in chosen)==40
    assert {cache[t]['game']['event_id'] for t in chosen if int(t)>=900}=={str(i) for i in range(10)}


def test_capacity_borrowing_and_closed_new_market_exclusion():
    from opportunity_lab.sports_market_lab_v12 import select_batch
    ordered = [str(i) for i in range(250)]
    cache = {t: {'game': {'league': 'epl', 'event_id': 'a'}, 'family': 'winner', 'market': {'status': 'active'}} for t in ordered}
    assert len(select_batch(ordered,set(),cache))==200
    for c in cache.values(): c['market']['status']='closed'
    assert select_batch(ordered,set(),cache)==[]
    assert len(select_batch(ordered,set(ordered),cache))==200


def test_interleaves_games_and_families():
    from opportunity_lab.sports_market_lab_v12 import fair_order
    cache = {t:{'game':{'league':'epl','event_id':g},'family':f} for t,g,f in [('a','1','total'),('b','1','total'),('c','1','winner'),('d','2','total')]}
    assert fair_order(list(cache),cache)==['a','d','c','b']
