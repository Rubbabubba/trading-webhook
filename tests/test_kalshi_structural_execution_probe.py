from copy import deepcopy
from datetime import datetime,timezone
import sqlite3
import pytest
from tests.test_kalshi_external_sleeves import market
from opportunity_lab.kalshi_external_sleeves import structural_candidates,confirm_structural_candidate
from opportunity_lab.kalshi_structural_execution_probe import evaluate,capture,status,init

def setup():
    low,high=market(10,yes_ask='.30',no_ask='.71'),market(20,yes_ask='.80',no_ask='.19')
    def quote(m,yes,no,t):
        return {'environment':'demo','ticker':m['ticker'],'market':m,
                'orderbook_fp':{'yes_dollars':[[yes,'2']],'no_dollars':[[no,'2']]},'observed_at':t}
    a=quote(low,'.29','.70',100); b=quote(high,'.80','.19',104)
    signal=confirm_structural_candidate(structural_candidates([low,high])[0],a,b,now=104)
    a['observed_at']=108; b['observed_at']=112
    return signal,a,b

def test_surviving_pair_is_never_a_fill_profit_or_promotion():
    signal,a,b=setup(); result=evaluate(signal,a,b,now=113)
    assert result['state']=='quote_stress_survived'
    assert result['actual_fills']==0 and result['realized_net_cents'] is None
    assert result['promotion_ready'] is False and result['actual_fee_verified'] is False
    assert result['sequential_tests'][0]['modeled_minimum_payout_surplus_cents']==38

def test_second_leg_vanishing_is_retained_as_rejection():
    signal,a,b=setup(); b['orderbook_fp']['yes_dollars']=[]
    result=evaluate(signal,a,b,now=113)
    assert result['state']=='rejected_quote_stress'
    assert result['sequential_tests'][0]['modeled_minimum_payout_surplus_cents'] is None
    assert result['sequential_tests'][0]['observed_unwind_cost_cents']==13

def test_stale_changed_crossed_and_no_delay_quotes_fail_closed():
    signal,a,b=setup()
    for mutate in ('stale','identity','crossed','no_delay','threshold'):
        x,y=deepcopy(a),deepcopy(b)
        if mutate=='stale': x['observed_at']=90
        if mutate=='identity': y['market']['rules_secondary']='Different rules'
        if mutate=='crossed': x['orderbook_fp']['yes_dollars']=[['.80','2']]
        if mutate=='no_delay': x['observed_at']=104
        if mutate=='threshold': y['market']['floor_strike']=30
        with pytest.raises(ValueError): evaluate(signal,x,y,now=113)

def test_failed_reads_survive_restart_and_cannot_be_retried_selectively():
    db=sqlite3.connect(':memory:',isolation_level=None)
    at=datetime.fromtimestamp(99,timezone.utc); init(db,at)
    signal,a,b=setup()
    class Client:
        calls=0
        def quote(self,payload):
            self.calls+=1
            raise ValueError('no_book')
    client=Client()
    assert capture(db,client,'pair-1',signal,clock=lambda:113,now=at)
    assert not capture(db,client,'pair-1',signal,clock=lambda:113,now=at)
    assert client.calls==1
    assert status(db)['attempts']==1 and status(db)['rejected_or_incomplete']==1
    assert status(db)['actual_fills']==0

def test_pre_registration_signal_cannot_count_as_forward_evidence():
    db=sqlite3.connect(':memory:',isolation_level=None)
    at=datetime.fromtimestamp(105,timezone.utc); init(db,at)
    signal,a,b=setup()
    class Client:
        def quote(self,payload): raise AssertionError('historical quote read')
    assert not capture(db,Client(),'old',signal,clock=lambda:113,now=at)
    assert status(db)['attempts']==0
