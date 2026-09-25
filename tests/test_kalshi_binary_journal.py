from copy import deepcopy
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from fractions import Fraction
import json
import time

import pytest
from opportunity_lab.kalshi_binary_journal import BinaryJournal
from opportunity_lab.kalshi_binary_broker import BinaryDemoBroker


def snapshot(j):
    at=j.clock().timestamp()
    return dict(environment='demo',started_at=at,observed_at=at,balance={'balance':50000},
                resting_orders=[],positions=[dict(ticker=t,position_fp=str(q)) for t,q in j.accounting()['positions'].items()])


def quote(j,cid):
    p=j.get(cid)['payload'];price=Decimal(p['price'])
    bid=price if p['side']=='ask' else price-Decimal('.01')
    ask=price+Decimal('.01') if p['side']=='ask' else price
    at=j.clock().timestamp()
    return dict(environment='demo',ticker=p['ticker'],started_at=at,observed_at=at,
                orderbook_fp=dict(yes_dollars=[[str(bid),'10']],no_dollars=[[str(1-ask),'10']]))


class Exchange:
    def __init__(self,j):
        self.j=j;self.posts=0;self.positions={};self.orders={};self.fills={};self.settlements=[];self.partial=False;self.timeout=False;self.resting_only=False
    def request(self,method,path,**kwargs):
        if path=='/exchange/status':return dict(exchange_active=True,trading_active=True)
        if path=='/portfolio/balance':return {'balance':50000}
        if method=='POST':
            self.posts+=1
            if self.timeout:raise TimeoutError()
            p=kwargs['body'];cid=p['client_order_id'];r=self.j.get(cid);intent=r['intent']
            count=int(Decimal(p['count']));filled=0 if self.resting_only else (1 if self.partial else count)
            price=Decimal(p['price']);economic=price if p['side']=='bid' else 1-price
            oid='broker-'+cid
            self.orders[oid]=dict(order_id=oid,client_order_id=cid,ticker=p['ticker'],book_side=p['side'],
                outcome_side='yes' if p['side']=='bid' else 'no',side=intent['outcome'],action=intent['action'],
                type='limit',subaccount_number=0,initial_count_fp=p['count'],yes_price_dollars=p['price'],
                fill_count_fp=str(filled),remaining_count_fp=str(count-filled),status='resting' if (self.partial or self.resting_only) else 'executed',
                taker_fees_dollars=str(Decimal(filled)/100),maker_fees_dollars='0',
                taker_fill_cost_dollars=str(filled*economic),maker_fill_cost_dollars='0')
            self.fills[oid]=[dict(fill_id=oid,order_id=oid,ticker=p['ticker'],book_side=p['side'],
                outcome_side='yes' if p['side']=='bid' else 'no',subaccount_number=0,count_fp=str(filled),
                yes_price_dollars=p['price'],fee_cost=str(Decimal(filled)/100),
                created_time=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat())] if filled else []
            delta=filled*(1 if p['side']=='bid' else -1)
            self.positions[p['ticker']]=self.positions.get(p['ticker'],0)+delta
            return dict(order_id=oid,client_order_id=cid,fill_count=str(filled),remaining_count=str(count-filled))
        if method=='DELETE':
            oid=path.rsplit('/',1)[-1];self.orders[oid].update(status='canceled',remaining_count_fp='0')
            return {'reduced_by':'1'}
        return {'order':deepcopy(self.orders[path.rsplit('/',1)[-1]])}
    def pages(self,path,field,**kwargs):
        if path.startswith('/historical/'):return []
        if field=='settlements':return deepcopy(self.settlements)
        if field=='fills':return deepcopy(self.fills.get(kwargs['order_id'],[]))
        if field=='market_positions':return [dict(ticker=t,position_fp=str(q)) for t,q in self.positions.items() if q]
        if kwargs.get('status')=='resting':return [deepcopy(o) for o in self.orders.values() if o['status']=='resting']
        return list(deepcopy(self.orders).values())


@pytest.mark.parametrize('outcome',['yes','no'])
def test_real_adapter_roundtrip_restart(tmp_path,outcome):
    path=tmp_path/'j.db';j=BinaryJournal(path);x=Exchange(j);b=BinaryDemoBroker(j,x)
    j.reserve('buy','T',3,30,3,outcome=outcome,action='buy',account_snapshot=snapshot(j))
    assert b.submit('buy',quote_snapshot=quote(j,'buy'))['state']=='terminal'
    assert j.accounting()['positions']=={'T':3 if outcome=='yes' else -3}
    j.close();j=BinaryJournal(path);x.j=j;b=BinaryDemoBroker(j,x)
    j.reserve('sell','T',1,50,1,outcome=outcome,action='sell',account_snapshot=snapshot(j))
    b.submit('sell',quote_snapshot=quote(j,'sell'))
    assert j.accounting()['realized']==Fraction(18,100)
    assert b.reconcile_positions()['positions_match']
    assert x.posts==2;j.close()


def test_timeout_persists_and_never_resends(tmp_path):
    path=tmp_path/'j.db';j=BinaryJournal(path);x=Exchange(j);x.timeout=True;b=BinaryDemoBroker(j,x)
    j.reserve('buy','T',1,30,1,outcome='no',action='buy',account_snapshot=snapshot(j))
    q=quote(j,'buy')
    with pytest.raises(TimeoutError):b.submit('buy',quote_snapshot=q)
    j.close();j=BinaryJournal(path);b=BinaryDemoBroker(j,x)
    assert j.get('buy')['state']=='uncertain'
    with pytest.raises(ValueError):b.submit('buy',quote_snapshot=q)
    assert x.posts==1 and j.accounting()['pending_reserves']==Fraction(31,100);j.close()


def test_settlement_reconciles_before_active_position_check(tmp_path):
    j=BinaryJournal(tmp_path/'j.db');x=Exchange(j);b=BinaryDemoBroker(j,x)
    j.reserve('buy','T',1,30,1,outcome='no',action='buy',account_snapshot=snapshot(j))
    b.submit('buy',quote_snapshot=quote(j,'buy'))
    x.positions={}
    settled=datetime.now(timezone.utc).isoformat()
    x.settlements=[dict(ticker='OTHER',market_result='yes',revenue=100),
        dict(ticker='T',market_result='no',revenue=100,yes_count_fp='0',no_count_fp='1',
             yes_total_cost_dollars='0',no_total_cost_dollars='.30',fee_cost='.01',value=0,
             exchange_index=0,settled_time=settled)]

    assert b.reconcile_settlements()['reconciled_settlements']==1
    assert b.reconcile_positions()['positions_match']
    assert j.accounting()['positions']=={}
    j.close()


def test_partial_cancel_then_exit_and_settle_no(tmp_path):
    j=BinaryJournal(tmp_path/'j.db');x=Exchange(j);b=BinaryDemoBroker(j,x);x.partial=True
    j.reserve('buy','T',3,30,3,outcome='no',action='buy',account_snapshot=snapshot(j))
    b.submit('buy',quote_snapshot=quote(j,'buy'))
    assert j.accounting()['pending_reserves']==Fraction(93,100)
    assert b.cancel('buy')['state']=='terminal'
    row=dict(ticker='T',market_result='no',revenue=100,yes_count_fp='0',no_count_fp='1',
             yes_total_cost_dollars='0',no_total_cost_dollars='.30',fee_cost='.01',value=0,
             exchange_index=0,settled_time=datetime.now(timezone.utc).isoformat())
    j.record_settlement(row);j.record_settlement(row)
    assert j.accounting()['positions']=={} and j.accounting()['realized']==Fraction(69,100)
    with pytest.raises(ValueError):j.record_settlement(dict(row,revenue=0))
    j.close()


def test_restart_ignores_settlement_after_complete_market_exit(tmp_path):
    path = tmp_path / 'j.db'
    j = BinaryJournal(path); x = Exchange(j); b = BinaryDemoBroker(j, x)
    for cid, action, price in [('buy', 'buy', 30), ('sell', 'sell', 50)]:
        j.reserve(cid, 'T', 1, price, 1, outcome='no', action=action,
                  account_snapshot=snapshot(j))
        b.submit(cid, quote_snapshot=quote(j, cid))
    assert j.accounting()['positions'] == {}
    j.close(); j = BinaryJournal(path); x.j = j; b = BinaryDemoBroker(j, x)
    x.settlements = [dict(
        ticker='T', market_result='no', revenue=0, yes_count_fp='0',
        no_count_fp='0', yes_total_cost_dollars='0',
        no_total_cost_dollars='0', fee_cost='0', value=0,
        exchange_index=0, settled_time=datetime.now(timezone.utc).isoformat(),
    )]

    assert b.reconcile_settlements() == {'reconciled_settlements': 0}
    assert j.db.execute('SELECT count(*) FROM settlements').fetchone()[0] == 0
    assert j.accounting()['positions'] == {}
    j.close()


def test_invalid_reconciliation_rolls_back_evidence(tmp_path):
    j=BinaryJournal(tmp_path/'j.db');x=Exchange(j);b=BinaryDemoBroker(j,x)
    j.reserve('buy','T',1,30,1,outcome='no',action='buy',account_snapshot=snapshot(j))
    b.submit('buy',quote_snapshot=quote(j,'buy'))
    r=j.get('buy');before=j.accounting()
    detail=json.loads(j.db.execute('SELECT detail FROM broker_evidence').fetchone()[0])
    detail['gross_dollars']='.99'
    with pytest.raises(ValueError):j.reconcile('buy',broker_id=r['broker_id'],filled=1,remaining=0,terminal=True,evidence=detail)
    assert j.accounting()==before;j.close()


def test_demo_only_policy_and_stale_submission(tmp_path):
    path=tmp_path/'j.db';j=BinaryJournal(path)
    with pytest.raises(ValueError):j.bind_environment('production')
    j.reserve('buy','T',1,30,1,outcome='no',action='buy',account_snapshot=snapshot(j))
    stale=snapshot(j);stale['observed_at']-=120
    with pytest.raises(ValueError):j.mark_submission_started('buy',account_snapshot=stale,quote_snapshot=quote(j,'buy'))
    assert j.get('buy')['state']=='reserved';j.close()
    with pytest.raises(ValueError):BinaryJournal(path,capital_limit_cents=6000)


def test_atomic_reservation_and_no_duplicate_exit(tmp_path):
    j=BinaryJournal(tmp_path/'j.db')
    j.db.execute("CREATE TRIGGER fail_intent BEFORE INSERT ON economic_intents BEGIN SELECT RAISE(ABORT,'simulated_disk_failure'); END")
    with pytest.raises(Exception):j.reserve('buy','T',1,30,1,outcome='no',action='buy',account_snapshot=snapshot(j))
    assert j.db.execute('SELECT count(*) FROM intents').fetchone()[0]==0
    j.db.execute('DROP TRIGGER fail_intent')
    x=Exchange(j);b=BinaryDemoBroker(j,x)
    j.reserve('buy','T',1,30,1,outcome='no',action='buy',account_snapshot=snapshot(j))
    b.submit('buy',quote_snapshot=quote(j,'buy'))
    j.reserve('exit','T',1,50,1,outcome='no',action='sell',account_snapshot=snapshot(j))
    with pytest.raises(ValueError):j.reserve('exit2','T',1,50,1,outcome='no',action='sell',account_snapshot=snapshot(j))
    assert j.get('exit')['intent']['action']=='sell';j.close()


def test_no_loss_latch_survives_restart(tmp_path):
    path=tmp_path/'j.db';j=BinaryJournal(path,daily_loss_cents=10);x=Exchange(j);b=BinaryDemoBroker(j,x)
    for cid,action,price in [('buy','buy',80),('sell','sell',20)]:
        j.reserve(cid,'T',1,price,1,outcome='no',action=action,account_snapshot=snapshot(j))
        b.submit(cid,quote_snapshot=quote(j,cid))
    with pytest.raises(ValueError,match='daily_loss_stop'):
        j.reserve('again','T',1,30,1,outcome='no',action='buy',account_snapshot=snapshot(j))
    j.close();j=BinaryJournal(path,daily_loss_cents=10)
    assert j.db.execute('SELECT count(*) FROM risk_stops').fetchone()[0]==1
    assert j.accounting()['realized']==Fraction(-62,100);j.close()


@pytest.mark.parametrize('result,payout',[('yes',0),('no',100)])
def test_no_settlement_after_partial_exit(tmp_path,result,payout):
    j=BinaryJournal(tmp_path/'j.db');x=Exchange(j);b=BinaryDemoBroker(j,x)
    for cid,action,count,price in [('buy','buy',2,30),('sell','sell',1,50)]:
        j.reserve(cid,'T',count,price,count,outcome='no',action=action,account_snapshot=snapshot(j))
        b.submit(cid,quote_snapshot=quote(j,cid))
    row=dict(ticker='T',market_result=result,revenue=payout,yes_count_fp='0',no_count_fp='1',
             yes_total_cost_dollars='0',no_total_cost_dollars='.30',fee_cost='.03',
             value=100-payout,exchange_index=0,settled_time=datetime.now(timezone.utc).isoformat())
    j.record_settlement(row)
    assert j.accounting()['realized']==Fraction(payout-13,100)
    assert not j.accounting()['positions'];j.close()


def test_earlier_unresolved_journal_blocks_network_mutation(tmp_path):
    from opportunity_lab.kalshi_order_journal import Journal
    oldpath=tmp_path/'old.db';old=Journal(oldpath)
    old.reserve('old','OLD',1,50,1,cash_cents=50000);old.mark_submission_started('old');old.close()
    j=BinaryJournal(tmp_path/'j.db');x=Exchange(j);b=BinaryDemoBroker(j,x,unresolved_journals=[oldpath])
    j.reserve('buy','T',1,30,1,outcome='no',action='buy',account_snapshot=snapshot(j))
    with pytest.raises(ValueError,match='earlier_demo'):
        b.submit('buy',quote_snapshot=quote(j,'buy'))
    assert x.posts==0 and j.get('buy')['state']=='reserved';j.close()


def test_process_death_after_submission_commit(tmp_path):
    import subprocess,sys
    path=tmp_path/'crash.db'
    script='''
import os,sys
from opportunity_lab.kalshi_binary_journal import BinaryJournal
j=BinaryJournal(sys.argv[1]);at=j.clock().timestamp()
s=dict(environment='demo',started_at=at,observed_at=at,balance={'balance':50000},positions=[],resting_orders=[])
j.reserve('crash','T',1,30,1,outcome='no',action='buy',account_snapshot=s)
q=dict(environment='demo',ticker='T',started_at=at,observed_at=at,orderbook_fp=dict(yes_dollars=[['.7','10']],no_dollars=[['.29','10']]))
j.mark_submission_started('crash',account_snapshot=s,quote_snapshot=q)
os._exit(17)
'''
    result=subprocess.run([sys.executable,'-c',script,str(path)],capture_output=True,timeout=20)
    assert result.returncode==17,result.stderr.decode()
    j=BinaryJournal(path)
    assert j.get('crash')['state']=='uncertain'
    assert j.get('crash')['intent']['outcome']=='no'
    assert j.db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
    with pytest.raises(ValueError):j.mark_submission_started('crash',account_snapshot=snapshot(j),quote_snapshot=quote(j,'crash'))
    j.close()


def test_demo_acceptance_probe_is_one_cent_only_and_audited(tmp_path):
    j=BinaryJournal(tmp_path/'probe.db')
    j.reserve('probe','T',1,1,10,outcome='no',action='buy',account_snapshot=snapshot(j))
    at=j.clock().timestamp()
    q=dict(environment='demo',ticker='T',started_at=at,observed_at=at,
           market=dict(ticker='T',status='active',market_type='binary'),
           orderbook_fp=dict(yes_dollars=[],no_dollars=[]))
    with pytest.raises(ValueError):j.mark_submission_started('probe',account_snapshot=snapshot(j),quote_snapshot=q)
    j.mark_submission_started('probe',account_snapshot=snapshot(j),quote_snapshot=q,demo_probe=True)
    saved=json.loads(j.db.execute('SELECT detail FROM submission_quotes').fetchone()[0])
    assert saved['probe_kind']=='one_cent_no_ioc_acceptance_only'
    assert j.get('probe')['state']=='uncertain';j.close()


def test_observed_no_fill_uses_canonical_cost_despite_legacy_yes(tmp_path):
    j=BinaryJournal(tmp_path/'observed.db');b=BinaryDemoBroker(j,Exchange(j))
    order=dict(book_side='ask',outcome_side='no',side='yes',action='sell')
    assert b.order_cost_basis({},order,Decimal('.83'),1)==Decimal('.17')
    assert b.order_cost_basis({},dict(book_side='bid',outcome_side='yes'),Decimal('.83'),1)==Decimal('.83')
    with pytest.raises(ValueError):b.order_cost_basis({},dict(book_side='ask',outcome_side='yes'),Decimal('.83'),1)
    j.close()


@pytest.mark.parametrize('outcome,side,wire_price',[('yes','bid','0.3000'),('no','ask','0.7000')])
def test_post_only_gtc_acceptance_intent_rests_then_cancels_flat(tmp_path,outcome,side,wire_price):
    j=BinaryJournal(tmp_path/f'{outcome}.db');x=Exchange(j);x.resting_only=True;b=BinaryDemoBroker(j,x)
    j.reserve('rest','T',1,30,2,outcome=outcome,action='buy',account_snapshot=snapshot(j),
              order_mode='post_only_gtc')
    payload=j.get('rest')['payload']
    assert payload['side']==side and payload['price']==wire_price
    assert payload['time_in_force']=='good_till_canceled' and payload['post_only'] is True
    at=j.clock().timestamp()
    q=dict(environment='demo',ticker='T',started_at=at,observed_at=at,
           orderbook_fp=dict(yes_dollars=[['.60','10']],no_dollars=[['.39','10']]))
    assert b.submit('rest',quote_snapshot=q)['state']=='working'
    assert b.cancel('rest')['state']=='terminal'
    assert j.accounting()['positions']=={} and x.posts==1
    assert b.reconcile_positions()['positions_match'];j.close()


def test_post_only_guard_rejects_cross_before_network(tmp_path):
    j=BinaryJournal(tmp_path/'cross.db');x=Exchange(j);x.resting_only=True;b=BinaryDemoBroker(j,x)
    j.reserve('rest','T',1,70,2,outcome='yes',action='buy',account_snapshot=snapshot(j),
              order_mode='post_only_gtc')
    at=j.clock().timestamp()
    q=dict(environment='demo',ticker='T',started_at=at,observed_at=at,
           orderbook_fp=dict(yes_dollars=[['.60','10']],no_dollars=[['.39','10']]))
    with pytest.raises(ValueError,match='post_only_order_would_cross'):
        b.submit('rest',quote_snapshot=q)
    assert x.posts==0 and j.get('rest')['state']=='reserved';j.close()


def test_definitely_unsent_reserved_intent_can_be_abandoned_only_once(tmp_path):
    j=BinaryJournal(tmp_path/'abandon.db')
    j.reserve('unsent','T',1,30,2,outcome='yes',action='buy',account_snapshot=snapshot(j))
    assert j.abandon_reserved('unsent')['state']=='abandoned'
    with pytest.raises(ValueError,match='reserved_abandon_not_allowed'):
        j.abandon_reserved('unsent')
    assert j.accounting()['positions']=={}
    assert j.db.execute('SELECT id FROM abandoned_intents').fetchone()==('unsent',)
    j.close()


def test_market_inactive_create_and_cancel_remain_uncertain_without_retry(tmp_path):
    from opportunity_lab.kalshi_demo_broker import BrokerError
    j=BinaryJournal(tmp_path/'inactive-create.db');x=Exchange(j);b=BinaryDemoBroker(j,x)
    j.reserve('buy','T',1,30,2,outcome='yes',action='buy',account_snapshot=snapshot(j))
    original=x.request
    def inactive_create(method,path,**kwargs):
        if method=='POST':
            x.posts+=1
            raise BrokerError(400,{'code':'market_inactive'})
        return original(method,path,**kwargs)
    x.request=inactive_create
    with pytest.raises(BrokerError) as caught:b.submit('buy',quote_snapshot=quote(j,'buy'))
    assert caught.value.diagnostics=={'code':'market_inactive'}
    assert x.posts==1 and j.get('buy')['state']=='uncertain';j.close()

    j=BinaryJournal(tmp_path/'inactive-cancel.db');x=Exchange(j);x.resting_only=True;b=BinaryDemoBroker(j,x)
    j.reserve('rest','T',1,30,2,outcome='yes',action='buy',account_snapshot=snapshot(j),
              order_mode='post_only_gtc')
    at=j.clock().timestamp();q=dict(environment='demo',ticker='T',started_at=at,observed_at=at,
        orderbook_fp=dict(yes_dollars=[['.20','10']],no_dollars=[['.60','10']]))
    b.submit('rest',quote_snapshot=q)
    original=x.request
    def inactive_cancel(method,path,**kwargs):
        if method=='DELETE':raise BrokerError(400,{'code':'market_inactive'})
        return original(method,path,**kwargs)
    x.request=inactive_cancel
    with pytest.raises(BrokerError):b.cancel('rest')
    assert j.get('rest')['state']=='uncertain';j.close()
