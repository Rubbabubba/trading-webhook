from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
import pytest
from opportunity_lab.kalshi_order_direction import terms
from opportunity_lab.kalshi_binary_accounting import replay_binary, reservation, reconcile_positions, check_budget

NOW=datetime(2026,9,14,20,tzinfo=timezone.utc)


def record(cid,action,count,price,*,outcome='no',filled=None,state='terminal',hour=12):
    filled=count if filled is None else filled
    wire=terms(outcome,action,price)
    p=dict(ticker='T',client_order_id=cid,side=wire['side'],price=wire['price'],count=f'{count}.00',
           reduce_only=wire['reduce_only'],subaccount=0)
    r=dict(payload=p,intent=dict(outcome=outcome,action=action,count=count,price_cents=price,fee_cents=count),
           reserve=reservation(outcome,action,count,price,count),filled=filled,state=state,broker_id=cid)
    f=dict(fill_id=cid,order_id=cid,ticker='T',book_side=wire['side'],outcome_side=wire['outcome_side'],
           subaccount_number=0,count_fp=str(filled),yes_price_dollars=wire['price'],fee_cost=f'{filled/100:.2f}',
           created_time=f'2026-09-14T{hour:02d}:00:00Z')
    return r,dict(fills=[f] if filled else [],gross_dollars=str(filled*Decimal(wire['price'])),fees_dollars=f'{filled/100:.2f}')


def replay(*pairs):
    return replay_binary([r for r,e in pairs],{r['payload']['client_order_id']:e for r,e in pairs},as_of=NOW)


@pytest.mark.parametrize('outcome,sign',[('yes',1),('no',-1)])
def test_fifo_partial_exit_and_signed_position(outcome,sign):
    s=replay(record('buy','buy',3,30,outcome=outcome),record('exit','sell',1,50,outcome=outcome,hour=13))
    assert s['positions']=={'T':2*sign}
    assert s['open_basis']==Fraction(62,100) and s['realized']==Fraction(18,100)
    assert s['cashflow']==Fraction(-44,100)
    assert reconcile_positions(s,[dict(ticker='T',position_fp=str(2*sign))])
    with pytest.raises(ValueError):reconcile_positions(s,[dict(ticker='T',position_fp=str(-2*sign))])


def test_pending_exit_reserves_inventory_and_fees():
    s=replay(record('buy','buy',3,30),record('exit','sell',2,50,filled=1,state='working',hour=13))
    assert s['available_to_exit']=={'T':1} and s['pending_reserves']==Fraction(2,100)
    assert check_budget(s,outcome='no',action='sell',ticker='T',count=1,price_cents=50,fee_cents=1,
                        cash_cents=3,capital_limit_cents=0,order_limit_cents=5)==1
    with pytest.raises(ValueError,match='exit_exceeds'):
        check_budget(s,outcome='no',action='sell',ticker='T',count=2,price_cents=50,fee_cents=1,
                     cash_cents=100,capital_limit_cents=100,order_limit_cents=100)


def test_unfilled_no_entry_reserves_no_cost_not_yes_price():
    s=replay(record('buy','buy',2,30,filled=0,state='uncertain'))
    assert s['pending_reserves']==Fraction(62,100) and not s['positions']
    with pytest.raises(ValueError,match='cash_or_order'):
        check_budget(s,outcome='no',action='buy',ticker='OTHER',count=1,price_cents=30,fee_cents=1,
                     cash_cents=92,capital_limit_cents=1000,order_limit_cents=100)


def test_invalid_evidence_and_overselling():
    buy=record('buy','buy',1,30)
    with pytest.raises(ValueError):replay(buy,record('sell','sell',2,40,hour=13))
    with pytest.raises(ValueError,match='opposing'):replay(buy,record('yes','buy',1,40,outcome='yes'))
    bad=record('bad','buy',1,30);bad[1]['fills'][0]['outcome_side']='yes'
    with pytest.raises(ValueError,match='direction'):replay(bad)
    with pytest.raises(ValueError,match='duplicate'):replay(buy,buy)
