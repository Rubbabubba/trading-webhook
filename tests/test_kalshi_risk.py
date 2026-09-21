from copy import deepcopy
from datetime import datetime, timezone
from fractions import Fraction

import pytest

from opportunity_lab.kalshi_execution_risk import replay, report
from opportunity_lab.kalshi_order_journal import Journal

NOW = datetime(2026, 9, 14, 20, tzinfo=timezone.utc)


def event(cid, side, count, price, fee, at, ticker="TEST"):
    fill = dict(fill_id=cid, order_id=cid, ticker=ticker, outcome_side="yes" if side == "bid" else "no", book_side=side,
                subaccount_number=0, count_fp=str(count), yes_price_dollars=price,
                fee_cost=fee, created_time=at)
    record = dict(payload=dict(client_order_id=cid, ticker=ticker, side=side),
                  state="terminal", filled=count, reserve=500, broker_id=cid)
    from decimal import Decimal
    evidence = dict(fills=[fill], gross_dollars=str(count * Decimal(price)), fees_dollars=fee)
    return record, evidence


def run(*events, now=NOW):
    return replay([r for r,e in events], {r['payload']['client_order_id']:e for r,e in events}, as_of=now)


def test_fifo_partial_fees_exact_and_accounting_identity():
    buy = event('b','bid',3,'0.40','0.02','2026-09-14T12:00:00Z')
    sell = event('s','ask',1,'0.60','0.01','2026-09-14T13:00:00Z')
    state = run(buy,sell)
    assert state['daily_realized'] == Fraction(59,100) - Fraction(122,300)
    assert state['open_basis'] == Fraction(244,300)
    assert state['realized'] == state['cashflow'] + state['open_basis']
    assert state['positions'] == {'TEST':2}
    assert isinstance(report(state)['daily_realized'],str)


def test_fractional_execution_pieces_reconcile_to_whole_contract():
    record, evidence = event(
        'b', 'bid', 1, '0.40', '0.01', '2026-09-14T12:00:00Z'
    )
    first = deepcopy(evidence['fills'][0])
    first.update(fill_id='fraction-one', count_fp='0.81', fee_cost='0.0081')
    second = deepcopy(evidence['fills'][0])
    second.update(fill_id='fraction-two', count_fp='0.19', fee_cost='0.0019')
    evidence.update(fills=[first, second], gross_dollars='0.4000', fees_dollars='0.0100')

    state = replay([record], {'b': evidence}, as_of=NOW)

    assert state['positions'] == {'TEST': 1}
    assert state['open_basis'] == Fraction(41, 100)


def test_central_midnight_and_daylight_saving_boundary():
    b=event('b','bid',1,'0.40','0.01','2026-09-14T04:00:00Z')
    s=event('s','ask',1,'0.50','0.01','2026-09-14T04:59:59Z')
    assert run(b,s)['daily_realized']==0
    assert run(b,s)['daily']['2026-09-13']==Fraction(8,100)
    s[1]['fills'][0]['created_time']='2026-09-14T05:00:00Z'
    assert run(b,s)['daily_realized']==Fraction(8,100)
    b[1]['fills'][0]['created_time']='2026-11-01T06:30:00Z'
    s[1]['fills'][0]['created_time']='2026-11-01T07:30:00Z'
    state=run(b,s,now=datetime(2026,11,1,12,tzinfo=timezone.utc))
    assert state['daily_realized']==Fraction(8,100)


def test_multiple_lots_and_final_exit_release_all_basis():
    state=run(event('a','bid',2,'0.30','0.02','2026-09-14T11:00:00Z'),
              event('b','bid',1,'0.50','0.01','2026-09-14T12:00:00Z'),
              event('c','ask',3,'0.40','0.03','2026-09-14T13:00:00Z'))
    assert state['open_basis']==0 and state['positions']=={}
    assert state['realized']==Fraction(4,100)


@pytest.mark.parametrize('change',[{'created_time':'bad'},{'created_time':'2026-09-14T12:00:00'},
    {'created_time':'2027-01-01T00:00:00Z'},{'fee_cost':'NaN'}, {'subaccount_number':True},
    {'count_fp':'0.5'},{'order_id':'OTHER'}])
def test_bad_evidence_blocks_accounting(change):
    b=event('b','bid',1,'0.40','0.01','2026-09-14T12:00:00Z')
    b[1]['fills'][0].update(change)
    with pytest.raises(ValueError):run(b)


def test_missing_duplicate_and_unowned_fill():
    b=event('b','bid',1,'0.40','0.01','2026-09-14T12:00:00Z')
    with pytest.raises(ValueError,match='missing_accounting'):replay([b[0]],{},as_of=NOW)
    duplicate=deepcopy(b);duplicate[0]['payload']['client_order_id']='different'
    with pytest.raises(ValueError,match='duplicate'):run(b,duplicate)
    with pytest.raises(ValueError,match='sale_without'):run(event('s','ask',1,'0.50','0.01','2026-09-14T12:00:00Z'))


def populate(j,cid,side,price,at):
    j.reserve(cid,'TEST',1,price,2,cash_cents=50000,side=side)
    j.mark_submission_started(cid)
    _,ev=event(cid,side,1,f'{price/100:.2f}','0.01',at)
    j.reconcile(cid,broker_id=cid,filled=1,remaining=0,terminal=True,evidence=ev)


def test_persistent_daily_stop_blocks_reserved_entries_but_allows_exit(tmp_path):
    path=tmp_path/'risk.sqlite3';j=Journal(path,clock=lambda:NOW)
    populate(j,'a','bid',80,'2026-09-14T11:00:00Z')
    populate(j,'b','bid',40,'2026-09-14T12:00:00Z')
    j.reserve('pending','TEST',1,40,2,cash_cents=50000)
    populate(j,'loss','ask',20,'2026-09-14T13:00:00Z')
    j.configure_risk(50)
    with pytest.raises(ValueError,match='daily_loss_stop'):j.mark_submission_started('pending')
    assert j.get('pending')['state']=='reserved'
    j.close();j=Journal(path,clock=lambda:NOW)
    with pytest.raises(ValueError,match='daily_loss_stop'):j.reserve('new','TEST',1,40,2,cash_cents=50000)
    j.reserve('exit','TEST',1,30,2,cash_cents=50000,side='ask')
    assert j.mark_submission_started('exit')['reduce_only']
    j.close()


def test_completed_trade_releases_capital_and_policy_cannot_change(tmp_path):
    j=Journal(tmp_path/'risk.sqlite3',clock=lambda:NOW);j.configure_risk(100)
    populate(j,'buy','bid',40,'2026-09-14T11:00:00Z')
    assert j.accounting()['capital_at_risk']==Fraction(41,100)
    populate(j,'sell','ask',50,'2026-09-14T12:00:00Z')
    assert j.accounting()['capital_at_risk']==0
    # Old retained reserve sum would exceed this new cash budget.
    assert j.reserve('next','TEST',1,40,2,cash_cents=42)
    with pytest.raises(ValueError,match='policy_change'):j.configure_risk(200)
    j.close()


def test_recovered_daily_pnl_does_not_erase_earlier_loss_stop():
    state=run(event('b1','bid',1,'0.90','0.01','2026-09-14T11:00:00Z'),
              event('b2','bid',1,'0.01','0.01','2026-09-14T12:00:00Z'),
              event('s1','ask',1,'0.10','0.01','2026-09-14T13:00:00Z'),
              event('s2','ask',1,'0.99','0.01','2026-09-14T14:00:00Z'))
    assert state['daily_low']==Fraction(-82,100)
    assert state['daily_realized']>0


def test_uncertain_reserve_remains_and_next_day_has_own_loss_window(tmp_path):
    path=tmp_path/'risk.sqlite3';j=Journal(path,clock=lambda:NOW)
    populate(j,'a','bid',80,'2026-09-14T11:00:00Z')
    populate(j,'s','ask',20,'2026-09-14T12:00:00Z');j.configure_risk(50)
    with pytest.raises(ValueError,match='daily_loss_stop'):j.reserve('blocked','TEST',1,40,2,cash_cents=50000)
    j.close();j=Journal(path,clock=lambda:datetime(2026,9,15,5,tzinfo=timezone.utc))
    j.reserve('nextday','TEST',1,40,2,cash_cents=50000);j.mark_submission_started('nextday')
    assert j.accounting()['pending_reserves']==Fraction(42,100)
    assert j.accounting()['daily_realized']==0
    with pytest.raises(ValueError,match='unreconciled'):j.reserve('duplicate','TEST',1,40,2,cash_cents=50000)
    j.close()


def settlement(result="yes"):
    return dict(ticker="TEST", exchange_index=0, market_result=result, yes_count_fp="1.00",
                no_count_fp="0.00", yes_total_cost_dollars="0.80", no_total_cost_dollars="0.00",
                fee_cost="0.01", revenue=100 if result == "yes" else 0,
                value=100 if result == "yes" else 0, settled_time="2026-09-14T14:00:00Z")


@pytest.mark.parametrize("result,profit", [("yes",19),("no",-81)])
def test_settlement_releases_basis_and_counts_fees_once(tmp_path,result,profit):
    path=tmp_path/'settle.sqlite3';j=Journal(path,clock=lambda:NOW)
    populate(j,'buy','bid',80,'2026-09-14T11:00:00Z')
    j.configure_risk(50)
    j.record_settlement(settlement(result))
    j.record_settlement(settlement(result))
    j.close();j=Journal(path,clock=lambda:NOW)
    state=j.accounting()
    assert state['realized']==Fraction(profit,100)
    assert state['fees']==Fraction(1,100)
    assert state['positions']=={} and state['capital_at_risk']==0
    assert state['realized']==state['cashflow']
    with pytest.raises(ValueError,match='already_settled'):
        j.reserve('exit','TEST',1,40,2,cash_cents=50000,side='ask')
    if result=='no':
        with pytest.raises(ValueError,match='daily_loss_stop'):
            j.reserve('next','OTHER',1,40,2,cash_cents=50000)
    j.close()


@pytest.mark.parametrize('change',[{'revenue':True},{'yes_count_fp':'2.00'},
    {'fee_cost':'0.02'},{'yes_total_cost_dollars':'0.81'}, {'market_result':'scalar'},
    {'no_count_fp':'1.00'},{'exchange_index':False},{'value':50},
    {'settled_time':'2026-09-14T10:00:00Z'}, {'settled_time':'2027-01-01T00:00:00Z'}])
def test_bad_settlement_rolls_back_without_releasing_inventory(tmp_path,change):
    j=Journal(tmp_path/'s.sqlite3',clock=lambda:NOW)
    populate(j,'buy','bid',80,'2026-09-14T11:00:00Z')
    row=settlement();row.update(change)
    with pytest.raises(ValueError):j.record_settlement(row)
    assert j.accounting()['positions']=={'TEST':1}
    assert j.db.execute('SELECT count(*) FROM settlements').fetchone()[0]==0
    j.close()


def test_changed_settlement_and_exit_history_rejected(tmp_path):
    j=Journal(tmp_path/'s.sqlite3',clock=lambda:NOW)
    populate(j,'buy','bid',80,'2026-09-14T11:00:00Z')
    j.record_settlement(settlement())
    with pytest.raises(ValueError,match='history_changed'):j.record_settlement(settlement('no'))
    j.close()


@pytest.mark.parametrize('result,payout', [('yes',200),('no',0)])
def test_partial_exit_then_settlement_exact_basis_and_fees(result,payout):
    buy=event('b','bid',3,'0.40','0.03','2026-09-13T12:00:00Z')
    sell=event('s','ask',1,'0.60','0.01','2026-09-13T13:00:00Z')
    row=settlement(result)
    row.update(yes_count_fp='2.00',yes_total_cost_dollars='0.80',fee_cost='0.04',revenue=payout)
    state=replay([buy[0],sell[0]],{'b':buy[1],'s':sell[1]},as_of=NOW,settlements=[row])
    assert state['daily']['2026-09-13']==Fraction(18,100)
    assert state['daily_realized']==Fraction(payout-82,100)
    assert state['fees']==Fraction(4,100)
    assert state['open_basis']==0 and state['positions']=={}
    assert state['realized']==state['cashflow']==Fraction(payout-64,100)


def test_partial_exit_multiple_lots_rejects_nonmatching_exchange_basis():
    events=[event('a','bid',1,'0.20','0.01','2026-09-14T10:00:00Z'),
            event('b','bid',2,'0.60','0.02','2026-09-14T11:00:00Z'),
            event('s','ask',2,'0.70','0.02','2026-09-14T12:00:00Z')]
    row=settlement();row.update(yes_total_cost_dollars='0.60',fee_cost='0.05')
    records=[e[0] for e in events];evidence={e[0]['payload']['client_order_id']:e[1] for e in events}
    state=replay(records,evidence,as_of=NOW,settlements=[row])
    assert state['realized']==Fraction(95,100)
    row['yes_total_cost_dollars']='0.4667'
    with pytest.raises(ValueError,match='settlement_not_reconciled'):
        replay(records,evidence,as_of=NOW,settlements=[row])


def test_fully_exited_position_cannot_settle_again(tmp_path):
    j=Journal(tmp_path/'exited.sqlite3',clock=lambda:NOW)
    populate(j,'buy','bid',80,'2026-09-14T11:00:00Z')
    populate(j,'exit','ask',90,'2026-09-14T12:00:00Z')
    with pytest.raises(ValueError,match='unsupported_settlement_history'):j.record_settlement(settlement())
    j.close()
