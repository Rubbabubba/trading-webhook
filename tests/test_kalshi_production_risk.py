from datetime import datetime, timezone
import sqlite3

import pytest

from opportunity_lab.kalshi_order_journal import Journal
from opportunity_lab.kalshi_production_risk import evaluate
from opportunity_lab.kalshi_demo_broker import DemoBroker

NOW = datetime(2026,9,14,20,tzinfo=timezone.utc)
POLICY = {"capital_limit_cents":5000,"daily_loss_limit_cents":1000}


def quote():
    return dict(environment='production',ticker='TEST',started_at=NOW.timestamp()-1,
                observed_at=NOW.timestamp(),orderbook_fp=dict(yes_dollars=[['0.20','20.00']],
                                                            no_dollars=[['0.70','20.00']]))


def snapshot():
    return dict(environment="production", subaccount=0, started_at=NOW.timestamp()-5,
                observed_at=NOW.timestamp(), balance={"balance":50000}, positions=[], resting_orders=[])


@pytest.fixture
def journal(tmp_path):
    j=Journal(tmp_path/'production.sqlite3',clock=lambda:NOW)
    j.bind_environment('production')
    j.configure_production_policy(order_limit_cents=500,**POLICY)
    yield j
    j.close()


def test_pass_is_diagnostic_only_and_works_readonly(journal):
    path=journal.db.execute('PRAGMA database_list').fetchone()[2]
    db=sqlite3.connect('file:'+path+'?mode=ro',uri=True)
    result=evaluate(db,snapshot(),POLICY,now=NOW)
    assert result['risk_checks_passed']
    assert result['execution_enabled'] is False and result['live_ready'] is False
    db.close()


@pytest.mark.parametrize('policy,blocker',[(None,'production_risk_policy_missing'),
    ({},'production_risk_policy_invalid'),
    ({'capital_limit_cents':True,'daily_loss_limit_cents':1000},'production_risk_policy_invalid')])
def test_no_implicit_live_limits(journal,policy,blocker):
    assert blocker in evaluate(journal.db,snapshot(),policy,now=NOW)['blockers']


@pytest.mark.parametrize('change,blocker',[
    ({'observed_at':NOW.timestamp()-61},'account_snapshot_stale_or_inconsistent'),
    ({'observed_at':NOW.timestamp()+1},'account_snapshot_stale_or_inconsistent'),
    ({'started_at':float('nan')},'account_snapshot_stale_or_inconsistent'),
    ({'environment':'demo'},'account_scope_mismatch'),
    ({'subaccount':False},'account_scope_mismatch'),
    ({'positions':[{'ticker':'MANUAL','position_fp':'1.00'}]},'external_or_unreconciled_position'),
    ({'positions':[{'ticker':'NO','position_fp':'-1.00'}]},'invalid_account_snapshot'),
    ({'resting_orders':[{'order_id':'manual'}]},'resting_orders_present')])
def test_bad_account_cannot_pass(journal,change,blocker):
    data=snapshot();data.update(change)
    result=evaluate(journal.db,data,POLICY,now=NOW)
    assert not result['risk_checks_passed'] and blocker in result['blockers']


def test_uncertain_and_hard_stop_block(journal):
    journal.reserve('pending','TEST',1,40,2,cash_cents=50000,account_snapshot=snapshot())
    journal.mark_submission_started('pending',account_snapshot=snapshot(),quote_snapshot=quote())
    journal.stop()
    blockers=evaluate(journal.db,snapshot(),POLICY,now=NOW)['blockers']
    assert 'orders_not_terminal' in blockers and 'journal_stopped' in blockers


def test_demo_scope_cannot_be_reused_for_production(tmp_path):
    path=tmp_path/'demo.sqlite3';j=Journal(path)
    DemoBroker(j,object());j.close();j=Journal(path)
    assert 'production_journal_scope_required' in evaluate(j.db,snapshot(),POLICY,now=NOW)['blockers']
    with pytest.raises(ValueError,match='environment_mismatch'):j.bind_environment('production')
    j.close()


def test_production_journal_cannot_use_demo_transport(journal):
    with pytest.raises(ValueError,match='environment_mismatch'):DemoBroker(journal,object())


def test_nonempty_unscoped_journal_cannot_be_promoted(tmp_path):
    j=Journal(tmp_path/'legacy.sqlite3')
    j.reserve('one','TEST',1,40,2,cash_cents=50000)
    with pytest.raises(ValueError,match='empty_scoped'):j.bind_environment('production')
    j.close()


def test_daily_latch_is_checked_without_mutation(journal):
    journal.db.execute("INSERT INTO risk_stops VALUES('2026-09-14')")
    before=journal.db.total_changes
    result=evaluate(journal.db,snapshot(),POLICY,now=NOW)
    assert 'daily_loss_stop_latched' in result['blockers']
    assert journal.db.total_changes==before


def test_realized_loss_and_open_basis_limits(journal):
    def fill(cid,side,price,at):
        data=snapshot()
        data['positions']=[dict(ticker=k,position_fp=str(v)) for k,v in journal.accounting()['positions'].items()]
        journal.reserve(cid,'TEST',1,price,2,cash_cents=50000,side=side,account_snapshot=data)
        journal.mark_submission_started(cid,account_snapshot=data,quote_snapshot=quote())
        evidence=dict(gross_dollars=f'{price/100:.2f}',fees_dollars='0.01',fills=[dict(
            fill_id=cid,order_id=cid,ticker='TEST',outcome_side='yes' if side=='bid' else 'no',book_side=side,
            subaccount_number=0,count_fp='1.00',yes_price_dollars=f'{price/100:.2f}',
            fee_cost='0.01',created_time=at)])
        journal.reconcile(cid,broker_id=cid,filled=1,remaining=0,terminal=True,evidence=evidence)
    fill('buy','bid',80,'2026-09-14T12:00:00Z')
    data=snapshot();data['positions']=[{'ticker':'TEST','position_fp':'1.00'}]
    policy={'capital_limit_cents':50,'daily_loss_limit_cents':50}
    assert 'capital_limit_exceeded' in evaluate(journal.db,data,policy,now=NOW)['blockers']
    fill('exit','ask',20,'2026-09-14T13:00:00Z')
    assert 'daily_loss_limit_reached' in evaluate(journal.db,snapshot(),policy,now=NOW)['blockers']


def test_restored_dependency_does_not_expand_observer_cli(tmp_path,monkeypatch):
    import json
    from opportunity_lab import kalshi_account_monitor as monitor
    path=tmp_path/'journal.sqlite3';j=Journal(path);j.bind_environment('production');j.close()
    monkeypatch.setenv('KALSHI_API_KEY_ID','test')
    monkeypatch.setenv('KALSHI_PRIVATE_KEY_PATH','unused')
    monkeypatch.setattr(monitor,'AccountClient',lambda *a:object())
    data=snapshot();data['started_at']=data['observed_at']=datetime.now(timezone.utc).timestamp()
    data['balance']['portfolio_value']=0
    monkeypatch.setattr(monitor,'collect',lambda c:data)
    monkeypatch.setattr('sys.argv',['observer','--output',str(tmp_path/'observer'),
        '--risk-journal',str(path),'--capital-limit-cents','5000','--daily-loss-limit-cents','1000'])
    # The source prototype also tested a later observer CLI integration which
    # is absent from this branch. Restoring a pure risk dependency must NOT add
    # those options or expand the installed production observer's authority.
    with pytest.raises(SystemExit) as error:
        monitor.main()
    assert error.value.code == 2
    assert not (tmp_path/'observer'/'status.json').exists()


def test_production_missing_policy_or_snapshot_blocks_reservation(tmp_path):
    j=Journal(tmp_path/'empty.sqlite3',clock=lambda:NOW);j.bind_environment('production')
    with pytest.raises(ValueError,match='policy_missing'):
        j.reserve('one','TEST',1,40,2,cash_cents=50000)
    j.configure_production_policy(order_limit_cents=500,**POLICY)
    with pytest.raises(ValueError,match='snapshot_required'):
        j.reserve('one','TEST',1,40,2,cash_cents=50000)
    assert j.records()==[]
    j.close()


def test_policy_and_submission_gate_survive_restart(journal):
    journal.reserve('one','TEST',1,40,2,cash_cents=50000,account_snapshot=snapshot())
    path=journal.db.execute('PRAGMA database_list').fetchone()[2]
    j=Journal(path,clock=lambda:NOW)
    with pytest.raises(ValueError,match='snapshot_required'):j.mark_submission_started('one')
    data=snapshot();data['positions']=[dict(ticker='MANUAL',position_fp='1.00')]
    with pytest.raises(ValueError,match='unreconciled_position'):
        j.mark_submission_started('one',account_snapshot=data)
    assert j.get('one')['state']=='reserved'
    assert j.mark_submission_started('one',account_snapshot=snapshot(),quote_snapshot=quote())['client_order_id']=='one'
    with pytest.raises(ValueError):j.mark_submission_started('one',account_snapshot=snapshot(),quote_snapshot=quote())
    j.close()


def test_production_policy_immutable_and_order_cost_enforced(journal):
    with pytest.raises(ValueError,match='policy_change'):
        journal.configure_production_policy(order_limit_cents=1000,**POLICY)
    with pytest.raises(ValueError,match='budget_exceeded'):
        journal.reserve('large','TEST',10,50,1,cash_cents=50000,account_snapshot=snapshot())
    with pytest.raises(ValueError,match='cash_mismatch'):
        journal.reserve('badcash','TEST',1,40,2,cash_cents=99999,account_snapshot=snapshot())
    assert journal.records()==[]


def test_changed_cash_and_stale_snapshot_block_submission(journal):
    journal.reserve('one','TEST',1,40,2,cash_cents=50000,account_snapshot=snapshot())
    data=snapshot();data['balance']['balance']=1
    with pytest.raises(ValueError,match='insufficient_available_cash'):
        journal.mark_submission_started('one',account_snapshot=data)
    data=snapshot();data['started_at']-=90;data['observed_at']-=90
    with pytest.raises(ValueError,match='snapshot_stale'):
        journal.mark_submission_started('one',account_snapshot=data)
    assert journal.get('one')['state']=='reserved'


def test_explicit_production_cap_replaces_demo_cap(tmp_path):
    j=Journal(tmp_path/'policy.sqlite3',clock=lambda:NOW);j.bind_environment('production')
    j.configure_production_policy(order_limit_cents=600,capital_limit_cents=700,daily_loss_limit_cents=50)
    j.reserve('one','TEST',10,50,10,cash_cents=50000,account_snapshot=snapshot())
    assert j.get('one')['reserve']==510
    assert j.mark_submission_started('one',account_snapshot=snapshot(),quote_snapshot=quote())
    j.close()


def test_production_exposure_cap_counts_fees(tmp_path):
    j=Journal(tmp_path/'cap.sqlite3',clock=lambda:NOW);j.bind_environment('production')
    j.configure_production_policy(order_limit_cents=40,capital_limit_cents=40,daily_loss_limit_cents=50)
    with pytest.raises(ValueError,match='budget_exceeded'):
        j.reserve('one','TEST',1,40,1,cash_cents=50000,account_snapshot=snapshot())
    assert j.records()==[]
    j.close()


def test_production_daily_loss_gate_commits_latch(tmp_path):
    journal=Journal(tmp_path/'loss.sqlite3',clock=lambda:NOW)
    journal.bind_environment('production')
    journal.configure_production_policy(order_limit_cents=500,capital_limit_cents=5000,daily_loss_limit_cents=50)
    journal.reserve('buy','TEST',1,80,2,cash_cents=50000,account_snapshot=snapshot())
    journal.mark_submission_started('buy',account_snapshot=snapshot(),quote_snapshot=quote())
    fill=dict(fill_id='buy',order_id='buy',ticker='TEST',outcome_side='yes',book_side='bid',
              subaccount_number=0,count_fp='1.00',yes_price_dollars='0.80',fee_cost='0.01',
              created_time='2026-09-14T12:00:00Z')
    journal.reconcile('buy',broker_id='buy',filled=1,remaining=0,terminal=True,
                      evidence=dict(fills=[fill],gross_dollars='0.80',fees_dollars='0.01'))
    journal.record_settlement(dict(ticker='TEST',exchange_index=0,market_result='no',yes_count_fp='1.00',
        no_count_fp='0.00',yes_total_cost_dollars='0.80',no_total_cost_dollars='0.00',
        fee_cost='0.01',revenue=0,value=0,settled_time='2026-09-14T14:00:00Z'))
    with pytest.raises(ValueError,match='daily_loss_limit_reached'):
        journal.reserve('next','OTHER',1,40,2,cash_cents=50000,account_snapshot=snapshot())
    assert journal.db.execute("SELECT day FROM risk_stops").fetchall()==[('2026-09-14',)]
    assert not journal.db.in_transaction
    journal.close()
