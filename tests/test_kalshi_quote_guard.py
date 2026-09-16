from datetime import datetime,timezone
import pytest
from opportunity_lab.kalshi_quote_guard import validate
from opportunity_lab.kalshi_order_journal import Journal

NOW=datetime(2026,9,14,20,tzinfo=timezone.utc)


def quote():
    return dict(environment='production',ticker='TEST',started_at=NOW.timestamp()-1,
                observed_at=NOW.timestamp(),orderbook_fp=dict(
                    yes_dollars=[['0.39','1.00'],['0.38','2.00']],
                    no_dollars=[['0.59','1.00'],['0.58','2.00']]))


def payload(side='bid',price='0.42'):
    return dict(ticker='TEST',side=side,price=price,count='2.00')


def test_depth_uses_complement_and_does_not_depend_on_sort_order():
    assert validate(payload(),quote(),now=NOW)['best_yes_ask']=='0.41'
    assert validate(payload('ask','0.38'),quote(),now=NOW)['best_yes_bid']=='0.39'
    with pytest.raises(ValueError,match='insufficient_depth'):validate(payload(price='0.41'),quote(),now=NOW)


@pytest.mark.parametrize('changes',[{'environment':'demo'},{'ticker':'OTHER'},
    {'observed_at':NOW.timestamp()+1},{'observed_at':NOW.timestamp()-6},
    {'started_at':NOW.timestamp()-3},{'started_at':float('nan')}])
def test_bad_scope_or_age_rejected(changes):
    q=quote();q.update(changes)
    with pytest.raises(ValueError):validate(payload(),q,now=NOW)


@pytest.mark.parametrize('levels',[[],[['NaN','1']], [['0.4','0']], [['0.4','1'],['0.40','1']],
    [['1.0','1']], [['0.5']], [['0.6','3']]])
def test_invalid_empty_or_crossed_book_rejected(levels):
    q=quote();q['orderbook_fp']['yes_dollars']=levels
    with pytest.raises(ValueError):validate(payload(),q,now=NOW)


def test_quote_failure_rolls_back_and_evidence_persists(tmp_path):
    j=Journal(tmp_path/'j.sqlite3',clock=lambda:NOW);j.bind_environment('production')
    j.configure_production_policy(order_limit_cents=500,capital_limit_cents=5000,daily_loss_limit_cents=1000)
    account=dict(environment='production',subaccount=0,started_at=NOW.timestamp()-1,
                 observed_at=NOW.timestamp(),balance=dict(balance=50000),positions=[],resting_orders=[])
    j.reserve('one','TEST',2,42,4,cash_cents=50000,account_snapshot=account)
    with pytest.raises(ValueError,match='quote_required'):
        j.mark_submission_started('one',account_snapshot=account)
    q=quote();q['observed_at']-=20
    with pytest.raises(ValueError):j.mark_submission_started('one',account_snapshot=account,quote_snapshot=q)
    assert j.get('one')['state']=='reserved'
    assert j.db.execute('SELECT count(*) FROM submission_quotes').fetchone()[0]==0
    j.mark_submission_started('one',account_snapshot=account,quote_snapshot=quote())
    j.close();j=Journal(tmp_path/'j.sqlite3')
    assert j.get('one')['state']=='uncertain'
    assert j.db.execute('SELECT count(*) FROM submission_quotes').fetchone()[0]==1
    j.close()
