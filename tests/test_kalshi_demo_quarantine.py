from copy import deepcopy
from datetime import datetime,timezone,timedelta
import pytest
from opportunity_lab.kalshi_demo_quarantine import enroll,validate,ORIGINAL_ID,ORIGINAL_TICKER
from opportunity_lab.kalshi_order_journal import Journal


def setup(tmp_path):
    j=Journal(tmp_path/'old.db');j.bind_environment('demo')
    j.reserve(ORIGINAL_ID,ORIGINAL_TICKER,1,50,10,cash_cents=50000);j.mark_submission_started(ORIGINAL_ID)
    now=datetime.now(timezone.utc).timestamp()
    m=dict(ticker=ORIGINAL_TICKER,status='closed',close_time=datetime.fromtimestamp(now-3600,timezone.utc).isoformat())
    e=dict(environment='demo',started_at=now-15,observed_at=now,market=m,balance={'balance':50000},
           positions=[],resting_orders=[],orders=[],historical_orders=[],fills=[],historical_fills=[],settlements=[])
    return j,now,m,e


def test_preserves_uncertainty_and_reserve(tmp_path):
    j,now,m,e=setup(tmp_path);before=j.get(ORIGINAL_ID)
    enroll(j,e,key_id='demo-key',now=now)
    assert j.get(ORIGINAL_ID)==before
    assert validate(j.db,before,key_id='demo-key',market=m,now=now+1)==60
    j.close()


def test_expired_quarantine_can_be_renewed_after_fresh_full_audit(tmp_path):
    j,now,m,e=setup(tmp_path);before=j.get(ORIGINAL_ID)
    first=enroll(j,e,key_id='demo-key',now=now)
    later=now+86401
    e=deepcopy(e);e['started_at']=later-1;e['observed_at']=later
    second=enroll(j,e,key_id='demo-key',now=later)
    assert second['expires_at']>first['expires_at']
    assert j.get(ORIGINAL_ID)==before
    assert j.db.execute('SELECT count(*) FROM demo_quarantine_history').fetchone()==(1,)
    assert validate(j.db,before,key_id='demo-key',market=m,now=later+1)==60
    j.close()


@pytest.mark.parametrize('change',['position','history','stale','reopened','wrong_record'])
def test_bad_evidence_never_quarantined(tmp_path,change):
    j,now,m,e=setup(tmp_path)
    if change=='position':e['positions']=[dict(ticker=ORIGINAL_TICKER,position_fp='1')]
    if change=='history':e['fills']=[{}]
    if change=='stale':now+=120
    if change=='reopened':m['status']='active'
    if change=='wrong_record':j.db.execute("UPDATE intents SET reserve=59")
    with pytest.raises(ValueError):enroll(j,e,key_id='demo-key',now=now)
    assert j.get(ORIGINAL_ID)['state']=='uncertain';j.close()


@pytest.mark.parametrize('change',['expired','key','market','record','scope'])
def test_quarantine_revalidated_on_every_use(tmp_path,change):
    j,now,m,e=setup(tmp_path);enroll(j,e,key_id='demo-key',now=now);key='demo-key';record=j.get(ORIGINAL_ID)
    if change=='expired':now+=86401
    if change=='key':key='different'
    if change=='market':m['status']='active'
    if change=='record':record['filled']=1
    if change=='scope':j.db.execute("UPDATE journal_scope SET environment='production'")
    with pytest.raises(ValueError):validate(j.db,record,key_id=key,market=m,now=now)
    j.close()
