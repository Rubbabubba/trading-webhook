import copy,json
from datetime import datetime,timezone
import pytest
from opportunity_lab.kalshi_wallet_profitability import Research
from opportunity_lab import kalshi_wallet_pool as pool
from opportunity_lab.wallet_qualification import POLICY_HASH,score,validate_report
from opportunity_lab.wallet_pool_policy import adjusted,validate_guard


def wallet(i):return '0x'+f'{i:040x}'


def source(offset=0,fail=()):
    def read(url,params):
        key=(params['category'],params['time_period'])
        if key in fail:raise OSError('fixture')
        start=[('OVERALL','WEEK'),('OVERALL','MONTH'),('SPORTS','WEEK'),('SPORTS','MONTH')].index(key)*25+offset
        assert params['limit']==25
        return ({'data':[{'rank':i+1,'user_id':wallet(start+i+1)} for i in range(25)]},)
    return read


def test_discovery_rotation_restart_and_outage_retention(tmp_path):
    now=[2000000000];w=Research(tmp_path,clock=lambda:now[0],read=source())
    active=w.candidates();assert len(active)==12
    assert pool.summary(w.db,now[0])['total_wallets']==100
    original=active[0]
    for i in range(10):w.db.execute('INSERT INTO observations VALUES(?,?,?,?,?)',(str(i),original,now[0],'unsupported','{}'))
    now[0]+=901
    after=w.candidates();assert original not in after and len(after)==12
    assert w.db.execute('SELECT state FROM wallet_pool WHERE wallet=?',(original,)).fetchone()[0]=='cooldown'
    assert w.db.execute('SELECT count(*) FROM observations').fetchone()[0]==10
    # Partial and complete refresh failures keep last-good wallets and evidence.
    now[0]+=3601
    old=set(x[0] for x in w.db.execute("SELECT wallet FROM wallet_pool WHERE state!='archived'"))
    w.read=source(100,fail=(('SPORTS','MONTH'),));w.candidates()
    assert old==set(x[0] for x in w.db.execute("SELECT wallet FROM wallet_pool WHERE state!='archived'"))
    w.read=lambda *a:(_ for _ in ()).throw(OSError('fixture'));w.candidates()
    assert old==set(x[0] for x in w.db.execute("SELECT wallet FROM wallet_pool WHERE state!='archived'"))
    assert w.db.execute('SELECT hash FROM protocol').fetchone()[0]==POLICY_HASH
    # Complete replacement archives rather than deletes historical records.
    w.read=source(100);w.candidates()
    assert pool.summary(w.db,now[0])['archived_wallets']==100
    w.db.close();again=Research(tmp_path,clock=lambda:now[0],read=source(100))
    assert again.db.execute('SELECT count(*) FROM observations').fetchone()[0]==10
    assert len(again.candidates())==12
    again.db.close()


def qualified_rows():
    return [dict(phase=p,event=str(i),day=str(i//7),net_cents=10,exit_kind='wallet_sell') for p in ('training','holdout') for i in range(20)]


def test_expanded_report_retains_all_scores_and_features_selected(tmp_path):
    now=[2000000000];w=Research(tmp_path,clock=lambda:now[0],read=source());w.candidates()
    for i,r in enumerate(qualified_rows()):
        d=dict(r,wallet=wallet(99),coverage_gap=False)
        w.db.execute('INSERT INTO trades(id,wallet,event,day,phase,detail) VALUES(?,?,?,?,?,?)',(str(i),d['wallet'],r['event'],r['day'],r['phase'],json.dumps(d)))
    result=w.report()
    assert result['selected_wallets']==[wallet(99)] and len(result['scores'])==8
    assert result['discovery']['scored_wallets']==100
    assert w.db.execute('SELECT count(*) FROM wallet_pool_scores').fetchone()[0]==100
    assert len(json.dumps(result).encode())<=16000
    assert validate_report(result,now=datetime.fromtimestamp(now[0],timezone.utc))==result
    forged=copy.deepcopy(result);forged['scores'][0]['holdout']['event_standard_error_cents']=1
    with pytest.raises(ValueError,match='lower_bound'):validate_report(forged,now=datetime.fromtimestamp(now[0],timezone.utc))
    raw=json.loads(w.db.execute('SELECT detail FROM trades LIMIT 1').fetchone()[0]);raw['coverage_gap']=True
    w.db.execute('UPDATE trades SET detail=? WHERE id=?',(json.dumps(raw),'0'))
    assert w.report()['selected_wallets']==[]
    w.db.close()


def test_pool_guard_rejects_borderline_confidence_and_coverage_gaps():
    row=dict(score(qualified_rows()),coverage_gaps=0)
    for phase in ('training','holdout'):
        row[phase].update(event_mean_cents=3.25,event_standard_error_cents=1,event_mean_lower_cents=.25)
    guarded=adjusted(row)
    assert row['qualified'] and not guarded['qualified']
    assert not validate_guard(guarded,True)
    row=dict(score(qualified_rows()),coverage_gaps=1)
    assert not adjusted(row)['qualified']


def test_shadow_owner_is_polled_after_rotation_and_expired_sell_blocks_qualification(tmp_path,monkeypatch):
    now=[2000000000];w=Research(tmp_path,clock=lambda:now[0],read=source());w.candidates();owner=wallet(1000)
    entry=dict(proxy_wallet=owner,condition_id='0x'+'a'*64,token_id='1',timestamp=now[0]-200,side='BUY')
    detail=dict(trade=entry,net_cents=None)
    w.db.execute('INSERT INTO trades(id,wallet,event,day,phase,detail) VALUES(?,?,?,?,?,?)',('owned',owner,'event','2020-01-01','training',json.dumps(detail)))
    sell=dict(entry,side='SELL',timestamp=now[0]-100)
    w.db.execute('INSERT INTO observations VALUES(?,?,?,?,?)',('sell',owner,now[0]-100,'observed',json.dumps(sell)))
    polled=[]
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_monitor.read_trades',lambda wallet,*a:polled.append(wallet) or [])
    monkeypatch.setattr(w,'settle',lambda:None)
    w.tick()
    assert owner in polled
    assert json.loads(w.db.execute('SELECT detail FROM trades').fetchone()[0])['coverage_gap'] is True
    assert w.db.execute('SELECT state FROM observations WHERE id=?',('sell',)).fetchone()[0]=='late'
    w.db.close()


def test_demo_prefers_qualified_without_replacing_daily_history(tmp_path,monkeypatch):
    import sqlite3
    from types import SimpleNamespace
    from opportunity_lab.kalshi_wallet_copy_worker import WalletCopy
    db=sqlite3.connect(':memory:',isolation_level=None)
    worker=WalletCopy(tmp_path,db,SimpleNamespace(journal=SimpleNamespace(db=db)),clock=lambda:2000000000)
    db.execute('INSERT INTO cohort VALUES(?,?)',('today',json.dumps([wallet(1)])))
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_profitability.read_report',lambda *a:{'selected_wallets':[wallet(2)]})
    assert worker.current_cohort('today')==[wallet(2)]
    assert json.loads(db.execute('SELECT wallets FROM cohort').fetchone()[0])==[wallet(1)]
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_profitability.read_report',lambda *a:(_ for _ in ()).throw(ValueError('stale')))
    assert worker.current_cohort('today')==[wallet(1)]
    db.close()
