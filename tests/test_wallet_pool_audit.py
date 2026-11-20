import copy,csv,io,json
from datetime import datetime,timezone
from opportunity_lab.kalshi_wallet_profitability import Research
from opportunity_lab.wallet_pool_audit import bound_report
from opportunity_lab.wallet_qualification import POLICY_HASH,validate_report
from test_wallet_pool import source,wallet,qualified_rows


def matrix(report):
    audit=report['pool_audit']
    return {row['wallet']:row for row in csv.DictReader(io.StringIO(audit['rows_csv']),fieldnames=audit['columns'])}


def test_complete_pool_evidence_rotations_and_history_survive_restart(tmp_path):
    now=[2000000000];w=Research(tmp_path,clock=lambda:now[0],read=source())
    active=w.candidates();target=active[0]
    for i in range(10):w.db.execute('INSERT INTO observations VALUES(?,?,?,?,?)',(str(i),target,now[0],'unsupported','{}'))
    now[0]+=901;assert target not in w.candidates()
    report=w.report();audit=report['pool_audit'];rows=matrix(report)
    assert audit['complete'] and audit['shown_rows']==audit['total_rows']==100 and len(rows)==100
    assert audit['state_codes'][int(rows[target]['state'])]=='cooldown'
    assert w.db.execute("SELECT 1 FROM wallet_pool_changes WHERE wallet=? AND previous_state='active' AND state='cooldown'",(target,)).fetchone()
    assert audit['observed_state_changes']>0
    assert rows[target]['training_resolved']==rows[target]['holdout_resolved']=='0'
    assert report['policy_hash']==POLICY_HASH
    assert validate_report(report,now=datetime.fromtimestamp(now[0],timezone.utc))==report
    count=w.db.execute('SELECT count(*) FROM wallet_pool_changes').fetchone()[0]
    start=audit['started_at'];w.db.close()
    again=Research(tmp_path,clock=lambda:now[0],read=source())
    assert again.report()['pool_audit']['started_at']==start
    assert again.db.execute('SELECT count(*) FROM wallet_pool_changes').fetchone()[0]==count
    assert again.db.execute('SELECT count(*) FROM observations').fetchone()[0]==10
    again.db.close()


def test_initial_observation_is_not_reconstructed_rotation(tmp_path):
    now=2000000000;w=Research(tmp_path,clock=lambda:now,read=source())
    w.db.execute("INSERT INTO wallet_pool(wallet,first_seen,last_seen,rank,sources,state) VALUES(?,?,?,1,'[]','active')",(wallet(1),now-86400,now))
    audit=w.report()['pool_audit']
    assert audit['observed_state_changes']==0
    assert audit['recent_state_changes'][0]['reason']=='initial_observation_only'
    assert audit['started_at']==now
    w.db.close()


def test_selected_full_scorecards_survive_audit_transport_bound(tmp_path):
    now=2000000000;w=Research(tmp_path,clock=lambda:now,read=source());w.candidates()
    for n in range(4):
        for i,row in enumerate(qualified_rows()):
            detail=dict(row,wallet=wallet(n+1),coverage_gap=False)
            w.db.execute('INSERT INTO trades(id,wallet,event,day,phase,detail) VALUES(?,?,?,?,?,?)',
                (f'{n}:{i}',detail['wallet'],row['event'],row['day'],row['phase'],json.dumps(detail)))
    report=w.report()
    assert len(report['selected_wallets'])==4
    assert set(report['selected_wallets'])<={row['wallet'] for row in report['scores']}
    assert len(json.dumps(report).encode())<=16000
    assert validate_report(report,now=datetime.fromtimestamp(now,timezone.utc))==report
    # Operational audit contents cannot promote another wallet.
    altered=copy.deepcopy(report);altered['pool_audit']['rows_csv']='untrusted audit text'
    assert validate_report(altered,now=datetime.fromtimestamp(now,timezone.utc))['selected_wallets']==report['selected_wallets']
    w.db.close()


def test_optional_large_audit_pages_without_dropping_selected_scorecards():
    result={'selected_wallets':['selected'],'scores':[{'wallet':'selected'}],
        'pool_audit':{'total_rows':100,'shown_rows':100,'complete':True,'rows_csv':'\n'.join('x'*300 for _ in range(100))+'\n'}}
    bound_report(result,2000000000)
    assert result['scores']==[{'wallet':'selected'}]
    assert result['selected_wallets']==['selected']
    assert not result['pool_audit']['complete'] and result['pool_audit']['shown_rows']<100
    assert len(json.dumps(result).encode())<=16000
