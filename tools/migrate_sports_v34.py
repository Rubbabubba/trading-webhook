"""Offline, no-overwrite, state-verified migration invoked by the recovery wrapper."""
from pathlib import Path
import sqlite3,json,hashlib
from datetime import datetime,timezone

ROOT=Path(__file__).resolve().parents[1]
stamp=datetime.now(timezone.utc).isoformat()
pairs=[('live_v33_20260912','live_v34_20260913','sports_live_v33_20260912','sports_live_v34_20260913'),
       ('churn_20260913','churn_v11_20260913','sports_churn_20260913','sports_churn_v11_20260913')]
for old,new,oc,nc in pairs:
    assert not (ROOT/'sports_paper'/new/'paper.sqlite3').exists(), 'destination_exists'
for old,new,oc,nc in pairs:
    source=ROOT/'sports_paper'/old;dest=ROOT/'sports_paper'/new;dest.mkdir(exist_ok=True)
    a=sqlite3.connect(source/'paper.sqlite3');b=sqlite3.connect(dest/'paper.sqlite3');a.backup(b)
    tables=['accounts','probes'] if old.startswith('live') else ['accounts','cursor']
    for table in tables:
        assert a.execute('select * from '+table+' order by 1,2').fetchall()==b.execute('select * from '+table+' order by 1,2').fetchall()
    c=json.loads((ROOT/'configs'/oc/'manifest.json').read_text())
    c['previous_lineage']=c.get('lineage');c['lineage']={'copied_from':str(source/'paper.sqlite3'),'at':stamp,'account_rows_identical':True,'rules_unchanged':True,'note':'Do not double count inherited history.'}
    if old.startswith('live'):
        c['version']='sports_live_3.4';c['registered_at']=stamp
        for name in ['opportunity_lab/sports_live_v34.py','opportunity_lab/sports_live_report_current34.py']:
            c['source_sha256'][name]=hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
    else:
        c['version']='ncaaf_churn_1.1';c['operational_migration_at']=stamp
        c['source_ledger']='sports_paper/live_v34_20260913/paper.sqlite3';c['output']='sports_paper/churn_v11_20260913'
    configdir=ROOT/'configs'/nc;configdir.mkdir(exist_ok=True);(configdir/'manifest.json').write_text(json.dumps(c,indent=2))
    b.execute('update protocol set frozen=?',(json.dumps(c,sort_keys=True),));b.commit();b.execute('pragma journal_mode=WAL');b.close();a.close()
    review=source/'automatic_review/decisions.json'
    if review.exists():
        (dest/'automatic_review').mkdir(exist_ok=True);(dest/'automatic_review/decisions.json').write_text(review.read_text())
    (dest/'migration.json').write_text(json.dumps(c['lineage'],indent=2))
for old,new,oc,nc in pairs:
    (ROOT/'sports_paper'/old/'PAUSE').write_text('migration_v34: superseded '+stamp)
print('Both ledgers copied; accounts/cursors/probes verified identical; old ledgers preserved.')
