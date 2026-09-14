"""Prospective matched-control paper test; reads live observations, never sends orders."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from .college_football_paper import lock_process, timestamp, write_json
from .sports_live_execution import initial, decide

ROOT = Path(__file__).resolve().parents[1]


def prepare(observation, now, entries, cap):
    obs = deepcopy(observation)
    age = (now-timestamp(obs['at'])).total_seconds()
    if not 0 <= age <= 3:
        obs['model']['blockers'].append('shadow_source_stale')
    for market in obs['markets'].values():
        if not 0 <= (now-timestamp(market['book_received_at'])).total_seconds() <= 5:
            market['valid'] = False
        if age > 120: market['settlement_ok'] = False
    # Both arms pay the same extra measured reader-processing delay.
    obs['latency_seconds'] = (obs.get('latency_seconds') or 5) + max(0, age)
    obs['admission_ok'] = bool(obs.get('admission_ok')) and age <= 3 and entries < cap
    return obs


def run(manifest):
    config = json.loads(Path(manifest).read_text())
    assert config['execution_enabled'] is False
    for path, digest in config['source_sha256'].items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest() != digest:
            raise ValueError('frozen_source_changed:'+path)
    output = ROOT/config['output'];output.mkdir(parents=True, exist_ok=True)
    lock = lock_process(output/'paper.lock')
    db = sqlite3.connect(output/'paper.sqlite3')
    db.execute('pragma journal_mode=WAL')
    db.executescript('''CREATE TABLE IF NOT EXISTS protocol(frozen TEXT);
      CREATE TABLE IF NOT EXISTS accounts(game TEXT,arm TEXT,horizon INTEGER,state TEXT,PRIMARY KEY(game,arm,horizon));
      CREATE TABLE IF NOT EXISTS cursor(game TEXT PRIMARY KEY,sample_id INTEGER);
      CREATE TABLE IF NOT EXISTS actions(game TEXT,arm TEXT,horizon INTEGER,at TEXT,source_sample_id INTEGER,detail TEXT,account TEXT);
      CREATE TABLE IF NOT EXISTS observations(game TEXT,at TEXT,source_sample_id INTEGER,age REAL);
    ''')
    frozen = json.dumps(config,sort_keys=True)
    existing = db.execute('select frozen from protocol').fetchone()
    if existing and existing[0] != frozen: raise ValueError('frozen_manifest_changed')
    if not existing: db.execute('insert into protocol values(?)',(frozen,))
    for game in config['games']:
        slug=game['league']+'_'+game['event_id']
        for arm in ('control','one_entry'):
            for h in (300,900):
                db.execute('insert or ignore into accounts values(?,?,?,?)',(slug,arm,h,json.dumps(initial())))
    db.commit()
    source=sqlite3.connect((ROOT/config['source_ledger']).resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
    next_status=0
    try:
        while datetime.now(timezone.utc)<timestamp(config['stop_at']):
            now=datetime.now(timezone.utc);paused=(output/'PAUSE').exists()
            for game in config['games']:
                slug=game['league']+'_'+game['event_id']
                if now<timestamp(game['kickoff']):continue
                row=source.execute('select id,at,observation from samples where slug=? order by id desc limit 1',(slug,)).fetchone()
                if not row or timestamp(row[1])<timestamp(config['registered_at']):continue
                previous=db.execute('select sample_id from cursor where game=?',(slug,)).fetchone()
                # Revisit stale observations for pending expiry/feed-failure guards;
                # unchanged book IDs cannot confirm new fills.
                obs=json.loads(row[2]);current=datetime.now(timezone.utc)
                for arm in ('control','one_entry'):
                    for h in (300,900):
                        state=json.loads(db.execute('select state from accounts where game=? and arm=? and horizon=?',(slug,arm,h)).fetchone()[0])
                        adjusted=prepare(obs,current,state['entries'],1 if arm=='one_entry' else 5)
                        actions=decide(state,adjusted,current,h,.04,paused or (output/(slug+'.PAUSE')).exists())
                        db.execute('update accounts set state=? where game=? and arm=? and horizon=?',(json.dumps(state),slug,arm,h))
                        db.execute('insert into actions values(?,?,?,?,?,?,?)',(slug,arm,h,current.isoformat(),row[0],json.dumps(actions),json.dumps(state)))
                db.execute('insert into observations values(?,?,?,?)',(slug,current.isoformat(),row[0],(current-timestamp(row[1])).total_seconds()))
                db.execute('insert or replace into cursor values(?,?)',(slug,row[0]))
            db.commit()
            if time.monotonic()>=next_status:
                accounts=[{'game':g,'arm':a,'horizon':h,**json.loads(s)} for g,a,h,s in db.execute('select * from accounts')]
                write_json(output/'status.json',{'at':datetime.now(timezone.utc).isoformat(),'version':config['version'],'execution_enabled':False,'accounts':accounts,'profitability_validated':False})
                next_status=time.monotonic()+10
            time.sleep(.5)
    finally:
        source.close();db.close();lock.close()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',default='configs/sports_churn_20260913/manifest.json')
    run(p.parse_args().manifest)
