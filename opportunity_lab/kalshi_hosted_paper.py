"""Portable hosted paper portfolio; independent public discovery, no credentials."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from .kalshi_shadow_live import PublicBooks, choose_universe, epoch
from .kalshi_process_lock import acquire
from .kalshi_hosted_discovery import Discovery
from .kalshi_shadow_settlement import SettlementShadow, SettlementCoordinator


def portfolio_state(engines):
    states=[engine.state() for engine in engines]
    return dict(cash_cents=sum(s['cash_cents'] for s in states),
                realized_cents=sum(s['realized_cents'] for s in states),
                open_basis_cents=sum(s['position']['basis'] if s['position'] else 0 for s in states),
                pending_entry_cents=sum(s['pending']['max_cost'] if s['pending'] and s['pending']['kind']=='entry' else 0 for s in states),
                open_positions=sum(s['position'] is not None for s in states), states=states)


def save(db,name,value):
    db.execute('INSERT OR REPLACE INTO portfolio_settings VALUES(?,?)',(name,json.dumps(value,sort_keys=True)))


def load(db,name,default=None):
    row=db.execute('SELECT detail FROM portfolio_settings WHERE name=?',(name,)).fetchone()
    return json.loads(row[0]) if row else default


def rotate(cache, seen, *, now, stop, count):
    available={k:v for k,v in cache.items() if k not in seen}
    cohort=choose_universe(available,now,stop,count)
    if not cohort:
        seen=[];cohort=choose_universe(cache,now,stop,count)
    return cohort,sorted(set(seen)|set(cohort))


def run(config_path, *, cycles=None):
    config=json.loads(Path(config_path).read_text())
    if config['execution_enabled'] is not False or config['slots']!=4 or config['total_cash_cents']!=50000:
        raise ValueError('paper_portfolio_configuration_required')
    for file,digest in config['source_sha256'].items():
        if hashlib.sha256(Path(file).read_bytes()).hexdigest()!=digest:raise ValueError('frozen_source_changed')
    output=Path(config['output']);output.mkdir(parents=True,exist_ok=True)
    lock=acquire(output/'worker.lock')
    db=sqlite3.connect(output/'portfolio.sqlite3',isolation_level=None)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE IF NOT EXISTS portfolio_settings(name TEXT PRIMARY KEY,detail TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS portfolio_actions(at REAL,lane INTEGER,ticker TEXT,detail TEXT)')
    engines=[]
    try:
        saved=load(db,'manifest')
        if saved is not None and saved!=config:raise ValueError('frozen_manifest_changed')
        save(db,'manifest',config)
        for i in range(config['slots']):
            engines.append(SettlementShadow(output/f'lane_{i}.sqlite3',
                dict(config['engine'],initial_cash_cents=config['total_cash_cents']//config['slots'])))
        stop=epoch(config['stop_at']);client=PublicBooks(db)
        cohort=load(db,'cohort',{});created=load(db,'cohort_at',0);seen=load(db,'seen',[])
        contexts=load(db,'contexts',{});scan=load(db,'scan',0);cycle=0
        discovery=Discovery(config['universe_source'],stop)
        while time.time()<stop and (cycles is None or cycle<cycles):
            errors=[]
            try:discovery.update()
            except Exception as exc:errors.append('discovery:'+type(exc).__name__)
            if time.time()-created>=1800 or not cohort:
                try:
                    source=Path(config['universe_source'])
                    if time.time()-source.stat().st_mtime>900:raise ValueError('stale_discovery')
                    cohort,seen=rotate(json.loads(source.read_text()),seen,now=time.time(),stop=stop,count=12)
                    created=time.time();contexts.update(cohort)
                    for k,v in [('cohort',cohort),('seen',seen),('cohort_at',created),('contexts',contexts)]:save(db,k,v)
                except Exception as exc:errors.append(type(exc).__name__)
            state=portfolio_state(engines)
            if state['realized_cents']<=-1000:save(db,'loss_stop',True)
            paused=(output/'PAUSE').exists() or load(db,'loss_stop',False) or time.time()>=stop-1200
            active=[];occupied_events=set()
            for i,s in enumerate(state['states']):
                p=s['position'] or s['pending']
                if p:
                    active.append((i,p['ticker']))
                    occupied_events.add(contexts[p['ticker']]['market']['event_ticker'])
            jobs=[]
            # One held lane each cycle plus one new observation keeps pending
            # fill rechecks bounded while discovery cannot starve exits.
            if active:
                jobs.append(min(active,key=lambda item:(
                    state['states'][item[0]]['pending'] is None,
                    state['states'][item[0]]['last_at'] or 0)))
            idle=[i for i,s in enumerate(state['states']) if not s['position'] and not s['pending']]
            if idle and cohort and not paused and time.time()-created<=1800:
                candidates=sorted(cohort)
                ticker=candidates[scan%len(candidates)];scan+=1;save(db,'scan',scan)
                if cohort[ticker]['market']['event_ticker'] not in occupied_events:
                    jobs.append((idle[0],ticker))
            for lane,ticker in jobs:
                try:
                    coordinator=SettlementCoordinator(engines[lane],contexts,stop)
                    result=coordinator.poll(client,ticker,paused=paused)
                except Exception as exc:
                    result=dict(action='observation_rejected',error_type=type(exc).__name__,execution_enabled=False)
                db.execute('INSERT INTO portfolio_actions VALUES(?,?,?,?)',(time.time(),lane,ticker,json.dumps(result)))
                # Persist the global latch immediately after each lane action.
                if portfolio_state(engines)['realized_cents']<=-1000:
                    save(db,'loss_stop',True);paused=True
            status=dict(at=datetime.now(timezone.utc).isoformat(),phase='running',execution_enabled=False,
                        allocation_cents=[12500]*4,cohort_size=len(cohort),markets_visited=len(seen),
                        universe_policy='12-market category rotation every 30 minutes',
                        loss_stop=load(db,'loss_stop',False),errors=errors,stop_at=config['stop_at'],
                        **portfolio_state(engines))
            temp=output/'status.tmp';temp.write_text(json.dumps(status,indent=2));temp.replace(output/'status.json')
            cycle+=1
            if cycles is None or cycle<cycles:time.sleep(2)
        if time.time()>=stop:
            status=dict(at=datetime.now(timezone.utc).isoformat(),phase='stopped',execution_enabled=False,
                        **portfolio_state(engines))
            (output/'status.json').write_text(json.dumps(status,indent=2))
    finally:
        for engine in engines:engine.close()
        db.close();lock.close()


def main():
    import os
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',default='configs/kalshi_hosted_paper_20260914/manifest.json')
    parser.add_argument('--data-root',default='/var/data/kalshi-paper')
    parser.add_argument('--cycles',type=int)
    args=parser.parse_args()
    os.chdir(Path(__file__).resolve().parents[1])
    config=json.loads(Path(args.manifest).read_text())
    root=Path(args.data_root).resolve();root.mkdir(parents=True,exist_ok=True)
    config['output']=str(root/'portfolio');config['universe_source']=str(root/'discovery/markets.json')
    path=root/'runtime_manifest.json'
    if path.exists() and json.loads(path.read_text())!=config:raise ValueError('hosted_protocol_changed')
    path.write_text(json.dumps(config,indent=2))
    run(path,cycles=args.cycles)


if __name__=='__main__':main()
