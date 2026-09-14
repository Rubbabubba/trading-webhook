"""Read-only daily accounting; writes only September 13 report artifacts."""
import json
import sqlite3
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

OUT = Path('sports_paper/daily_email/2026-09-13')
OUT.mkdir(parents=True, exist_ok=True)
START = datetime(2026, 9, 13, tzinfo=ZoneInfo('America/Chicago')).astimezone(timezone.utc)
END = START + timedelta(days=1)
A, B = START.isoformat(), END.isoformat()

def connect(path):
    c = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=20)
    c.execute('begin')
    return c

def blank():
    return dict(entries=0, exit_events=0, partial_exits=0, net_cents=0, fees_cents=0,
                slippage_cents=0, open_start=0, open_end=0, pending_end=0, sampled_games=0,
                traded_games=0, scheduled_games=0, scheduled_zero_entry_games=[])

def actions_into(out, details):
    for x in details:
        action = x.get('action')
        if action == 'buy': out['entries'] += 1
        if action in ('sell', 'settle', 'partial_sell'):
            out['exit_events'] += 1
            out['partial_exits'] += action == 'partial_sell'
            out['net_cents'] += x.get('profit_cents', 0)

def research(folder):
    c = connect(Path('sports_paper') / folder / 'paper.sqlite3')
    result, games = defaultdict(blank), []
    configs = {slug:json.loads(config) for slug,config in c.execute('select slug,config from games')}
    states = {}
    per = defaultdict(blank)
    # One sequential action scan avoids repeated scans of the unindexed original ledger.
    for slug,at,h,detail,account in c.execute('select s.slug,s.at,a.horizon,a.detail,a.account from actions a join samples s on s.id=a.sample_id where s.at<? order by a.rowid',(B,)):
        key=(slug,h)
        old,new=states.get(key, ({},{}))
        current=json.loads(account)
        if at<A:old=current
        else:
            per[key]['sampled_games']=1
            if any(token in detail for token in ['"buy"','"sell"','"partial_sell"','"settle"']):actions_into(per[key],json.loads(detail))
        states[key]=(old,current)
    c.close()
    for (slug,h),(old,new) in states.items():
        cfg=configs[slug];r=per[(slug,h)]
        for k in ['fees_cents','slippage_cents']:r[k]=new.get(k,0)-old.get(k,0)
        r['open_start']=int(bool(old.get('position')));r['open_end']=int(bool(new.get('position')))
        r['pending_end']=int(bool(new.get('pending')))
        assert r['net_cents']==new.get('realized_cents',0)-old.get('realized_cents',0),(folder,slug,h,r)
        assert r['entries']==new.get('entries',0)-old.get('entries',0),(folder,slug,h)
        r['traded_games']=int(r['entries']>0)
        scheduled=A<=cfg.get('kickoff','')<B
        r['scheduled_games']=int(scheduled)
        if scheduled and not r['entries']:r['scheduled_zero_entry_games']=[slug]
        agg=result[f"{cfg['league']}|{h}"]
        for k,v in r.items():
            if isinstance(v,list):agg[k].extend(v)
            else:agg[k]+=v
        games.append(dict(slug=slug,game=cfg['game'],league=cfg['league'],horizon=h,**r))
    return dict(summary=dict(result),games=games)

def timestamp(value):
    if not value:return None
    return datetime.fromtimestamp(value, timezone.utc) if isinstance(value,(float,int)) else datetime.fromisoformat(value)

def probes(folder,db):
    c=connect(Path('sports_paper')/folder/db)
    result=defaultdict(lambda:dict(entries=0,exits=0,net_cents=0,costs_cents=0,open_start=0,open_end=0,canceled_or_rejected=0))
    for text, in c.execute('select state from probes'):
        x=json.loads(text); en,ex=timestamp(x.get('entry_at')),timestamp(x.get('exit_at'))
        stage=timestamp(x.get('stage_at'))
        r=result[f"{x.get('league','unknown')}|{x['horizon']}"]
        if en:
            r['open_start']+=int(en<START and (not ex or ex>=START))
            r['open_end']+=int(en<END and (not ex or ex>=END))
            if START<=en<END:
                r['entries']+=1
                cost=x.get('entry_cost',{});r['costs_cents']+=cost.get('fee',0)+cost.get('slippage',0)
        if ex and START<=ex<END:
            r['exits']+=1;r['net_cents']+=x.get('net_cents',0)
            cost=x.get('exit_cost',{});r['costs_cents']+=cost.get('fee',0)+cost.get('slippage',0)
        if stage and START<=stage<END and not en and x.get('state') in ['entry_rejected','canceled','cancelled','entry_canceled']:
            r['canceled_or_rejected']+=1
    c.close();return dict(result)

def churn():
    c=connect('sports_paper/churn_v11_20260913/paper.sqlite3'); result=defaultdict(blank)
    for game,arm,h in c.execute('select game,arm,horizon from accounts').fetchall():
        r=result[f'ncaaf|{arm}|{h}']; states=[]
        for cutoff in (A,B):
            row=c.execute('select account from actions where game=? and arm=? and horizon=? and at<? order by at desc limit 1',(game,arm,h,cutoff)).fetchone()
            states.append(json.loads(row[0]) if row else {})
        old,new=states
        for detail, in c.execute('select detail from actions where game=? and arm=? and horizon=? and at>=? and at<?',(game,arm,h,A,B)):
            actions_into(r,json.loads(detail))
        for k in ['fees_cents','slippage_cents']:r[k]+=new.get(k,0)-old.get(k,0)
        r['open_start']+=bool(old.get('position'));r['open_end']+=bool(new.get('position'))
        r['pending_end']+=bool(new.get('pending'));r['sampled_games']+=bool(new)
    c.close();return dict(result)

data={'report_date':'2026-09-13','start':A,'end_exclusive':B,'generated_at':datetime.now(timezone.utc).isoformat(),'strategy':{},'probes':{},'notes':['Each database read in a consistent read-only transaction. Exact cutoff snapshots and realized action sums cross-checked against account changes.','Live3.4 is the sole authoritative continuation of3.1-3.3; churn1.1 includes1.0. Never sum copies.','Sampled games include pregame observations; this is not a count of adequately covered games.']}
for folder in ['research_20260910','research_v11_20260912','live_v34_20260913']:
    data['strategy'][folder]=research(folder)
    print('accounted',folder,flush=True)
for folder,db in [('market_lab_20260910','laboratory.sqlite3'),('market_lab_v11_20260910','laboratory.sqlite3'),('market_lab_v12_20260912','laboratory.sqlite3'),('live_v34_20260913','paper.sqlite3')]:
    data['probes'][folder]=probes(folder,db)
data['churn']=churn()
(OUT/'source_notes.json').write_text(json.dumps(data,indent=2),encoding='utf-8')
print('saved',OUT/'source_notes.json',flush=True)
