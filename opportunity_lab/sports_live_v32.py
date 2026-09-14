"""Prospective paper-only core-market service. Streaming preferred, explicit REST fallback."""
import argparse
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import queue
import sqlite3
import threading
import time

from .college_football_paper import lock_process, timestamp, utcnow, write_json
from .kalshi_market_data import BASE_URL
from .sports_capture_v3 import open_store, persist, request
from .sports_research_models_v11 import estimate, fit_prior, identity
from .sports_research_mapping import schedule_events
from .sports_paper_research_v11 import soccer_sources_agree
from .sports_paper_v2 import parse_game
from .sports_live_books_v32 import Book, credentials, stream_loop
from .sports_live_coverage import core_markets, polling_batch, admission
from .sports_live_sync_v31 import audit
from .sports_live_execution import initial, decide
from .sports_live_timing import Latency, schema, signal, update_markouts, expire_markouts
from .sports_market_lab_v12 import suitable, opposite

VERSION = 'sports_live_3.2'
ROOT = Path(__file__).resolve().parents[1]
SPORTS = {'ncaaf':'football/college-football','nfl':'football/nfl','epl':'soccer/eng.1',
          'mls':'soccer/usa.1','atp':'tennis/atp','wta':'tennis/wta','mlb':'baseball/mlb'}


def open_db(output, config):
    if config['version'] != VERSION or config.get('execution_enabled') is not False:
        raise ValueError('paper_only_required')
    if not config.get('source_sha256'):
        raise ValueError('source_fingerprints_required')
    for name,digest in config['source_sha256'].items():
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest() != digest:
            raise ValueError('frozen_source_changed: '+name)
    db = open_store(output/'paper.sqlite3',config)
    db.executescript('''
      CREATE TABLE IF NOT EXISTS games(slug TEXT PRIMARY KEY,config TEXT,anchor TEXT,memory TEXT,state TEXT);
      CREATE TABLE IF NOT EXISTS accounts(slug TEXT,horizon INTEGER,state TEXT,PRIMARY KEY(slug,horizon));
      CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY,slug TEXT,at TEXT,snapshot TEXT,observation TEXT);
      CREATE INDEX IF NOT EXISTS sample_game ON samples(slug,id);
      CREATE TABLE IF NOT EXISTS actions(sample_id INTEGER,horizon INTEGER,detail TEXT,account TEXT);
      CREATE INDEX IF NOT EXISTS action_sample ON actions(sample_id);
      CREATE TABLE IF NOT EXISTS market_rules(ticker TEXT PRIMARY KEY,frozen TEXT);
      CREATE TABLE IF NOT EXISTS probes(ticker TEXT,side TEXT,horizon INTEGER,slot INTEGER,state TEXT,
        PRIMARY KEY(ticker,side,horizon,slot));
    ''')
    schema(db)
    with db:
        for c in config['games']:
            slug=c['league']+'_'+c['event_id']
            db.execute('INSERT OR IGNORE INTO games VALUES(?,?,NULL,?,?)',(slug,json.dumps(c),'{}','waiting'))
            for h in (300,900):
                db.execute('INSERT OR IGNORE INTO accounts VALUES(?,?,?)',(slug,h,json.dumps(initial())))
    return db


def record_ok(record, now, max_age=15):
    if not record:
        return False
    r=record['response']
    try:
        age=float({k.lower():v for k,v in r['headers'].items()}.get('age',0))
        return (not r['error'] and r['status']==200 and r['duration'] <= 8 and 0 <= age <= 20
                and 0 <= (now-timestamp(r['received_at'])).total_seconds() <= max_age)
    except (ValueError,TypeError):
        return False


def market_view(ticker, cache, books, now):
    c=cache.get(ticker,{}); b=books.get(ticker,{})
    q=b.get('quote'); valid=False
    try:
        valid=(q is not None and not c.get('mapping_changed') and c['coefficient'] is not None
               and c['market']['status']=='active' and 0 <= (now-timestamp(c['metadata_at'])).total_seconds()<=600
               and 0 <= now.timestamp()-b['at'] <= 5 and b.get('valid',False))
    except (KeyError,TypeError,ValueError):
        pass
    return {'ticker':ticker,'quote':q,'valid':valid,'fee_coefficient':c.get('coefficient'),
            'book_id':b.get('id'),'book_received_at':datetime.fromtimestamp(b.get('at',0),timezone.utc).isoformat(),
            'transport':b.get('transport'),'settlement_ok':False,'result':''}


def observation(c, cache, books, feeds, anchor, memory, now):
    slug=c['league']+'_'+c['event_id']; league=c['league']
    summary_record=feeds.get(slug); summary=(summary_record or {}).get('data',{})
    competition=(summary.get('header',{}).get('competitions') or [{}])[0]
    blockers=[]; sources=[summary_record] if league not in ('atp','wta') else []
    if league in ('epl','mls','atp','wta'):
        board=feeds.get(league);sources.append(board)
        events=schedule_events((board or {}).get('data',{}),league)
        event=next((e for e in events if str(e.get('id'))==c['event_id']),{})
        competition=(event.get('competitions') or [{}])[0]
        if league in ('epl','mls'):
            try:
                if not soccer_sources_agree(competition,(summary.get('header',{}).get('competitions') or [{}])[0],c):
                    blockers.append('soccer_sources_disagree')
            except (ValueError,TypeError,KeyError):
                blockers.append('soccer_summary_missing_scores')
    if any(not record_ok(r,now) for r in sources):
        blockers.append('live_game_transport_over_15_seconds')
    markets={side:market_view(t,cache,books,now) for side,t in c['markets'].items()}
    for m in markets.values():
        m.update(max_spread=c.get('max_spread',.04),max_entry_loss_fraction=c.get('max_entry_loss_fraction',.15))
    evidence=c.get('mapping_evidence',{}).get('markets',c.get('mapping_evidence',{}))
    for side,m in markets.items():
        meta=cache.get(m['ticker'],{}).get('market',{})
        expected=evidence.get(side,{})
        if (meta.get('event_ticker')!=c['market_event'] or meta.get('market_type')!='binary'
            or not expected or any(meta.get(k,'')!=expected.get(k,'') for k in ('rules_primary','rules_secondary'))):
            m['valid']=False
    phase=competition.get('status',{}).get('type',{})
    before=(timestamp(c['kickoff'])-now).total_seconds()
    if (anchor is None and league not in ('ncaaf','nfl') and 0<before<=7200
        and phase.get('state')=='pre' and identity(competition,c) and not blockers
        and all(m['valid'] for m in markets.values())):
        mids={s:(m['quote']['bid']+m['quote']['ask'])/2 for s,m in markets.items()}
        total=sum(mids.values())
        if .9<=total<=1.1 and all(.02<p<.98 for p in mids.values()) and all(m['quote']['ask']-m['quote']['bid']<=.040000001 for m in markets.values()):
            try:
                anchor={**fit_prior(league,{s:p/total for s,p in mids.items()}),'observed_at':now.isoformat(),
                        'book_ids':{s:m['book_id'] for s,m in markets.items()}}
            except ValueError as exc:
                blockers.append(str(exc))
    try:
        if league=='nfl':
            p=parse_game(summary,{**c,'max_signal_age_seconds':90},now)
            model={'probabilities':{} if p['home_probability'] is None else {'home':p['home_probability'],'away':1-p['home_probability']},
                   'completed':p['completed'],'state':p['state'],'phase':p['phase'],'blockers':p['blockers'],
                   'signature':str(p['play_id']),'details':p}
        else:
            model=estimate(c,summary,competition,{},anchor,now)
    except (ValueError,KeyError,TypeError) as exc:
        model={'probabilities':{},'completed':False,'state':phase.get('state'),'phase':phase.get('name'),
               'blockers':[str(exc)],'signature':None}
    model['blockers'].extend(blockers)
    sync={}
    if league in ('ncaaf','nfl'):
        sync=audit(summary,model,(summary_record or {}).get('response',{}).get('received_at'),now,memory)
    signature=model.get('signature')
    if signature and signature!=memory.get('signature'):
        memory.update(signature=signature,changed_at=now.isoformat())
    if signature and league in ('atp','wta','epl','mls') and (now-timestamp(memory['changed_at'])).total_seconds()>180:
        model['blockers'].append('game_state_unchanged_over_180_seconds')
    model['blockers']=list(dict.fromkeys(model['blockers']))
    return {'at':now.isoformat(),'snapshot':str(time.time_ns()),'model':model,'markets':markets,'sync':sync,
            'source_ids':[r['id'] for r in sources if r]},anchor,memory


def probe_step(p, m, now, latency):
    """One-contract probe, observed arrival, identical cost/price guards to prior labs."""
    from .sports_research_execution import cost
    q=m['quote'] if m['valid'] else None
    at=now.timestamp()
    if p['state']=='staged' and at-p['stage_at']>30:
        p.update(state='entry_expired');return p
    if p['state']=='open' and at-p['entry_at']>=p['horizon']:
        if q and q['bid_size']>=1:
            p.update(state='exit_staged',stage_at=at,book_id=m['book_id'],bid=q['bid'],latency=latency)
        return p
    if p['state'] not in ('staged','exit_staged'):
        return p
    if p['state']=='exit_staged' and at-p['stage_at']>30:
        p['state']='open';return p
    if not q or m['book_id']==p['book_id'] or timestamp(m['book_received_at']).timestamp()<p['stage_at']+p['latency']:
        return p
    if p['state']=='staged':
        if not suitable(q,m['fee_coefficient']) or q['ask']>p['ask']+.020000001:
            p['state']='entry_rejected'
        else:
            p.update(state='open',entry_at=at,entry_price=max(p['ask'],q['ask']),
                     entry_cost=cost(max(p['ask'],q['ask']),1,m['fee_coefficient'],True))
    elif q['bid_size']>=1:
        price=min(p['bid'],q['bid']); proceeds=cost(price,1,m['fee_coefficient'],False)
        p.update(state='closed',exit_at=at,exit_price=price,exit_cost=proceeds,
                 net_cents=proceeds['total']-p['entry_cost']['total'],exit_delay_seconds=at-p['entry_at']-p['horizon'])
    return p


def run(manifest, output, once=False):
    config=json.loads(Path(manifest).read_text());output=Path(output);output.mkdir(exist_ok=True,parents=True)
    lock=lock_process(output/'paper.lock');db=open_db(output,config)
    inbox=queue.Queue(maxsize=4096);stop=threading.Event();overflow=threading.Event();selected=set()
    def publish(item):
        try:inbox.put_nowait(item)
        except queue.Full:overflow.set()
    thread=threading.Thread(target=lambda:asyncio.run(stream_loop(lambda:set(selected),publish,stop)),daemon=True)
    thread.start();pool=ThreadPoolExecutor(max_workers=12)
    def reports():
        from .sports_live_report_v32 import report
        while not stop.wait(60):
            try:report(output)
            except Exception as exc:write_json(output/'report_error.json',{'at':utcnow().isoformat(),'error':type(exc).__name__})
    threading.Thread(target=reports,daemon=True).start()
    jobs={};cache={};books={};feeds={};last={};last_feed={};rtt=Latency()
    stream_state='starting';next_meta=next_tick=next_status=next_book=0;backoff_until=0;errors={};service_gap={};book_queue=[]
    try:
        while utcnow()<timestamp(config['stop_at']):
            now=utcnow();epoch=now.timestamp();mono=time.monotonic()
            paused=(output/'PAUSE').exists()
            if overflow.is_set():
                for b in books.values():b['valid']=False
                errors['stream']='queue_overflow';overflow.clear()
            updates={};stream_messages=[]
            for _ in range(min(256,inbox.qsize())):
                item=inbox.get_nowait()
                if item['kind']=='stream_state':
                    stream_state=item['state']
                    if stream_state not in ('connected','credentials_missing','no_targets'):
                        updates.clear()
                        for book in books.values():
                            if book.get('transport')=='websocket':book['valid']=False
                else:
                    stream_messages.append(item['message'])
                    updates[item['ticker']]=item
            if stream_messages:
                # Preserve normalized events and full resulting views; coalesce decision work only.
                raw=json.dumps({'messages':stream_messages,'views':{t:i['quote'] for t,i in updates.items()}}).encode()
                response={'raw':raw,'received_at':utcnow().isoformat(),'status':200,'error':None,
                          'headers':{},'duration':0,'url':'kalshi_websocket_normalized_batch'}
                persist(db,now.isoformat(),'stream_batch','stream',response)
                rid=db.execute('select last_insert_rowid()').fetchone()[0]
                for ticker,item in updates.items():
                    previous=books.get(ticker);service_gap[ticker]=item['at']-previous['at'] if previous else 999
                    books[ticker]={'at':item['at'],'quote':item['quote'],'valid':True,'id':rid,'transport':'websocket'}
                    update_markouts(db,ticker,item['at'],market_view(ticker,cache,books,utcnow()))
            for future in list(jobs):
                if not future.done():continue
                kind,key=jobs.pop(future);response=future.result()
                persist(db,now.isoformat(),key,kind,response);rid=db.execute('select last_insert_rowid()').fetchone()[0]
                if '429' in str(response.get('error')):
                    backoff_until=mono+30;errors['rate_limit']='30_second_backoff'
                try:
                    record={'response':response,'id':rid,'data':json.loads(response['raw'])}
                    if not record_ok(record,utcnow()):raise ValueError('transport_gate')
                    if kind=='book':
                        rtt.add(response['duration'])
                        payload=record['data']['orderbook_fp'];book=Book()
                        received=timestamp(response['received_at']).timestamp()
                        book.snapshot(payload['yes_dollars'],payload['no_dollars'],received,rid)
                        previous=books.get(key);service_gap[key]=received-previous['at'] if previous else 999
                        books[key]={'at':received,'quote':book.quote(),'valid':True,'id':rid,'transport':'rest'}
                        update_markouts(db,key,received,market_view(key,cache,books,utcnow()))
                    elif kind=='settlement':
                        feeds['settlement:'+key]=record
                    else:feeds[key]=record
                    errors.pop(kind+':'+key,None)
                except (ValueError,KeyError,TypeError) as exc:
                    errors[kind+':'+key]=str(exc)[:100]
                    if kind=='book' and key in books:books[key]['valid']=False
            states={};held=set();pending_count=0
            for slug,h,raw in db.execute('select * from accounts'):
                s=json.loads(raw);states[slug,h]=s
                for p in (s['position'],s['pending']):
                    if p:
                        c=next(c for c in config['games'] if c['league']+'_'+c['event_id']==slug)
                        held.add(c['markets'][p['side']]);pending_count+=1
            probes=[(t,side,h,slot,json.loads(raw)) for t,side,h,slot,raw in db.execute('select * from probes')]
            for t,side,h,slot,p in probes:
                if p['state'] in ('staged','open','exit_staged'):held.add(t)
            pending_count=len(held)
            games={slug:(json.loads(c),json.loads(a) if a else None,json.loads(m),state)
                   for slug,c,a,m,state in db.execute('select * from games')}
            eligible={s for s,(c,a,m,state) in games.items() if timestamp(c['kickoff'])-timedelta(hours=2)<=now<timestamp(c['stop_at'])
                      and state!='completed' and not (output/(s+'.PAUSE')).exists()}
            if mono>=next_meta:
                for path in config['metadata_paths']:
                    try:
                        data=json.loads((ROOT/path).read_text())
                        for t,c in data.items():
                            frozen=json.dumps({k:c['market'].get(k) for k in ('ticker','event_ticker','market_type','rules_primary','rules_secondary')},sort_keys=True)
                            old=db.execute('select frozen from market_rules where ticker=?',(t,)).fetchone()
                            if old and old[0]!=frozen:c['mapping_changed']=True
                            db.execute('insert or ignore into market_rules values(?,?)',(t,frozen))
                            cache[t]=c
                    except (OSError,ValueError) as exc:errors['metadata']=type(exc).__name__
                next_meta=mono+30
            selected,missing=core_markets(cache,eligible,held)
            inflight=set(jobs.values())
            if not paused and mono>=backoff_until and len(jobs)<10:
                # REST remains available for periodic integrity checks and RTT measurement.
                if mono>=next_book:
                    targets=[t for t in selected if ('book',t) not in inflight and
                             (stream_state!='connected' or epoch-last.get(t,0)>=30)]
                    if not book_queue:
                        book_queue=polling_batch(targets,held,last,epoch,4)
                    if book_queue:
                        t=book_queue.pop(0)
                        if t in selected and ('book',t) not in inflight:
                            jobs[pool.submit(request,BASE_URL+'/markets/'+t+'/orderbook?depth=20')]=('book',t);last[t]=epoch
                    next_book=mono+.25
                due_feeds=[]
                for s in eligible:
                    c=games[s][0];league=c['league']
                    if league not in ('atp','wta'):
                        due_feeds.append((s,f"https://site.api.espn.com/apis/site/v2/sports/{SPORTS[league]}/summary?event={c['event_id']}"))
                    if league in ('atp','wta','epl','mls'):
                        date=now.strftime('%Y%m%d')
                        due_feeds.append((league,f'https://site.api.espn.com/apis/site/v2/sports/{SPORTS[league]}/scoreboard?dates={date}&limit=1000'))
                for key,url in sorted(set(due_feeds),key=lambda kv:last_feed.get(kv[0],0))[:2]:
                    if epoch-last_feed.get(key,0)>=10 and ('game',key) not in inflight and len(jobs)<10:
                        jobs[pool.submit(request,url)]=('game',key);last_feed[key]=epoch
                for t in held:
                    key='settlement:'+t
                    if epoch-last_feed.get(key,0)>=60 and ('settlement',t) not in inflight and len(jobs)<10:
                        jobs[pool.submit(request,BASE_URL+'/markets/'+t)]=('settlement',t);last_feed[key]=epoch
            if mono>=next_tick:
                statuses=[];latency=rtt.estimate(max(0,time.monotonic()-mono)) or 5.0
                for slug,(c,anchor,memory,state) in games.items():
                    if slug not in eligible and not any(states[slug,h]['position'] for h in (300,900)):
                        statuses.append({'slug':slug,'game':c['game'],'state':state});continue
                    current=utcnow()
                    try:
                        obs,anchor,memory=observation(c,cache,books,feeds,anchor,memory,current)
                        obs['latency_seconds']=latency
                        views=list(obs['markets'].values())
                        for v in views:
                            v['entry_service_ok']=admission(current.timestamp()-books.get(v['ticker'],{}).get('at',0),service_gap.get(v['ticker'],999),pending_count,config['max_pending'])
                        obs['admission_ok']=(rtt.estimate() is not None and latency<=5 and pending_count<config['max_pending']
                            and any(admission(current.timestamp()-books.get(v['ticker'],{}).get('at',0),service_gap.get(v['ticker'],999),pending_count,config['max_pending']) for v in views))
                        if current>=timestamp(c['stop_at']):obs['model']['blockers'].append('experiment_deadline')
                        for v in views:
                            rec=feeds.get('settlement:'+v['ticker']);meta=cache.get(v['ticker'],{}).get('market',{})
                            sm=(rec or {}).get('data',{}).get('market',{})
                            if record_ok(rec,current,120) and sm.get('status')=='finalized' and sm.get('result') in ('yes','no') and all(sm.get(k)==meta.get(k) for k in ('ticker','event_ticker','rules_primary','rules_secondary')):
                                v.update(settlement_ok=True,result=sm['result'])
                        sample=db.execute('insert into samples values(NULL,?,?,?,?)',
                            (slug,current.isoformat(),obs['snapshot'],json.dumps(obs))).lastrowid
                        for h in (300,900):
                            s=states[slug,h]
                            actions=decide(s,obs,current,h,config['entry_margin'][c['league']],paused or (output/(slug+'.PAUSE')).exists())
                            for position in (s['pending'],s['position']):
                                if position:held.add(c['markets'][position['side']])
                            pending_count=len(held)
                            obs['admission_ok'] &= pending_count<config['max_pending']
                            db.execute('update accounts set state=? where slug=? and horizon=?',(json.dumps(s),slug,h))
                            db.execute('insert into actions values(?,?,?,?)',(sample,h,json.dumps(actions),json.dumps(s)))
                        for side,v in obs['markets'].items():
                            p=obs['model']['probabilities'].get(side)
                            if p is not None and not obs['model']['blockers']:
                                signal(db,slug,v['ticker'],str(obs['model']['signature']),current.timestamp(),v,p,obs['admission_ok'])
                        terminal=obs['model']['completed'] and not any(states[slug,h]['position'] for h in (300,900))
                        state='completed' if terminal else 'running' if current>=timestamp(c['kickoff']) else 'waiting'
                        db.execute('update games set anchor=?,memory=?,state=? where slug=?',(json.dumps(anchor) if anchor else None,json.dumps(memory),state,slug))
                        detail={'valid_model':not obs['model']['blockers'],'valid_markets':sum(v['valid'] for v in views),
                                'blockers':obs['model']['blockers'],'sync':obs['sync'],'admission_ok':obs['admission_ok']}
                        db.execute('insert into coverage values(?,?,?)',(current.timestamp(),slug,json.dumps(detail)))
                        statuses.append({'slug':slug,'game':c['game'],'state':state,**detail,
                            'accounts':{str(h):{k:states[slug,h][k] for k in ('entries','exits','realized_cents')} for h in (300,900)}})
                        errors.pop('game:'+slug,None)
                    except Exception as exc:errors['game:'+slug]=type(exc).__name__+': '+str(exc)[:160]
                for t,side,h,slot,p in probes:
                    if p['state'] not in ('staged','open','exit_staged'):continue
                    m=market_view(t,cache,books,now)
                    if side=='no' and m['quote']:m['quote']=opposite(m['quote'])
                    if not paused and not (output/(p['game']+'.PAUSE')).exists():
                        probe_step(p,m,now,latency)
                        db.execute('update probes set state=? where ticker=? and side=? and horizon=? and slot=?',(json.dumps(p),t,side,h,slot))
                # Bounded independent execution probes: one active ticker per game.
                active_games={p['game'] for *_,p in probes if p['state'] in ('staged','open','exit_staged')}
                active_probe_tickers={t for t,side,h,slot,p in probes if p['state'] in ('staged','open','exit_staged')}
                new_probes=0
                for t in sorted(selected,key=lambda t:last.get(t,0)):
                    c=cache.get(t)
                    if not c:continue
                    slug=c['game']['league']+'_'+c['game']['event_id'];m=market_view(t,cache,books,now)
                    if paused or slug not in eligible or slug in active_games or now<timestamp(c['game']['kickoff']):continue
                    if len(active_probe_tickers)>=config['max_probe_pending']:continue
                    if not admission(epoch-books.get(t,{}).get('at',0),service_gap.get(t,999),pending_count+new_probes,config['max_pending']):continue
                    if rtt.estimate() is None or latency>5 or not m['valid'] or not suitable(m['quote'],m['fee_coefficient']):continue
                    slot=int(epoch)//900
                    for side in ('yes','no'):
                        q=m['quote'] if side=='yes' else opposite(m['quote'])
                        if not suitable(q,m['fee_coefficient']):continue
                        for h in (300,900):
                            if pending_count+new_probes>=config['max_pending']:break
                            p={'state':'staged','stage_at':epoch,'latency':latency,'ask':q['ask'],'book_id':m['book_id'],
                               'horizon':h,'game':slug,'league':c['league'],'family':c['family']}
                            cursor=db.execute('insert or ignore into probes values(?,?,?,?,?)',(t,side,h,slot,json.dumps(p)))
                            if cursor.rowcount and t not in active_probe_tickers:
                                active_probe_tickers.add(t);new_probes+=1
                    active_games.add(slug)
                expire_markouts(db,epoch);db.commit();next_tick=mono+1
                if mono>=next_status:
                    write_json(output/'status.json',{'at':utcnow().isoformat(),'version':VERSION,'execution_enabled':False,
                        'transport':'websocket' if stream_state=='connected' else 'rest_fallback','stream_state':stream_state,
                        'latency_proxy_seconds':rtt.estimate(),'selected_markets':len(selected),'held_markets':len(held),
                        'missing_core_families':missing,'games':statuses,'errors':errors,
                        'profitability_validated':False,'note':'GET RTT is a conservative latency proxy; REST fallback cannot guarantee streaming coverage.'})
                    next_status=mono+5
            if once:break
            time.sleep(.05)
    finally:
        stop.set();pool.shutdown(wait=True,cancel_futures=True);db.commit();db.close();lock.close()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',default='configs/sports_live_v32_20260912/manifest.json')
    p.add_argument('--output',default='sports_paper/live_v32_20260912');p.add_argument('--once',action='store_true')
    a=p.parse_args();run(a.manifest,a.output,a.once)
