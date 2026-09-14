"""Bounded prospective public-data experiment. GET only; no order routes."""
import argparse
from collections import Counter, defaultdict, deque
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .college_football_paper import lock_process, quotes, timestamp, utcnow, write_json
from .kalshi_market_data import BASE_URL
from .sports_capture_v3 import open_store, persist
from .sports_market_lab_v12 import advance, opposite, suitable
from .sports_research_execution import cost

VERSION = 'broad_paper_1.1'
OUTPUT = Path('sports_paper/broad_v11_20260914')
MANIFEST = Path('configs/kalshi_broad_v11_20260914/manifest.json')


def eligible(m, now, end):
    try:
        return (m.get('market_type') == 'binary' and m.get('status') == 'active'
                and bool(m.get('rules_primary'))
                and now + timedelta(minutes=20) < timestamp(m['close_time']) <= end)
    except (KeyError, ValueError, TypeError):
        return False


def identity(m):
    return tuple(m.get(k) for k in ('ticker', 'event_ticker', 'rules_primary', 'rules_secondary'))


def reversal(history, q, coefficient, now):
    """Pre-registered hypothesis: a >=4c fall may revert to an observed 5m midpoint."""
    if not suitable(q, coefficient):
        return False
    old = [v for v in history if 240 <= now-v[0] <= 420]
    if not old:
        return False
    reference = old[-1][1]
    return (reference-(q['ask']+q['bid'])/2 >= .04-1e-9
            and cost(reference, 1, coefficient, False)['total']
            - cost(q['ask'], 1, coefficient, True)['total'] >= 2)


def settlement(p, market, expected):
    if identity(market) != identity(expected) or market.get('status') != 'settled':
        return p
    if market.get('result') not in ('yes', 'no') or p['state'] not in ('open', 'exit_staged'):
        return p
    payout = 100 if market['result'] == p['side'] else 0
    return dict(p, state='closed', exit_at=utcnow().isoformat(), exit_price=payout/100,
                exit_reason='verified_exchange_settlement', exit_cost={'total':payout,'fee':0,'slippage':0},
                net_cents=payout-p['entry_cost']['total'])


class Client:
    def __init__(self, db):
        self.db, self.next_at, self.backoff = db, 0, 0

    def get(self, path, params=None):
        time.sleep(max(0, self.next_at-time.monotonic()))
        url = BASE_URL+path+('?' + urlencode(params) if params else '')
        start = time.monotonic()
        r = dict(url=url, started_at=utcnow().isoformat(), headers={}, raw=b'', status=None, error=None)
        try:
            with urlopen(Request(url, headers={'Accept':'application/json'}), timeout=8) as response:
                r.update(raw=response.read(), headers=dict(response.headers), status=response.status)
        except Exception as exc:
            r['error'] = str(exc)[:250]
            if isinstance(exc, HTTPError):
                r['status'] = exc.code
        r.update(received_at=utcnow().isoformat(), duration=time.monotonic()-start)
        with self.db:
            persist(self.db, r['received_at'], path, 'public_get', r)
        rid = self.db.execute('select max(id) from responses').fetchone()[0]
        self.backoff = min(120, max(30, self.backoff*2)) if r['status']==429 else 0
        self.next_at = time.monotonic()+max(2, self.backoff)
        if r['error'] or r['duration']>8 or float(r['headers'].get('Age',0))>20:
            raise ValueError(r['error'] or 'stale_transport')
        return json.loads(r['raw']), rid


def run(once=False):
    config=json.loads(MANIFEST.read_text());end=timestamp(config['stop_at'])
    if config['execution_enabled'] is not False or config['version']!=VERSION:
        raise ValueError('Paper-only manifest required')
    for path,digest in config['source_sha256'].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=digest:
            raise ValueError('Frozen source mismatch: '+path)
    OUTPUT.mkdir(parents=True,exist_ok=True);lock=lock_process(OUTPUT/'worker.lock')
    db=open_store(OUTPUT/'paper.sqlite3',config);db.execute('pragma journal_mode=WAL')
    db.executescript('create table if not exists trials(id text primary key,state text); create table if not exists observations(at text,ticker text,detail text);')
    client=Client(db);cache={};history=defaultdict(list);cursor='';catalog={};rotation=0;cycle=0
    try:
        while utcnow()<end:
            started=time.monotonic();errors={};reasons=Counter();now=utcnow()
            if (OUTPUT/'PAUSE').exists():
                time.sleep(10)
                if once:break
                continue
            try:
                if not catalog or cycle%120==0:
                    data,_=client.get('/series');catalog={s['ticker']:s for s in data['series']}
                    write_json(OUTPUT/'catalog.json',catalog)
                # Incremental complete event pagination plus dedicated MLB/NFL discovery.
                data,_=client.get('/events',{'status':'open','with_nested_markets':'true','limit':200,'cursor':cursor})
                cursor=data.get('cursor','')
                events=data.get('events',[])
                category_series=defaultdict(list)
                for entry in catalog.values():
                    if entry.get('category')!='Sports':category_series[entry.get('category','Unknown')].append(entry)
                cats=sorted(category_series)
                extra=[]
                for i in range(min(4,len(cats))):
                    cat=cats[(cycle*4+i)%len(cats)]
                    items=sorted(category_series[cat],key=lambda z:(z.get('frequency') not in ('daily','hourly','weekly'),z['ticker']))
                    extra.append(items[(cycle*4//max(1,len(cats)))%len(items)]['ticker'])
                for series in ['KXMLBGAME','KXNFLGAME']+extra:
                    data,_=client.get('/markets',{'status':'open','series_ticker':series,'limit':1000})
                    events.append({'series_ticker':series,'markets':data.get('markets',[])})
                for e in events:
                    series=catalog.get(e.get('series_ticker'),{})
                    for m in e.get('markets',[]):
                        if eligible(m,now,end) and series.get('fee_type') in ('quadratic','quadratic_with_maker_fees'):
                            coefficient=.07*float(series.get('fee_multiplier',1))
                            if not 0<=coefficient<=.14:continue
                            old=cache.get(m['ticker'])
                            if old and identity(old['market'])!=identity(m):
                                old['changed']=True;continue
                            cache[m['ticker']]={'market':m,'series':series,'coefficient':coefficient}
                write_json(OUTPUT/'markets.json',cache)
            except Exception as exc:errors['discovery']=str(exc)
            trials={k:json.loads(s) for k,s in db.execute('select id,state from trials')}
            pending={p['ticker'] for p in trials.values() if p['state'] in ('staged','open','exit_staged')}
            # Recover original identities for unresolved trials after a process restart.
            for p in trials.values():
                if p['ticker'] in pending:cache.setdefault(p['ticker'],p['context'])
            groups=defaultdict(deque)
            for t,c in sorted(cache.items(),key=lambda kv:-float(kv[1]['market'].get('volume_24h_fp') or kv[1]['market'].get('volume_24h') or 0)):
                if t not in pending and eligible(c['market'],now,end):groups[c['series'].get('category','Unknown')].append(t)
            categories=sorted(groups);categories=categories[rotation%max(1,len(categories)):]+categories[:rotation%max(1,len(categories))]
            new=[]
            while categories and len(new)<12:
                for cat in list(categories):
                    new.append(groups[cat].popleft())
                    if not groups[cat]:categories.remove(cat)
                    if len(new)>=12:break
            rotation+=1
            for t in sorted(pending)+new:
                c=cache[t]
                try:
                    data,mid=client.get('/markets/'+t);m=data['market']
                    if identity(m)!=identity(c['market']) or c.get('changed'):
                        reasons['identity_changed']+=1;continue
                    local=[(k,p) for k,p in trials.items() if p['ticker']==t]
                    for k,p in local:
                        trials[k]=settlement(p,m,c['market'])
                    with db:
                        for k,_ in local:db.execute('insert or replace into trials values(?,?)',(k,json.dumps(trials[k])))
                    if m.get('status')!='active':
                        reasons['market_not_active']+=1
                        continue
                    data,rid=client.get('/markets/'+t+'/orderbook',{'depth':20});q=quotes(data);now=utcnow()
                    local=[(k,trials[k]) for k,_ in local]
                    for k,p in local:
                        if p['state'] not in ('staged','open','exit_staged'):continue
                        nq=q if p['side']=='yes' or q is None else opposite(q)
                        updated=settlement(p,m,c['market'])
                        if updated['state']!='closed':updated=advance(p,nq,now,rid,p['coefficient'])
                        if updated['state']=='closed':updated.setdefault('exit_reason','holding_limit')
                        trials[k]=updated
                    if q:
                        history[t].append((now.timestamp(),(q['ask']+q['bid'])/2))
                        history[t]=[v for v in history[t] if now.timestamp()-v[0]<=600]
                    for side in ('yes','no'):
                        nq=q if side=='yes' or q is None else opposite(q)
                        for arm in ('scheduled_control','reversal'):
                            reason='eligible'
                            if not eligible(m,now,end):reason='closed_or_near_close'
                            elif not suitable(nq,c['coefficient']):reason='unavailable_or_costly_book'
                            elif arm=='reversal' and 'NCAAF' in c['series'].get('ticker',''):reason='existing_ncaaf_challenger'
                            elif arm=='reversal' and not reversal(history[t] if side=='yes' else [(at,1-v) for at,v in history[t]],nq,c['coefficient'],now.timestamp()):reason='no_reversal_signal'
                            elif sum(p['state'] in ('staged','open','exit_staged') and p['arm']==arm for p in trials.values())>=6:reason='six_position_arm_cap'
                            elif sum(p.get('net_cents',0) for p in trials.values() if p['arm']==arm)<=-1000:reason='ten_dollar_arm_loss_stop'
                            elif any(p['event']==m['event_ticker'] and p['arm']==arm and p['state'] in ('staged','open','exit_staged') for p in trials.values()):reason='event_already_exposed'
                            reasons[arm+':'+reason]+=1
                            if reason!='eligible':continue
                            slot=int(now.timestamp())//1800
                            for h in (300,900):
                                if sum(p['state'] in ('staged','open','exit_staged') and p['arm']==arm for p in trials.values())>=6:break
                                k=f'{t}|{side}|{arm}|{h}|{slot}'
                                if k not in trials:
                                    trials[k]=dict(state='staged',ticker=t,side=side,arm=arm,horizon=h,event=m['event_ticker'],category=c['series'].get('category'),stage_at=now.isoformat(),ask=nq['ask'],book_id=rid,coefficient=c['coefficient'],context=c)
                    with db:db.execute('insert into observations values(?,?,?)',(now.isoformat(),t,json.dumps({'quote':q,'book_id':rid,'market_id':mid,'reasons':dict(reasons)})))
                except Exception as exc:errors[t]=str(exc)[:200]
                with db:
                    for k,p in trials.items():db.execute('insert or replace into trials values(?,?)',(k,json.dumps(p)))
            summary=defaultdict(Counter)
            for p in trials.values():
                row=summary[f"{p['category']}|{p['arm']}|{p['horizon']}"];row[p['state']]+=1;row['net_cents']+=p.get('net_cents',0)
            status={'at':utcnow().isoformat(),'version':VERSION,'execution_enabled':False,'catalog_series':len(catalog),'eligible_markets_seen':len(cache),'pagination_more':bool(cursor),'sampled':len(pending)+len(new),'reasons':dict(reasons),'errors':errors,'groups':dict(summary),'stop_at':config['stop_at']}
            write_json(OUTPUT/'status.json',status);write_json(OUTPUT/'report.json',status)
            cycle+=1
            if once:break
            time.sleep(max(1,60-(time.monotonic()-started)))
    finally:db.close();lock.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--once',action='store_true');run(parser.parse_args().once)
