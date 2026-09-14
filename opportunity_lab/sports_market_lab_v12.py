"""Prospective fixed-schedule quote probes; no accounts, signals or order APIs."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlencode

from .college_football_paper import lock_process, quotes, timestamp, utcnow, write_json
from .kalshi_market_data import BASE_URL
from .sports_capture_v3 import open_store, persist, request
from .sports_research_execution import cost

VERSION = 'sports_market_lab_1.2'


def game_key(ticker):
    parts = ticker.split('-')
    return parts[1] if len(parts) > 1 else None


def family(series):
    if 'SPREAD' in series:
        return 'spread'
    if 'TEAMTOTAL' in series:
        return 'team_total'
    if 'TOTAL' in series:
        return 'total'
    if series.endswith(('GAME', 'MATCH')):
        return 'winner'
    return 'period_or_player_or_special'


def suitable(q, coefficient):
    if not q or min(q['ask_size'], q['bid_size']) < 1 or q['ask'] - q['bid'] > .040000001:
        return False
    entry = cost(q['ask'], 1, coefficient, True)['total']
    exit_value = cost(q['bid'], 1, coefficient, False)['total']
    return entry > 0 and (entry - exit_value) / entry <= .15


def opposite(q):
    return {'ask': 1-q['bid'], 'bid': 1-q['ask'], 'ask_size': q['bid_size'], 'bid_size': q['ask_size']}


def advance(probe, q, now, book_id, coefficient):
    """Two distinct book observations confirm both entry and exit; no inferred settlement."""
    p = dict(probe)
    elapsed = (now - timestamp(p['stage_at'])).total_seconds()
    if p['state'] == 'staged':
        if elapsed > 180:
            p['state'] = 'entry_expired'
        elif elapsed >= 10 and book_id != p['book_id']:
            if not suitable(q, coefficient) or q['ask'] > p['ask'] + .020000001:
                p['state'] = 'entry_rejected'
            else:
                p.update(state='open', entry_at=now.isoformat(), entry_price=max(p['ask'], q['ask']), coefficient=coefficient)
                p['entry_cost'] = cost(p['entry_price'], 1, coefficient, True)
    elif p['state'] == 'open' and (now-timestamp(p['entry_at'])).total_seconds() >= p['horizon']:
        if q and q['bid_size'] >= 1:
            p.update(state='exit_staged', stage_at=now.isoformat(), book_id=book_id, bid=q['bid'])
    elif p['state'] == 'exit_staged':
        if elapsed > 180:
            p.update(state='open')  # Retry only on subsequent fresh quotes; retain original entry.
        elif elapsed >= 10 and book_id != p['book_id'] and q and q['bid_size'] >= 1:
            price = min(p['bid'], q['bid'])
            proceeds = cost(price, 1, coefficient, False)
            p.update(state='closed', exit_at=now.isoformat(), exit_price=price, exit_cost=proceeds,
                     net_cents=proceeds['total']-p['entry_cost']['total'])
    return p


def fetch(db, group, endpoint, url):
    response = request(url)
    with db:
        persist(db, utcnow().isoformat(), group, endpoint, response)
    if response['error']:
        raise ValueError(response['error'])
    return json.loads(response['raw']), response


def targets(config):
    result = {}
    for path in config['game_ledgers']:
        db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True)
        try:
            rows = db.execute('SELECT config FROM games').fetchall()
        finally:
            db.close()
        for raw, in rows:
            c = json.loads(raw)
            result[(c['league'], game_key(c['market_event']))] = c
    for c in config['extra_games']:
        result[(c['league'], game_key(c['market_event']))] = c
    return result


def discover(db, config, cache):
    games = targets(config)
    errors = {}
    for series, league in config['series'].items():
        try:
            data, response = fetch(db, series, 'fees', BASE_URL+'/series/'+series)
            fee = data['series']
            coefficient = .07 * float(fee.get('fee_multiplier', -1))
            fee_ok = fee.get('fee_type') in ('quadratic', 'quadratic_with_maker_fees') and 0 <= coefficient <= .14
            cursor, seen = '', set()
            while True:
                data, response = fetch(db, series, 'discovery', BASE_URL+'/markets?'+urlencode({'series_ticker':series,'status':'open','limit':1000,'cursor':cursor}))
                for m in data['markets']:
                    key = (league, game_key(m['event_ticker']))
                    g = games.get(key)
                    if not g or m.get('market_type') != 'binary' or not m.get('rules_primary'):
                        continue
                    # Exact league and full event-code segment, not partial names/dates.
                    c = {'market': m, 'game': g, 'league': league, 'family': family(series),
                         'series': series, 'coefficient': coefficient if fee_ok else None,
                         'metadata_at': response['received_at']}
                    old = cache.get(m['ticker'])
                    if old and any(old['market'].get(k) != m.get(k) for k in ('event_ticker','rules_primary','rules_secondary')):
                        old['mapping_changed'] = True
                    elif not old or not old.get('mapping_changed'):
                        cache[m['ticker']] = c
                cursor = data.get('cursor', '')
                if not cursor:
                    break
                if cursor in seen:
                    raise ValueError('Repeated pagination cursor')
                seen.add(cursor)
        except Exception as exc:
            errors[series] = str(exc)[:200]
    return errors


def report(db, output):
    groups = {}
    for ticker, side, horizon, slot, raw in db.execute('SELECT * FROM probes'):
        p = json.loads(raw)
        key = (p['game'], p['league'], p['family'], horizon)
        r = groups.setdefault(key, Counter())
        r[p['state']] += 1
        if p['state'] == 'closed':
            r['net_cents'] += p['net_cents']
    rows = [{'game':k[0], 'league':k[1], 'family':k[2], 'horizon':k[3], **v} for k,v in sorted(groups.items())]
    write_json(output/'report.json', {'at':utcnow().isoformat(),'mode':'fixed_schedule_quote_probes','groups':rows})
    (output/'report.md').write_text('# Multi-market quote laboratory\n\nIndependent one-contract YES and NO probes sampled every 15 minutes, with 5/15-minute exits. These are execution-cost experiments, not a funded portfolio, signal-driven trades, or independent evidence of profitable edge. Opposing sides, horizons and nearby strikes are correlated. No baseline account risk limits change.\n\n'+json.dumps(rows,indent=2)+'\n\nOpen/exit-staged probes remain unresolved when liquidity disappears. No scoreboard settlement or retrospective entry. Entry and exit require distinct fresh book observations. No queue priority or guaranteed fills. Mapping uses the exact league and game event-code segment; new ambiguous encodings are excluded. Per-game raw evidence and probe rows are in laboratory.sqlite3.\n',encoding='utf-8')


def fair_order(ordered, cache):
    # Interleave games, then families within each game.
    from collections import defaultdict, deque
    groups = defaultdict(lambda: defaultdict(deque))
    for ticker in ordered:
        c = cache[ticker]
        groups[c['game']['league'] + '_' + c['game']['event_id']][c['family']].append(ticker)
    games = deque(deque(families.values()) for families in groups.values())
    result = []
    while games:
        families = games.popleft()
        family = families.popleft()
        result.append(family.popleft())
        if family:
            families.append(family)
        if families:
            games.append(families)
    return result


def select_batch(ordered, pending, cache):
    priority = fair_order([t for t in ordered if t in pending], cache)
    other = fair_order([t for t in ordered if t not in pending
                        and cache[t]['market'].get('status') == 'active'], cache)
    # Reserve 40 of 200 requests for broader coverage; borrow unused capacity.
    new_count = min(40, len(other))
    selected = priority[:200-new_count] + other[:new_count]
    selected += (priority[200-new_count:] + other[new_count:])[:200-len(selected)]
    return selected


def run(manifest, output, once=False):
    config = json.loads(Path(manifest).read_text())
    root = Path(__file__).resolve().parents[1]
    if config['version'] != VERSION or config['execution_enabled'] is not False:
        raise ValueError('Paper laboratory manifest required')
    for name, digest in config['source_sha256'].items():
        if hashlib.sha256((root/name).read_bytes()).hexdigest() != digest:
            raise ValueError('Frozen source mismatch: '+name)
    output = Path(output); output.mkdir(parents=True, exist_ok=True)
    lock = lock_process(output/'lab.lock')
    db = open_store(output/'laboratory.sqlite3', config)
    db.execute('CREATE TABLE IF NOT EXISTS probes(ticker,side,horizon,slot,state,PRIMARY KEY(ticker,side,horizon,slot))')
    cache = json.loads((output/'markets.json').read_text()) if (output/'markets.json').exists() else {}
    next_discovery, offset, errors = 0, 0, {}
    try:
        while utcnow() < timestamp(config['stop_at']):
            start = time.monotonic(); now = utcnow()
            if not (output/'PAUSE').exists():
                if start >= next_discovery:
                    errors = discover(db, config, cache)
                    write_json(output/'markets.json', cache)
                    next_discovery = time.monotonic()+300
                due = sorted(t for t,c in cache.items() if timestamp(c['game']['kickoff'])-timedelta(hours=2) <= now < timestamp(c['game']['stop_at'])
                             and not (output/(c['game']['league']+'_'+c['game']['event_id']+'.PAUSE')).exists())
                pending = {t for t,raw in db.execute('SELECT ticker,state FROM probes') if json.loads(raw)['state'] in ('staged','open','exit_staged')}
                ordered = due[offset:]+due[:offset]
                selected = select_batch(ordered, pending, cache)
                offset = (offset+len(selected)) % max(1,len(due))
                with ThreadPoolExecutor(max_workers=3) as pool:
                    for index in range(0,len(selected),3):
                        batch = selected[index:index+3]
                        for ticker, response in zip(batch, pool.map(lambda t:request(BASE_URL+'/markets/'+t+'/orderbook?depth=20'),batch)):
                            with db:
                                persist(db, now.isoformat(), ticker, 'book', response)
                            book_id = db.execute('SELECT max(id) FROM responses').fetchone()[0]
                            c = cache[ticker]; current = utcnow()
                            try:
                                if response['error'] or response['duration']>8 or float(response['headers'].get('Age',0))>20:
                                    raise ValueError('book_transport_unavailable')
                                q = quotes(json.loads(response['raw']))
                                if c.get('mapping_changed') or c['coefficient'] is None or (current-timestamp(c['metadata_at'])).total_seconds()>600:
                                    raise ValueError('metadata_or_fee_gate')
                                for side in ('yes','no'):
                                    quote = q if side=='yes' or q is None else opposite(q)
                                    with db:
                                        for h,slot,raw in db.execute('SELECT horizon,slot,state FROM probes WHERE ticker=? AND side=?',(ticker,side)).fetchall():
                                            p=advance(json.loads(raw),quote,current,book_id,c['coefficient'])
                                            db.execute('UPDATE probes SET state=? WHERE ticker=? AND side=? AND horizon=? AND slot=?',(json.dumps(p),ticker,side,h,slot))
                                        if current >= timestamp(c['game']['kickoff']) and c['market'].get('status')=='active' and suitable(quote,c['coefficient']):
                                            slot=int(current.timestamp())//900
                                            for h in (300,900):
                                                p={'state':'staged','stage_at':current.isoformat(),'ask':quote['ask'],'book_id':book_id,'horizon':h,'league':c['league'],'family':c['family'],'game':c['game']['league']+'_'+c['game']['event_id']}
                                                db.execute('INSERT OR IGNORE INTO probes VALUES(?,?,?,?,?)',(ticker,side,h,slot,json.dumps(p)))
                                errors.pop(ticker,None)
                            except Exception as exc:
                                errors[ticker]=str(exc)[:160]
                        time.sleep(.5)
                report(db,output)
                write_json(output/'status.json',{'at':utcnow().isoformat(),'version':VERSION,'execution_enabled':False,'markets':len(cache),'due':len(due),'sampled':len(selected),'cycle_seconds':time.monotonic()-start,'errors':errors})
            if once: break
            time.sleep(max(1,60-(time.monotonic()-start)))
    finally:
        db.close();lock.close()


if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',default='configs/sports_market_lab_v12_20260912/manifest.json');p.add_argument('--output',default='sports_paper/market_lab_v12_20260912');p.add_argument('--once',action='store_true');a=p.parse_args();run(a.manifest,a.output,a.once)
