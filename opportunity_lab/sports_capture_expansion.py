"""Public raw capture for college football, EPL, MLS and tennis. No trading."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urlencode

from .sports_capture_v3 import open_store, persist, request
from .college_football_paper import lock_process, timestamp, utcnow, write_json
from .kalshi_market_data import BASE_URL

VERSION = 'expansion_capture_1.0'


def event_date(ticker):
    match = re.search(r'-(\d{2}[A-Z]{3}\d{2})', ticker)
    if not match:
        return None
    try:
        return datetime.strptime(match[1], '%y%b%d').date().isoformat()
    except ValueError:
        return None


def in_scope(market, config, series):
    day = event_date(market.get('event_ticker', ''))
    if not day or not config['first_date'] <= day <= config['last_date']:
        return False
    # ATP/WTA series include other tournaments; this experiment is US Open only.
    return series not in ('KXATPMATCH', 'KXWTAMATCH') or 'US Open' in market.get('rules_primary', '')


def archived_get(db, group, endpoint, url):
    response = request(url)
    with db:
        persist(db, utcnow().isoformat(), group, endpoint, response)
    if response['error']:
        raise ValueError(response['error'])
    return json.loads(response['raw'])


def discover(db, config, cache, errors):
    for league, source in config['sources'].items():
        try:
            archived_get(db, league, 'fees', BASE_URL + '/series/' + source['series'])
            cursor, seen, markets = '', set(), {}
            while True:
                query = urlencode({'series_ticker': source['series'], 'status': 'open', 'limit': 1000, 'cursor': cursor})
                data = archived_get(db, league, 'discovery', BASE_URL + '/markets?' + query)
                for market in data['markets']:
                    if in_scope(market, config, source['series']):
                        markets[market['ticker']] = market
                cursor = data.get('cursor', '')
                if not cursor:
                    break
                if cursor in seen:
                    raise ValueError('Repeated discovery cursor')
                seen.add(cursor)
            # Retain previously discovered markets to observe closing/settlement.
            cache.setdefault(league, {}).update(markets)
        except Exception as exc:
            errors[league + '_discovery'] = str(exc)[:300]


def book_due(market, now):
    day = event_date(market['event_ticker'])
    # Broad window from 00:00 Eastern (September EDT) through next noon UTC.
    # This is a collection window, never an inferred kickoff or trade signal.
    begin = timestamp(day + 'T04:00:00Z')
    return begin <= now < begin + timedelta(hours=32)


def capture_cycle(db, config, cache, output):
    now, urls, errors = utcnow(), [], {}
    for league, source in config['sources'].items():
        date_query = now.strftime('%Y%m%d') if league in ('atp', 'wta') else '20260910-20260917'
        url = f"https://site.api.espn.com/apis/site/v2/sports/{source['sport']}/scoreboard?dates={date_query}&limit=1000"
        if league == 'ncaaf':
            url += '&groups=80'
        try:
            scoreboard = archived_get(db, league, 'scoreboard', url)
            if league not in ('atp', 'wta'):
                for event in scoreboard.get('events', []):
                    kickoff = timestamp(event['date'])
                    if kickoff - timedelta(hours=2) <= now < kickoff + timedelta(hours=12):
                        group = league + '_' + event['id']
                        if not (output / (group + '.PAUSE')).exists():
                            urls.append((group, 'summary', f"https://site.api.espn.com/apis/site/v2/sports/{source['sport']}/summary?event={event['id']}"))
        except Exception as exc:
            errors[league + '/scoreboard'] = str(exc)[:300]
        for market in cache.get(league, {}).values():
            event = market['event_ticker']
            if book_due(market, now) and not (output / (event + '.PAUSE')).exists():
                urls.append((event, market['ticker'] + '_book', BASE_URL + '/markets/' + market['ticker'] + '/orderbook?depth=20'))
    completed = 0
    cycle = now.isoformat()
    with ThreadPoolExecutor(max_workers=4) as pool:
        for start in range(0, len(urls), 4):
            if (output / 'PAUSE').exists():
                break
            batch = urls[start:start + 4]
            for (group, endpoint, _), response in zip(batch, pool.map(request, [row[2] for row in batch])):
                with db:
                    persist(db, cycle, group, endpoint, response)
                completed += 1
                if response['error']:
                    errors[group + '/' + endpoint] = response['error']
            time.sleep(0.5)
    return {'started_at': cycle, 'finished_at': utcnow().isoformat(), 'requested': len(urls),
            'recorded': completed, 'errors': errors}


def validate(config):
    if config.get('version') != VERSION or config.get('execution_enabled') is not False:
        raise ValueError('Capture-only manifest required')
    root = Path(__file__).resolve().parents[1]
    if not config.get('source_sha256'):
        raise ValueError('Source fingerprints required')
    for name, digest in config['source_sha256'].items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise ValueError('Frozen source mismatch: ' + name)


def run(manifest, output, once=False):
    config = json.loads(Path(manifest).read_text())
    validate(config)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    lock = lock_process(output / 'capture.lock')
    db = open_store(output / 'capture.sqlite3', config)
    cache_path = output / 'markets.json'
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    next_discovery = 0
    discovery_errors = {}
    try:
        while utcnow() < timestamp(config['stop_at']):
            start, errors = time.monotonic(), {}
            paused = (output / 'PAUSE').exists()
            if not paused and start >= next_discovery:
                discover(db, config, cache, errors)
                discovery_errors = errors
                write_json(cache_path, cache)
                next_discovery = time.monotonic() + 300
            result = {} if paused else capture_cycle(db, config, cache, output)
            write_json(output / 'status.json', {'at': utcnow().isoformat(), 'version': VERSION,
                'execution_enabled': False, 'validation': 'unvalidated; game-to-market mapping pending',
                'paused': paused, 'discovery_errors': discovery_errors, 'cycle': result,
                'events': {league: len({m['event_ticker'] for m in markets.values()}) for league, markets in cache.items()},
                'market_counts': {league: len(markets) for league, markets in cache.items()}})
            if once:
                break
            # Measured cycle duration is retained; no assertion of exact cadence.
            time.sleep(max(1, 60 - (time.monotonic() - start)))
    finally:
        db.close()
        lock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', default='configs/sports_expansion_20260910/manifest.json')
    parser.add_argument('--output', default='sports_paper/expansion_20260910')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    run(args.manifest, args.output, args.once)
