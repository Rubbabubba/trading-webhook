"""Raw-feed validation only. No account, trading decisions, credentials or orders."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from urllib.request import Request, urlopen

from .college_football_paper import lock_process, timestamp, utcnow, write_json
from .sports_paper_v2 import SPORTS, parse_game
from .kalshi_market_data import BASE_URL

VERSION = 'capture_v3.0'


def request(url):
    start = utcnow().isoformat()
    timer = time.monotonic()
    try:
        with urlopen(Request(url, headers={'Accept': 'application/json'}), timeout=8) as response:
            raw = response.read()
            headers = dict(response.headers.items())
            status = response.status
        return {'url': url, 'started_at': start, 'received_at': utcnow().isoformat(),
                'duration': time.monotonic() - timer, 'status': status,
                'headers': headers, 'raw': raw, 'error': None}
    except Exception as exc:
        return {'url': url, 'started_at': start, 'received_at': utcnow().isoformat(),
                'duration': time.monotonic() - timer, 'status': None,
                'headers': {}, 'raw': b'', 'error': str(exc)[:300]}


def open_store(path, manifest):
    db = sqlite3.connect(path)
    db.executescript('''
        CREATE TABLE IF NOT EXISTS protocol(id INTEGER PRIMARY KEY CHECK(id=1), frozen TEXT);
        CREATE TABLE IF NOT EXISTS responses(id INTEGER PRIMARY KEY, cycle TEXT, game TEXT,
            endpoint TEXT, metadata TEXT, sha256 TEXT, body_gzip BLOB);
        CREATE TABLE IF NOT EXISTS diagnostics(cycle TEXT, game TEXT, detail TEXT);
    ''')
    frozen = json.dumps(manifest, sort_keys=True)
    old = db.execute('SELECT frozen FROM protocol').fetchone()
    if old and old[0] != frozen:
        db.close()
        raise ValueError('Frozen capture manifest mismatch')
    with db:
        db.execute('INSERT OR IGNORE INTO protocol VALUES(1,?)', (frozen,))
    return db


def persist(db, cycle, game, endpoint, response):
    raw = response['raw']
    db.execute('INSERT INTO responses VALUES(NULL,?,?,?,?,?,?)',
        (cycle, game, endpoint, json.dumps({k: v for k, v in response.items() if k != 'raw'}),
         hashlib.sha256(raw).hexdigest(), gzip.compress(raw)))


def diagnose(data, config, now):
    g = parse_game(data.get('game', {}), config, now)
    result = {'baseline_parser': g, 'trading_enabled': False, 'validation': 'unvalidated'}
    reference = data.get('reference', {})
    if reference:
        identity = str(reference.get('gamePk')) == str(config.get('reference_game_pk'))
        lines = reference.get('liveData', {}).get('linescore', {})
        scores = {s: lines.get('teams', {}).get(s, {}).get('runs') for s in ('home', 'away')}
        result['reference'] = {'identity_ok': identity, 'scores': scores,
            'inning': lines.get('currentInning'), 'half': lines.get('inningHalf'),
            'outs': lines.get('outs'), 'balls': lines.get('balls'), 'strikes': lines.get('strikes'),
            'current_play': reference.get('liveData', {}).get('plays', {}).get('currentPlay')}
        result['score_agreement'] = bool(identity and g['identity_ok'] and all(
            scores[s] is not None and str(scores[s]) == str(g['scores'][s]) for s in scores))
    return result


def capture(db, config):
    sport, series = SPORTS[config['league']]
    urls = {'game': f"https://site.api.espn.com/apis/site/v2/sports/{sport}/summary?event={config['event_id']}",
            'fees': BASE_URL + '/series/' + series}
    for side, ticker in config['markets'].items():
        urls[side] = BASE_URL + '/markets/' + ticker
        urls[side + '_book'] = urls[side] + '/orderbook?depth=20'
    if config.get('reference_game_pk'):
        urls['reference'] = f"https://statsapi.mlb.com/api/v1.1/game/{config['reference_game_pk']}/feed/live"
    cycle = utcnow().isoformat()
    slug = config['league'] + '_' + config['event_id']
    with ThreadPoolExecutor(max_workers=7) as pool:
        responses = dict(zip(urls, pool.map(request, urls.values())))
    # Commit original bytes even when JSON decoding or diagnostics fail.
    with db:
        for key, response in responses.items():
            persist(db, cycle, slug, key, response)
    data, errors = {}, {}
    for key, response in responses.items():
        try:
            if response['error']:
                raise ValueError(response['error'])
            data[key] = json.loads(response['raw'])
        except (ValueError, UnicodeError) as exc:
            errors[key] = str(exc)[:300]
    try:
        diagnostic = diagnose(data, config, utcnow())
    except Exception as exc:
        diagnostic = {'validation': 'unvalidated', 'diagnostic_error': str(exc)[:300]}
    diagnostic.update(at=utcnow().isoformat(), errors=errors)
    with db:
        db.execute('INSERT INTO diagnostics VALUES(?,?,?)', (cycle, slug, json.dumps(diagnostic)))
    return diagnostic


def run(manifest_path, output, once=False):
    manifest = json.loads(Path(manifest_path).read_text())
    if manifest.get('version') != VERSION or manifest.get('execution_enabled') is not False:
        raise ValueError('Capture-only manifest required')
    for relative, digest in manifest.get('source_sha256', {}).items():
        if hashlib.sha256((Path(__file__).resolve().parents[1] / relative).read_bytes()).hexdigest() != digest:
            raise ValueError('Frozen source mismatch: ' + relative)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    lock = lock_process(output / 'capture.lock')
    db = open_store(output / 'capture.sqlite3', manifest)
    next_due, saved = {}, {}
    for slug, detail in db.execute('SELECT game,detail FROM diagnostics ORDER BY rowid'):
        saved[slug] = json.loads(detail)
    try:
        while True:
            now = utcnow()
            paused = (output / 'PAUSE').exists()
            pending, rows = False, []
            for config in manifest['games']:
                slug = config['league'] + '_' + config['event_id']
                old = saved.get(slug, {})
                completed = old.get('baseline_parser', {}).get('completed', False)
                expired = now >= timestamp(config['stop_at'])
                if not completed and not expired:
                    pending = True
                    due = now >= timestamp(config['kickoff']) - timedelta(hours=2)
                    if due and not paused and not (output / (slug + '.PAUSE')).exists() and now >= next_due.get(slug, now):
                        old = capture(db, config)
                        saved[slug] = old
                        next_due[slug] = utcnow() + timedelta(seconds=20 if now >= timestamp(config['kickoff']) else 60)
                        completed = old.get('baseline_parser', {}).get('completed', False)
                rows.append({'slug': slug, 'game': config['game'], 'kickoff': config['kickoff'],
                    'state': 'completed' if completed else 'expired' if expired else 'collecting' if old else 'waiting',
                    'last_sample': old.get('at'), 'errors': old.get('errors', {}),
                    'diagnostic_error': old.get('diagnostic_error'),
                    'baseline_blockers': old.get('baseline_parser', {}).get('blockers'),
                    'score_agreement': old.get('score_agreement')})
            write_json(output / 'status.json', {'at': utcnow().isoformat(), 'version': VERSION,
                'execution_enabled': False, 'validation': 'unvalidated', 'paused': paused, 'games': rows})
            if once or not pending:
                break
            time.sleep(2)
    finally:
        db.close()
        lock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', default='configs/sports_capture_20260910/manifest.json')
    parser.add_argument('--output', default='sports_paper/capture_20260910')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    run(args.manifest, args.output, args.once)
