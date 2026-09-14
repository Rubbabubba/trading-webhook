"""Prospective, archive-backed multi-sport paper tests; public settlement GETs only."""
import argparse
from collections import Counter
import gzip
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlparse, parse_qs

from .college_football_paper import lock_process, quotes, timestamp, utcnow, write_json
from .kalshi_market_data import BASE_URL
from .sports_capture_v3 import open_store, persist, request
from .sports_research_mapping import discover, schedule_events
from .sports_research_models import estimate, fit_prior, identity, participants, score
from .sports_research_execution import initial, decide

VERSION = 'sports_paper_research_1.0'
ROOT = Path(__file__).resolve().parents[1]


class Archive:
    def __init__(self, path):
        self.path, self.cursor, self.cache = Path(path), 0, {}

    def refresh(self):
        if not self.path.exists():
            return
        db = sqlite3.connect(self.path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
        try:
            if self.cursor:
                rows = db.execute('SELECT id,game,endpoint,metadata,sha256,body_gzip FROM responses WHERE id>? ORDER BY id', (self.cursor,)).fetchall()
            else:
                rows = db.execute('SELECT id,game,endpoint,metadata,sha256,body_gzip FROM responses WHERE id IN (SELECT max(id) FROM responses GROUP BY game,endpoint) ORDER BY id').fetchall()
        finally:
            db.close()
        latest = {}
        for row in rows:
            self.cursor = max(self.cursor, row[0])
            latest[row[1], row[2]] = row
        for key, row in latest.items():
            record = {'id': row[0], 'archive': str(self.path), 'sha256': row[4], 'metadata': json.loads(row[3]), 'data': {}}
            try:
                raw = gzip.decompress(row[5])
                if hashlib.sha256(raw).hexdigest() != row[4]:
                    raise ValueError('Archive hash mismatch')
                record['data'] = json.loads(raw)
            except Exception as exc:
                record['error'] = str(exc)
            self.cache[key] = record
            if key[1] == 'discovery':
                # Full page is required for mappings; never treat a partial page as complete.
                data = record['data']
                requested_cursor = parse_qs(urlparse(record['metadata'].get('url', '')).query).get('cursor')
                if not requested_cursor and not data.get('cursor') and 'markets' in data:
                    self.cache[key[0], 'market_list'] = {**record, 'data': data['markets']}


def fresh(record, now, age=120):
    if not record or record.get('error'):
        return False
    m = record['metadata']
    try:
        headers = {k.lower(): v for k, v in m.get('headers', {}).items()}
        cache_age = float(headers.get('age', 0))
        return (not m.get('error') and m.get('status') == 200 and m.get('duration', 999) <= 8
                and math.isfinite(cache_age) and 0 <= cache_age <= 20
                and 0 <= (now - timestamp(m['received_at'])).total_seconds() <= age)
    except (ValueError, TypeError, KeyError):
        return False


def open_paper(path, protocol):
    if protocol.get('version') != VERSION or protocol.get('execution_enabled') is not False:
        raise ValueError('Paper-only protocol required')
    if not protocol.get('source_sha256'):
        raise ValueError('Source fingerprints required')
    for name, digest in protocol['source_sha256'].items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest:
            raise ValueError('Frozen source changed: ' + name)
    db = open_store(path, protocol)
    db.executescript('''
        CREATE TABLE IF NOT EXISTS games(slug TEXT PRIMARY KEY,config TEXT,anchor TEXT,memory TEXT,state TEXT);
        CREATE TABLE IF NOT EXISTS accounts(slug TEXT,horizon INTEGER,state TEXT,PRIMARY KEY(slug,horizon));
        CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY,slug TEXT,at TEXT,snapshot TEXT,observation TEXT,UNIQUE(slug,snapshot));
        CREATE TABLE IF NOT EXISTS actions(sample_id INTEGER,horizon INTEGER,detail TEXT,account TEXT);
    ''')
    return db


def add_game(db, config, horizons):
    slug = config['league'] + '_' + config['event_id']
    frozen = json.dumps(config, sort_keys=True)
    old = db.execute('SELECT config FROM games WHERE slug=?', (slug,)).fetchone()
    if old:
        # Schedule/identity/rule changes require a separately reviewed mapping.
        if old[0] != frozen:
            raise ValueError('Frozen mapping changed: ' + slug)
        return
    with db:
        db.execute('INSERT INTO games VALUES(?,?,NULL,?,?)', (slug, frozen, '{}', 'waiting'))
        for horizon in horizons:
            db.execute('INSERT INTO accounts VALUES(?,?,?)', (slug, horizon, json.dumps(initial())))


def build_observation(config, archives, anchor, memory, now):
    cache = archives[config['archive']].cache
    slug = config['league'] + '_' + config['event_id']
    league = config['league']
    records, competition, summary, reference = {}, {}, {}, {}
    blockers = []
    if league in ('atp', 'wta', 'epl', 'mls'):
        records['scoreboard'] = cache.get((league, 'scoreboard'), {})
        events = schedule_events(records['scoreboard'].get('data', {}), league)
        event = next((e for e in events if str(e['id']) == config['event_id']), {})
        competition = (event.get('competitions') or [{}])[0]
    if league not in ('atp', 'wta'):
        key = 'game' if league == 'mlb' else 'summary'
        records['summary'] = cache.get((slug, key), {})
        summary = records['summary'].get('data', {})
        summary_comp = (summary.get('header', {}).get('competitions') or [{}])[0]
        if league in ('epl', 'mls'):
            try:
                left, right = participants(competition), participants(summary_comp)
                if not identity(summary_comp, config) or any(score(left[s]['score']) != score(right[s]['score']) for s in ('home', 'away')):
                    blockers.append('soccer_summary_score_or_identity_disagreement')
            except (KeyError, TypeError, ValueError):
                blockers.append('soccer_summary_unavailable')
        else:
            competition = summary_comp
    if league == 'mlb':
        records['reference'] = cache.get((slug, 'reference'), {})
        reference = records['reference'].get('data', {})
    for key, record in records.items():
        if not fresh(record, now):
            blockers.append(key + '_transport_stale_missing_or_failed')
    fee_record = cache.get((slug if league == 'mlb' else league, 'fees'), {})
    records['fees'] = fee_record
    series = fee_record.get('data', {}).get('series', {})
    multiplier = series.get('fee_multiplier')
    fee_ok = (fresh(fee_record, now, 600) and series.get('ticker') == config['series']
        and series.get('fee_type') in ('quadratic', 'quadratic_with_maker_fees')
        and isinstance(multiplier, (int, float)) and math.isfinite(multiplier) and 0 <= multiplier <= 2)
    coefficient = .07 * multiplier if fee_ok else None
    market_list = cache.get((league, 'market_list'), {})
    indexed = {m['ticker']: m for m in market_list.get('data', [])}
    markets = {}
    for side, ticker in config['markets'].items():
        if league == 'mlb':
            market_record = cache.get((slug, side), {})
            market = market_record.get('data', {}).get('market', {})
            book_record = cache.get((slug, side + '_book'), {})
        else:
            market_record = market_list
            market = indexed.get(ticker, {})
            book_record = cache.get((config['market_event'], ticker + '_book'), {})
        records[side + '_book'], records[side + '_market'] = book_record, market_record
        q = quotes(book_record.get('data', {}))
        evidence = config['mapping_evidence']
        expected = evidence.get('markets', evidence).get(side, {})
        same = (market.get('ticker') == ticker and market.get('event_ticker') == config['market_event']
            and market.get('market_type') == 'binary' and market.get('rules_primary') == expected.get('rules_primary')
            and market.get('rules_secondary') == expected.get('rules_secondary'))
        markets[side] = {'ticker': ticker, 'quote': q, 'book_id': book_record.get('id'),
            'book_received_at': book_record.get('metadata', {}).get('received_at'),
            'valid': bool(same and fee_ok and fresh(book_record, now) and fresh(market_record, now, 600)
                          and market.get('status') == 'active' and q),
            'fee_coefficient': coefficient if coefficient is not None else .14,
            'settlement_ok': bool(same and fresh(market_record, now) and market.get('status') == 'finalized'),
            'result': market.get('result'), 'metadata_ok': same}
    if not fee_ok:
        blockers.append('fee_metadata_missing_stale_or_unsupported')
    phase = competition.get('status', {}).get('type', {})
    # Freeze the first eligible anchor observed prospectively within two hours
    # of scheduled start. Archived pregame quotes are never backfilled after start.
    before = (timestamp(config['kickoff']) - now).total_seconds()
    if (not anchor and league != 'ncaaf' and 0 < before <= 7200 and phase.get('state') == 'pre'
            and identity(competition, config) and not blockers and all(m['valid'] for m in markets.values())):
        mids = {s: (m['quote']['bid'] + m['quote']['ask']) / 2 for s, m in markets.items()}
        total = sum(mids.values())
        if .90 <= total <= 1.10 and all(.02 < p < .98 for p in mids.values()) and all(
                m['quote']['ask'] - m['quote']['bid'] <= .04 for m in markets.values()):
            prior = {s: p / total for s, p in mids.items()}
            try:
                anchor = {**fit_prior(league, prior), 'prior': prior, 'observed_at': now.isoformat(),
                          'book_ids': {s: m['book_id'] for s, m in markets.items()}}
            except ValueError as exc:
                blockers.append(str(exc))
    try:
        model = estimate(config, summary, competition, reference, anchor, now)
    except Exception as exc:
        model = {'probabilities': {}, 'blockers': [str(exc)[:200]], 'completed': False, 'signature': None,
                 'phase': phase.get('name'), 'state': phase.get('state')}
    model['blockers'].extend(blockers)
    signature = model.get('signature')
    if signature and signature != memory.get('signature'):
        memory.update(signature=signature, changed_at=now.isoformat())
    if signature and league in ('epl', 'mls', 'atp', 'wta'):
        if (now - timestamp(memory['changed_at'])).total_seconds() > 180:
            model['blockers'].append('game_state_unchanged_over_180_seconds')
    if now >= timestamp(config['stop_at']):
        model['blockers'].append('experiment_deadline')
    snapshot = hashlib.sha256(json.dumps({k: r.get('id') for k, r in records.items()}, sort_keys=True).encode()).hexdigest()
    return {'at': now.isoformat(), 'snapshot': snapshot, 'model': model, 'markets': markets,
            'anchor': anchor, 'sources': {k: {f: r.get(f) for f in ('id', 'archive', 'sha256', 'metadata')} for k, r in records.items()}}, anchor, memory


def report_game(db, slug, output):
    config_raw, _, _, phase = db.execute('SELECT config,anchor,memory,state FROM games WHERE slug=?', (slug,)).fetchone()
    config = json.loads(config_raw)
    count, last = db.execute('SELECT count(*),max(at) FROM samples WHERE slug=?', (slug,)).fetchone()
    accounts = {str(h): json.loads(s) for h, s in db.execute('SELECT horizon,state FROM accounts WHERE slug=? ORDER BY horizon', (slug,))}
    report = {'game': config['game'], 'league': config['league'], 'mode': 'paper_research', 'phase': phase,
              'samples': count, 'last_sample': last, 'accounts': accounts, 'profitability_validated': False}
    folder = output / 'games' / slug
    folder.mkdir(parents=True, exist_ok=True)
    write_json(folder / 'report.json', report)
    text = [f"# {config['game']}", '', f"Experimental paper simulation. State: {phase}. Samples: {count}. Last observation: {last}.",
            'These are separate hypothetical $1,000 accounts, not a combined portfolio. Models are uncalibrated and market-anchored where applicable. No real orders.', '',
            '| Holding limit | Entries / exits | Realized net P&L | Modeled fees | Slippage | Max drawdown | Open position |',
            '|---|---:|---:|---:|---:|---:|---|']
    for horizon, state in accounts.items():
        text.append(f"| {int(horizon)//60} minutes | {state['entries']} / {state['exits']} | ${state['realized_cents']/100:.2f} | ${state['fees_cents']/100:.2f} | ${state['slippage_cents']/100:.2f} | ${state['drawdown_cents']/100:.2f} | {'Yes; unresolved' if state['position'] else 'No'} |")
    blockers = Counter()
    for raw, in db.execute('SELECT observation FROM samples WHERE slug=?', (slug,)):
        blockers.update(json.loads(raw)['model']['blockers'])
    text += ['', '## Signal exclusions', '', json.dumps(dict(blockers), indent=2), '', '## Simulated trade examples', '']
    rows = db.execute('SELECT s.at,a.horizon,a.detail FROM actions a JOIN samples s ON s.id=a.sample_id WHERE s.slug=? ORDER BY s.id', (slug,))
    examples = []
    for at, horizon, detail in rows:
        for action in json.loads(detail):
            if action['action'] in ('buy', 'sell', 'settle'):
                examples.append(f"- {at}, {horizon//60}m: `{json.dumps(action)}`")
    text += examples or ['No simulated trades yet. Zero entries do not establish profitability.']
    text += ['', 'Net results include modeled taker fees rounded upward to whole cents and one cent per contract per transaction slippage. Exits require fresh displayed size and another observation. Missing liquidity can exceed the intended holding/loss limits. Unresolved positions are never assigned an invented payout. Capture gaps and game-specific model limits require review before interpreting returns.']
    (folder / 'report.md').write_text('\n'.join(text), encoding='utf-8')
    return report


def run(manifest, output, once=False):
    protocol = json.loads(Path(manifest).read_text())
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    lock = lock_process(output / 'paper.lock')
    db = open_paper(output / 'paper.sqlite3', protocol)
    archives = {key: Archive(ROOT / path) for key, path in protocol['archives'].items()}
    next_discovery = next_report = 0
    last_settlement = {}
    try:
        while utcnow() < timestamp(protocol['stop_at']):
            now, errors = utcnow(), {}
            for key, archive in archives.items():
                try:
                    archive.refresh()
                except Exception as exc:
                    errors[key] = str(exc)[:200]
            if time.monotonic() >= next_discovery:
                configs, missing = discover(archives['expansion'].cache, protocol['sources'], protocol)
                configs += protocol['mlb_games']
                for config in configs:
                    try:
                        add_game(db, config, protocol['holding_seconds'])
                    except Exception as exc:
                        errors[config['market_event']] = str(exc)
                        slug = config['league'] + '_' + config['event_id']
                        with db:
                            db.execute("UPDATE games SET state='mapping_changed' WHERE slug=?", (slug,))
                write_json(output / 'unmatched.json', missing)
                next_discovery = time.monotonic() + 300
            report_due = time.monotonic() >= next_report
            statuses = []
            for slug, raw, anchor_raw, memory_raw, phase in db.execute('SELECT * FROM games').fetchall():
                config = json.loads(raw)
                expired = now >= timestamp(config['stop_at'])
                if phase in ('completed', 'deadline_reached'):
                    statuses.append({'slug': slug, 'state': phase})
                    continue
                paused = ((output / 'PAUSE').exists() or (output / (slug + '.PAUSE')).exists()
                          or phase == 'mapping_changed'
                          or (archives[config['archive']].path.parent / 'PAUSE').exists()
                          or (archives[config['archive']].path.parent / (slug + '.PAUSE')).exists()
                          or (archives[config['archive']].path.parent / (config['market_event'] + '.PAUSE')).exists())
                try:
                    obs, anchor, memory = build_observation(config, archives,
                        json.loads(anchor_raw) if anchor_raw else None, json.loads(memory_raw), now)
                    if phase == 'mapping_changed':
                        obs['model']['blockers'].append('frozen_mapping_changed')
                    states = {h: json.loads(s) for h, s in db.execute('SELECT horizon,state FROM accounts WHERE slug=?', (slug,))}
                    for state in states.values():
                        p = state['position']
                        if not p:
                            continue
                        m = obs['markets'][p['side']]
                        ticker = p['ticker']
                        if (not m['valid'] or obs['model']['completed']) and time.monotonic() >= last_settlement.get(ticker, 0):
                            response = request(BASE_URL + '/markets/' + ticker)
                            with db:
                                persist(db, now.isoformat(), slug, 'settlement_' + ticker, response)
                            last_settlement[ticker] = time.monotonic() + 60
                            try:
                                settled = json.loads(response['raw'])['market']
                                evidence = config['mapping_evidence']
                                expected_rules = evidence.get('markets', evidence)[p['side']]
                                if (not response['error'] and settled['ticker'] == ticker and settled['event_ticker'] == config['market_event']
                                        and settled.get('rules_primary') == expected_rules.get('rules_primary')
                                        and settled.get('rules_secondary') == expected_rules.get('rules_secondary')
                                        and settled['status'] == 'finalized' and settled['result'] in ('yes', 'no')):
                                    m.update(settlement_ok=True, result=settled['result'])
                                    obs['snapshot'] += ':' + settled['result']
                            except (ValueError, KeyError):
                                pass
                    changed = False
                    with db:
                        old = db.execute('SELECT id FROM samples WHERE slug=? AND snapshot=?', (slug, obs['snapshot'])).fetchone()
                        if not old:
                            sample_id = db.execute('INSERT INTO samples VALUES(NULL,?,?,?,?)', (slug, now.isoformat(), obs['snapshot'], json.dumps(obs))).lastrowid
                            for horizon, state in states.items():
                                actions = decide(state, obs, now, horizon, protocol['entry_margin'][config['league']], paused)
                                changed |= any(a['action'] in ('buy', 'sell', 'settle') for a in actions)
                                db.execute('UPDATE accounts SET state=? WHERE slug=? AND horizon=?', (json.dumps(state), slug, horizon))
                                db.execute('INSERT INTO actions VALUES(?,?,?,?)', (sample_id, horizon, json.dumps(actions), json.dumps(state)))
                        terminal = obs['model']['completed'] and not any(s['position'] for s in states.values())
                        state_name = ('mapping_changed' if phase == 'mapping_changed' else 'deadline_reached' if expired else 'completed' if terminal
                                      else 'paused' if paused else 'running' if now >= timestamp(config['kickoff']) else 'waiting')
                        db.execute('UPDATE games SET anchor=?,memory=?,state=? WHERE slug=?',
                                   (json.dumps(anchor) if anchor else None, json.dumps(memory), state_name, slug))
                    statuses.append({'slug': slug, 'game': config['game'], 'state': state_name,
                        'anchor_ready': bool(anchor) or config['league'] == 'ncaaf', 'blockers': obs['model']['blockers'],
                        'accounts': {str(h): {'entries': s['entries'], 'exits': s['exits'], 'realized_cents': s['realized_cents'],
                                            'open_position': bool(s['position'])} for h, s in states.items()}})
                    if report_due or changed or terminal or expired:
                        report_game(db, slug, output)
                except Exception as exc:
                    errors[slug] = str(exc)[:300]
                    statuses.append({'slug': slug, 'state': 'error', 'error': str(exc)[:300]})
            write_json(output / 'status.json', {'at': utcnow().isoformat(), 'version': VERSION,
                'mode': 'paper_research', 'execution_enabled': False, 'profitability_validated': False,
                'games': statuses, 'errors': errors})
            if report_due:
                next_report = time.monotonic() + 300
            if once:
                break
            time.sleep(5)
        if not once:
            with db:
                db.execute("UPDATE games SET state='deadline_reached' WHERE state!='completed'")
            finished = [report_game(db, slug, output) for slug, in db.execute('SELECT slug FROM games').fetchall()]
            write_json(output / 'status.json', {'at': utcnow().isoformat(), 'version': VERSION,
                'mode': 'paper_research', 'execution_enabled': False, 'profitability_validated': False,
                'worker_state': 'deadline_reached', 'games': finished, 'errors': {}})
    finally:
        db.close()
        lock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', default='configs/sports_paper_research_20260910/manifest.json')
    parser.add_argument('--output', default='sports_paper/research_20260910')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    run(args.manifest, args.output, args.once)
