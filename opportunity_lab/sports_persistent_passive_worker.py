"""Continuous Demo sports shadow worker. Public reads only; contains no order path."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .college_football_paper import timestamp
from .kalshi_demo_market_data import DemoMarkets
from .sports_dynamic_mapping import schedule_events
from .sports_paper_v2 import parse_game
from .sports_persistent_passive import evaluate, initial
from .sports_research_models_v11 import estimate, fit_prior


SPORTS = {'ncaaf': 'football/college-football', 'nfl': 'football/nfl',
          'mlb': 'baseball/mlb', 'epl': 'soccer/eng.1', 'mls': 'soccer/usa.1',
          'atp': 'tennis/atp', 'wta': 'tennis/wta'}
CONFIG = {'contracts': 1, 'tick_size': .01, 'improve_by': .01, 'max_spread': .04,
          'minimum_stressed_edge': .08, 'adverse_selection_stress': .02,
          'minimum_confirmation_seconds': 15, 'contradictory_states_for_exit': 2}


def utcnow():
    return datetime.now(timezone.utc)


def public_json(url):
    with urlopen(Request(url, headers={'Accept': 'application/json'}), timeout=12) as response:
        return json.load(response)


def open_db(path):
    db = sqlite3.connect(path)
    db.executescript('''
      CREATE TABLE IF NOT EXISTS games(slug TEXT PRIMARY KEY, config TEXT, anchor TEXT, state TEXT, updated_at TEXT,
        active INTEGER NOT NULL DEFAULT 1);
      CREATE TABLE IF NOT EXISTS observations(id INTEGER PRIMARY KEY, slug TEXT, at TEXT, signature TEXT, detail TEXT,
        UNIQUE(slug,signature));
      CREATE TABLE IF NOT EXISTS decisions(observation_id INTEGER, detail TEXT);
      CREATE TABLE IF NOT EXISTS signals(slug TEXT PRIMARY KEY, at TEXT, detail TEXT);
      CREATE INDEX IF NOT EXISTS observation_slug ON observations(slug,id);
    ''')
    columns = {row[1] for row in db.execute('PRAGMA table_info(games)')}
    if 'active' not in columns:
        with db:
            db.execute('ALTER TABLE games ADD COLUMN active INTEGER NOT NULL DEFAULT 1')
    return db


def sync_manifest(db, manifest):
    with db:
        # Keep the historical rows and strategy state, but only schedule games that
        # are present in the latest complete registry.  Without this membership
        # marker, closed games remain eligible forever and repeatedly fail quotes.
        db.execute('UPDATE games SET active=0')
        for game in manifest['games']:
            slug = game['league'] + '_' + game['event_id']
            db.execute('''
              INSERT INTO games(slug,config,anchor,state,updated_at,active)
              VALUES(?,?,NULL,?,?,1)
              ON CONFLICT(slug) DO UPDATE SET
                config=excluded.config,
                updated_at=excluded.updated_at,
                active=1
            ''', (slug, json.dumps(game, sort_keys=True), json.dumps(initial()),
                  manifest['generated_at']))
    return len(manifest['games'])


def active_games(db):
    return list(db.execute('SELECT slug,config FROM games WHERE active=1 ORDER BY slug'))


def market_views(client, config):
    result = {}
    for side, ticker in config['markets'].items():
        quote = client.quote({'ticker': ticker})
        market, book = quote['market'], quote.get('orderbook_fp') or {}
        yes = book.get('yes_dollars') or []
        no = book.get('no_dollars') or []
        yes_bid = max((float(row[0]) for row in yes), default=None)
        no_bid = max((float(row[0]) for row in no), default=None)
        if yes_bid is None or no_bid is None:
            valid = False; bid = ask = None
        else:
            bid, ask, valid = yes_bid, 1 - no_bid, yes_bid < 1 - no_bid
        result[side] = {'ticker': ticker, 'valid': valid,
                        'maker_fee_coefficient': .0175,
                        'quote': {'bid': bid, 'ask': ask} if valid else None,
                        'market_status': market.get('status')}
    return result


def scoreboard(config, now, fetch=public_json):
    league = config['league']
    day = timestamp(config['kickoff']).strftime('%Y%m%d')
    url = ('https://site.api.espn.com/apis/site/v2/sports/' + SPORTS[league]
           + '/scoreboard?' + urlencode({'dates': day, 'limit': 1000}))
    board = fetch(url)
    event = next((row for row in schedule_events(board, league)
                  if str(row.get('id')) == config['event_id']), None)
    if not event:
        raise ValueError('authoritative_schedule_event_missing')
    competition = event['competitions'][0]
    summary = board
    if league not in ('atp', 'wta'):
        summary = fetch('https://site.api.espn.com/apis/site/v2/sports/' + SPORTS[league]
                        + '/summary?event=' + config['event_id'])
        competition = (summary.get('header', {}).get('competitions') or [competition])[0]
    return summary, competition


def model_for(config, summary, competition, anchor, now):
    if config['league'] in ('ncaaf', 'nfl', 'mlb'):
        parsed = parse_game(summary, {**config, 'max_signal_age_seconds': 90}, now)
        return {'probabilities': {} if parsed['home_probability'] is None else
                {'home': parsed['home_probability'], 'away': 1-parsed['home_probability']},
                'completed': parsed['completed'], 'state': parsed['state'],
                'blockers': parsed['blockers'], 'signature': str(parsed.get('play_id')),
                'details': parsed}
    return estimate(config, summary, competition, {}, anchor, now)


def maybe_anchor(config, competition, markets, anchor, now):
    if anchor or config['league'] in ('ncaaf', 'nfl', 'mlb'):
        return anchor
    before = (timestamp(config['kickoff']) - now).total_seconds()
    phase = competition.get('status', {}).get('type', {})
    if not 0 < before <= 7200 or phase.get('state') != 'pre' or not all(m['valid'] for m in markets.values()):
        return anchor
    mids = {side: (market['quote']['bid'] + market['quote']['ask']) / 2
            for side, market in markets.items()}
    total = sum(mids.values())
    if not .9 <= total <= 1.1:
        return anchor
    prior = {side: value / total for side, value in mids.items()}
    return {**fit_prior(config['league'], prior), 'prior': prior,
            'observed_at': now.isoformat()}


def observe_one(db, client, slug, now, fetch=public_json):
    raw, anchor_raw, state_raw = db.execute(
        'SELECT config,anchor,state FROM games WHERE slug=?', (slug,)).fetchone()
    config = json.loads(raw); anchor = json.loads(anchor_raw) if anchor_raw else None
    state = json.loads(state_raw)
    summary, competition = scoreboard(config, now, fetch)
    try:
        markets = market_views(client, config)
    except ValueError as exc:
        if str(exc) != 'demo_market_not_active_binary':
            raise
        # The registry and exchange can change between discovery and observation.
        # Retire this event until a later manifest refresh explicitly re-admits it.
        with db:
            db.execute('UPDATE games SET active=0,updated_at=? WHERE slug=?',
                       (now.isoformat(), slug))
        return {'action': 'market_inactive', 'reason': str(exc)}
    anchor = maybe_anchor(config, competition, markets, anchor, now)
    model = model_for(config, summary, competition, anchor, now)
    observation = {'at': now.isoformat(), 'admission_ok': all(m['valid'] for m in markets.values()),
                   'model': model, 'markets': markets}
    signature = model.get('signature')
    if not signature:
        signature = 'blocked:' + now.replace(second=0, microsecond=0).isoformat()
    with db:
        cursor = db.execute('INSERT OR IGNORE INTO observations VALUES(NULL,?,?,?,?)',
                            (slug, now.isoformat(), signature, json.dumps(observation)))
        if not cursor.rowcount:
            return {'action': 'duplicate_observation'}
        decision = evaluate(state, observation, now, CONFIG)
        db.execute('INSERT INTO decisions VALUES(?,?)', (cursor.lastrowid, json.dumps(decision)))
        if decision['action'] == 'shadow_post_only_signal':
            db.execute('INSERT OR IGNORE INTO signals VALUES(?,?,?)',
                       (slug, now.isoformat(), json.dumps(decision['signal'])))
        phase = 'completed' if model.get('completed') else 'running'
        db.execute('UPDATE games SET anchor=?,state=?,updated_at=? WHERE slug=?',
                   (json.dumps(anchor) if anchor else None, json.dumps(state), now.isoformat(), slug))
    return decision


def status(db, registry_count, error=None):
    games = db.execute('SELECT count(*) FROM games').fetchone()[0]
    active = db.execute('SELECT count(*) FROM games WHERE active=1').fetchone()[0]
    observations = db.execute('SELECT count(*) FROM observations').fetchone()[0]
    signals = db.execute('SELECT count(*) FROM signals').fetchone()[0]
    return {'at': utcnow().isoformat(), 'strategy_id': 'sports_persistent_passive_v1_shadow',
            'execution_enabled': False, 'registry_events': registry_count,
            'persisted_games': games, 'active_games': active,
            'observations': observations, 'signals': signals,
            'error': error}


def run(data_root, cycles=None, refresh_seconds=1800):
    from tools.build_sports_challenger_manifest import build
    root = Path(data_root); root.mkdir(parents=True, exist_ok=True)
    db = open_db(root/'sports_shadow.sqlite3'); client = DemoMarkets()
    registry = 0; next_refresh = 0; cycle = 0
    try:
        while cycles is None or cycle < cycles:
            now = utcnow(); error = None
            try:
                if time.monotonic() >= next_refresh:
                    manifest = build(now, None, client=client)
                    registry = sync_manifest(db, manifest)
                    (root/'events.json').write_text(json.dumps(manifest, indent=2))
                    print(json.dumps({
                        'at': now.isoformat(),
                        'event': 'sports_challenger_registry_refreshed',
                        'strategy_id': 'sports_persistent_passive_v1_shadow',
                        'registry_events': registry,
                        'execution_enabled': False,
                    }), flush=True)
                    next_refresh = time.monotonic() + refresh_seconds
                due = [(slug, timestamp(json.loads(raw)['kickoff'])) for slug, raw in active_games(db)]
                due = [row for row in due if row[1]-timedelta(hours=2) <= now <= row[1]+timedelta(hours=12)]
                if due:
                    slug = due[cycle % len(due)][0]
                    decision = observe_one(db, client, slug, now)
                    if decision.get('action') == 'shadow_post_only_signal':
                        print(json.dumps({
                            'at': now.isoformat(),
                            'event': 'sports_challenger_shadow_signal',
                            'slug': slug,
                            'execution_enabled': False,
                            'signal': decision['signal'],
                        }), flush=True)
                    elif decision.get('action') == 'market_inactive':
                        print(json.dumps({
                            'at': now.isoformat(),
                            'event': 'sports_challenger_market_retired',
                            'slug': slug,
                            'reason': decision['reason'],
                            'execution_enabled': False,
                        }), flush=True)
            except Exception as exc:
                error = type(exc).__name__ + ':' + str(exc)[:120]
                print(json.dumps({
                    'at': now.isoformat(),
                    'event': 'sports_challenger_cycle_error',
                    'error': error,
                }), flush=True)
            (root/'status.json').write_text(json.dumps(status(db, registry, error), indent=2))
            cycle += 1
            if cycles is None or cycle < cycles:
                time.sleep(30)
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-root', default='/var/data/kalshi-demo-v9/sports-challenger')
    parser.add_argument('--cycles', type=int)
    args = parser.parse_args(argv)
    run(args.data_root, cycles=args.cycles)


if __name__ == '__main__':
    main()
