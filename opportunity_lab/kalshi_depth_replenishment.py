"""Frozen prospective depth recorder. Displayed depletion is never a fill."""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math

PROTOCOL = {
    'capability_id': 'depth_replenishment_quote_v1', 'version': 1,
    'maximum_snapshots': 20000, 'maximum_transition_gap_seconds': 120,
    'depletion_ratio': '0.20', 'refill_ratio': '0.80',
    'maximum_levels_per_side': 20, 'execution_enabled': False,
    'fill_assumed': False, 'profitability_evidence': False,
    'hypothesis': 'Displayed depth may recover after depletion at an unchanged bid; measure prospectively before proposing execution.',
    'required_independent_events': 50, 'required_depletion_episodes': 200,
    'missing_for_execution': ['trade-versus-cancel attribution', 'announcement exclusions',
                              'fee-verified latency replay', 'actual Demo fills', 'untouched holdout'],
}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def init(db, *, now=None):
    raw = encoded(PROTOCOL)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    at = datetime.now(timezone.utc).timestamp() if now is None else now
    db.execute('CREATE TABLE IF NOT EXISTS depth_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,sha256 TEXT NOT NULL,started_at REAL NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS depth_snapshots(id TEXT PRIMARY KEY,event_id TEXT NOT NULL,ticker TEXT NOT NULL,observed_at REAL NOT NULL,detail TEXT NOT NULL)')
    db.execute("CREATE TRIGGER IF NOT EXISTS depth_no_update BEFORE UPDATE ON depth_snapshots BEGIN SELECT RAISE(ABORT,'depth_snapshots_append_only'); END")
    db.execute("CREATE TRIGGER IF NOT EXISTS depth_no_delete BEFORE DELETE ON depth_snapshots BEGIN SELECT RAISE(ABORT,'depth_snapshots_append_only'); END")
    db.execute('CREATE TABLE IF NOT EXISTS depth_latest(ticker TEXT PRIMARY KEY,observed_at REAL NOT NULL,detail TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS depth_episodes(id TEXT PRIMARY KEY,event_id TEXT NOT NULL,ticker TEXT NOT NULL,side TEXT NOT NULL,started_at REAL NOT NULL,detail TEXT NOT NULL,resolution TEXT)')
    db.execute('CREATE INDEX IF NOT EXISTS depth_pending ON depth_episodes(ticker,resolution)')
    db.execute('INSERT OR IGNORE INTO depth_protocol VALUES(1,?,?,?)', (raw, digest, at))
    if db.execute('SELECT detail FROM depth_protocol WHERE id=1').fetchone()[0] != raw:
        raise ValueError('depth_protocol_changed')


def _levels(book, key):
    rows = book.get(key)
    if not isinstance(rows, list) or len(rows) > PROTOCOL['maximum_levels_per_side']:
        raise ValueError('invalid_depth_levels')
    levels = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            raise ValueError('invalid_depth_row')
        price, size = (Decimal(str(value)) for value in row)
        if not price.is_finite() or not size.is_finite() or not 0 < price < 1 or not 0 <= size <= 1000000000:
            raise ValueError('invalid_depth_value')
        if price in levels:
            raise ValueError('duplicate_depth_price')
        levels[price] = size
    return [[str(price), str(size)] for price, size in sorted(levels.items(), reverse=True)]


def _best(levels):
    return next(((Decimal(price), Decimal(size)) for price, size in levels if Decimal(size) > 0), None)


def capture(db, ticker, event_id, frame):
    protocol = db.execute('SELECT started_at,sha256 FROM depth_protocol WHERE id=1').fetchone()
    observed = frame.get('received_at')
    if (not protocol or not isinstance(ticker, str) or not 1 <= len(ticker) <= 160
            or not isinstance(event_id, str) or not 1 <= len(event_id) <= 160
            or frame.get('ticker') != ticker or isinstance(observed, bool)
            or not isinstance(observed, (int, float)) or not math.isfinite(observed)
            or observed < protocol[0]):
        raise ValueError('invalid_depth_identity_or_time')
    if db.execute('SELECT count(*) FROM depth_snapshots').fetchone()[0] >= PROTOCOL['maximum_snapshots']:
        return {'captured': False, 'reason': 'snapshot_cap_reached'}
    book = frame.get('orderbook_fp')
    if not isinstance(book, dict):
        raise ValueError('invalid_depth_book')
    sides = {side: _levels(book, side + '_dollars') for side in ('yes', 'no')}
    best = {side: _best(rows) for side, rows in sides.items()}
    if best['yes'] and best['no'] and best['yes'][0] + best['no'][0] >= 1:
        raise ValueError('crossed_depth_book')
    previous = db.execute('SELECT observed_at,detail FROM depth_latest WHERE ticker=?', (ticker,)).fetchone()
    if previous and observed <= previous[0]:
        return {'captured': False, 'reason': 'nonincreasing_snapshot'}
    if previous and json.loads(previous[1])['event_id'] != event_id:
        raise ValueError('depth_event_identity_changed')
    snapshot = {'ticker': ticker, 'event_id': event_id, 'observed_at': observed,
                'protocol_sha256': protocol[1], 'sides': sides,
                'fill_assumed': False, 'execution_enabled': False}
    digest = hashlib.sha256(encoded(snapshot).encode()).hexdigest()
    db.execute('SAVEPOINT depth_capture')
    try:
        db.execute('INSERT INTO depth_snapshots VALUES(?,?,?,?,?)', (digest, event_id, ticker, observed, encoded(snapshot)))
        for episode_id, side, started_at, raw in db.execute(
            'SELECT id,side,started_at,detail FROM depth_episodes WHERE ticker=? AND resolution IS NULL', (ticker,)).fetchall():
            episode = json.loads(raw); current = best[side]
            reason = None
            if observed - started_at > PROTOCOL['maximum_transition_gap_seconds']:
                reason = 'incomplete_sampling_gap'
            elif current and current[0] != Decimal(episode['price']):
                reason = 'bid_price_changed'
            elif current and current[1] >= Decimal(episode['baseline_size']) * Decimal(PROTOCOL['refill_ratio']):
                reason = 'displayed_refill_observed'
            if reason:
                db.execute('UPDATE depth_episodes SET resolution=? WHERE id=?',
                           (encoded({'state': reason, 'observed_at': observed,
                                     'elapsed_seconds': observed - started_at,
                                     'actual_fills': 0, 'realized_net_cents': None}), episode_id))
        if previous and observed - previous[0] <= PROTOCOL['maximum_transition_gap_seconds']:
            prior = json.loads(previous[1])
            for side in ('yes', 'no'):
                before, after = _best(prior['sides'][side]), best[side]
                pending = db.execute('SELECT 1 FROM depth_episodes WHERE ticker=? AND side=? AND resolution IS NULL', (ticker, side)).fetchone()
                if (not pending and before and after and before[0] == after[0] and before[1] >= 1
                        and after[1] <= before[1] * Decimal(PROTOCOL['depletion_ratio'])):
                    episode = {'price': str(before[0]), 'baseline_size': str(before[1]),
                               'remaining_size': str(after[1]), 'prior_observed_at': previous[0],
                               'snapshot_id': digest, 'trade_cancel_attribution': 'unknown',
                               'fill_assumed': False, 'execution_enabled': False}
                    episode_id = hashlib.sha256((digest + side).encode()).hexdigest()
                    db.execute('INSERT INTO depth_episodes VALUES(?,?,?,?,?,?,NULL)',
                               (episode_id, event_id, ticker, side, observed, encoded(episode)))
        db.execute('INSERT OR REPLACE INTO depth_latest VALUES(?,?,?)', (ticker, observed, encoded(snapshot)))
        db.execute('RELEASE depth_capture')
    except Exception:
        db.execute('ROLLBACK TO depth_capture'); db.execute('RELEASE depth_capture'); raise
    return {'captured': True, 'snapshot_id': digest}


def status(db):
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='depth_protocol'").fetchone()
    if not exists:
        return None
    started, digest = db.execute('SELECT started_at,sha256 FROM depth_protocol WHERE id=1').fetchone()
    count, events, latest = db.execute('SELECT count(*),count(DISTINCT event_id),max(observed_at) FROM depth_snapshots').fetchone()
    episodes = db.execute('SELECT count(*) FROM depth_episodes').fetchone()[0]
    refills = db.execute("SELECT count(*) FROM depth_episodes WHERE json_extract(resolution,'$.state')='displayed_refill_observed'").fetchone()[0]
    incomplete = db.execute("SELECT count(*) FROM depth_episodes WHERE json_extract(resolution,'$.state') IN ('incomplete_sampling_gap','bid_price_changed')").fetchone()[0]
    return {'schema': 'depth_replenishment_quote_v1', 'protocol_sha256': digest,
            'started_at': started, 'latest_observed_at': latest, 'snapshots': count,
            'independent_events': events, 'depletion_episodes': episodes,
            'displayed_refills': refills, 'incomplete_episodes': incomplete,
            'pending_episodes': episodes - refills - incomplete,
            'capacity_reached': count >= PROTOCOL['maximum_snapshots'],
            'execution_enabled': False, 'fill_assumed': False,
            'actual_fills': 0, 'realized_net_cents': None, 'promotion_ready': False}
