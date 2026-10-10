"""Separate bounded quote/trade research; independent of capped V1 depth tables."""
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path

PROTOCOL = json.loads((Path(__file__).parents[1] / 'configs' /
    'kalshi_scan_trade_capture_v4_20261010/registration.json').read_text())


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def init(db, now):
    db.executescript('''CREATE TABLE IF NOT EXISTS scan_trade_v4_protocol(
        id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS scan_trade_v4_polls(
        id INTEGER PRIMARY KEY,attempted_at REAL NOT NULL,ticker TEXT NOT NULL,
        event_id TEXT NOT NULL,quote_json TEXT NOT NULL,detail TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS scan_trade_v4_trades(
        id TEXT PRIMARY KEY,ticker TEXT NOT NULL,event_id TEXT NOT NULL,
        executed_at REAL NOT NULL,observed_at REAL NOT NULL,detail TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS scan_trade_v4_no_update BEFORE UPDATE ON scan_trade_v4_trades
        BEGIN SELECT RAISE(ABORT,'scan_trade_v4_append_only'); END;
        CREATE TRIGGER IF NOT EXISTS scan_trade_v4_no_delete BEFORE DELETE ON scan_trade_v4_trades
        BEGIN SELECT RAISE(ABORT,'scan_trade_v4_append_only'); END;''')
    raw = encoded(PROTOCOL)
    db.execute('INSERT OR IGNORE INTO scan_trade_v4_protocol VALUES(1,?,?)', (raw, now))
    if db.execute('SELECT detail FROM scan_trade_v4_protocol WHERE id=1').fetchone()[0] != raw:
        raise ValueError('scan_trade_v4_protocol_changed')


def quote_summary(frame, now):
    at = frame.get('received_at')
    ticker = frame.get('ticker')
    if (not isinstance(ticker, str) or not 1 <= len(ticker) <= 160
            or isinstance(at, bool) or not isinstance(at, (float, int))
            or not math.isfinite(at) or not 0 <= now - at <= 30):
        raise ValueError('scan_trade_v4_stale_quote')
    bids = {}
    for side in ('yes', 'no'):
        rows = frame['orderbook_fp'][side + '_dollars']
        if not isinstance(rows, list) or not 1 <= len(rows) <= 200:
            raise ValueError('scan_trade_v4_invalid_book')
        levels = [(Decimal(str(price)), Decimal(str(size))) for price, size in rows]
        if any(not p.is_finite() or not s.is_finite() or not 0 < p < 1
               or not 0 <= s <= 1000000000 for p, s in levels):
            raise ValueError('scan_trade_v4_invalid_level')
        active = [(p, s) for p, s in levels if s > 0]
        if not active:
            raise ValueError('scan_trade_v4_empty_side')
        price, size = max(active)
        bids[side] = {'price_dollars': str(price), 'size_fp': str(size)}
    if Decimal(bids['yes']['price_dollars']) + Decimal(bids['no']['price_dollars']) >= 1:
        raise ValueError('scan_trade_v4_crossed_book')
    return {'ticker': ticker, 'observed_at': at, 'best_bids': bids,
            'v12_signal_present_on_scan': frame.get('v12_trial_signal') is not None,
            'execution_enabled': False, 'owned_fill': False}


def ingest(db, payload, *, ticker, event_id, now, received):
    start = db.execute('SELECT started_at FROM scan_trade_v4_protocol WHERE id=1').fetchone()[0]
    rows = payload.get('trades') if isinstance(payload, dict) else None
    cursor = payload.get('cursor') if isinstance(payload, dict) else None
    if (not isinstance(rows, list) or len(rows) > 100 or not isinstance(cursor, str)
            or len(cursor) > 2000 or len(encoded(payload)) > 256000
            or not math.isfinite(received) or received < now):
        raise ValueError('scan_trade_v4_invalid_page')
    prepared = []
    seen = set()
    for row in rows:
        identity = row.get('trade_id')
        if (row.get('ticker') != ticker or not isinstance(identity, str)
                or not 1 <= len(identity) <= 160 or identity in seen):
            raise ValueError('scan_trade_v4_invalid_identity')
        seen.add(identity)
        at = datetime.fromisoformat(row['created_time'].replace('Z', '+00:00'))
        if at.tzinfo is None:
            raise ValueError('scan_trade_v4_timezone_required')
        executed = at.timestamp()
        values = [Decimal(str(row[key])) for key in ('count_fp', 'yes_price_dollars', 'no_price_dollars')]
        size, yes, no = values
        if (any(not v.is_finite() for v in values) or not 0 < size <= 1000000000
                or not 0 <= yes <= 1 or not 0 <= no <= 1):
            raise ValueError('scan_trade_v4_invalid_value')
        if executed < max(start, now - 300):
            continue
        if executed > now:
            raise ValueError('scan_trade_v4_future_trade')
        detail = {'trade_id': identity, 'ticker': ticker, 'event_id': event_id,
            'executed_at': executed, 'count_fp': str(size),
            'yes_price_dollars': str(yes), 'no_price_dollars': str(no),
            'on_book': row.get('is_block_trade') is False,
            'block_classification_known': type(row.get('is_block_trade')) is bool,
            'owned_fill': False, 'execution_enabled': False}
        raw = encoded(detail)
        prior = db.execute('SELECT detail FROM scan_trade_v4_trades WHERE id=?', (identity,)).fetchone()
        if prior and prior[0] != raw:
            raise ValueError('scan_trade_v4_conflicting_identity')
        if not prior:
            prepared.append((identity, ticker, event_id, executed, received, raw))
    if db.execute('SELECT count(*) FROM scan_trade_v4_trades').fetchone()[0] + len(prepared) > 20000:
        raise ValueError('scan_trade_v4_trade_cap')
    db.executemany('INSERT INTO scan_trade_v4_trades VALUES(?,?,?,?,?,?)', prepared)
    return len(prepared), cursor


def poll(db, markets, frame, event_id, now):
    last, count = db.execute('SELECT max(attempted_at),count(*) FROM scan_trade_v4_polls').fetchone()
    if count >= 4096 or db.execute('SELECT count(*) FROM scan_trade_v4_trades').fetchone()[0] >= 20000:
        return
    if last is not None and now - last < 60:
        return
    if not isinstance(event_id, str) or not 1 <= len(event_id) <= 160:
        raise ValueError('scan_trade_v4_invalid_event')
    quote = quote_summary(frame, now)
    ticker = quote['ticker']
    # Persist the attempt before transport so errors or restarts cannot bypass cadence.
    identifier = db.execute('INSERT INTO scan_trade_v4_polls(attempted_at,ticker,event_id,quote_json,detail) '
        'VALUES(?,?,?,?,?)', (now, ticker, event_id, encoded(quote), encoded({'state': 'started'}))).lastrowid
    pages = trades = 0
    cursor = None
    try:
        start = db.execute('SELECT started_at FROM scan_trade_v4_protocol WHERE id=1').fetchone()[0]
        while pages < 2:
            params = {'ticker': ticker, 'min_ts': int(max(start, now - 300)), 'max_ts': int(now), 'limit': 100}
            if cursor:
                params['cursor'] = cursor
            payload, _, received = markets.get_targeted_trades(params=params)
            added, cursor = ingest(db, payload, ticker=ticker, event_id=event_id, now=now, received=received)
            pages += 1
            trades += added
            if not cursor:
                break
        detail = {'state': 'observed', 'pages': pages, 'new_trades': trades,
                  'ticker_window_complete': not bool(cursor), 'execution_enabled': False}
    except Exception:
        detail = {'state': 'blocked', 'pages': pages, 'new_trades': trades,
                  'error_code': 'scan_trade_v4_capture_failed', 'ticker_window_complete': False,
                  'execution_enabled': False}
    db.execute('UPDATE scan_trade_v4_polls SET detail=? WHERE id=?', (encoded(detail), identifier))


def status(db):
    raw, start = db.execute('SELECT detail,started_at FROM scan_trade_v4_protocol WHERE id=1').fetchone()
    polls = db.execute('SELECT count(*) FROM scan_trade_v4_polls').fetchone()[0]
    trades, events, onbook = db.execute("SELECT count(*),count(DISTINCT event_id),coalesce(sum(json_extract(detail,'$.on_book')),0) FROM scan_trade_v4_trades").fetchone()
    last = db.execute('SELECT detail FROM scan_trade_v4_polls ORDER BY id DESC LIMIT 1').fetchone()
    capped = polls >= 4096 or trades >= 20000
    ready = events >= 50 and onbook >= 100
    return {'schema': PROTOCOL['schema'], 'protocol_sha256': hashlib.sha256(raw.encode()).hexdigest(),
            'state': ('coverage_ready_for_review' if ready else
                      'capacity_insufficient_coverage' if capped else 'collecting'),
            'started_at': start, 'polls': polls, 'public_trades': trades, 'trade_events': events,
            'onbook_trades': onbook, 'latest_poll': json.loads(last[0]) if last else None,
            'capacity_reached': capped,
            'coverage_review_ready': ready,
            'execution_enabled': False, 'owned_fills': 0, 'promotion_ready': False,
            'global_coverage': False, 'quotes_after_trade_are_not_prior_depth': True}
