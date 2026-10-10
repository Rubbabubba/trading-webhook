"""Separate bounded quote/trade research; independent of capped V1 depth tables."""
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
from bisect import bisect_right
from collections import defaultdict

PROTOCOL = json.loads((Path(__file__).parents[1] / 'configs' /
    'kalshi_activity_trade_capture_v5_20261010/registration.json').read_text())


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def init(db, now):
    db.executescript('''CREATE TABLE IF NOT EXISTS activity_trade_v5_protocol(
        id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS activity_trade_v5_polls(
        id INTEGER PRIMARY KEY,attempted_at REAL NOT NULL,ticker TEXT NOT NULL,
        event_id TEXT NOT NULL,quote_json TEXT NOT NULL,detail TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS activity_trade_v5_trades(
        id TEXT PRIMARY KEY,ticker TEXT NOT NULL,event_id TEXT NOT NULL,
        executed_at REAL NOT NULL,observed_at REAL NOT NULL,detail TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS activity_trade_v5_no_update BEFORE UPDATE ON activity_trade_v5_trades
        BEGIN SELECT RAISE(ABORT,'activity_trade_v5_append_only'); END;
        CREATE TRIGGER IF NOT EXISTS activity_trade_v5_no_delete BEFORE DELETE ON activity_trade_v5_trades
        BEGIN SELECT RAISE(ABORT,'activity_trade_v5_append_only'); END;''')
    raw = encoded(PROTOCOL)
    db.execute('INSERT OR IGNORE INTO activity_trade_v5_protocol VALUES(1,?,?)', (raw, now))
    if db.execute('SELECT detail FROM activity_trade_v5_protocol WHERE id=1').fetchone()[0] != raw:
        raise ValueError('activity_trade_v5_protocol_changed')


def quote_summary(frame, now):
    at = frame.get('received_at')
    ticker = frame.get('ticker')
    if (not isinstance(ticker, str) or not 1 <= len(ticker) <= 160
            or isinstance(at, bool) or not isinstance(at, (float, int))
            or not math.isfinite(at) or not 0 <= now - at <= 30):
        raise ValueError('activity_trade_v5_stale_quote')
    bids = {}
    for side in ('yes', 'no'):
        rows = frame['orderbook_fp'][side + '_dollars']
        if not isinstance(rows, list) or not 1 <= len(rows) <= 200:
            raise ValueError('activity_trade_v5_invalid_book')
        levels = [(Decimal(str(price)), Decimal(str(size))) for price, size in rows]
        if any(not p.is_finite() or not s.is_finite() or not 0 < p < 1
               or not 0 <= s <= 1000000000 for p, s in levels):
            raise ValueError('activity_trade_v5_invalid_level')
        active = [(p, s) for p, s in levels if s > 0]
        if not active:
            raise ValueError('activity_trade_v5_empty_side')
        price, size = max(active)
        bids[side] = {'price_dollars': str(price), 'size_fp': str(size)}
    if Decimal(bids['yes']['price_dollars']) + Decimal(bids['no']['price_dollars']) >= 1:
        raise ValueError('activity_trade_v5_crossed_book')
    return {'ticker': ticker, 'observed_at': at, 'best_bids': bids,
            'v12_signal_present_on_scan': frame.get('v12_trial_signal') is not None,
            'execution_enabled': False, 'owned_fill': False}


def ingest(db, payload, *, ticker, event_id, now, received):
    start = db.execute('SELECT started_at FROM activity_trade_v5_protocol WHERE id=1').fetchone()[0]
    rows = payload.get('trades') if isinstance(payload, dict) else None
    cursor = payload.get('cursor') if isinstance(payload, dict) else None
    if (not isinstance(rows, list) or len(rows) > 100 or not isinstance(cursor, str)
            or len(cursor) > 2000 or len(encoded(payload)) > 256000
            or not math.isfinite(received) or received < now):
        raise ValueError('activity_trade_v5_invalid_page')
    prepared = []
    seen = set()
    for row in rows:
        identity = row.get('trade_id')
        if (row.get('ticker') != ticker or not isinstance(identity, str)
                or not 1 <= len(identity) <= 160 or identity in seen):
            raise ValueError('activity_trade_v5_invalid_identity')
        seen.add(identity)
        at = datetime.fromisoformat(row['created_time'].replace('Z', '+00:00'))
        if at.tzinfo is None:
            raise ValueError('activity_trade_v5_timezone_required')
        executed = at.timestamp()
        values = [Decimal(str(row[key])) for key in ('count_fp', 'yes_price_dollars', 'no_price_dollars')]
        size, yes, no = values
        if (any(not v.is_finite() for v in values) or not 0 < size <= 1000000000
                or not 0 <= yes <= 1 or not 0 <= no <= 1):
            raise ValueError('activity_trade_v5_invalid_value')
        if executed < max(start, now - 300):
            continue
        if executed > now:
            raise ValueError('activity_trade_v5_future_trade')
        detail = {'trade_id': identity, 'ticker': ticker, 'event_id': event_id,
            'executed_at': executed, 'count_fp': str(size),
            'yes_price_dollars': str(yes), 'no_price_dollars': str(no),
            'on_book': row.get('is_block_trade') is False,
            'block_classification_known': type(row.get('is_block_trade')) is bool,
            'owned_fill': False, 'execution_enabled': False}
        raw = encoded(detail)
        prior = db.execute('SELECT detail FROM activity_trade_v5_trades WHERE id=?', (identity,)).fetchone()
        if prior and prior[0] != raw:
            raise ValueError('activity_trade_v5_conflicting_identity')
        if not prior:
            prepared.append((identity, ticker, event_id, executed, received, raw))
    if db.execute('SELECT count(*) FROM activity_trade_v5_trades').fetchone()[0] + len(prepared) > 20000:
        raise ValueError('activity_trade_v5_trade_cap')
    db.executemany('INSERT INTO activity_trade_v5_trades VALUES(?,?,?,?,?,?)', prepared)
    return len(prepared), cursor


def poll(db, markets, now):
    last, count = db.execute('SELECT max(attempted_at),count(*) FROM activity_trade_v5_polls').fetchone()
    if count >= 4096 or db.execute('SELECT count(*) FROM activity_trade_v5_trades').fetchone()[0] >= 20000:
        return
    if last is not None and now - last < 180:
        return
    identifier = db.execute('INSERT INTO activity_trade_v5_polls(attempted_at,ticker,event_id,quote_json,detail) '
        'VALUES(?,?,?,?,?)', (now, '', '', '{}', encoded({'state': 'started'}))).lastrowid
    stage='global_read'
    try:
        start = db.execute('SELECT started_at FROM activity_trade_v5_protocol WHERE id=1').fetchone()[0]
        payload, _, received = markets.get_trades(params={'limit':100,'min_ts':int(max(start,now-300)),'max_ts':int(now)})
        stage='global_validation'
        # Reuse strict immutable trade validation in an isolated temporary DB.
        import sqlite3
        with sqlite3.connect(':memory:') as validation:
            init(validation, start)
            rows = payload.get('trades', []) if isinstance(payload,dict) else []
            if (not isinstance(rows,list) or len(rows)>100 or len(encoded(payload))>256000
                    or any(not isinstance(r,dict) for r in rows)):
                raise ValueError('invalid_activity_page')
            cursor=payload.get('cursor')
            if not isinstance(cursor,str) or len(cursor)>2000:
                raise ValueError('invalid_activity_cursor')
            tickers=sorted({r.get('ticker') for r in rows if isinstance(r,dict) and isinstance(r.get('ticker'),str)})
            for ticker in tickers:
                ingest(validation, {'trades':[r for r in rows if r.get('ticker')==ticker],'cursor':''},
                       ticker=ticker,event_id='unresolved',now=now,received=received)
            active=[r[0] for r in validation.execute('SELECT DISTINCT ticker FROM activity_trade_v5_trades')]
        if not active:
            detail={'state':'no_recent_activity','global_page_complete':not bool(cursor),'execution_enabled':False}
        else:
            counts=dict(db.execute('SELECT ticker,count(*) FROM activity_trade_v5_polls WHERE ticker!=? GROUP BY ticker',('',)))
            ticker=min(active,key=lambda t:(counts.get(t,0),t))
            # Count failed selections too, preventing an inactive or malformed
            # contract from monopolizing subsequent polls.
            db.execute('UPDATE activity_trade_v5_polls SET ticker=? WHERE id=?',(ticker,identifier))
            stage='quote_read'
            quote=markets.quote({'ticker':ticker})
            stage='quote_validation'
            event=quote['market']['event_ticker']
            if not isinstance(event,str) or not 1<=len(event)<=160:
                raise ValueError('invalid_activity_event')
            frame={'ticker':ticker,'received_at':quote['observed_at'],'orderbook_fp':quote['orderbook_fp']}
            q=quote_summary(frame,quote['observed_at'])
            # Record receipt time, not the earlier global query time. Earlier
            # trades can never masquerade as trades occurring after this quote.
            db.execute('UPDATE activity_trade_v5_polls SET attempted_at=?,ticker=?,event_id=?,quote_json=? WHERE id=?',
                       (quote['observed_at'],ticker,event,encoded(q),identifier))
            stage='targeted_read'
            p,_,end=markets.get_targeted_trades(params={'ticker':ticker,'limit':100,
                'min_ts':int(max(start,now-300)),'max_ts':int(quote['observed_at'])})
            stage='targeted_validation'
            added,next_cursor=ingest(db,p,ticker=ticker,event_id=event,now=quote['observed_at'],received=end)
            detail={'state':'observed','new_trades':added,'ticker_window_complete':not bool(next_cursor),
                    'global_page_complete':not bool(cursor),'execution_enabled':False}
    except Exception as error:
        detail={'state':'blocked','error_code':'activity_trade_v5_capture_failed',
                'failure_stage':stage,'error_type':type(error).__name__,'execution_enabled':False}
    db.execute('UPDATE activity_trade_v5_polls SET detail=? WHERE id=?',(encoded(detail),identifier))


def prior_quote_coverage(db):
    """Count sampled trades with a quote already recorded before execution.

    A later quote or the existence of a public trade cannot prove our fill.
    This diagnostic changes no frozen protocol, observations or trading gates.
    """
    quotes = defaultdict(list)
    for ticker, attempted, raw in db.execute(
            'SELECT ticker,attempted_at,quote_json FROM activity_trade_v5_polls ORDER BY attempted_at LIMIT 4096'):
        quote = json.loads(raw)
        observed = quote.get('observed_at')
        if (type(observed) in (int, float) and math.isfinite(observed)
                and observed <= attempted):
            # The quote must also have been recorded before the trade.
            quotes[ticker].append((attempted, observed))
    matched = absent = stale = 0
    events = set()
    for ticker, event, executed in db.execute(
            'SELECT ticker,event_id,executed_at FROM activity_trade_v5_trades LIMIT 20000'):
        rows = quotes.get(ticker, [])
        index = bisect_right(rows, (executed, float('inf'))) - 1
        if index < 0:
            absent += 1
        elif executed - rows[index][1] > 30:
            stale += 1
        else:
            matched += 1
            events.add(event)
    return {'matched_prior_quote_trades': matched, 'matched_prior_quote_events': len(events),
            'no_prior_quote_trades': absent, 'prior_quote_too_old_trades': stale,
            'price_side_and_queue_not_assessed': True}


def status(db):
    raw, start = db.execute('SELECT detail,started_at FROM activity_trade_v5_protocol WHERE id=1').fetchone()
    polls = db.execute('SELECT count(*) FROM activity_trade_v5_polls').fetchone()[0]
    trades, events, onbook = db.execute("SELECT count(*),count(DISTINCT event_id),coalesce(sum(json_extract(detail,'$.on_book')),0) FROM activity_trade_v5_trades").fetchone()
    last = db.execute('SELECT detail FROM activity_trade_v5_polls ORDER BY id DESC LIMIT 1').fetchone()
    capped = polls >= 4096 or trades >= 20000
    ready = events >= 50 and onbook >= 100
    return {'schema': PROTOCOL['schema'], 'protocol_sha256': hashlib.sha256(raw.encode()).hexdigest(),
            **prior_quote_coverage(db),
            'state': ('coverage_ready_for_review' if ready else
                      'capacity_insufficient_coverage' if capped else 'collecting'),
            'started_at': start, 'polls': polls, 'public_trades': trades, 'trade_events': events,
            'onbook_trades': onbook, 'latest_poll': json.loads(last[0]) if last else None,
            'capacity_reached': capped,
            'coverage_review_ready': ready,
            'execution_enabled': False, 'owned_fills': 0, 'promotion_ready': False,
            'global_coverage': False, 'quotes_after_trade_are_not_prior_depth': True}
