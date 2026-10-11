"""Prospective price/depth context only; public prints never establish fills."""
from decimal import Decimal, InvalidOperation
from bisect import bisect_right
from pathlib import Path
import json
from .kalshi_activity_trade_capture import encoded

PROTOCOL = json.loads((Path(__file__).parents[1] / 'configs/kalshi_postquote_diagnostic_v7_20261011/registration.json').read_text())


def init(db, now):
    db.execute('CREATE TABLE IF NOT EXISTS postquote_v7_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL)')
    raw = encoded(PROTOCOL)
    db.execute('INSERT OR IGNORE INTO postquote_v7_protocol VALUES(1,?,?)', (raw, now))
    if db.execute('SELECT detail FROM postquote_v7_protocol WHERE id=1').fetchone()[0] != raw:
        raise ValueError('postquote_diagnostic_protocol_changed')


def compare(quote, trade):
    try:
        if quote['ticker'] != trade['ticker'] or trade.get('on_book') is not True:
            return None
        bids = [Decimal(str(quote['best_bids'][s]['price_dollars'])) for s in ('yes', 'no')]
        depth = [Decimal(str(quote['best_bids'][s]['size_fp'])) for s in ('yes', 'no')]
        prices = [Decimal(str(trade[s + '_price_dollars'])) for s in ('yes', 'no')]
        size = Decimal(str(trade['count_fp']))
        if (any(not x.is_finite() for x in bids + depth + prices + [size])
                or any(not 0 < x < 1 for x in bids) or sum(bids) >= 1
                or any(x <= 0 for x in depth) or size <= 0
                or any(not 0 <= x <= 1 for x in prices) or sum(prices) != 1):
            return None
        return {side: {'at_or_below_prior_bid': prices[i] <= bids[i],
                       'print_size_at_least_prior_depth': size >= depth[i]}
                for i, side in enumerate(('yes', 'no'))}
    except (KeyError, TypeError, InvalidOperation, ValueError):
        return None


def status(db):
    start = db.execute('SELECT started_at FROM postquote_v7_protocol WHERE id=1').fetchone()[0]
    windows = db.execute('SELECT source_id,at,ticker,event_id,quote_json,detail FROM postquote_v6_windows WHERE at>=? ORDER BY source_id LIMIT 4096', (start,)).fetchall()
    trades = db.execute('SELECT id,ticker,event_id,executed_at,detail FROM postquote_v6_trades WHERE executed_at>? ORDER BY executed_at,id LIMIT 20000', (start,)).fetchall()
    by_market = {}
    for identity, ticker, event, at, detail in trades:
        by_market.setdefault((ticker,event), []).append((identity,at,detail))
    times = {key:[row[1] for row in rows] for key,rows in by_market.items()}
    seen = set(); invalid = set(); events = set(); incomplete = 0
    counts = {side: {'comparable_prints':0,'at_or_below_prior_bid':0,'print_size_at_least_prior_depth':0} for side in ('yes','no')}
    for source, at, ticker, event, quote, detail in windows:
        meta = json.loads(detail)
        if meta.get('state') != 'observed' or meta.get('page_complete') is not True:
            incomplete += 1
            continue
        key = (ticker,event)
        market_rows = by_market.get(key, [])
        market_times = times.get(key, [])
        left = bisect_right(market_times, at)
        right = bisect_right(market_times, at + 30)
        for identity, executed, raw in market_rows[left:right]:
            if identity in seen or not at < executed <= at + 30:
                continue
            # Earliest complete eligible quote owns a print; overlapping windows cannot inflate counts.
            seen.add(identity)
            result = compare(json.loads(quote), json.loads(raw))
            if result is None:
                invalid.add(identity)
                continue
            events.add(event)
            for side in counts:
                counts[side]['comparable_prints'] += 1
                for name, value in result[side].items():
                    counts[side][name] += int(value)
    return {'schema':PROTOCOL['schema'],'started_at':start,'prospective_windows':len(windows),
            'incomplete_windows':incomplete,'distinct_compared_prints':len(seen-invalid),
            'excluded_prints':len(invalid),'independent_events':len(events),'by_side':counts,
            'aggressor_side_known':False,'queue_position_known':False,'fee_edge_assessed':False,
            'owned_fills':0,'execution_enabled':False,'promotion_ready':False,
            'scope':'Public price/depth context; no fill probability or profit claim'}
