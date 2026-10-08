"""Separate read-only Demo trade sample for fresh, observed-depth tickers."""
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import math

PROTOCOL = {
    'schema': 'kalshi_depth_cohort_trades_v3', 'environment': 'demo',
    'hypothesis': 'Rotating fresh-depth tickers finds public trade activity missed by a single all-market page.',
    'candidate_source': 'fresh_depth_ticker_when_no_fresh_v12_signal',
    'lookback_seconds': 300, 'poll_interval_seconds': 60,
    'page_limit': 100, 'maximum_pages_per_poll': 2,
    'maximum_polls': 4096, 'maximum_trades': 20000,
    'execution_enabled': False, 'fill_assumed': False,
    'profitability_evidence': False, 'global_coverage_claim': False,
    'review_is_not_execution_approval': True,
}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def init(db, now):
    db.execute('CREATE TABLE IF NOT EXISTS depth_trade_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS depth_cohort_trades(id TEXT PRIMARY KEY,ticker TEXT NOT NULL,executed_at REAL NOT NULL,observed_at REAL NOT NULL,detail TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS depth_trade_polls(id INTEGER PRIMARY KEY,attempted_at REAL NOT NULL,ticker TEXT,detail TEXT NOT NULL)')
    for action in ('UPDATE', 'DELETE'):
        db.execute('CREATE TRIGGER IF NOT EXISTS depth_cohort_no_'+action.lower()+
                   ' BEFORE '+action+' ON depth_cohort_trades BEGIN SELECT RAISE(ABORT,\'depth_cohort_append_only\'); END')
    raw = encoded(PROTOCOL)
    db.execute('INSERT OR IGNORE INTO depth_trade_protocol VALUES(1,?,?)', (raw, now))
    if db.execute('SELECT detail FROM depth_trade_protocol WHERE id=1').fetchone()[0] != raw:
        raise ValueError('depth_trade_protocol_changed')


def candidate(db, now):
    row = db.execute('''SELECT d.ticker FROM depth_latest d
        LEFT JOIN (SELECT ticker,max(attempted_at) last_at FROM depth_trade_polls
                   WHERE ticker IS NOT NULL GROUP BY ticker) p ON p.ticker=d.ticker
        WHERE d.observed_at>=?
        ORDER BY coalesce(p.last_at,0),d.observed_at DESC LIMIT 1''', (now-120,)).fetchone()
    return row[0] if row else None


def ingest(db, payload, *, ticker, attempted_at, received_at):
    start = db.execute('SELECT started_at FROM depth_trade_protocol WHERE id=1').fetchone()[0]
    rows = payload.get('trades') if isinstance(payload, dict) else None
    cursor = payload.get('cursor') if isinstance(payload, dict) else None
    if (not isinstance(rows, list) or len(rows)>100 or not isinstance(cursor, str)
            or len(cursor)>2000 or len(encoded(payload))>256000
            or not math.isfinite(received_at) or received_at<attempted_at):
        raise ValueError('invalid_depth_trade_page')
    lower = max(start, attempted_at-300)
    prepared = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or row.get('ticker') != ticker:
            raise ValueError('depth_trade_ticker_mismatch')
        identity = row.get('trade_id')
        if not isinstance(identity, str) or not 1<=len(identity)<=160 or identity in seen:
            raise ValueError('invalid_depth_trade_identity')
        seen.add(identity)
        at = datetime.fromisoformat(row['created_time'].replace('Z','+00:00'))
        if at.tzinfo is None: raise ValueError('depth_trade_timezone_required')
        executed = at.timestamp()
        size = Decimal(row['count_fp'])
        yes = Decimal(row['yes_price_dollars'])
        no = Decimal(row['no_price_dollars'])
        if (not size.is_finite() or not 0<size<=1000000000
                or not yes.is_finite() or not no.is_finite()
                or not 0<=yes<=1 or not 0<=no<=1):
            raise ValueError('invalid_depth_trade_value')
        if executed<lower: continue
        if executed>attempted_at: raise ValueError('depth_trade_after_requested_window')
        detail = {'trade_id':identity,'ticker':ticker,'executed_at':executed,
                  'count_fp':str(size),'yes_price_dollars':str(yes),'no_price_dollars':str(no),
                  'on_book':row.get('is_block_trade') is False,
                  'block_classification_known':type(row.get('is_block_trade')) is bool,
                  'owned_fill':False,'execution_enabled':False}
        raw = encoded(detail)
        prior = db.execute('SELECT detail FROM depth_cohort_trades WHERE id=?',(identity,)).fetchone()
        if prior and prior[0]!=raw: raise ValueError('conflicting_depth_trade_id')
        prepared.append((identity,ticker,executed,received_at,raw))
    new = sum(not db.execute('SELECT 1 FROM depth_cohort_trades WHERE id=?',(r[0],)).fetchone()
              for r in prepared)
    if db.execute('SELECT count(*) FROM depth_cohort_trades').fetchone()[0]+new>20000:
        raise ValueError('depth_trade_capacity_reached')
    db.executemany('INSERT OR IGNORE INTO depth_cohort_trades VALUES(?,?,?,?,?)',prepared)
    return len(prepared),cursor


def poll(db, markets, now, *, signal_candidate=None):
    last,count = db.execute('SELECT max(attempted_at),count(*) FROM depth_trade_polls').fetchone()
    if count>=4096 or db.execute('SELECT count(*) FROM depth_cohort_trades').fetchone()[0]>=20000:
        return
    if last is not None and now-last<60: return
    if signal_candidate is not None:
        return
    ticker = candidate(db,now)
    if not ticker:
        detail = {'state':'no_fresh_depth','execution_enabled':False,'global_coverage':False}
    else:
        cursor = None
        pages = 0
        rows = 0
        try:
            start = db.execute('SELECT started_at FROM depth_trade_protocol WHERE id=1').fetchone()[0]
            while pages<2:
                params = {'limit':100,'min_ts':int(max(start,now-300)),
                          'max_ts':int(now),'ticker':ticker}
                if cursor: params['cursor']=cursor
                payload,_,received = markets.get_targeted_trades(params=params)
                added,cursor = ingest(db,payload,ticker=ticker,attempted_at=now,received_at=received)
                rows += added
                pages += 1
                if not cursor: break
            detail = {'state':'observed','ticker':ticker,'pages':pages,'rows':rows,
                      'window_start':max(start,now-300),'window_end':now,
                      'ticker_window_complete':not bool(cursor),
                      'global_coverage':False,'execution_enabled':False,'owned_fills':0}
        except Exception as error:
            code = str(error)
            detail = {'state':'blocked','ticker':ticker,'pages':pages,'rows':rows,
                      'error_code':code if code.startswith(('demo_market_http_','stale_demo_market_data',
                          'invalid_depth_trade','depth_trade_','conflicting_depth_trade'))
                          else 'depth_trade_probe_failed',
                      'ticker_window_complete':False,'global_coverage':False,'execution_enabled':False}
    db.execute('INSERT INTO depth_trade_polls(attempted_at,ticker,detail) VALUES(?,?,?)',
               (now,ticker,encoded(detail)))


def status(db):
    row = db.execute('SELECT detail,started_at FROM depth_trade_protocol WHERE id=1').fetchone()
    if not row: return None
    last = db.execute('SELECT detail FROM depth_trade_polls ORDER BY id DESC LIMIT 1').fetchone()
    return {'schema':PROTOCOL['schema'],'protocol_sha256':hashlib.sha256(row[0].encode()).hexdigest(),
            'started_at':row[1],
            'polls':db.execute('SELECT count(*) FROM depth_trade_polls').fetchone()[0],
            'tickers':db.execute('SELECT count(DISTINCT ticker) FROM depth_trade_polls WHERE ticker IS NOT NULL').fetchone()[0],
            'public_trades':db.execute('SELECT count(*) FROM depth_cohort_trades').fetchone()[0],
            'onbook_trades':db.execute("SELECT coalesce(sum(json_extract(detail,'$.on_book')),0) FROM depth_cohort_trades").fetchone()[0],
            'latest_poll':json.loads(last[0]) if last else None,
            'continuous_coverage':False,'execution_enabled':False,
            'actual_owned_fills':0,'promotion_ready':False}
