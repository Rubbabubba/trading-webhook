"""Prospective, ticker-specific Demo trades for a separate fillability research protocol."""
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import math

PROTOCOL = {
    'schema': 'kalshi_targeted_trade_liquidity_v2', 'environment': 'demo',
    'hypothesis': 'Ticker-filtered public trades cover more relevant events than one all-market page.',
    'candidate_source': 'recent_v12_shadow_signal_with_fresh_depth',
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
    db.execute('CREATE TABLE IF NOT EXISTS targeted_trade_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS targeted_public_trades(id TEXT PRIMARY KEY,ticker TEXT NOT NULL,executed_at REAL NOT NULL,observed_at REAL NOT NULL,detail TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS targeted_trade_polls(id INTEGER PRIMARY KEY,attempted_at REAL NOT NULL,ticker TEXT,detail TEXT NOT NULL)')
    for action in ('UPDATE', 'DELETE'):
        db.execute("CREATE TRIGGER IF NOT EXISTS targeted_trades_no_"+action.lower()+
                   " BEFORE "+action+" ON targeted_public_trades BEGIN SELECT RAISE(ABORT,'targeted_trades_append_only'); END")
    raw = encoded(PROTOCOL)
    db.execute('INSERT OR IGNORE INTO targeted_trade_protocol VALUES(1,?,?)', (raw,now))
    if db.execute('SELECT detail FROM targeted_trade_protocol WHERE id=1').fetchone()[0] != raw:
        raise ValueError('targeted_trade_protocol_changed')


def candidate(db, now):
    """Rotate only among recent signal tickers with fresh observed depth."""
    row = db.execute('''SELECT s.ticker FROM v12_shadow_signals s
        JOIN depth_latest d ON d.ticker=s.ticker
        LEFT JOIN (SELECT ticker,max(attempted_at) last_at FROM targeted_trade_polls
                   WHERE ticker IS NOT NULL GROUP BY ticker) p ON p.ticker=s.ticker
        WHERE s.observed_at>=? AND d.observed_at>=?
        GROUP BY s.ticker
        ORDER BY coalesce(p.last_at,0),max(s.observed_at) DESC LIMIT 1''',
        (now-1800,now-120)).fetchone()
    return row[0] if row else None


def ingest(db, payload, *, ticker, attempted_at, received_at):
    start = db.execute('SELECT started_at FROM targeted_trade_protocol WHERE id=1').fetchone()[0]
    rows = payload.get('trades') if isinstance(payload,dict) else None
    cursor = payload.get('cursor') if isinstance(payload,dict) else None
    if (not isinstance(rows,list) or len(rows)>100 or not isinstance(cursor,str)
            or len(cursor)>2000 or len(encoded(payload))>256000
            or not math.isfinite(received_at) or received_at<attempted_at):
        raise ValueError('invalid_targeted_trade_page')
    lower = max(start,attempted_at-300)
    prepared = []
    seen = set()
    for row in rows:
        if not isinstance(row,dict) or row.get('ticker')!=ticker:
            raise ValueError('targeted_trade_ticker_mismatch')
        identity = row.get('trade_id')
        if not isinstance(identity,str) or not 1<=len(identity)<=160 or identity in seen:
            raise ValueError('invalid_targeted_trade_identity')
        seen.add(identity)
        at = datetime.fromisoformat(row['created_time'].replace('Z','+00:00'))
        if at.tzinfo is None: raise ValueError('targeted_trade_timezone_required')
        executed = at.timestamp()
        size = Decimal(row['count_fp'])
        yes = Decimal(row['yes_price_dollars'])
        no = Decimal(row['no_price_dollars'])
        if (not size.is_finite() or not 0<size<=1000000000
                or not yes.is_finite() or not no.is_finite()
                or not 0<=yes<=1 or not 0<=no<=1):
            raise ValueError('invalid_targeted_trade_value')
        if executed<lower: continue
        if executed>attempted_at: raise ValueError('targeted_trade_after_requested_window')
        detail = {'trade_id':identity,'ticker':ticker,'executed_at':executed,
                  'count_fp':str(size),'yes_price_dollars':str(yes),'no_price_dollars':str(no),
                  'on_book':row.get('is_block_trade') is False,
                  'block_classification_known':type(row.get('is_block_trade')) is bool,
                  'owned_fill':False,'execution_enabled':False}
        raw = encoded(detail)
        prior = db.execute('SELECT detail FROM targeted_public_trades WHERE id=?',(identity,)).fetchone()
        if prior and prior[0]!=raw: raise ValueError('conflicting_targeted_trade_id')
        prepared.append((identity,ticker,executed,received_at,raw))
    new = sum(not db.execute('SELECT 1 FROM targeted_public_trades WHERE id=?',(r[0],)).fetchone()
              for r in prepared)
    if db.execute('SELECT count(*) FROM targeted_public_trades').fetchone()[0]+new>20000:
        raise ValueError('targeted_trade_capacity_reached')
    db.executemany('INSERT OR IGNORE INTO targeted_public_trades VALUES(?,?,?,?,?)',prepared)
    return len(prepared),cursor


def poll(db, markets, now):
    last,count = db.execute('SELECT max(attempted_at),count(*) FROM targeted_trade_polls').fetchone()
    if count>=4096 or db.execute('SELECT count(*) FROM targeted_public_trades').fetchone()[0]>=20000:
        return
    if last is not None and now-last<60: return
    ticker = candidate(db,now)
    if not ticker:
        detail = {'state':'no_fresh_candidate','execution_enabled':False,'global_coverage':False}
    else:
        cursor = None
        pages = 0
        rows = 0
        try:
            start = db.execute('SELECT started_at FROM targeted_trade_protocol WHERE id=1').fetchone()[0]
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
                          'invalid_targeted_trade','targeted_trade_','conflicting_targeted_trade'))
                          else 'targeted_trade_probe_failed',
                      'ticker_window_complete':False,'global_coverage':False,'execution_enabled':False}
    db.execute('INSERT INTO targeted_trade_polls(attempted_at,ticker,detail) VALUES(?,?,?)',
               (now,ticker,encoded(detail)))


def status(db):
    row = db.execute('SELECT detail,started_at FROM targeted_trade_protocol WHERE id=1').fetchone()
    if not row: return None
    last = db.execute('SELECT detail FROM targeted_trade_polls ORDER BY id DESC LIMIT 1').fetchone()
    return {'schema':PROTOCOL['schema'],'protocol_sha256':hashlib.sha256(row[0].encode()).hexdigest(),
            'started_at':row[1],
            'polls':db.execute('SELECT count(*) FROM targeted_trade_polls').fetchone()[0],
            'tickers':db.execute('SELECT count(DISTINCT ticker) FROM targeted_trade_polls WHERE ticker IS NOT NULL').fetchone()[0],
            'public_trades':db.execute('SELECT count(*) FROM targeted_public_trades').fetchone()[0],
            'onbook_trades':db.execute("SELECT coalesce(sum(json_extract(detail,'$.on_book')),0) FROM targeted_public_trades").fetchone()[0],
            'latest_poll':json.loads(last[0]) if last else None,
            'continuous_coverage':False,'execution_enabled':False,
            'actual_owned_fills':0,'promotion_ready':False}
