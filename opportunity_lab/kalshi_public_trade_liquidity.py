"""Separately frozen Demo public-trade probe; no order or fill assumptions."""
from datetime import datetime,timezone
from decimal import Decimal
import hashlib,json,math

PROTOCOL={'schema':'kalshi_public_trade_liquidity_v1','environment':'demo',
    'hypothesis':'Measure executed on-book transactions before designing a new fillability challenger.',
    'poll_interval_seconds':300,'lookback_seconds':300,'page_limit':100,
    'maximum_trades':20000,'maximum_polls':4096,
    'execution_enabled':False,'fill_assumed':False,'profitability_evidence':False,
    'required_future_trade_events_for_review':50,'required_onbook_trades_for_review':200,
    'review_is_not_execution_approval':True,
    'missing_for_execution':['queue position and cancellation attribution','fee-verified replay',
                             'prospective independent validation','actual owned Demo fills']}
def encoded(value): return json.dumps(value,sort_keys=True,separators=(',',':'))
def init(db,now):
    db.execute('CREATE TABLE IF NOT EXISTS liquidity_trade_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS liquidity_public_trades(id TEXT PRIMARY KEY,ticker TEXT NOT NULL,executed_at REAL NOT NULL,observed_at REAL NOT NULL,detail TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS liquidity_trade_polls(id INTEGER PRIMARY KEY,attempted_at REAL NOT NULL,detail TEXT NOT NULL)')
    for action in ('UPDATE','DELETE'):
        db.execute("CREATE TRIGGER IF NOT EXISTS liquidity_trades_no_"+action.lower()+" BEFORE "+action+" ON liquidity_public_trades BEGIN SELECT RAISE(ABORT,'liquidity_trades_append_only'); END")
    raw=encoded(PROTOCOL)
    db.execute('INSERT OR IGNORE INTO liquidity_trade_protocol VALUES(1,?,?)',(raw,now))
    if db.execute('SELECT detail FROM liquidity_trade_protocol').fetchone()[0]!=raw:
        raise ValueError('liquidity_protocol_changed')

def ingest(db,payload,*,attempted_at,received_at):
    start=db.execute('SELECT started_at FROM liquidity_trade_protocol').fetchone()[0]
    rows=payload.get('trades') if isinstance(payload,dict) else None
    cursor=payload.get('cursor') if isinstance(payload,dict) else None
    if (not isinstance(rows,list) or len(rows)>100 or not isinstance(cursor,str)
            or len(cursor)>2000 or not math.isfinite(received_at) or received_at<attempted_at
            or len(encoded(payload))>256000): raise ValueError('invalid_trade_page')
    prepared=[]; discarded=0
    lower=max(start,attempted_at-300); seen=set()
    for row in rows:
        if not isinstance(row,dict): raise ValueError('invalid_trade')
        identity=row.get('trade_id'); ticker=row.get('ticker')
        if (not isinstance(identity,str) or not 1<=len(identity)<=160 or identity in seen
                or not isinstance(ticker,str) or not 1<=len(ticker)<=160): raise ValueError('invalid_trade_identity')
        seen.add(identity)
        at=datetime.fromisoformat(row['created_time'].replace('Z','+00:00'))
        if at.tzinfo is None: raise ValueError('trade_timezone_required')
        executed=at.timestamp()
        size=Decimal(row['count_fp']); yes=Decimal(row['yes_price_dollars']); no=Decimal(row['no_price_dollars'])
        if not (size.is_finite() and 0<size<=1000000000 and yes.is_finite() and no.is_finite()
                and 0<=yes<=1 and 0<=no<=1): raise ValueError('invalid_trade_value')
        if executed<lower: discarded+=1; continue
        if executed>attempted_at: raise ValueError('trade_after_requested_window')
        # Missing block classification cannot certify on-book liquidity.
        detail={'trade_id':identity,'ticker':ticker,'executed_at':executed,'count_fp':str(size),
            'yes_price_dollars':str(yes),'no_price_dollars':str(no),
            'on_book':row.get('is_block_trade') is False,
            'block_classification_known':type(row.get('is_block_trade')) is bool,
            'taker_outcome_side':row.get('taker_outcome_side') if row.get('taker_outcome_side') in ('yes','no') else None,
            'taker_book_side':row.get('taker_book_side') if row.get('taker_book_side') in ('bid','ask') else None,
            'owned_fill':False,'execution_enabled':False}
        raw=encoded(detail)
        prior=db.execute('SELECT detail FROM liquidity_public_trades WHERE id=?',(identity,)).fetchone()
        if prior and prior[0]!=raw: raise ValueError('conflicting_trade_id')
        prepared.append((identity,ticker,executed,received_at,raw))
    count=db.execute('SELECT count(*) FROM liquidity_public_trades').fetchone()[0]
    if count+sum(not db.execute('SELECT 1 FROM liquidity_public_trades WHERE id=?',(r[0],)).fetchone() for r in prepared)>20000:
        raise ValueError('trade_capacity_reached')
    db.executemany('INSERT OR IGNORE INTO liquidity_public_trades VALUES(?,?,?,?,?)',prepared)
    return {'state':'observed','received_at':received_at,'rows':len(rows),'discarded_pre_registration_or_window':discarded,
            'page_complete':not bool(cursor),'window_start':lower,'window_end':attempted_at,
            'continuous_coverage':False,'execution_enabled':False}

def poll(db,markets,now):
    last=db.execute('SELECT max(attempted_at),count(*) FROM liquidity_trade_polls').fetchone()
    if last[1]>=4096 or db.execute('SELECT count(*) FROM liquidity_public_trades').fetchone()[0]>=20000: return
    if last[0] is not None and now-last[0]<300: return
    start=db.execute('SELECT started_at FROM liquidity_trade_protocol').fetchone()[0]
    try:
        payload,_,received=markets.get_trades(params={'limit':100,'min_ts':int(max(start,now-300)),'max_ts':int(now)})
        detail=ingest(db,payload,attempted_at=now,received_at=received)
    except Exception as error:
        detail={'state':'blocked','error_type':type(error).__name__,'error_code':str(error) if str(error).startswith(('demo_market_http_','stale_demo_market_data','trade_capacity_reached','conflicting_trade_id','invalid_trade','trade_after_requested_window')) else 'trade_probe_failed','execution_enabled':False,'continuous_coverage':False}
    db.execute('INSERT INTO liquidity_trade_polls(attempted_at,detail) VALUES(?,?)',(now,encoded(detail)))

def status(db):
    row=db.execute('SELECT detail,started_at FROM liquidity_trade_protocol').fetchone()
    if not row: return None
    count,onbook=db.execute("SELECT count(*),coalesce(sum(json_extract(detail,'$.on_book')),0) FROM liquidity_public_trades").fetchone()
    matched=db.execute("SELECT count(DISTINCT json_extract(d.detail,'$.event_id')) FROM liquidity_public_trades t JOIN depth_latest d ON t.ticker=d.ticker WHERE json_extract(t.detail,'$.on_book')=1").fetchone()[0]
    last=db.execute('SELECT attempted_at,detail FROM liquidity_trade_polls ORDER BY id DESC LIMIT 1').fetchone()
    return {'schema':PROTOCOL['schema'],'protocol_sha256':hashlib.sha256(row[0].encode()).hexdigest(),
            'started_at':row[1],'public_trades':count,'onbook_trades':onbook,
            'matched_depth_events':matched,'latest_poll':json.loads(last[1]) if last else None,
            'polls':db.execute('SELECT count(*) FROM liquidity_trade_polls').fetchone()[0],
            'continuous_coverage':False,'execution_enabled':False,'fill_assumed':False,
            'actual_owned_fills':0,'promotion_ready':False,'trade_cancel_attribution':'unresolved'}
