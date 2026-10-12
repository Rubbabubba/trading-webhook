"""Independent prospective public-trade windows; never orders or assumed fills."""
import json
import sqlite3
from pathlib import Path
from .kalshi_activity_trade_capture import init as init_validation, ingest, encoded

PROTOCOL=json.loads((Path(__file__).parents[1]/'configs/kalshi_postquote_capture_v6_20261010/registration.json').read_text())

def init(db,now):
    from .kalshi_trade_side_capture import init as init_side
    init_side(db,now)
    db.executescript("CREATE TABLE IF NOT EXISTS postquote_v6_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL); CREATE TABLE IF NOT EXISTS postquote_v6_windows(source_id INTEGER PRIMARY KEY,at REAL NOT NULL,ticker TEXT NOT NULL,event_id TEXT NOT NULL,quote_json TEXT NOT NULL,detail TEXT NOT NULL); CREATE TABLE IF NOT EXISTS postquote_v6_trades(id TEXT PRIMARY KEY,ticker TEXT NOT NULL,event_id TEXT NOT NULL,executed_at REAL NOT NULL,detail TEXT NOT NULL); CREATE TRIGGER IF NOT EXISTS postquote_v6_no_update BEFORE UPDATE ON postquote_v6_trades BEGIN SELECT RAISE(ABORT,'append_only'); END; CREATE TRIGGER IF NOT EXISTS postquote_v6_no_delete BEFORE DELETE ON postquote_v6_trades BEGIN SELECT RAISE(ABORT,'append_only'); END;")
    db.execute('INSERT OR IGNORE INTO postquote_v6_protocol VALUES(1,?,?)',(encoded(PROTOCOL),now))
    if db.execute('SELECT detail FROM postquote_v6_protocol WHERE id=1').fetchone()[0]!=encoded(PROTOCOL): raise ValueError('postquote_protocol_changed')

def poll(db,markets,now):
    start=db.execute('SELECT started_at FROM postquote_v6_protocol WHERE id=1').fetchone()[0]
    if db.execute('SELECT count(*) FROM postquote_v6_windows').fetchone()[0]>=4096 or db.execute('SELECT count(*) FROM postquote_v6_trades').fetchone()[0]>=20000: return
    row=db.execute("SELECT p.id,p.attempted_at,p.ticker,p.event_id,p.quote_json FROM activity_trade_v5_polls p LEFT JOIN postquote_v6_windows w ON w.source_id=p.id WHERE w.source_id IS NULL AND p.attempted_at>=? AND p.attempted_at<=? AND p.event_id!='' AND p.quote_json!='{}' ORDER BY p.id LIMIT 1",(start,now-30)).fetchone()
    if not row: return
    source,attempted,ticker,event,quote=row
    at=json.loads(quote).get("observed_at")
    if not isinstance(at,(int,float)) or at<attempted or at>now: return
    if now-at<30: return
    # Claim durably before transport; ambiguous responses are never retried.
    db.execute('INSERT INTO postquote_v6_windows VALUES(?,?,?,?,?,?)',(source,at,ticker,event,quote,encoded({'state':'requested'})))
    db.commit()
    detail={'state':'expired','execution_enabled':False}
    if now-at<=90:
        try:
            payload,_,received=markets.get_targeted_trades(params={'ticker':ticker,'limit':100,'min_ts':int(at),'max_ts':int(at+30)})
            with sqlite3.connect(':memory:') as validation:
                init_validation(validation,start)
                ingest(validation,payload,ticker=ticker,event_id=event,now=now,received=received)
                selected=validation.execute('SELECT id,ticker,event_id,executed_at,detail FROM activity_trade_v5_trades WHERE executed_at>? AND executed_at<=?',(at,at+30)).fetchall()
            remaining=20000-db.execute('SELECT count(*) FROM postquote_v6_trades').fetchone()[0]
            from .kalshi_trade_side_capture import capture as capture_side
            # The transport validator verifies received >= request start. Record
            # response receipt, not request start, for latency evidence. Existing
            # immutable receipts are retained; only new receipts use this fix.
            capture_side(db,payload,selected,received)
            for trade in selected[:remaining]:
                prior=db.execute('SELECT detail FROM postquote_v6_trades WHERE id=?',(trade[0],)).fetchone()
                if prior and prior[0]!=trade[4]: raise ValueError('conflicting_public_trade')
                db.execute('INSERT OR IGNORE INTO postquote_v6_trades VALUES(?,?,?,?,?)',trade)
            detail={'state':'observed','new_trades':len(selected[:remaining]),'page_complete':not bool(payload.get('cursor')),'execution_enabled':False}
        except Exception as error:
            detail={'state':'blocked','error_type':type(error).__name__,'execution_enabled':False}
    db.execute('UPDATE postquote_v6_windows SET detail=? WHERE source_id=?',(encoded(detail),source))

def status(db):
    windows=db.execute('SELECT detail FROM postquote_v6_windows').fetchall()
    trades,events=db.execute('SELECT count(*),count(DISTINCT event_id) FROM postquote_v6_trades').fetchone()
    return {'schema':'kalshi_postquote_capture_v6','windows':len(windows),'public_trades':trades,'trade_events':events,'states':{state:sum(json.loads(r[0]).get('state')==state for r in windows) for state in ('observed','expired','blocked','requested')},'execution_enabled':False,'owned_fills':0,'promotion_ready':False,'sampled_only':True,'price_side_queue_and_fee_edge_not_assessed':True}
