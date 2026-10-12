"""Separate prospective provider-side receipt. No requests or broker interface."""
import json
from datetime import datetime
from pathlib import Path
from .kalshi_activity_trade_capture import encoded

PROTOCOL=json.loads((Path(__file__).parents[1]/'configs/kalshi_trade_side_v8_20261012/registration.json').read_text())

def init(db,now):
    db.execute('CREATE TABLE IF NOT EXISTS trade_side_v8_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS trade_side_v8_receipts(id TEXT PRIMARY KEY,event_id TEXT NOT NULL,detail TEXT NOT NULL)')
    db.execute('INSERT OR IGNORE INTO trade_side_v8_protocol VALUES(1,?,?)',(encoded(PROTOCOL),now))
    if db.execute('SELECT detail FROM trade_side_v8_protocol WHERE id=1').fetchone()[0]!=encoded(PROTOCOL):
        raise ValueError('trade_side_protocol_changed')

def capture(db,payload,selected,now):
    start=db.execute('SELECT started_at FROM trade_side_v8_protocol WHERE id=1').fetchone()[0]
    allowed={row[0]:row[2] for row in selected}
    remaining=PROTOCOL['maximum_records']-db.execute('SELECT count(*) FROM trade_side_v8_receipts').fetchone()[0]
    for row in payload.get('trades',[]):
        identity=row.get('trade_id')
        if identity not in allowed or remaining<=0:continue
        at=datetime.fromisoformat(row['created_time'].replace('Z','+00:00')).timestamp()
        if not start<=at<=now:continue
        outcome=row.get('taker_outcome_side'); legacy=row.get('taker_side'); book=row.get('taker_book_side')
        conflict=outcome in ('yes','no') and legacy in ('yes','no') and outcome!=legacy
        detail={'executed_at':at,'received_at':now,'provider_outcome_side':outcome if outcome in ('yes','no') and not conflict else None,
            'provider_legacy_side':legacy if legacy in ('yes','no') else None,
            'provider_book_side':book if book in ('bid','ask') else None,'side_conflict':conflict,
            'on_book':row.get('is_block_trade') is False,'page_complete':not bool(payload.get('cursor')),
            'owned_fill':False,'execution_enabled':False}
        prior=db.execute('SELECT 1 FROM trade_side_v8_receipts WHERE id=?',(identity,)).fetchone()
        if not prior:
            db.execute('INSERT INTO trade_side_v8_receipts VALUES(?,?,?)',(identity,allowed[identity],encoded(detail)))
            remaining-=1

def status(db):
    rows=db.execute('SELECT event_id,detail FROM trade_side_v8_receipts LIMIT 20000').fetchall()
    complete=[(event,json.loads(raw)) for event,raw in rows if json.loads(raw)['page_complete'] and json.loads(raw)['on_book']]
    known=[(event,r) for event,r in complete if r['provider_outcome_side'] and r['provider_book_side']]
    return {'schema':PROTOCOL['schema'],'records':len(rows),'complete_onbook_records':len(complete),
        'known_side_records':len(known),'unknown_side_records':len(complete)-len(known),
        'independent_events':len({event for event,_ in known}),'owned_fills':0,
        'execution_enabled':False,'promotion_ready':False,'scope':'Provider-reported side receipts; queue and fees unverified'}
