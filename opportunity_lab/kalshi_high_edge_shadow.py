"""Prospective two-tick challenger; no transport, order adapter or fill assumptions."""
import json
import statistics
from decimal import Decimal
from pathlib import Path
from .kalshi_maker_v12 import shadow_decision
from .kalshi_maker_v10 import shadow_decision as baseline_decision, stressed_markout
from .kalshi_shadow import price_book

PROTOCOL=json.loads((Path(__file__).parents[1]/'configs/kalshi_high_edge_two_tick_v15_20261012/registration.json').read_text())

def init(db,now):
    db.executescript('''CREATE TABLE IF NOT EXISTS high_edge_v15_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS high_edge_v15_signals(id INTEGER PRIMARY KEY,ticker TEXT,event TEXT,at REAL,detail TEXT);
    CREATE TABLE IF NOT EXISTS high_edge_v15_marks(signal_id INTEGER,horizon INTEGER,at REAL,detail TEXT,PRIMARY KEY(signal_id,horizon));''')
    raw=json.dumps(PROTOCOL,sort_keys=True)
    db.execute('INSERT OR IGNORE INTO high_edge_v15_protocol VALUES(1,?,?)',(raw,now))
    if db.execute('SELECT detail FROM high_edge_v15_protocol WHERE id=1').fetchone()[0]!=raw:
        raise ValueError('high_edge_protocol_changed')

def observe(db,ticker,event,history,frame):
    now=frame['received_at']; start=db.execute('SELECT started_at FROM high_edge_v15_protocol').fetchone()[0]
    if now<start:return
    bid,ask,_,_=price_book(frame,'yes');mid=(bid+ask)/2
    for identity,at,raw in db.execute('SELECT id,at,detail FROM high_edge_v15_signals WHERE ticker=?',(ticker,)).fetchall():
        detail=json.loads(raw)
        for horizon in PROTOCOL['horizons_seconds']:
            if horizon<=now-at<=horizon+PROTOCOL['maximum_markout_lateness_seconds']:
                result={name:stressed_markout(signal,mid) if signal else {'gross_cents':'0','stressed_cents':'0','no_trade':True}
                        for name,signal in detail.items()}
                db.execute('INSERT OR IGNORE INTO high_edge_v15_marks VALUES(?,?,?,?)',(identity,horizon,now,json.dumps(result,sort_keys=True)))
    if status(db)['state'] in ('rejected','capacity_reached'):return
    prior=db.execute('SELECT max(at) FROM high_edge_v15_signals WHERE event=?',(event,)).fetchone()[0]
    if prior is not None and now-prior<PROTOCOL['event_cooldown_seconds']:return
    signal,_=shadow_decision(history,frame)
    if not signal or Decimal(signal['gross_edge_cents'])<5:return
    # The frozen V12 spread floor is 4c: +2c remains strictly below its ask.
    challenger={**signal,'price_cents':signal['price_cents']+2,'strategy_id':PROTOCOL['schema']}
    baseline,_=baseline_decision(history,frame)
    detail={'challenger':challenger,'frozen_v12':signal,'frozen_v10':baseline}
    db.execute('INSERT INTO high_edge_v15_signals(ticker,event,at,detail) VALUES(?,?,?,?)',
               (ticker,event,now,json.dumps(detail,sort_keys=True)))

def status(db):
    total=db.execute('SELECT count(*) FROM high_edge_v15_signals').fetchone()[0]
    complete=db.execute('SELECT s.id,s.event FROM high_edge_v15_signals s JOIN high_edge_v15_marks m ON m.signal_id=s.id GROUP BY s.id HAVING count(*)=3').fetchall()
    ids={r[0] for r in complete};events={r[1] for r in complete}
    pnl={};lcbs={};means={};benchmarks={}
    for h in PROTOCOL['horizons_seconds']:
        clustered={}; totals={name:0.0 for name in ('challenger','frozen_v10','frozen_v12')}
        for identity,event,raw in db.execute('SELECT s.id,s.event,m.detail FROM high_edge_v15_marks m JOIN high_edge_v15_signals s ON s.id=m.signal_id WHERE horizon=?',(h,)):
            if identity not in ids:continue
            values=json.loads(raw)
            for name in totals:totals[name]+=float(values[name]['stressed_cents'])
            clustered.setdefault(event,[]).append(float(values['challenger']['stressed_cents']))
        clusters=[statistics.mean(v) for v in clustered.values()]
        key=str(h);pnl[key]=totals['challenger'];benchmarks[key]=totals
        means[key]=statistics.mean(clusters) if clusters else None
        lcbs[key]=statistics.mean(clusters)-1.96*statistics.stdev(clusters)/len(clusters)**.5 if len(clusters)>1 else None
    rejected=len(events)>=40 and any(v is not None and v<=0 for v in means.values())
    passed=len(ids)>=100 and len(events)>=40 and all(pnl[str(h)]>0 and lcbs[str(h)] is not None and lcbs[str(h)]>0 for h in PROTOCOL['horizons_seconds'])
    return {'schema':PROTOCOL['schema'],'signals':total,'complete_signals':len(ids),'independent_events':len(events),
            'state':'rejected' if rejected else 'shadow_gate_passed' if passed else 'capacity_reached' if total>=500 else 'collecting',
            'stressed_quote_net_cents':pnl,'event_cluster_lcb_cents':lcbs,'same_observation_benchmarks':benchmarks,
            'execution_enabled':False,'owned_fills':0,'fill_assumed':False,'promotion_ready':False,
            'markout_maximum_lateness_seconds':5}
