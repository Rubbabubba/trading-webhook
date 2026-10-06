"""Prospective sequential-book stress tests; never infer orders or fills."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime, timezone
import hashlib
import json
import math

from .kalshi_external_sleeves import threshold_identity

PROTOCOL = {'schema':'kalshi_structural_execution_probe_v1',
            'fee_reserve_cents_per_leg':5,'slippage_cents_per_leg':1,
            'minimum_delay_seconds':2,'maximum_delay_seconds':30,
            'execution_enabled':False,'fill_assumed':False}


def init(db, now):
    db.execute('CREATE TABLE IF NOT EXISTS structural_execution_protocol(id INTEGER PRIMARY KEY, detail TEXT NOT NULL, started_at TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS structural_execution_probes(signal_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,observed_at TEXT NOT NULL,detail TEXT NOT NULL,sha256 TEXT NOT NULL)')
    raw=json.dumps(PROTOCOL,sort_keys=True,separators=(',',':'))
    db.execute('INSERT OR IGNORE INTO structural_execution_protocol VALUES(1,?,?)',(raw,now.isoformat()))
    if db.execute('SELECT detail FROM structural_execution_protocol WHERE id=1').fetchone()[0]!=raw:
        raise ValueError('structural_probe_protocol_changed')


def _book(quote, outcome):
    book=quote['orderbook_fp']
    def best(key):
        rows=[]
        for price,size in book.get(key,[]):
            p,q=Decimal(str(price)),Decimal(str(size))
            if not p.is_finite() or not q.is_finite() or not 0<p<1 or q<0:
                raise ValueError('invalid_structural_depth')
            if q>=1: rows.append(p)
        return max(rows) if rows else None
    own=best('yes_dollars' if outcome=='yes' else 'no_dollars')
    opposing=best('no_dollars' if outcome=='yes' else 'yes_dollars')
    bid=int((own*100).to_integral_value(rounding=ROUND_FLOOR)) if own is not None else None
    ask=int(((1-opposing)*100).to_integral_value(rounding=ROUND_CEILING)) if opposing is not None else None
    if bid is not None and ask is not None and bid>=ask:
        raise ValueError('crossed_structural_book')
    return bid,ask


def evaluate(signal,low,high,*,now):
    if signal.get('fill_assumed') is not False or signal.get('execution_enabled') is not False:
        raise ValueError('quote_only_signal_required')
    observed=float(signal['observed_at'])
    times=[float(low['observed_at']),float(high['observed_at'])]
    if (not all(math.isfinite(t) for t in [now,observed,*times])
            or any(not 0<=now-t<=10 for t in times) or abs(times[0]-times[1])>5
            or not PROTOCOL['minimum_delay_seconds']<=min(times)-observed
                   <=max(times)-observed<=PROTOCOL['maximum_delay_seconds']):
        raise ValueError('structural_probe_timing_invalid')
    for quote,key in ((low,'low_ticker'),(high,'high_ticker')):
        market=quote['market']; identity,_strike=threshold_identity(market)
        if (quote.get('environment')!='demo' or market.get('ticker')!=signal[key]
                or quote.get('ticker')!=signal[key] or identity!=signal['relationship_id']):
            raise ValueError('structural_probe_identity_changed')
    _,low_strike=threshold_identity(low['market']); _,high_strike=threshold_identity(high['market'])
    if (low_strike>=high_strike or low_strike!=Decimal(signal['low_strike'])
            or high_strike!=Decimal(signal['high_strike'])):
        raise ValueError('structural_probe_threshold_changed')
    lb,la=_book(low,'yes'); hb,ha=_book(high,'no')
    reserve=PROTOCOL['fee_reserve_cents_per_leg']; slip=PROTOCOL['slippage_cents_per_leg']
    sequential=[]
    for name,first,second,unwind in (
        ('low_yes_first',signal['yes_low_ask_cents'],ha,lb),
        ('high_no_first',signal['no_high_ask_cents'],la,hb)):
        net=100-first-second-2*reserve-2*slip if second is not None else None
        loss=first-unwind+2*reserve+2*slip if unwind is not None else None
        sequential.append({'order':name,'modeled_minimum_payout_surplus_cents':net,
                           'observed_unwind_cost_cents':loss,'second_leg_depth_available':second is not None})
    survived=all(row['modeled_minimum_payout_surplus_cents'] is not None
                 and row['modeled_minimum_payout_surplus_cents']>=1
                 and row['observed_unwind_cost_cents'] is not None for row in sequential)
    return {**PROTOCOL,'event_id':signal['event_id'],'observed_at':max(times),
            'signal_observed_at':observed,'sequential_tests':sequential,
            'state':'quote_stress_survived' if survived else 'rejected_quote_stress',
            'actual_orders':0,'actual_fills':0,'realized_net_cents':None,
            'actual_fee_verified':False,'promotion_ready':False,
            'blockers':['actual_paired_fills_missing','actual_fees_missing','settlement_reconciliation_missing']}


def capture(db,client,signal_id,signal,*,clock,now):
    init(db,now)
    start=db.execute('SELECT started_at FROM structural_execution_protocol WHERE id=1').fetchone()[0]
    if datetime.fromtimestamp(signal['observed_at'],timezone.utc).isoformat()<start:
        return False
    if db.execute('SELECT 1 FROM structural_execution_probes WHERE signal_id=?',(signal_id,)).fetchone():
        return False
    # Persist the attempt before acquiring books so restarts cannot selectively
    # repeat a rejected opportunity. Failed reads remain part of the denominator.
    value={**PROTOCOL,'event_id':signal['event_id'],'state':'acquisition_incomplete',
           'actual_orders':0,'actual_fills':0,'realized_net_cents':None,'promotion_ready':False}
    raw=json.dumps(value,sort_keys=True,separators=(',',':'))
    db.execute('INSERT INTO structural_execution_probes VALUES(?,?,?,?,?)',
        (signal_id,signal['event_id'],now.isoformat(),raw,hashlib.sha256(raw.encode()).hexdigest()))
    try:
        low=client.quote({'ticker':signal['low_ticker']})
        high=client.quote({'ticker':signal['high_ticker']})
        value=evaluate(signal,low,high,now=clock())
    except (KeyError,TypeError,ValueError,ArithmeticError):
        value['state']='rejected_missing_or_invalid_books'
    raw=json.dumps(value,sort_keys=True,separators=(',',':'))
    db.execute('UPDATE structural_execution_probes SET detail=?,sha256=? WHERE signal_id=?',
               (raw,hashlib.sha256(raw.encode()).hexdigest(),signal_id))
    return True


def status(db):
    attempts,events,survived=db.execute("SELECT count(*),count(DISTINCT event_id),coalesce(sum(json_extract(detail,'$.state')='quote_stress_survived'),0) FROM structural_execution_probes").fetchone()
    return {**PROTOCOL,'attempts':attempts,'independent_events':events,
            'quote_stress_survived':survived,'rejected_or_incomplete':attempts-survived,
            'actual_orders':0,'actual_fills':0,'realized_net_cents':None,'promotion_ready':False}
