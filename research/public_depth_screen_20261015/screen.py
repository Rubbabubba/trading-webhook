"""Independent read-only replay. Never imports provider simulator or submits orders."""
import bisect
import hashlib
import json
import math
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def book(row):
    levels = []
    for side in ('yes_bids', 'no_bids'):
        raw = json.loads(row[side]) if isinstance(row[side], str) else row[side]
        valid = []
        for x in raw or []:
            p = x.get('price_cents', x.get('price'))
            if isinstance(p, float) and 0 < p < 1:
                p = round(p * 100)
            if p is not None and math.isfinite(float(p)) and 0 < p < 100 and x.get('size', 0) >= 1:
                valid.append(float(p))
        if not valid:
            return None
        levels.append(max(valid))
    y, n = levels
    if y + n >= 100:
        return None
    return {'yes_bid': y, 'yes_ask': 100-n, 'no_bid': n, 'no_ask': 100-y,
            'mid': (y+100-n)/2}


def fee(price):
    from decimal import Decimal, ROUND_CEILING
    p = Decimal(str(price))/100
    return int((Decimal('7')*p*(1-p)).to_integral_value(rounding=ROUND_CEILING))


def trading_window(market):
    cuts=[datetime.fromisoformat(market[k].replace('Z','+00:00')) for k in ('close_time','expected_expiration_time','occurrence_datetime') if market.get(k)]
    if not cuts or not market.get('open_time'):
        raise ValueError('missing_trading_window')
    opened=datetime.fromisoformat(market['open_time'].replace('Z','+00:00'))
    if min(cuts)<=opened:
        raise ValueError('invalid_trading_window')
    return opened,min(cuts)


def future_fill(rows, times, decision, side, action):
    i = bisect.bisect_left(times, decision+1)
    if i == len(rows) or times[i] > decision+6:
        return None
    q = book(rows[i])
    if q is None:
        return None
    return {'at': times[i], 'price': q[side+('_ask' if action == 'buy' else '_bid')]}


def replay(rows, settlement, track):
    rows = sorted(rows, key=lambda r:(r['ts'],r['sequence_id']))
    times = [r['ts'].timestamp() for r in rows]
    history, trades, reasons = [], [], Counter()
    position = None
    entered = False
    for t in range(math.ceil(times[0]/300)*300, math.floor(times[-1]/300)*300+1, 300):
        i = bisect.bisect_right(times,t)-1
        if i < 0 or t-times[i] > 5:
            reasons['stale_or_missing_decision_quote'] += 1
            history = []  # require contiguous fresh decision observations
            continue
        q = book(rows[i])
        if q is None:
            reasons['invalid_or_empty_book'] += 1
            history = []
            continue
        mean = statistics.mean(history[-20:]) if len(history) >= 20 else None
        sd = statistics.pstdev(history[-20:]) if mean is not None else None
        if position and track == 'bollinger_taker':
            recovered = mean is not None and ((position['side']=='yes' and q['mid']>=mean) or (position['side']=='no' and q['mid']<=mean))
            if recovered or t-position['entry_at']>=3600:
                fill = future_fill(rows,times,t,position['side'],'sell')
                if fill:
                    position.update(exit_at=fill['at'],exit_price=fill['price'],exit_fee=fee(fill['price']),exit_reason='mean_recovery' if recovered else 'deadline',complete=True)
                    trades.append(position)
                    position = None
                else:
                    reasons['exit_unfilled'] += 1
        if not entered and position is None:
            side = None
            if track == 'favorite_taker':
                side = next((s for s in ('yes','no') if 90 <= q[s+'_ask'] <= 98),None)
            elif mean is not None and sd > 0:
                side = 'yes' if q['mid'] < mean-2*sd else 'no' if q['mid'] > mean+2*sd else None
            if side:
                fill = future_fill(rows,times,t,side,'buy')
                # A delayed favorite must still satisfy its original price band.
                if fill and (track != 'favorite_taker' or 90 <= fill['price'] <= 98):
                    position = dict(side=side,decision_at=t,entry_at=fill['at'],entry_price=fill['price'],entry_fee=fee(fill['price']))
                    entered = True
                else:
                    reasons['entry_unfilled_or_price_guard'] += 1
        history.append(q['mid'])
    if position:
        result = str(settlement.get('result', '')).lower()
        settled = settlement.get('settled_ts')
        if result in ('yes','no') and settled and datetime.fromisoformat(settled.replace('Z','+00:00')).timestamp() >= position['entry_at']:
            position.update(exit_price=100 if result==position['side'] else 0,exit_fee=0,exit_reason='settlement',exit_at=settled,complete=True)
        else:
            position.update(complete=False,exit_reason='unresolved_or_invalid_settlement')
        trades.append(position)
    for tr in trades:
        if tr['complete']:
            tr['net_cents'] = tr['exit_price']-tr['entry_price']-tr['entry_fee']-tr['exit_fee']
            count = 1 if tr['exit_reason']=='settlement' else 2
            tr['stressed_net_cents'] = {str(s):tr['net_cents']-s*count for s in (0,2,5)}
    return trades, dict(reasons)


def run(source, destination, official=None):
    import pyarrow.parquet as pq
    reg = Path(__file__).with_name('registration.json')
    report = {'registration_sha256':digest(reg),'source_revision':'611d66941717310858683278940df21c33c406f2','files':{},'episodes':[],
              'execution_enabled':False,'live_qualified':False,'profitability_proven':False,
              'limitations':['Four publicly selected episodes, not representative unused holdout','No independent venue provenance verification','No complete contract lifecycle or true resolution cutoff validation; post-outcome contamination possible','No receipt-clock or transport-gap guarantees','Historical fee schedule only; current market-specific fees not verified','Recorded displayed depth is a conditional simulated fill, never our broker fill','Correlated tickers aggregated by event; not independent observations']}
    for folder in sorted(source.iterdir()):
        if not folder.is_dir():
            continue
        for f in folder.iterdir():
            if f.is_file(): report['files'][str(f.relative_to(source))] = digest(f)
        rows = pq.read_table(folder/'orderbook.parquet').to_pylist()
        removed=0
        source_rows=len(rows)
        official_checks={}
        settlements = json.loads((folder/'settlement.json').read_text())
        if official:
            clean=[]
            for ticker in {r['ticker'] for r in rows}:
                path=official/(ticker+'.json')
                receipt=json.loads(path.read_text())
                market=receipt['payload']['market']
                assert receipt['status']==200 and market['ticker']==ticker
                assert market['result'].lower()==settlements[ticker]['result'].lower()
                opened,cutoff=trading_window(market)
                group=[r for r in rows if r['ticker']==ticker]
                accepted=[r for r in group if opened <= r['ts'] < cutoff]
                clean.extend(accepted)
                removed+=len(group)-len(accepted)
                official_checks[ticker]={'metadata_sha256':digest(path),'cutoff':cutoff.isoformat(),'settlement_label_matches':True}
            rows=clean
        by_ticker = {}
        for r in rows: by_ticker.setdefault(r['ticker'],[]).append(r)
        ep = {'event':folder.name,'rows':len(rows),'source_rows':source_rows,'excluded_outside_trading_window':removed,'official_checks':official_checks,'tickers':len(by_ticker),'tracks':{}}
        for track in ('bollinger_taker','favorite_taker'):
            trades, rejects = [], Counter()
            for ticker, group in by_ticker.items():
                ts, rs = replay(group,settlements.get(ticker,{}),track)
                trades.extend(dict(ticker=ticker,**x) for x in ts)
                rejects.update(rs)
            ep['tracks'][track] = {'trades':trades,'rejections':dict(rejects),'complete':sum(t['complete'] for t in trades),
                'net_cents_by_stress':{str(s):sum(t['stressed_net_cents'][str(s)] for t in trades if t['complete']) for s in (0,2,5)}}
        report['episodes'].append(ep)
    report['totals'] = {track:{'entries':sum(len(e['tracks'][track]['trades']) for e in report['episodes']),
        'complete':sum(e['tracks'][track]['complete'] for e in report['episodes']),
        'net_cents_by_stress':{str(s):sum(e['tracks'][track]['net_cents_by_stress'][str(s)] for e in report['episodes']) for s in (0,2,5)}} for track in ('bollinger_taker','favorite_taker')}
    with destination.open('x',encoding='utf-8') as out: json.dump(report,out,indent=2)
    print(json.dumps(report['totals']))


if __name__ == '__main__':
    import sys
    run(Path(sys.argv[1]),Path(sys.argv[2]),Path(sys.argv[3]) if len(sys.argv)>3 else None)
