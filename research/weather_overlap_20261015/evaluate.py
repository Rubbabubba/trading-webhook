"""Offline fixed forecast calibration and indicative accounting, never fills."""
import hashlib
import json
import math
import statistics
import sys
from collections import Counter
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,ROUND_CEILING
from pathlib import Path

ROOT=Path(__file__).parent
sys.path.insert(0,str(ROOT.parents[1]))
from opportunity_lab.kalshi_weather_ensemble import parse_temperature_bucket


def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()


def hourly_max(payload,target):
    if payload.get('timezone')!='America/New_York' or payload.get('hourly_units',{}).get('temperature_2m')!='°F':
        raise ValueError('forecast_timezone_or_units')
    h=payload['hourly']
    values=[(t,v) for t,v in zip(h['time'],h['temperature_2m'],strict=True) if t.startswith(target+'T')]
    if len(values)!=24 or len({t for t,v in values})!=24:
        raise ValueError('incomplete_target_day')
    if any(v is None or not math.isfinite(v) for t,v in values):
        raise ValueError('nonfinite_target_day')
    return max(v for t,v in values)


def probability(mu,bounds):
    normal=statistics.NormalDist(mu=mu,sigma=4)
    return normal.cdf(bounds[1])-normal.cdf(bounds[0])


def fee_cents(price):
    p=Decimal(str(price))
    return int((Decimal(7)*p*(1-p)).to_integral_value(rounding=ROUND_CEILING))


def decision_quote(payload,at):
    rows=[c for c in payload.get('candlesticks',[]) if c['end_period_ts']==at]
    if len(rows)!=1: raise ValueError('missing_exact_completed_candle')
    bid=rows[0]['yes_bid']['close'];ask=rows[0]['yes_ask']['close']
    if bid is None or ask is None: raise ValueError('missing_bid_ask')
    bid,ask=float(bid),float(ask)
    if not (math.isfinite(bid) and math.isfinite(ask) and 0<=bid<=ask<=1): raise ValueError('invalid_bid_ask')
    if ask-bid>0.100000001: raise ValueError('spread_exceeds_limit')
    return bid,ask


def choose(inputs):
    candidates=[]
    for row in inputs:
        for side in ('yes','no'):
            price=row['ask'] if side=='yes' else 1-row['bid']
            ps=row['model_probabilities'] if side=='yes' else [1-p for p in row['model_probabilities']]
            edges=[p-price for p in ps]
            if not 0.10<=price<=0.90 or min(edges)<0.12: continue
            fee=fee_cents(price)
            if statistics.mean(edges)-fee/100-0.02<=0: continue
            candidates.append(dict(row,side=side,entry_price_proxy=price,entry_fee_cents=fee,raw_edge=statistics.mean(edges),fill_assumed=False))
    return sorted(candidates,key=lambda r:(-r['raw_edge'],r['ticker'],r['side']))[:1]


def evaluate(out):
    cache=ROOT/'cache'
    report={'schema':'weather_point_forecast_overlap_v2_result','execution_enabled':False,'fill_assumed':False,'actual_fills':0,'live_qualified':False,'profitability_proven':False,
        'registration_sha256':sha(ROOT/'registration.json'),'hourly_v2_registration_sha256':sha(ROOT/'hourly-v2-registration.json'),'implementation_sha256':sha(Path(__file__)),
        'files':{p.name:sha(p) for p in sorted(cache.glob('*.json'))},'days':[],
        'limitations':['Indicative minute closing quotes lack depth, verified quote age or execution latency','Run initialization+8h is assumed, not historical receipt evidence','Hourly point forecast maxima with fixed4Fnormal proxy, not calibrated ensembles or station continuous maxima','Only one city and season;30dates are not frozenV14 evidence','Retrospective API metadata may differ from original vintages','Even positive conditional accounting never authorizes a Demo/live order trial']}
    for i in range(30):
        day=date(2026,6,1)+timedelta(days=i); prior=day-timedelta(days=1)
        event='KXHIGHNY-'+day.strftime('%y%b%d').upper()
        decision=int(datetime.combine(prior,datetime.min.time(),tzinfo=timezone.utc).timestamp())+20*3600
        rec={'date':str(day),'event':event,'decision_utc':datetime.fromtimestamp(decision,timezone.utc).isoformat(),'rejections':{},'selected':[],'inputs':[]}
        rejects=Counter()
        def load(key):
            p=cache/(key+'.json')
            if not p.exists(): raise ValueError('missing_cached_input:'+key)
            r=json.loads(p.read_text())
            if r['status']!=200: raise ValueError('source_status_'+str(r['status'])+':'+key)
            return r['payload']
        try:
            forecast=[hourly_max(load(str(day)+'-'+m+'-hourly-v2'),str(day)) for m in ('ecmwf_ifs','gfs_seamless')]
            rec['forecast_maxima_f']=forecast
            if abs(forecast[0]-forecast[1])>3: raise ValueError('model_center_disagreement')
            events=load(event)
            if events.get('cursor'): raise ValueError('incomplete_event_page')
            markets=events.get('markets',[])
            if not markets: raise ValueError('no_markets')
            for m in markets:
                ticker=m['ticker']
                try:
                    if m.get('event_ticker')!=event: raise ValueError('wrong_event')
                    rules=str(m.get('rules_primary','')).lower()
                    if 'central park' not in rules or 'national weather service' not in rules: raise ValueError('station_rules_unverified')
                    opened=datetime.fromisoformat(m['open_time'].replace('Z','+00:00')).timestamp()
                    cuts=[datetime.fromisoformat(m[k].replace('Z','+00:00')).timestamp() for k in ('close_time','expected_expiration_time','occurrence_datetime') if m.get(k)]
                    if not cuts or not opened<=decision<min(cuts): raise ValueError('outside_trading_window')
                    bounds=parse_temperature_bucket(m)
                    if bounds is None: raise ValueError('unparsed_bounds')
                    bid,ask=decision_quote(load(ticker+'-decision'),decision)
                    probabilities=[probability(mu,bounds) for mu in forecast]
                    rec['inputs'].append({'ticker':ticker,'bounds':[None if math.isinf(x) else x for x in bounds],'bid':bid,'ask':ask,'model_probabilities':probabilities,'forecast_yes':statistics.mean(probabilities),'market_yes':(bid+ask)/2})
                except (ValueError,KeyError,TypeError) as e: rejects[str(e)]+=1
            # Selection receives no settlement outcomes.
            selected=choose(rec['inputs'])
            by_ticker={m['ticker']:m for m in markets}
            deltas=[]
            for row in rec['inputs']:
                m=by_ticker[row['ticker']]; result=str(m.get('result','')).lower()
                if result not in ('yes','no'):
                    rejects['missing_settlement']+=1;continue
                y=int(result=='yes')
                row['outcome_yes']=y
                row['brier_delta']=(row['forecast_yes']-y)**2-(row['market_yes']-y)**2
                deltas.append(row['brier_delta'])
            rec['city_day_mean_brier_delta']=statistics.mean(deltas) if deltas else None
            for row in selected:
                m=by_ticker[row['ticker']]; result=str(m.get('result','')).lower()
                if result not in ('yes','no'):
                    row['outcome_complete']=False
                else:
                    row['outcome_complete']=True
                    win=int(result==row['side'])
                    base=100*win-100*row['entry_price_proxy']-row['entry_fee_cents']-2
                    row['conditional_net_cents_by_extra_stress']={str(s):round(base-s,8) for s in (0,2,5)}
                rec['selected'].append(row)
        except (ValueError,KeyError,TypeError) as e: rec['day_blocker']=str(e)
        rec['rejections']=dict(rejects);report['days'].append(rec)
    selected=[r for d in report['days'] for r in d['selected']]
    complete=[r for r in selected if r.get('outcome_complete')]
    briers=[d['city_day_mean_brier_delta'] for d in report['days'] if d.get('city_day_mean_brier_delta') is not None]
    values=[r['conditional_net_cents_by_extra_stress']['0'] for r in complete]
    report['summary']={'calendar_days':30,'days_with_both_forecasts':sum('forecast_maxima_f' in d for d in report['days']),
        'days_with_comparable_quotes':len(briers),'complete_binary_inputs':sum(len(d['inputs']) for d in report['days']),
        'selected_city_days':len(selected),'complete_conditional_outcomes':len(complete),
        'mean_city_day_brier_delta':statistics.mean(briers) if briers else None,
        'conditional_net_cents_by_extra_stress':{str(s):round(sum(r['conditional_net_cents_by_extra_stress'][str(s)] for r in complete),8) for s in (0,2,5)},
        'city_day_normal_95_lcb_cents':statistics.mean(values)-1.96*statistics.stdev(values)/math.sqrt(len(values)) if len(values)>=2 else None,
        'actual_executable_signals':0,'historical_receipt_verified':False,'held_out_live_qualification':False}
    report['decision']='research_only_no_order_qualification'
    with out.open('x',encoding='utf-8') as f: json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps(report['summary']))


if __name__=='__main__': evaluate(ROOT/'result-v2.json')
