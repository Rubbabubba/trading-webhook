"""Unpriced measurement qualification; selection cannot receive settlements."""
import csv,hashlib,io,json,re
from pathlib import Path
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,ROUND_HALF_UP
from collections import Counter

ROOT=Path(__file__).parent
UTC=timezone.utc
FIELDS=('ticker','event_ticker','market_type','yes_sub_title','strike_type','floor_strike','cap_strike','rules_primary','open_time','close_time')


def projection(m):return {k:m.get(k) for k in FIELDS}


def integer(value):
    d=Decimal(str(value))
    if not d.is_finite() or d!=d.to_integral_value():raise ValueError('noninteger_value')
    return int(d)


def interval(m):
    kind=m.get('strike_type');low=high=None
    if kind=='greater':low=integer(m['floor_strike'])+1
    elif kind=='less':high=integer(m['cap_strike'])-1
    elif kind=='between':
        low=integer(m['floor_strike']);high=integer(m['cap_strike'])
        if low>high:raise ValueError('reversed_bounds')
    else:raise ValueError('unsupported_strike_type')
    text=' '.join(str(m.get('yes_sub_title') or '').replace('°','').replace('–','-').lower().split())
    pair=re.fullmatch(r'(-?\d+)\s*(?:to|through|-)\s*(-?\d+)',text)
    above=re.fullmatch(r'(-?\d+)\s+or\s+(?:above|higher|greater)',text)
    below=re.fullmatch(r'(-?\d+)\s+or\s+(?:below|lower|less)',text)
    expected=(tuple(map(int,pair.groups())) if pair else
              (int(above.group(1)),None) if above else
              (None,int(below.group(1))) if below else None)
    if expected!=(low,high):raise ValueError('text_machine_bounds_disagree')
    return low,high


def report(row,station):
    raw=' '.join(str(row.get('metar') or '').split())
    if 'COR' in raw.split() or '$' in raw:raise ValueError('corrected_or_maintenance_report')
    if row.get('station') not in (station['station'],station['iem']):raise ValueError('csv_station_mismatch')
    at=datetime.fromisoformat(row['valid'].replace(' ','T'))
    at=at.replace(tzinfo=UTC) if at.tzinfo is None else at.astimezone(UTC)
    header=re.match(r'^(?:(?:METAR|SPECI)\s+)?(K[A-Z0-9]{3})\s+(\d{2})(\d{2})(\d{2})Z\b',raw)
    if not header or header.group(1)!=station['station'] or tuple(map(int,header.groups()[1:]))!=(at.day,at.hour,at.minute):
        raise ValueError('raw_csv_identity_or_clock_mismatch')
    if ' RMK ' not in raw:raise ValueError('no_remarks')
    groups=re.findall(r'\b1([01])(\d{3})\b',raw.split(' RMK ',1)[1])
    if len(groups)!=1:raise ValueError('missing_or_ambiguous_six_hour_max')
    sign,digits=groups[0];c=Decimal(digits)/10*(-1 if sign=='1' else 1)
    fahrenheit=c*Decimal(9)/5+32;whole=fahrenheit.to_integral_value(rounding=ROUND_HALF_UP)
    if abs(fahrenheit-whole)>Decimal('.100001') or not -150<=whole<=160:
        raise ValueError('no_whole_F_fidelity')
    base=at.replace(hour=(at.hour//6)*6,minute=0,second=0,microsecond=0)
    nominal=min((base,base+timedelta(hours=6)),key=lambda t:abs((at-t).total_seconds()))
    if abs((at-nominal).total_seconds())>900:raise ValueError('not_synoptic_six_hour_window')
    return dict(valid_at=at.timestamp(),nominal_at=nominal.timestamp(),
        interval_start=(nominal-timedelta(hours=6,minutes=15)).timestamp(),
        interval_end=(nominal+timedelta(minutes=15)).timestamp(),
        available_at=(nominal+timedelta(hours=1)).timestamp(),maximum_f=int(whole),
        lower_bound_f=int(whole)-2,raw=raw,publication_verified=False)


def climate_window(day,station):
    begin=datetime.combine(day,datetime.min.time(),tzinfo=UTC)-timedelta(hours=station['standard_utc_offset_hours'])
    return begin.timestamp(),(begin+timedelta(days=1)).timestamp()


def station_rules(m,station):
    rules=str(m.get('rules_primary') or '').lower()
    if not all(k in rules for k in station['rules_keywords']):return False
    if station['station']=='KLAX':
        return ('los angeles international' in rules or 'clilax' in rules or bool(re.search(r'\blax\b',rules))
            or str(m.get('event_ticker') or '').startswith('KXHIGHLAX-')
            and bool(re.search(r'los angeles airport,?\s+ca\b',rules)))
    return True


def source_agency(m):
    rules=str(m.get('rules_primary') or '').lower()
    if 'the weather company' in rules:return 'TWC'
    if 'national weather service' in rules:return 'NWS'
    return 'unverified'


def select_day(day,station,reports,markets):
    if any(set(m)-set(FIELDS) for m in markets):raise ValueError('unapproved_selection_field')
    begin,end=climate_window(day,station);base=datetime.combine(day,datetime.min.time(),tzinfo=UTC)
    relevant=[r for r in reports if begin<=r['interval_start'] and r['interval_end']<=end]
    rejects=Counter()
    for hours in (13,19,25):
        decision=(base+timedelta(hours=hours)).timestamp()
        available=[r for r in relevant if r['available_at']<=decision]
        if not available:continue
        obs=max(available,key=lambda r:(r['lower_bound_f'],r['valid_at']))
        lb=obs['lower_bound_f'];candidates=[]
        for m in markets:
            try:
                if m.get('market_type')!='binary' or not station_rules(m,station):raise ValueError('station_or_contract_unverified')
                if not (datetime.fromisoformat(m['open_time'].replace('Z','+00:00')).timestamp()<=decision<
                        datetime.fromisoformat(m['close_time'].replace('Z','+00:00')).timestamp()):raise ValueError('outside_trading_window')
                lo,hi=interval(m)
                if hi is None and lb>=lo:side='yes';crossed=lo
                elif hi is not None and lb>hi:side='no';crossed=hi+1
                else:continue
                candidates.append(dict(ticker=m['ticker'],side=side,decision_at=decision,
                    lower_bound_f=lb,interval=[lo,hi],distance=lb-crossed,report=obs))
            except (ValueError,KeyError,TypeError,ArithmeticError) as e:rejects[str(e)]+=1
        if candidates:return sorted(candidates,key=lambda r:(r['distance'],r['ticker'],r['side']))[0],relevant,dict(rejects)
    return None,relevant,dict(rejects)


def numeric_final(markets):
    values=set();faults=[];verified=0
    for m in markets:
        if m.get('status') not in ('settled','finalized') or m.get('result') not in ('yes','no'):continue
        try:
            high=integer(m['expiration_value']);lo,hi=interval(projection(m))
            if not -150<=high<=160:raise ValueError('invalid_final_high')
            yes=(lo is None or high>=lo) and (hi is None or high<=hi)
            if (m['result']=='yes')!=yes:raise ValueError('numeric_label_disagreement')
            if Decimal(str(m['settlement_value_dollars']))!=int(yes):raise ValueError('money_label_disagreement')
            values.add(high);verified+=1
        except (ValueError,KeyError,TypeError,ArithmeticError) as e:
            faults.append(dict(ticker=m.get('ticker'),reason=str(e)))
    return (next(iter(values)) if len(values)==1 else None),verified,faults


def run():
    spec=json.loads((ROOT/'registration.json').read_text());capture=json.loads((ROOT/'collection.json').read_text())
    recovered=json.loads((ROOT/'routing-recovery.json').read_text())
    capture['events']={**capture['events'],**recovered['events']}
    files={};by_station={};source_counts={}
    def body(receipt):
        if receipt.get('status')!=200:raise ValueError('source_not_successful')
        p=Path(receipt['body_path']);raw=p.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=receipt['body_sha256']:raise ValueError('source_body_changed')
        files[str(p)]=receipt['body_sha256'];return raw
    for station in spec['stations']:
        rows=[];counts=Counter();groups={}
        try:
            text=body(capture['observations'][station['station']]).decode('utf-8-sig')
            reader=csv.DictReader(line for line in text.splitlines() if not line.startswith('#'))
            if not {'station','valid','metar'}<=set(reader.fieldnames or []):raise ValueError('observation_csv_schema')
            for row in reader:groups.setdefault((row.get('station'),row.get('valid')),set()).add(row.get('metar'))
            for (site,valid),versions in groups.items():
                if len(versions)!=1:counts['conflicting_timestamp_versions']+=1;continue
                try:rows.append(report(dict(station=site,valid=valid,metar=next(iter(versions))),station))
                except (ValueError,KeyError,TypeError,ArithmeticError) as e:counts[str(e)]+=1
        except (ValueError,KeyError,UnicodeError) as e:counts['source_error:'+str(e)]+=1
        by_station[station['station']]=rows;source_counts[station['station']]=dict(counts)
    excluded={(x['station'],x['date']) for x in spec['dates']['excluded_prior_inspected']}
    days=[];contradictions=[]
    for station in spec['stations']:
        for i in range(62):
            day=date(2026,7,1)+timedelta(days=i);event=station['series']+'-'+day.strftime('%y%b%d').upper()
            record=dict(station=station['station'],date=str(day),event=event,selected=None)
            if (station['station'],str(day)) in excluded:record['state']='excluded_prior_inspected';days.append(record);continue
            try:
                payload=json.loads(body(capture['events'][event]));markets=payload.get('markets',[])
                if payload.get('cursor') or not markets or len(markets)>24 or any(m.get('event_ticker')!=event for m in markets):
                    raise ValueError('missing_incomplete_or_wrong_event')
                agencies=sorted({source_agency(projection(m)) for m in markets})
                record['settlement_source_agencies']=agencies
                if agencies!=['NWS']:
                    record.update(state='unsupported_settlement_source');days.append(record);continue
                if any(m.get('market_type')!='binary' or not station_rules(projection(m),station) for m in markets):
                    raise ValueError('event_station_mapping_unverified')
                candidate,relevant,rejects=select_day(day,station,by_station[station['station']],[projection(m) for m in markets])
                final,verified,faults=numeric_final(markets)
                available=[x for x in relevant if x['available_at']<=(datetime.combine(day,datetime.min.time(),tzinfo=UTC)+timedelta(hours=25)).timestamp()]
                record.update(selected=candidate,usable_reports=len(available),final_high_f=final,
                    verified_numeric_contracts=verified,settlement_field_faults=faults,selection_rejections=rejects)
                if final is None:record['state']='final_measurement_unverified'
                elif not available:record['state']='no_available_wholly_contained_report'
                else:
                    bad=[x for x in available if x['lower_bound_f']>final]
                    record.update(state='bound_contradiction' if bad else 'settlement_fields_incomplete' if faults else 'bound_consistent',bound_violations=bad)
                    if bad:contradictions.append(dict(station=station['station'],date=str(day),final_high_f=final,violations=bad))
                    if candidate:
                        lo,hi=candidate['interval'];yes=(lo is None or final>=lo) and (hi is None or final<=hi)
                        record['selected_logical_outcome_correct']=(candidate['side']=='yes')==yes
            except (ValueError,KeyError,TypeError,ArithmeticError) as e:record.update(state='blocked',error=str(e))
            days.append(record)
    evaluated=[d for d in days if d['state']!='excluded_prior_inspected']
    complete=[d for d in evaluated if d['state'] in ('bound_consistent','bound_contradiction')]
    station_counts=dict(Counter(d['station'] for d in complete));date_count=len({d['date'] for d in complete})
    passed=(len(complete)>=150 and date_count>=45 and len(station_counts)==3 and min(station_counts.values(),default=0)>=40
        and not contradictions and not capture['access_blocks'] and all(d['state']=='bound_consistent' for d in evaluated))
    result=dict(schema='weather_bound_unpriced_result_v2_routed',registration_sha256=hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),
        source_repair_registration_sha256=hashlib.sha256((ROOT/'source-routing-repair.json').read_bytes()).hexdigest(),
        source_identity_evidence_sha256=hashlib.sha256((ROOT/'source-identity-evidence.json').read_bytes()).hexdigest(),
        routing_recovery_sha256=hashlib.sha256((ROOT/'routing-recovery.json').read_bytes()).hexdigest(),
        states=dict(Counter(d['state'] for d in days)),complete_city_days=len(complete),complete_distinct_dates=date_count,
        complete_by_station=station_counts,selected_logical_candidates=sum(d['selected'] is not None for d in evaluated),
        selected_logical_incorrect=sum(d.get('selected_logical_outcome_correct') is False for d in evaluated),
        bound_contradictions=contradictions,data_qualification_passed=passed,source_counts=source_counts,files=files,days=days,
        actual_orders=0,actual_fills=0,price_inputs=0,pnl_cents=None,execution_enabled=False,profitability_proven=False,
        historical_publication_verified=False,promotion_ready=False,
        limitations=['Unpriced measurement validation cannot demonstrate a trading edge or fills',
            'Nominal report time plus60minute publication lag is an assumption, not original receipt proof',
            'Archive may contain retrospective corrections; COR and conflicts rejected but missing original versions cannot be recovered',
            'Modern official metadata and numeric settlement fields are not independent original NWS CLI receipts',
            'Two summer months, three stations; frozen V14 evidence unchanged'])
    with (ROOT/'result.json').open('x') as f:json.dump(result,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in result.items() if k not in ('days','files','bound_contradictions','limitations')}))


if __name__=='__main__':run()
