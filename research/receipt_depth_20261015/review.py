import hashlib,json,math,statistics
from collections import Counter,defaultdict
from datetime import datetime
from pathlib import Path
ROOT=Path(__file__).parent


def eligible(signal,market):
    if not market or market.get('ticker')!=signal['ticker']:return 'missing_official_metadata'
    if market.get('market_type')!='binary' or market.get('mve_collection_ticker') or market['ticker'].startswith('KXMVE'):return 'unsupported_contract'
    if not market.get('event_ticker') or not market.get('rules_primary'):return 'missing_event_or_rules'
    cuts=[datetime.fromisoformat(market[k].replace('Z','+00:00')).timestamp() for k in ('close_time','expected_expiration_time','occurrence_datetime') if market.get(k)]
    if not cuts or not market.get('open_time'):return 'missing_trading_window'
    opened=datetime.fromisoformat(market['open_time'].replace('Z','+00:00')).timestamp()
    times=[signal['decision_at']]+[signal[k] for k in ('entry_at','exit_at') if k in signal]
    if not all(opened<=t<min(cuts) for t in times):return 'outside_official_trading_window'
    return None


def review(input_name='preliminary-v2-invalid-book-corrected.json',output_name='review-v1.json'):
    pre=json.loads((ROOT/input_name).read_text())
    markets={}
    for p in (ROOT/'official-metadata').glob('*.json'):
        for m in json.loads(p.read_text())['payload']['markets']:markets[m['ticker']]=m
    selected=[];seen=set();rejects=Counter()
    for s in sorted(pre['preliminary_signals'],key=lambda s:(s['decision_at'],s['ticker'])):
        m=markets.get(s['ticker']);reason=eligible(s,m)
        if reason:rejects[reason]+=1;continue
        event=m['event_ticker']
        if event in seen:rejects['duplicate_event_attempt']+=1;continue
        if len(selected)>=100:rejects['attempt_cap']+=1;continue
        seen.add(event)
        selected.append(dict(s,event_id=event,family=s['ticker'].split('-',1)[0]))
    complete=[s for s in selected if s['state']=='conditional_round_trip_complete']
    unresolved=[s for s in selected if s.get('entry_at') is not None and s['state']!='conditional_round_trip_complete']
    scenarios={}
    for stress in (0,2,5):
        vals=[s['net_cents_by_extra_stress'][str(stress)] for s in complete]
        # Unresolved one-contract long inventory receives a full principal-loss
        # stress, rather than being silently excluded or assigned zero.
        unresolved_loss=-sum(s['entry_price']+6+stress for s in unresolved)
        scenarios[str(stress)]={'complete_net_cents':sum(vals),'unresolved_full_principal_loss_stress_cents':unresolved_loss,'net_with_unresolved_loss_stress_cents':sum(vals)+unresolved_loss,
            'independent_event_mean_cents':statistics.mean(vals) if vals else None,'normal_event_95_lcb_cents':statistics.mean(vals)-1.96*statistics.stdev(vals)/math.sqrt(len(vals)) if len(vals)>=2 else None}
    report={'schema':'external_receipt_depth_review_v1','source_records':pre['counts']['records'],'sequence_defects':len(pre['sequence_defects']),'official_metadata_returned':len(markets),
        'preliminary_signals':len(pre['preliminary_signals']),'selected_event_attempts':len(selected),'complete_conditional_round_trips':len(complete),'unresolved_owned_inventory_paths':len(unresolved),'states':dict(Counter(s['state'] for s in selected)),
        'families':dict(Counter(s['family'] for s in selected)),'rejections':dict(rejects),'scenarios':scenarios,'selected':selected,
        'execution_enabled':False,'actual_orders':0,'actual_fills':0,'profitability_proven':False,'promotion_ready':False,
        'decision':'do_not_advance_insufficient_or_failed_executable_evidence',
        'limitations':['One externally selected short period from one day; independent larger holdout absent','Data license other; no production feed grant established','Historical fee applicability unverified; fixed5c/fill reserve not actual fees','Modern rules/close metadata may not establish original outcome-known timing','Receipt timestamps are recorder clocks, not our actual fills or exchange confirmations','Top3depth simulation excludes market impact and orders racing the displayed quotes','Unresolved exits retained; complete subset never overall realized profit']}
    report['preliminary_input']=input_name
    report['tail_recovery']=pre.get('tail_recovery')
    with (ROOT/output_name).open('x') as f:json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in report.items() if k not in ('selected','limitations')}))


if __name__=='__main__':
    import sys
    review(*(sys.argv[1:]))
