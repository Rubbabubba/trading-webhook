import json,math,statistics,urllib.request
from pathlib import Path
from collections import Counter
from datetime import datetime,timezone
from urllib.parse import urlencode
from research.receipt_depth_20261015.review import eligible
ROOT=Path(__file__).parent

def settlement(s,m):
    result=m.get('result')
    if m.get('status') not in ('settled','finalized') or result not in ('yes','no'):return None
    # Require independent numeric settlement field to agree with the label.
    value=m.get('settlement_value_dollars')
    if value is None:return None
    try:actual=float(value)*100
    except (ValueError,TypeError):return None
    if not math.isfinite(actual) or actual!=(100 if result=='yes' else 0):return None
    return 100 if s['side']==result else 0

def run():
    pre=json.loads((ROOT/'preliminary.json').read_text());markets={}
    folder=ROOT/'official-metadata';folder.mkdir(exist_ok=True)
    # Reuse all previously downloaded official records before issuing new GETs.
    for directory in (ROOT.parent/'receipt_depth_20261015/official-metadata',folder):
        for p in directory.glob('*.json'):
            for m in json.loads(p.read_text())['payload']['markets']:markets[m['ticker']]=m
    tickers=sorted({s['ticker'] for s in pre['preliminary_signals']}-markets.keys())
    if len(tickers)>1000:raise ValueError('bounded_metadata_review_exceeded_preserve_preliminary')
    for i in range(0,len(tickers),25):
        batch=tickers[i:i+25];path=folder/('batch-'+str(i//25)+'.json')
        url='https://external-api.kalshi.com/trade-api/v2/historical/markets?'+urlencode({'tickers':','.join(batch),'limit':1000})
        if not path.exists():
            with urllib.request.urlopen(url,timeout=20) as response:payload=json.loads(response.read(4000000))
            if payload.get('cursor'):raise ValueError('unexpected_pagination')
            with path.open('x') as f:json.dump(dict(url=url,captured_at=datetime.now(timezone.utc).isoformat(),requested_tickers=batch,payload=payload),f,indent=2)
        saved=json.loads(path.read_text())
        if saved['url']!=url:raise ValueError('cache_request_changed')
        for m in saved['payload']['markets']:markets[m['ticker']]=m
        print(json.dumps({'metadata_batch':i//25,'requested':len(batch),'returned':len(saved['payload']['markets'])}),flush=True)
    selected=[];seen=set();rejects=Counter()
    for s in sorted(pre['preliminary_signals'],key=lambda s:(s['decision_at'],s['ticker'])):
        m=markets.get(s['ticker']);reason=eligible(s,m)
        if reason:rejects[reason]+=1;continue
        event=m['event_ticker']
        if event in seen:rejects['duplicate_event_attempt']+=1;continue
        if len(selected)>=100:rejects['event_cap']+=1;continue
        seen.add(event);s=dict(s,event_id=event,family=s['ticker'].split('-',1)[0])
        if s['state']=='conditional_entry_awaiting_settlement':
            payout=settlement(s,m)
            if payout is None:s['state']='settlement_unverified'
            else:
                s.update(state='conditional_settlement_complete',payout_cents=payout,official_result=m['result'])
                s['net_cents_by_extra_stress']={str(k):payout-s['entry_price']-6-k for k in (0,2,5)}
        selected.append(s)
    complete=[s for s in selected if s['state']=='conditional_settlement_complete']
    unknown=[s for s in selected if 'entry_at' in s and s['state']!='conditional_settlement_complete']
    scenarios={}
    for stress in (0,2,5):
        vals=[s['net_cents_by_extra_stress'][str(stress)] for s in complete]
        loss=-sum(s['entry_price']+6+stress for s in unknown)
        scenarios[str(stress)]=dict(complete_net_cents=sum(vals),unresolved_full_principal_stress_cents=loss,net_with_unresolved_stress_cents=sum(vals)+loss,event_mean_cents=statistics.mean(vals) if vals else None,normal_event_95_lcb_cents=statistics.mean(vals)-1.96*statistics.stdev(vals)/math.sqrt(len(vals)) if len(vals)>1 else None)
    report=dict(schema='external_ask_markdown_review_v1',source_fault=pre['source_fault'],complete_registered_window=pre['complete_registered_window'],preliminary_signals=len(pre['preliminary_signals']),selected_event_attempts=len(selected),states=dict(Counter(s['state'] for s in selected)),families=dict(Counter(s['family'] for s in selected)),rejections=dict(rejects),scenarios=scenarios,selected=selected,actual_orders=0,actual_fills=0,execution_enabled=False,promotion_ready=False,profitability_proven=False,decision='exploratory_only_no_qualification',limitations=['One short unused window from same date; not independent multi-date holdout','Recorder clocks and modern official rule metadata do not prove our latency or original rule publication','5c fee and1c slippage reserves are assumptions; actual historical fees unverified','Top3displayed depth and settlement accounting are conditional, never actual fills','Source license other, no production rights assumed','Unfilled attempts and unknown settlements remain retained','Source-clock fault stops processing; prefix is not the complete registered window'])
    with (ROOT/'review.json').open('x') as f:json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in report.items() if k not in ('selected','limitations')}))
if __name__=='__main__':run()
