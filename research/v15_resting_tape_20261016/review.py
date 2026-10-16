"""Official event selection, public tape join and separate horizon accounting."""
import gzip,hashlib,json,math,time,urllib.request
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
from urllib.parse import urlencode
from research.receipt_depth_20261015.review import eligible
from .model import classify,exit_observation,summarize
from .tape import Tape

ROOT=Path(__file__).parent


def causal_attempt(source):
    at=source['decision_at']
    return dict(source,source_plan_v1={k:source[k] for k in ('activation_at','cancel_request_at','cancel_ack_at')},
        client_check_at=at+1,activation_at=at+2,cancel_request_at=at+62,cancel_ack_at=at+63)


def tape_identity_conflicts(tapes):
    identities={};conflicts=set()
    for ticker,observation in tapes.items():
        for row in observation['trades']:
            old=identities.get(row['trade_id'])
            if old is not None and old[1]!=row:
                conflicts.update((ticker,old[0]))
            identities.setdefault(row['trade_id'],(ticker,row))
    return conflicts


def run():
    pre=json.loads((ROOT/'preliminary.json').read_text());spec=json.loads((ROOT/'registration-v2.json').read_text())
    view_path=Path(pre['views']['path'])
    if hashlib.sha256(view_path.read_bytes()).hexdigest()!=pre['views']['sha256']:
        raise ValueError('source_view_cache_changed')
    views={}
    for line in gzip.open(view_path,'rt',encoding='utf-8'):
        row=json.loads(line);key=(row['ticker'],row['at'])
        if key in views:raise ValueError('duplicate_source_view')
        views[key]=row['view']
        if len(views)>spec['limits']['maximum_cached_views']:raise ValueError('view_capacity')
    markets={};requested=set();metadata_errors=[]
    folder=ROOT/'official-metadata';folder.mkdir(exist_ok=True)
    for p in sorted(ROOT.parent.glob('*/official-metadata/*.json')):
        cached=json.loads(p.read_text());requested.update(cached.get('requested_tickers',[]))
        for market in (cached.get('payload') or {}).get('markets',[]):
            markets.setdefault(market['ticker'],market)
    missing=sorted({a['ticker'] for a in pre['preliminary_attempts']}-markets.keys()-requested)
    if len(missing)>1000:raise ValueError('metadata_capacity')
    for i in range(0,len(missing),25):
        batch=missing[i:i+25]
        url='https://external-api.kalshi.com/trade-api/v2/historical/markets?'+urlencode({'tickers':','.join(batch),'limit':1000})
        path=folder/(hashlib.sha256(url.encode()).hexdigest()+'.json')
        receipt=dict(url=url,requested_tickers=batch,captured_at=datetime.now(timezone.utc).isoformat())
        try:
            with urllib.request.urlopen(url,timeout=20) as response:raw=response.read(4000001)
            if len(raw)>4000000:raise ValueError('metadata_capacity')
            payload=json.loads(raw)
            if payload.get('cursor'):raise ValueError('metadata_pagination')
            receipt.update(payload=payload,status=200)
            for market in payload['markets']:markets[market['ticker']]=market
        except (ValueError,KeyError,OSError) as e:
            receipt.update(error=type(e).__name__,status='blocked');metadata_errors.append(receipt)
        with path.open('x') as f:json.dump(receipt,f,indent=2)
        time.sleep(.5)
    selected=[];seen=set();rejections=Counter()
    for original in sorted(pre['preliminary_attempts'],key=lambda a:(a['decision_at'],a['ticker'])):
        a=causal_attempt(original)
        m=markets.get(a['ticker'])
        reason=eligible(dict(a,entry_at=a['activation_at'],exit_at=a['cancel_ack_at']),m)
        if reason:rejections[reason]+=1;continue
        event=m['event_ticker']
        if event in seen:rejections['duplicate_event_attempt']+=1;continue
        if len(selected)>=100:rejections['event_capacity']+=1;continue
        seen.add(event);selected.append(dict(a,event_id=event,family=a['ticker'].split('-',1)[0]))
    tape=Tape(view_path.parent/'trades');tapes={};paths={name:[] for name in ('v15','v12','v10')}
    for index,a in enumerate(selected):
        activation=views.get((a['ticker'],a['client_check_at']))
        arrival=views.get((a['ticker'],a['activation_at']))
        signals=dict(v15=a['signal_details']['v15_ioc'],v12=a['signal_details']['frozen_v12'],v10=a['signal_details']['frozen_v10'])
        # Avoid pointless GETs where every hypothetical order fails activation.
        possible=any(classify(a,s,activation,[],complete=True,arrival_view=arrival)['state']=='no_qualifying_public_trade' for s in signals.values())
        observation=tape.fetch(a) if possible else dict(complete=False,error='no_valid_passive_activation',pages=[],trades=[],owned_fills=0)
        tapes[a['ticker']]=observation;a['paths']={};a['client_view']=activation;a['arrival_view']=arrival
        for name,signal in signals.items():
            path=classify(a,signal,activation,observation['trades'],complete=observation['complete'],arrival_view=arrival)
            if path['state']=='conditional_trade_through':
                path['exits']={}
                for h in (5,30,300):
                    at=math.ceil(path['entry_at']+h+1);v=views.get((a['ticker'],at))
                    exit=exit_observation(path,h,v,at)
                    if exit['state']=='conditional_exit' and eligible(dict(a,exit_at=at),markets[a['ticker']]):
                        exit=dict(state='unresolved_exit_after_official_close',observed_at=at)
                    exit['source_view']=v;path['exits'][str(h)]=exit
            a['paths'][name]=path;paths[name].append(path)
        print(json.dumps(dict(tape_event=index+1,total=len(selected),rows=len(observation['trades']),complete=observation['complete'],primary_state=a['paths']['v15']['state'])),flush=True)
    conflicts=tape_identity_conflicts(tapes)
    for a in selected:
        if a['ticker'] not in conflicts:continue
        observation=tapes[a['ticker']];observation.update(complete=False,error='global_trade_identity_conflict')
        signals=dict(v15=a['signal_details']['v15_ioc'],v12=a['signal_details']['frozen_v12'],v10=a['signal_details']['frozen_v10'])
        a['paths']={name:classify(a,s,a['client_view'],observation['trades'],complete=False,
            arrival_view=a['arrival_view']) for name,s in signals.items()}
    paths={name:[a['paths'][name] for a in selected] for name in ('v15','v12','v10')}
    result=dict(schema='v15_resting_trade_through_review_v2_causal',
        registration_sha256=hashlib.sha256((ROOT/'registration-v2.json').read_bytes()).hexdigest(),
        source_registration_sha256=pre['registration_sha256'],preliminary_sha256=hashlib.sha256((ROOT/'preliminary.json').read_bytes()).hexdigest(),
        source_fault=pre['source_fault'],complete_registered_window=pre['complete_registered_window'],
        decision_window=pre['decision_window'],counts=pre['counts'],preliminary_tickers=len(pre['preliminary_attempts']),
        selected_official_events=len(selected),families=dict(Counter(a['family'] for a in selected)),rejections=dict(rejections),
        metadata_errors=metadata_errors,actual_tape_requests=tape.requests,
        tape_attempted_events=sum(bool(t['pages']) for t in tapes.values()),
        tape_blocked_events=sum(bool(t['pages']) and not t['complete'] for t in tapes.values()),
        global_identity_conflicts=sorted(conflicts),
        tape_complete_events=sum(t['complete'] for t in tapes.values()),public_tape_rows=sum(len(t['trades']) for t in tapes.values()),
        tracks={name:dict(states=dict(Counter(p['state'] for p in rows)),**summarize(rows)) for name,rows in paths.items()},
        selected=selected,tapes=tapes,actual_orders=0,actual_fills=0,execution_enabled=False,profitability_proven=False,promotion_ready=False,
        parent_event_independence_verified=False,session_cluster_lcb_cents=None,
        decision='no_promotion_without_all_registered_independent_evidence',
        limitations=['Counterfactual trade-through is not own queue/acknowledgement/fill evidence',
            'Public execution UTC versus recorder receipt UTC has unverified assumed1second margin',
            'One source date; official event IDs may share parent outcomes',
            'Historical fees and original publication vintages unverified',
            'Each horizon and benchmark is separate, never pooled as independent profits',
            'Source license other, no production data grant inferred'])
    with (ROOT/'review.json').open('x') as f:json.dump(result,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in result.items() if k not in ('selected','tapes','limitations')}))


if __name__=='__main__':run()
