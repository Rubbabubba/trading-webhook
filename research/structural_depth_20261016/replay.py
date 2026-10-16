"""Causal conditional two-leg state replay; never broker orders or fills."""
import json,gzip,hashlib,math,heapq
from pathlib import Path
from collections import Counter
from research.reconstructed_markdown_20261016.feed import Feed
from research.structural_rules_20261016.matcher import relation,trading_window
ROOT=Path(__file__).parent

def net(first,second,extra=0):return 100-first-second-2*(3+extra)

def step(s,at,due,views):
    phase=s['state'];late=at-due>1
    both=all(views.get(k) for k in ('low','high'))
    first=s['first'];second='high' if first=='low' else 'low'
    if phase=='awaiting_first':
        if late or not both:s['state']='first_unobserved_or_unavailable';return None
        price=views[first]['ask']
        if price>s['decision_asks'][first]:s['state']='first_price_guard_rejected';return None
        s.update(first_price=price,first_at=at,state='awaiting_second')
        return at+1
    if phase=='awaiting_second':
        price=views[second]['ask'] if both else None
        if late or price is None or price>s['decision_asks'][second]:
            s.update(state='awaiting_orphan_unwind',second_failure_at=at,second_failure_reason='late' if late else 'unavailable' if price is None else 'price_guard')
            return at+1
        s.update(second_price=price,second_at=at,state='conditional_normal_bundle',normal_min_net_cents={str(k):net(s['first_price'],price,k) for k in (0,2,5)})
        return None
    if phase=='awaiting_orphan_unwind':
        if late or not views.get(first):
            s.update(state='orphan_unwind_unresolved',full_principal_loss_stress_cents={str(k):-s['first_price']-3-k for k in (0,2,5)})
        else:
            bid=views[first]['bid']
            s.update(state='conditional_orphan_liquidation',unwind_at=at,unwind_bid=bid,modeled_net_cents={str(k):bid-s['first_price']-2*(3+k) for k in (0,2,5)})
        return None
    raise ValueError('unknown_probe_phase')

def run():
    spec=json.loads((ROOT/'registration.json').read_text());manifest=ROOT.parent/'structural_rules_20261016/metadata-audit.json'
    if hashlib.sha256(manifest.read_bytes()).hexdigest()!=spec['metadata_audit_sha256']:raise ValueError('metadata_manifest_changed')
    audit=json.loads(manifest.read_text());markets={}
    for name in audit['cached_sources']:
        for m in json.loads((ROOT.parents[1]/name).read_text()).get('payload',{}).get('markets',[]):markets.setdefault(m['ticker'],m)
    pairs=[]
    from decimal import Decimal
    for group in audit['memberships']:
        if group['relationship_id'] not in spec['relationship_ids']:continue
        members=sorted(group['tickers'],key=lambda r:(Decimal(r['strike']),r['ticker']))
        for i,lo in enumerate(members):
            for hi in members[i+1:]:
                if Decimal(lo['strike'])>=Decimal(hi['strike']):continue
                pair=relation(markets[lo['ticker']],markets[hi['ticker']])
                if pair['relationship_id']!=group['relationship_id']:raise ValueError('cached_relationship_changed')
                pairs.append(pair)
    pairs.sort(key=lambda p:(p['event_id'],Decimal(p['low_strike']),Decimal(p['high_strike']),p['low_ticker'],p['high_ticker']))
    feed=Feed();universe=None;latest={};counts=Counter();attempts=[];seen=set();queue=[];grid=None;window=None;fault=None
    def view(ticker,side,at):
        r=latest.get(ticker)
        if r is None or universe is None or ticker not in universe or not feed.valid(r['key']):return None
        channel=tuple(r['key'][:2]);received=feed.audit.channel_times.get(channel)
        if received is None or not 0<=at-received/1e9<=1 or not trading_window(markets[ticker],at):return None
        return dict(ask=math.ceil(r['quote']['ya' if side=='yes' else 'na']),bid=math.floor(r['quote']['yb' if side=='yes' else 'nb']),ticker_receipt_at=r['receipt_at'],channel_receipt_at=received/1e9,availability_at=r['available_at'],epoch_key=list(r['key']))
    def views(pair,at):
        # Entry phases require both views; unwind requires only its owned leg.
        return dict(low=view(pair['low_ticker'],'yes',at),high=view(pair['high_ticker'],'no',at))
    def decide(at):
        for pair in pairs:
            if pair['event_id'] in seen:continue
            counts['pair_grid_checks']+=1;v=views(pair,at)
            if not all(v.values()):counts['unavailable_pair_checks']+=1;continue
            asks={k:r['ask'] for k,r in v.items()}
            if net(asks['low'],asks['high'],2)<1:counts['no_cost_stressed_surplus']+=1;continue
            if len(attempts)>=100:counts['attempt_cap_checks']+=1;continue
            seen.add(pair['event_id']);index=len(attempts)
            attempt=dict(**pair,decision_at=at,decision_views=v,orders={})
            for label,first in [('low_yes_first','low'),('high_no_first','high')]:
                attempt['orders'][label]=dict(first=first,decision_asks=asks,state='awaiting_first',actual_orders=0,actual_fills=0)
                heapq.heappush(queue,(at+1,index,label))
            attempts.append(attempt)
    files=json.loads((ROOT/'acquisition.json').read_text())['files']
    try:
        for i,receipt in enumerate(files):
            p=Path(receipt['path'])
            if hashlib.sha256(p.read_bytes()).hexdigest()!=receipt['sha256']:raise ValueError('source_hash_changed')
            for line in gzip.open(p,'rt',encoding='utf-8'):
                counts['records']+=1
                if counts['records']>15000000:raise ValueError('record_cap')
                raw=json.loads(line);at=max(feed.available_ns or raw['received_ts_ns'],raw['received_ts_ns'])/1e9
                if i>=spec['warmup_files']:
                    if window is None:window=at;grid=math.ceil(at/5)*5
                    while grid<at:decide(grid);grid+=5
                r=feed.ingest(raw)
                if raw['record_type']=='universe':universe={m['ticker'] for m in raw['markets']}
                elif raw['ticker'] in markets:
                    if r is None:latest.pop(raw['ticker'],None)
                    else:latest[raw['ticker']]=r
                while queue and queue[0][0]<=at:
                    due,index,label=heapq.heappop(queue);a=attempts[index];s=a['orders'][label]
                    v=views(a,at);next_due=step(s,at,due,v)
                    s.setdefault('observations',[]).append(dict(at=at,due=due,views=v,state=s['state']))
                    if next_due is not None:heapq.heappush(queue,(next_due,index,label))
            print(json.dumps(dict(file_complete=i+1,global_file=spec['global_file_numbers'][i],records=counts['records'],attempts=len(attempts))),flush=True)
    except (ValueError,KeyError,TypeError,ArithmeticError,OSError) as e:fault=dict(reason=type(e).__name__+': '+str(e),record=counts['records'])
    for a in attempts:
        for s in a['orders'].values():
            if s['state'].startswith('awaiting_'):
                s['state']='unresolved_capture_end'
                if 'first_price' in s:s['full_principal_loss_stress_cents']={str(k):-s['first_price']-3-k for k in (0,2,5)}
    report=dict(schema='structural_delayed_state_replay_v2',registration_sha256=hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),timing_clarification_sha256=hashlib.sha256((ROOT/'timing-clarification.json').read_bytes()).hexdigest(),complete_registered_window=fault is None,source_fault=fault,decision_window=[window,feed.available_ns/1e9],validated_pairs=len(pairs),counts=dict(counts),feed_counts=dict(feed.counts),audit_counts=dict(feed.audit.counts),attempts=attempts,primary_states=dict(Counter(a['orders']['low_yes_first']['state'] for a in attempts)),actual_orders=0,actual_fills=0,realized_profit_cents=None,promotion_ready=False,profitability_proven=False,execution_enabled=False,historical_rule_publication_verified=False,administrative_cancellation_payoff_verified=False,note='Continuous state/channels may be current while last ticker update is older. This modeled feed-availability assumption is explicit; no fresh REST/broker execution evidence is claimed.')
    with (ROOT/'replay-result.json').open('x') as f:json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in report.items() if k!='attempts'}))
if __name__=='__main__':run()
