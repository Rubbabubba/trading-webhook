"""Independent selection, raw-tape, causal-clock and cash consistency audit.

Never imports the execution model, tape reader or summary function. Shared
external source truth and genuine own fills remain separate unverified gates.
"""
import gzip,hashlib,json,math
from pathlib import Path
from decimal import Decimal
from datetime import datetime
from urllib.parse import urlparse,parse_qs
from research.v15_delayed_ioc_20261016.trade_contract import validate

ROOT=Path(__file__).parent


def read_view(view,side):
    if view is None:return None
    return ((view['yes_bid'],view['yes_ask'],view['yes_bid_size']) if side=='yes'
            else (100-view['yes_ask'],100-view['yes_bid'],view['no_bid_size']))


def run():
    r=json.loads((ROOT/'review.json').read_text());pre=json.loads((ROOT/'preliminary.json').read_text())
    assert r['registration_sha256']==hashlib.sha256((ROOT/'registration-v2.json').read_bytes()).hexdigest()
    assert r['source_registration_sha256']==hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest()
    assert r['preliminary_sha256']==hashlib.sha256((ROOT/'preliminary.json').read_bytes()).hexdigest()
    assert r['actual_orders']==r['actual_fills']==0 and not r['profitability_proven']
    assert len(r['selected'])==len({a['event_id'] for a in r['selected']})<=100
    source_path=Path(pre['views']['path'])
    assert hashlib.sha256(source_path.read_bytes()).hexdigest()==pre['views']['sha256']
    views={};checked_views=0
    for line in gzip.open(source_path,'rt',encoding='utf-8'):
        row=json.loads(line);key=(row['ticker'],row['at']);assert key not in views
        views[key]=row['view'];v=row['view']
        if v:
            assert v['ticker_receipt_at']<=v['available_at']<=row['at']
            assert 0<=row['at']-v['channel_at']<=1
            checked_views+=1
    markets={}
    for p in sorted(ROOT.parent.glob('*/official-metadata/*.json')):
        for m in (json.loads(p.read_text()).get('payload') or {}).get('markets',[]):
            markets.setdefault(m['ticker'],m)
    def window(a):
        m=markets.get(a['ticker'])
        if not m or m.get('market_type')!='binary' or m.get('mve_collection_ticker') or not m.get('rules_primary') or not m.get('event_ticker'):return None
        cuts=[datetime.fromisoformat(m[k].replace('Z','+00:00')).timestamp()
              for k in ('close_time','expected_expiration_time','occurrence_datetime') if m.get(k)]
        if not cuts or not m.get('open_time'):return None
        begin=datetime.fromisoformat(m['open_time'].replace('Z','+00:00')).timestamp();end=min(cuts)
        return (m['event_ticker'],begin,end) if begin<=a['decision_at'] and a['decision_at']+63<end else None
    expected=[];seen=set()
    for a in sorted(pre['preliminary_attempts'],key=lambda a:(a['decision_at'],a['ticker'])):
        w=window(a)
        if w and w[0] not in seen and len(expected)<100:
            seen.add(w[0]);expected.append(a['ticker'])
    assert expected==[a['ticker'] for a in r['selected']]
    tape_rows=pages=conditional=uncertain=0;global_ids={}
    for a in r['selected']:
        d=a['decision_at'];ticker=a['ticker']
        assert (a['client_check_at'],a['activation_at'],a['cancel_request_at'],a['cancel_ack_at'])==(d+1,d+2,d+62,d+63)
        assert a['client_view']==views.get((ticker,d+1)) and a['arrival_view']==views.get((ticker,d+2))
        obs=r['tapes'][ticker];recovered={};cursor=None
        for page in obs['pages']:
            query=parse_qs(urlparse(page['url']).query)
            assert query['ticker']==[ticker] and query['min_ts']==[str(d+1)] and query['max_ts']==[str(d+64)]
            assert query.get('cursor',[None])==[cursor]
            key=hashlib.sha256(page['url'].encode()).hexdigest()
            body=source_path.parent/'trades'/(key+'.body')
            if page.get('status')!=200:
                assert not obs['complete'];break
            raw=body.read_bytes();assert hashlib.sha256(raw).hexdigest()==page['raw_sha256']
            payload=json.loads(raw);cursor=payload['cursor'];pages+=1
            try:
                for raw_row in payload['trades']:
                    row=validate(raw_row,d+1,d+64);assert row['ticker']==ticker
                    old=recovered.get(row['trade_id']);assert old is None or old==row
                    recovered[row['trade_id']]=row
            except (ValueError,AssertionError):
                assert not obs['complete'];break
        if obs['complete']:
            assert not cursor and recovered=={x['trade_id']:x for x in obs['trades']}
        for row in obs['trades']:
            old=global_ids.get(row['trade_id'])
            if old is not None and old!=row:assert r['global_identity_conflicts']
            global_ids[row['trade_id']]=row;tape_rows+=1
        for name,p in a['paths'].items():
            signal=a['signal_details'][{'v15':'v15_ioc','v12':'frozen_v12','v10':'frozen_v10'}[name]]
            if signal is None:
                assert p['state']=='benchmark_no_trade';continue
            side=signal['outcome'];limit=signal['price_cents']
            client=read_view(a['client_view'],side);arrival=read_view(a['arrival_view'],side)
            if client is None:assert p['state']=='activation_unobserved';continue
            if not client[0]<=limit<client[1]:assert p['state']=='activation_quote_rejected';continue
            if arrival is None:assert p['state']=='arrival_unobserved_uncertain_possible_fill';continue
            if arrival[1]<=limit:assert p['state']=='post_only_arrival_rejected';continue
            if not obs['complete']:assert p['state']=='tape_incomplete_uncertain_possible_fill';continue
            candidates=[x for x in obs['trades'] if x['taker_outcome_side']!=side and Decimal(x[side+'_price'])*100<=limit
                        and d+1<=x['at']<=d+64]
            if not candidates:assert p['state']=='no_qualifying_public_trade';continue
            first=min(x['at'] for x in candidates);group=[x for x in candidates if x['at']==first]
            safe=d+3<=first<=d+61 and all(Decimal(x['count'])>=1 and Decimal(x[side+'_price'])*100<=limit-1 for x in group)
            assert p['state']==('conditional_trade_through' if safe else 'uncertain_possible_fill')
            assert sorted(p['public_trade_ids'])==sorted(x['trade_id'] for x in group)
            if not safe:uncertain+=1;continue
            conditional+=1;assert p['entry_at']==first and p['modeled_entry_cost']==limit+3
            end=window(a)[2]
            for h in (5,30,300):
                e=p['exits'][str(h)];at=math.ceil(first+h+1);v=views.get((ticker,at));q=read_view(v,side)
                assert e['source_view']==v
                if q is None or q[2]<1 or at-first>h+5 or at>=end:
                    assert e['state'].startswith('unresolved');continue
                assert e['state']=='conditional_exit' and e['at']==at
                assert e['net_proceeds_before_extra_stress']==max(0,math.floor(q[0])-1)-2
    for name,track in r['tracks'].items():
        paths=[a['paths'][name] for a in r['selected']]
        for h in (5,30,300):
            complete=[p for p in paths if p['state']=='conditional_trade_through' and p['exits'][str(h)]['state']=='conditional_exit']
            unknown=[p for p in paths if 'uncertain_possible_fill' in p['state'] or
                p['state']=='conditional_trade_through' and p['exits'][str(h)]['state']!='conditional_exit']
            for stress in (0,2,5):
                net=sum(p['exits'][str(h)]['net_proceeds_before_extra_stress']-stress for p in complete)
                net-=sum(p['modeled_entry_cost']+stress for p in complete)
                loss=-sum(p['modeled_entry_cost']+stress for p in unknown)
                s=track['horizons'][str(h)]['scenarios'][str(stress)]
                assert s['complete_net_cents']==(net if complete else None)
                assert s['combined_cents']==(net+loss if complete or unknown else None)
    receipt=dict(schema='v15_resting_independent_audit_v2',review_sha256=hashlib.sha256((ROOT/'review.json').read_bytes()).hexdigest(),
        selected_events_checked=len(r['selected']),causal_source_views_checked=checked_views,raw_tape_pages_checked=pages,
        public_tape_rows=tape_rows,conditional_paths_checked=conditional,ambiguous_trade_paths_checked=uncertain,
        result='passed_internal_consistency_not_profit_or_own_fill_certification',actual_orders=0,actual_fills=0,profitability_proven=False)
    with (ROOT/'independent-audit.json').open('x') as f:json.dump(receipt,f,indent=2,allow_nan=False)
    print(json.dumps(receipt))


if __name__=='__main__':run()
