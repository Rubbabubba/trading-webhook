"""Audit previously cached rule metadata only; no source prices are read."""
from pathlib import Path
from collections import Counter,defaultdict
from datetime import datetime,timezone
import json,hashlib
from .matcher import identity,relation
from opportunity_lab.kalshi_external_sleeves import threshold_identity
ROOT=Path(__file__).parent
FIELDS=('ticker','market_type','strike_type','floor_strike','cap_strike','rules_primary','rules_secondary','event_ticker','occurrence_datetime','expiration_time','close_time','open_time','expected_expiration_time','exchange_index','mve_collection_ticker')

def run():
    markets={};conflicts=set();sources=[]
    for p in sorted(ROOT.parent.glob('*/official-metadata/*.json')):
        payload=json.loads(p.read_text()).get('payload',{})
        rows=payload.get('markets',[])
        if not rows:continue
        sources.append(str(p.relative_to(ROOT.parents[1])))
        for row in rows:
            ticker=row['ticker'];m={k:row.get(k) for k in FIELDS}
            if m.get('exchange_index') is None:m['exchange_index']=0
            if ticker in markets and markets[ticker]!=m:conflicts.add(ticker)
            markets.setdefault(ticker,m)
    if len(markets)>3000:raise ValueError('metadata_audit_cap')
    groups=defaultdict(list);rejects=Counter();old_accepted=0;new_accepted=0;examples=[]
    for ticker,m in sorted(markets.items()):
        if ticker in conflicts:rejects['conflicting_cached_rule_metadata']+=1;continue
        try:threshold_identity(m);old_accepted+=1
        except ValueError:pass
        try:key,strike=identity(m)
        except ValueError as e:rejects[str(e)]+=1;continue
        groups[key].append((strike,m));new_accepted+=1
    pairs=0;memberships=[]
    for key,rows in sorted(groups.items()):
        rows.sort(key=lambda r:(r[0],r[1]['ticker']));n=0
        for i,(low,lo) in enumerate(rows):
            for high,hi in rows[i+1:]:
                if low==high:continue
                r=relation(lo,hi);n+=1
                if len(examples)<20:examples.append(r)
        pairs+=n
        memberships.append(dict(relationship_id=key,event_id=rows[0][1]['event_ticker'],tickers=[dict(ticker=m['ticker'],strike=str(s)) for s,m in rows],normal_settlement_pairs=n))
    report=dict(schema='structural_rule_metadata_audit_v2',audited_at=datetime.now(timezone.utc).isoformat(),registration_sha256=hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),cached_records=len(markets),cached_sources=sources,conflicting_tickers=sorted(conflicts),v1_accepted=old_accepted,v2_accepted=new_accepted,v2_rejections=dict(rejects),groups=len(groups),groups_with_multiple_strikes=sum(g['normal_settlement_pairs']>0 for g in memberships),normal_settlement_pairs=pairs,memberships=memberships,illustrative_pairs_only=examples,source_prices_read=False,executable_opportunities_tested=0,actual_orders=0,actual_fills=0,profitability_proven=False,historical_rule_publication_verified=False,administrative_cancellation_payoff_verified=False,promotion_ready=False)
    with (ROOT/'metadata-audit.json').open('x') as f:json.dump(report,f,indent=2)
    print(json.dumps({k:v for k,v in report.items() if k not in ('memberships','illustrative_pairs_only','cached_sources')}))
if __name__=='__main__':run()
