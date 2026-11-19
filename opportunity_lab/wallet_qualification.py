"""Frozen forward wallet qualification protocol and bounded report validation."""
from datetime import datetime,timezone
import hashlib,json,math,re,statistics
WALLET=re.compile(r"0x[a-f0-9]{40}")
POLICY = dict(version='wallet_forward_costs_v2', training_entries=20,
    minimum_resolved=20, minimum_events=10, minimum_days=3,
    confidence_multiplier=3, stress_cents_per_leg=1, maximum_selected=4,
    maximum_quote_age_seconds=5, maximum_signal_seconds=90,
    execution='one_contract_executable_bid_ask', fees='conservative_quadratic_whole_cent',
    holdout='all_entries_after_first_20_per_wallet', evaluation_cutoff='prior_utc_sessions_only', maximum_report_age_seconds=180)
POLICY_HASH = hashlib.sha256(json.dumps(POLICY,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def score(rows):
    """Include every resolved event in its entry-frozen partition; cluster by event."""
    result={}; reasons=[]
    for phase in ('training','holdout'):
        part=[r for r in rows if r['phase']==phase and r.get('net_cents') is not None]
        clusters={}
        for r in part:clusters.setdefault(r['event'],[]).append(r['net_cents'])
        values=[sum(v)/len(v) for v in clusters.values()]
        lower=(statistics.mean(values)-POLICY['confidence_multiplier']*statistics.stdev(values)/math.sqrt(len(values))) if len(values)>1 else None
        days=len({r['day'] for r in part});net=sum(r['net_cents'] for r in part)
        stressed=sum(r['net_cents']-POLICY['stress_cents_per_leg']*(2 if r['exit_kind']=='wallet_sell' else 1) for r in part)
        checks=dict(resolved=len(part)>=POLICY['minimum_resolved'],events=len(clusters)>=POLICY['minimum_events'],
                    days=days>=POLICY['minimum_days'],net_positive=net>0,
                    stress_positive=stressed>0,event_lower_positive=lower is not None and lower>0)
        reasons.extend(phase+':'+k for k,v in checks.items() if not v)
        result[phase]=dict(resolved=len(part),events=len(clusters),days=days,net_cents=net,
            stressed_net_cents=stressed,event_mean_lower_cents=round(lower,4) if lower is not None else None)
    pending=sum(r.get('net_cents') is None for r in rows)
    if pending:reasons.append('unresolved_entries')
    result.update(qualified=not reasons,reasons=reasons)
    return result



def validate_report(value,*,now=None):
    now=now or datetime.now(timezone.utc)
    if len(json.dumps(value,allow_nan=False).encode())>16000:raise ValueError('research_report_too_large')
    if (not isinstance(value,dict) or value.get('schema')!='wallet_profitability_v1'
            or value.get('policy_hash')!=POLICY_HASH or value.get('policy')!=POLICY
            or value.get('mode')!='prospective_shadow' or value.get('actual_fills') is not False
            or value.get('state')!='observed' or value.get('errors')!=[]):raise ValueError('invalid_wallet_research')
    at=datetime.fromisoformat(value['at'])
    if at.tzinfo is None or not 0<=(now-at).total_seconds()<=180:raise ValueError('stale_wallet_research')
    rows=value.get('scores');selected=value.get('selected_wallets')
    if not isinstance(rows,list) or len(rows)>8 or not isinstance(selected,list) or len(selected)>4:raise ValueError('invalid_wallet_selection')
    eligible=[];seen=set()
    for r in rows:
        if not isinstance(r,dict):raise ValueError('invalid_wallet_score')
        if not isinstance(r.get('reasons'),list) or len(r['reasons'])>20 or any(not isinstance(x,str) or len(x)>80 for x in r['reasons']):raise ValueError('invalid_wallet_reasons')
        if r.get('assessment_before_utc_day')!=now.astimezone(timezone.utc).date().isoformat():raise ValueError('invalid_wallet_assessment_cutoff')
        w=r.get('wallet')
        if not isinstance(w,str) or not WALLET.fullmatch(w) or w in seen:raise ValueError('invalid_wallet_selection')
        seen.add(w)
        ok=True
        for phase in ('training','holdout'):
            p=r.get(phase) or {}
            for key,minimum in [('resolved',20),('events',10),('days',3)]:
                if type(p.get(key)) is not int or p[key]<0:raise ValueError('invalid_wallet_score')
                ok=ok and p[key]>=minimum
            for key in ('net_cents','stressed_net_cents','event_mean_lower_cents'):
                n=p.get(key)
                if n is not None and (type(n) not in (int,float) or not math.isfinite(n)):raise ValueError('invalid_wallet_score')
                ok=ok and n is not None and n>0
        if type(r.get('open')) is not int or r['open']<0:raise ValueError('invalid_wallet_pending')
        ok=ok and r['open']==0
        if r.get('qualified') is not ok:raise ValueError('wallet_qualification_mismatch')
        if ok:eligible.append(w)
    expected=[r['wallet'] for r in sorted(rows,key=lambda r:r['holdout']['event_mean_lower_cents'] or -1e9,reverse=True) if r['wallet'] in eligible][:4]
    if selected!=expected:raise ValueError('wallet_selection_mismatch')
    return value
