"""Separate discovery/selection guard; never rewrites the frozen v2 trade cohorts."""
import hashlib,json,math

POLICY=dict(version='wallet_pool_v1',categories=['OVERALL','SPORTS'],periods=['WEEK','MONTH'],
    leaderboard_size=25,maximum_wallets=100,active_slots=12,exploration_slots=4,maximum_shadow_owners=12,
    refresh_seconds=3600,maximum_discovery_age_seconds=86400,
    incompatible_minimum_observations=10,incompatible_grace_seconds=900,
    inactivity_seconds=3600,cooldown_seconds=21600,confidence_multiplier=3.5)
HASH=hashlib.sha256(json.dumps(POLICY,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def adjusted(row):
    """A stricter event-cluster bound for the larger discovery pool."""
    row=dict(row);row['base_qualified']=row['qualified'];row['reasons']=list(row['reasons'])
    for phase in ('training','holdout'):
        p=dict(row[phase]);mean,se=p.get('event_mean_cents'),p.get('event_standard_error_cents')
        p['pool_event_lower_cents']=round(mean-POLICY['confidence_multiplier']*se,4) if mean is not None and se is not None else None
        row[phase]=p
        if p['pool_event_lower_cents'] is None or p['pool_event_lower_cents']<=0:
            row['qualified']=False;row['reasons'].append('pool:'+phase+'_lower_bound')
    if row.get('coverage_gaps',0):row['qualified']=False;row['reasons'].append('pool:coverage_gap')
    return row


def validate_guard(row,base_ok):
    if row.get('base_qualified') is not base_ok:raise ValueError('wallet_pool_base_mismatch')
    gaps=row.get('coverage_gaps')
    if type(gaps) is not int or gaps<0:raise ValueError('wallet_pool_invalid_coverage')
    ok=base_ok and gaps==0
    for phase in ('training','holdout'):
        p=row[phase];mean,se=p.get('event_mean_cents'),p.get('event_standard_error_cents')
        if mean is None or se is None:
            if p.get('pool_event_lower_cents') is not None or p.get('event_mean_lower_cents') is not None:
                raise ValueError('wallet_pool_missing_statistics')
            ok=False;continue
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in (mean,se)) or se<0:
            raise ValueError('wallet_pool_invalid_statistics')
        for field,multiplier in [('event_mean_lower_cents',3),('pool_event_lower_cents',POLICY['confidence_multiplier'])]:
            value=p.get(field)
            if type(value) not in (int,float) or not math.isfinite(value) or not math.isclose(value,mean-multiplier*se,abs_tol=.001):
                raise ValueError('wallet_pool_lower_bound_mismatch')
        ok=ok and p['pool_event_lower_cents']>0
    return ok
