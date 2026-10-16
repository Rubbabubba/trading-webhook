"""Official-event filtering and independent conditional cash accounting."""
import hashlib, json, math, statistics, urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from research.receipt_depth_20261015.review import eligible

ROOT = Path(__file__).parent


def account(paths):
    entered = [p for p in paths if p['state'] == 'conditional_entry']
    horizons = {}
    for horizon in (5, 30, 300):
        complete = [p for p in entered if p['exits'][str(horizon)]['state'] == 'conditional_exit']
        unknown = [p for p in entered if p not in complete]
        scenarios = {}
        for stress in (0, 2, 5):
            values = [p['exits'][str(horizon)]['proceeds_price'] -
                      p['entry_cost_price'] - 4 - 2*stress for p in complete]
            full_loss = -sum(p['entry_cost_price'] + 2 + stress for p in unknown)
            scenarios[str(stress)] = dict(
                complete_net_cents=sum(values) if values else None,
                unresolved_full_principal_stress_cents=full_loss if unknown else None,
                combined_with_unresolved_stress_cents=sum(values)+full_loss if entered else None,
                normal_official_event_proxy_lcb_cents=(statistics.mean(values) -
                    1.96*statistics.stdev(values)/math.sqrt(len(values))) if len(values)>1 else None,
                session_cluster_lcb_cents=None)
        horizons[str(horizon)] = dict(complete_conditional_roundtrips=len(complete),
            unresolved_exits=len(unknown), gross_cents=sum(
                p['exits'][str(horizon)]['bid']-p['entry_ask'] for p in complete) if complete else None,
            scenarios=scenarios)
    return dict(states=dict(Counter(p['state'] for p in paths)),
        conditional_entries=len(entered), complete_all_horizons=sum(
            all(p['exits'][str(h)]['state']=='conditional_exit' for h in (5,30,300))
            for p in entered), horizons=horizons)


def run():
    pre = json.loads((ROOT/'preliminary.json').read_text())
    markets = {}; folder = ROOT/'official-metadata'; folder.mkdir(exist_ok=True)
    for path in sorted(ROOT.parent.glob('*/official-metadata/*.json')):
        for m in json.loads(path.read_text()).get('payload', {}).get('markets', []):
            markets.setdefault(m['ticker'], m)
    missing = sorted({a['ticker'] for a in pre['preliminary_attempts']} - markets.keys())
    if len(missing)>1000:
        raise ValueError('metadata_budget_exceeded')
    for i in range(0, len(missing), 25):
        batch = missing[i:i+25]; path = folder/('batch-'+str(i//25)+'.json')
        url = 'https://external-api.kalshi.com/trade-api/v2/historical/markets?'+urlencode(
            {'tickers': ','.join(batch), 'limit': 1000})
        if not path.exists():
            with urllib.request.urlopen(url, timeout=20) as response:
                raw = response.read(4000001)
            if len(raw)>4000000:
                raise ValueError('metadata_response_limit')
            payload = json.loads(raw)
            if payload.get('cursor'):
                raise ValueError('unexpected_metadata_pagination')
            with path.open('x') as f:
                json.dump(dict(url=url, captured_at=datetime.now(timezone.utc).isoformat(),
                    requested_tickers=batch, payload=payload), f, indent=2)
        saved = json.loads(path.read_text())
        if saved['url'] != url:
            raise ValueError('metadata_request_changed')
        for m in saved['payload']['markets']:
            markets[m['ticker']] = m
        print(json.dumps(dict(metadata_batch=i//25, requested=len(batch),
                              returned=len(saved['payload']['markets']))), flush=True)
    selected = []; seen = set(); rejects = Counter(); path_rejects = Counter()
    for a in sorted(pre['preliminary_attempts'], key=lambda x:(x['decision_at'],x['ticker'])):
        m = markets.get(a['ticker']); reason = eligible(a, m)
        if reason:
            rejects[reason] += 1; continue
        event = m['event_ticker']
        if event in seen:
            rejects['duplicate_event_attempt'] += 1; continue
        if len(selected)>=100:
            rejects['event_cap'] += 1; continue
        seen.add(event)
        for name, path in a['paths'].items():
            if path['state'] != 'conditional_entry':
                continue
            if eligible(dict(ticker=a['ticker'],decision_at=a['decision_at'],
                             entry_at=path['entry_at']), m):
                path['state'] = 'entry_outside_official_window'
                path_rejects[name+'_entry_outside_official_window'] += 1
                continue
            for horizon, exit in path['exits'].items():
                if exit['state'] == 'conditional_exit' and eligible(dict(
                        ticker=a['ticker'],decision_at=a['decision_at'],exit_at=exit['at']), m):
                    exit['original_state'] = exit['state']
                    exit['state'] = 'unresolved_exit_after_official_close'
                    path_rejects[name+'_exit_outside_official_window'] += 1
        selected.append(dict(a, event_id=event, family=a['ticker'].split('-',1)[0]))
    report = dict(schema='v15_delayed_ioc_review_v1',
        preliminary_sha256=hashlib.sha256((ROOT/'preliminary.json').read_bytes()).hexdigest(),
        registration_sha256=pre['registration_sha256'], source_fault=pre['source_fault'],
        complete_registered_window=pre['complete_registered_window'],
        decision_window=pre['decision_window'], counts=pre['counts'],
        preliminary_tickers=len(pre['preliminary_attempts']), selected_official_events=len(selected),
        families=dict(Counter(a['family'] for a in selected)), rejections=dict(rejects),
        path_rejections=dict(path_rejects), tracks={name:account([a['paths'][name] for a in selected])
            for name in ('v15_ioc','frozen_v12','frozen_v10')}, selected=selected,
        actual_orders=0, actual_fills=0, execution_enabled=False, profitability_proven=False,
        promotion_ready=False, session_cluster_lcb_cents=None,
        decision='do_not_advance_without_independent_execution_and_all_registered_gates',
        limitations=['One source date, not independent-session validation',
            'Official event IDs may share parent outcomes; normal proxy is not valid parent/session inference',
            'Recorder availability fence and IOC book depth are conditional, not broker acknowledgements or fills',
            'Two-cent fee reserve and slippage assumptions are not authenticated historical fees',
            'Modern official rules/windows do not verify original publication vintages',
            'Each exit horizon is a separate counterfactual; never sum them as independent profits',
            'Source license other, no production data permission inferred'])
    with (ROOT/'review.json').open('x') as f:
        json.dump(report, f, indent=2, allow_nan=False)
    print(json.dumps({k:v for k,v in report.items() if k not in ('selected','limitations')}))


if __name__ == '__main__':
    run()
