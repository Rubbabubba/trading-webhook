"""Independent raw-source and selected-bound audit, never a profit verifier."""
import csv,hashlib,io,json,re
from pathlib import Path
from fractions import Fraction
from datetime import datetime,date,timedelta,timezone
from collections import Counter

ROOT=Path(__file__).parent;UTC=timezone.utc


def run():
    result=json.loads((ROOT/'result.json').read_text());spec=json.loads((ROOT/'registration.json').read_text())
    capture=json.loads((ROOT/'collection.json').read_text());recovery=json.loads((ROOT/'routing-recovery.json').read_text())
    probes=json.loads((ROOT/'routing-source-probes.json').read_text());events={**capture['events'],**recovery['events']}
    assert result['registration_sha256']==hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest()
    assert result['price_inputs']==0 and result['pnl_cents'] is None
    assert not result['execution_enabled'] and not result['profitability_proven'] and not result['data_qualification_passed']
    receipts=list(capture['observations'].values())+list(capture['events'].values())+list(recovery['events'].values())+list(probes.values())
    urls=set()
    for r in receipts:
        assert r['status']==200 and r['url'] not in urls;urls.add(r['url'])
        assert hashlib.sha256(Path(r['body_path']).read_bytes()).hexdigest()==r['body_sha256']
    assert len(urls)==recovery['cumulative_requests']==242
    raw_reports={}
    for station,r in capture['observations'].items():
        text=Path(r['body_path']).read_bytes().decode('utf-8-sig')
        raw_reports[station]={ ' '.join(row['metar'].split()) for row in csv.DictReader(
            line for line in text.splitlines() if not line.startswith('#')) }
    frame={(d['station'],d['date']) for d in result['days']}
    assert len(frame)==len(result['days'])==186
    excluded={(x['station'],x['date']) for x in spec['dates']['excluded_prior_inspected']}
    assert {(d['station'],d['date']) for d in result['days'] if d['state']=='excluded_prior_inspected'}==excluded
    stations={s['station']:s for s in spec['stations']};complete=selected=unsupported=contracts=0
    for d in result['days']:
        if d['state']=='excluded_prior_inspected':continue
        r=events[d['event']];markets=json.loads(Path(r['body_path']).read_bytes())['markets']
        assert markets and all(m['event_ticker']==d['event'] for m in markets)
        if d['state']=='unsupported_settlement_source':
            assert all('the weather company' in m['rules_primary'].lower() for m in markets)
            unsupported+=1;continue
        assert all('national weather service' in m['rules_primary'].lower() for m in markets)
        if d['state']!='bound_consistent':continue
        complete+=1;finals=set()
        for m in markets:
            h=Fraction(str(m['expiration_value']));assert h.denominator==1
            high=int(h);kind=m['strike_type']
            if kind=='greater':yes=high>Fraction(str(m['floor_strike']))
            elif kind=='less':yes=high<Fraction(str(m['cap_strike']))
            else:yes=Fraction(str(m['floor_strike']))<=high<=Fraction(str(m['cap_strike']))
            assert (m['result']=='yes')==yes
            assert Fraction(str(m['settlement_value_dollars']))==int(yes)
            finals.add(high);contracts+=1
        assert finals=={d['final_high_f']} and not d['bound_violations']
        c=d['selected']
        if c is None:continue
        selected+=1;r=c['report'];raw=r['raw']
        assert raw in raw_reports[d['station']] and 'COR' not in raw.split() and '$' not in raw
        groups=re.findall(r'\b1([01])(\d{3})\b',raw.split(' RMK ',1)[1]);assert len(groups)==1
        sign,n=groups[0];celsius=Fraction(int(n),10)*(-1 if sign=='1' else 1)
        fahrenheit=celsius*Fraction(9,5)+32
        assert abs(fahrenheit-r['maximum_f'])<=Fraction(100001,1000000)
        assert r['lower_bound_f']==r['maximum_f']-2==c['lower_bound_f']<=d['final_high_f']
        assert r['available_at']==r['nominal_at']+3600<=c['decision_at']
        target=date.fromisoformat(d['date']);base=datetime.combine(target,datetime.min.time(),tzinfo=UTC)
        begin=(base-timedelta(hours=stations[d['station']]['standard_utc_offset_hours'])).timestamp()
        assert begin<=r['nominal_at']-6*3600-900 and r['nominal_at']+900<=begin+86400
        assert c['decision_at'] in [(base+timedelta(hours=h)).timestamp() for h in (13,19,25)]
        low,high=c['interval'];yes=(low is None or d['final_high_f']>=low) and (high is None or d['final_high_f']<=high)
        assert d['selected_logical_outcome_correct']==((c['side']=='yes')==yes)
    assert complete==result['complete_city_days']==41 and selected==result['selected_logical_candidates']==28
    assert unsupported==54 and result['selected_logical_incorrect']==0
    assert result['complete_by_station']=={'KMDW':26,'KLAX':15}
    receipt=dict(schema='weather_bound_independent_source_audit_v1',
        result_sha256=hashlib.sha256((ROOT/'result.json').read_bytes()).hexdigest(),
        immutable_public_responses_checked=len(urls),planned_city_days_checked=186,
        usable_city_days_checked=complete,numeric_contract_labels_checked=contracts,
        selected_measurement_bounds_checked=selected,unsupported_TWC_city_days=unsupported,
        result='passed_partial_data_consistency; source qualification remains failed',
        actual_orders=0,actual_fills=0,price_inputs=0,pnl_cents=None,profitability_proven=False)
    with (ROOT/'independent-audit.json').open('x') as f:json.dump(receipt,f,indent=2)
    print(json.dumps(receipt))


if __name__=='__main__':run()
