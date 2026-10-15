"""Conditional price bounds only, never semantic validation or realized profit."""
import bisect
import itertools
import json
import math
from pathlib import Path
from screen import book, digest, future_fill


def surplus(low_yes,high_no):
    return 100-low_yes-high_no-2-2-2


def run(source,out):
    import pyarrow.parquet as pq
    folder=source/'KXBTCD-26JAN2017'
    rows=pq.read_table(folder/'orderbook.parquet').to_pylist()
    groups={}
    for r in rows: groups.setdefault(r['ticker'],[]).append(r)
    ordered=sorted(groups,key=lambda t:float(t.rsplit('-T',1)[1]))
    times={}
    for t in ordered:
        groups[t].sort(key=lambda r:(r['ts'],r['sequence_id']))
        times[t]=[r['ts'].timestamp() for r in groups[t]]
    start=min(x[0] for x in times.values())
    end=max(x[-1] for x in times.values())
    counts={'pair_decisions':0,'missing_or_invalid_decision_book':0,'no_stressed_price_surplus':0,'delayed_leg_unfilled':0,'surplus_lost_at_delay':0}
    candidates=[]
    for now in range(math.ceil(start/300)*300,math.floor(end/300)*300+1,300):
        quotes={}
        for t in ordered:
            i=bisect.bisect_right(times[t],now)-1
            if i>=0 and now-times[t][i]<=5:
                q=book(groups[t][i])
                if q: quotes[t]=q
        for low,high in itertools.combinations(ordered,2):
            counts['pair_decisions']+=1
            if low not in quotes or high not in quotes:
                counts['missing_or_invalid_decision_book']+=1
                continue
            if surplus(quotes[low]['yes_ask'],quotes[high]['no_ask'])<1:
                counts['no_stressed_price_surplus']+=1
                continue
            a=future_fill(groups[low],times[low],now,'yes','buy')
            b=future_fill(groups[high],times[high],now,'no','buy')
            if not a or not b or abs(a['at']-b['at'])>5:
                counts['delayed_leg_unfilled']+=1
                continue
            net=surplus(a['price'],b['price'])
            if net<1:
                counts['surplus_lost_at_delay']+=1
                continue
            candidates.append({'at':now,'low':low,'high':high,'yes':a,'no':b,'conditional_stressed_bound_cents':net,'rejected':'missing_exact_resolution_rules_and_lifecycle'})
    result={'registration_sha256':digest(Path(__file__).with_name('structural-registration.json')),'implementation_sha256':digest(Path(__file__)),'input_sha256':digest(folder/'orderbook.parquet'),'counts':counts,'conditional_candidates':candidates,'candidate_count':len(candidates),'qualified_opportunities':0,'actual_fills':0,'profitability_proven':False,'live_qualified':False,'execution_enabled':False,'independent_source_events':1,'limitation':'Suffix does not establish exact nested settlement rules. Multi-leg snapshot execution is conditional and cannot establish atomic fills. No live or historical profitability pass.'}
    with out.open('x') as f: json.dump(result,f,indent=2)
    print(json.dumps({k:v for k,v in result.items() if k!='conditional_candidates'}))


if __name__=='__main__':
    import sys
    run(Path(sys.argv[1]),Path(sys.argv[2]))
