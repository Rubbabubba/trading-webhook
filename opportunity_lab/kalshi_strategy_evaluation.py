"""Deterministic event-cluster evaluation for completed paper episodes."""
from collections import defaultdict
from datetime import datetime
import json
import math
import random


REQUIRED = {'event_id', 'episode_id', 'closed_at', 'net_cents', 'fees_cents', 'slippage_cents'}


def _number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('invalid_' + name)
    return value


def validate(episodes):
    seen=set();rows=[]
    for row in episodes:
        if not isinstance(row,dict) or not REQUIRED <= row.keys():raise ValueError('missing_field')
        if not all(isinstance(row[k],str) and row[k] for k in ('event_id','episode_id','closed_at')):
            raise ValueError('invalid_identity')
        if row['episode_id'] in seen:raise ValueError('duplicate_episode')
        seen.add(row['episode_id'])
        try:closed=datetime.fromisoformat(row['closed_at'].replace('Z','+00:00'))
        except ValueError:raise ValueError('invalid_closed_at') from None
        if closed.tzinfo is None:raise ValueError('timezone_required')
        for key in ('net_cents','fees_cents','slippage_cents'):_number(row[key],key)
        if row['fees_cents']<0 or row['slippage_cents']<0:raise ValueError('negative_cost')
        rows.append(dict(row,_closed=closed))
    return sorted(rows,key=lambda r:(r['_closed'],r['episode_id']))


def cluster_lower_bound(groups, *, samples=10000, seed=20260916):
    if not groups:return None
    ids=sorted(groups);rng=random.Random(seed);means=[]
    for _ in range(samples):
        selected=[ids[rng.randrange(len(ids))] for _ in ids]
        values=[value for event in selected for value in groups[event]]
        means.append(sum(values)/len(values))
    means.sort();return means[max(0,int(.025*len(means))-1)]


def evaluate(episodes, *, extra_cost_cents=2, minimum_events=30, minimum_episodes=100):
    rows=validate(episodes);groups=defaultdict(list);equity=peak=drawdown=0
    for row in rows:
        groups[row['event_id']].append(row['net_cents'])
        equity+=row['net_cents'];peak=max(peak,equity);drawdown=max(drawdown,peak-equity)
    net=sum(r['net_cents'] for r in rows)
    stressed=net-extra_cost_cents*len(rows)
    lower=cluster_lower_bound(groups)
    gates=dict(minimum_events=len(groups)>=minimum_events,
               minimum_episodes=len(rows)>=minimum_episodes,
               positive_net=net>0,positive_stressed_net=stressed>0,
               positive_cluster_lower_bound=lower is not None and lower>0)
    return dict(events=len(groups),episodes=len(rows),net_cents=net,
                fees_cents=sum(r['fees_cents'] for r in rows),
                slippage_cents=sum(r['slippage_cents'] for r in rows),
                stressed_net_cents=stressed,maximum_drawdown_cents=drawdown,
                mean_net_cents=(net/len(rows) if rows else None),
                event_cluster_mean_95_lower_cents=lower,gates=gates,
                release_screen_passed=all(gates.values()))


def main():
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('episodes');parser.add_argument('--output')
    args=parser.parse_args();report=evaluate(json.load(open(args.episodes,encoding='utf-8')))
    payload=json.dumps(report,indent=2)
    if args.output:
        from pathlib import Path
        Path(args.output).write_text(payload,encoding='utf-8')
    print(payload)


if __name__=='__main__':main()
