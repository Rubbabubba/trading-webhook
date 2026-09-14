"""Reports from an online snapshot; never blocks the live decision loop for analysis."""
from collections import Counter
import json
from pathlib import Path
import sqlite3
from .college_football_paper import utcnow, write_json


def report(output):
    output=Path(output)
    source=sqlite3.connect((output/'paper.sqlite3').resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
    # WAL permits concurrent local readers. Per-game reports use short reads;
    # accounts of completed games are final, active-game reports are diagnostic.
    db=source
    summaries=[]
    try:
        probes=[json.loads(r[0]) for r in db.execute('select state from probes')]
        for slug,raw,anchor,memory,state in db.execute('select * from games').fetchall():
            c=json.loads(raw);accounts={str(h):json.loads(a) for h,a in db.execute('select horizon,state from accounts where slug=?',(slug,))}
            blockers=Counter();coverage=Counter();actions=Counter();fill_delays=[]
            for raw, in db.execute('select detail from coverage where game=?',(slug,)):
                row=json.loads(raw);coverage['observations']+=1
                coverage['valid_model']+=row['valid_model'];coverage['with_valid_market']+=row['valid_markets']>0
                coverage['model_and_market']+=bool(row['valid_model'] and row['valid_markets'])
                blockers.update(row['blockers'])
            for raw, in db.execute('select a.detail from samples s join actions a on a.sample_id=s.id where s.slug=?',(slug,)):
                for a in json.loads(raw):
                    actions[a['action']]+=1
                    if 'observed_fill_delay_seconds' in a:fill_delays.append(a['observed_fill_delay_seconds'])
            timing={}
            for seconds,raw in db.execute('select m.seconds,m.detail from signals s join markouts m on s.id=m.signal_id where s.game=?',(slug,)):
                m=json.loads(raw);r=timing.setdefault(str(seconds),Counter());r[m['status']]+=1
                if m['status']=='observed':r['net_cents_sum']+=m['modeled_net_cents']
            pl={}
            for p in probes:
                if p['game']!=slug:continue
                r=pl.setdefault(p['family']+'/'+str(p['horizon']),Counter())
                r[p['state']]+=1;r['entries']+=bool(p.get('entry_at'));r['exits']+=bool(p.get('exit_at'))
                r['unresolved']+=bool(p.get('entry_at')) and not p.get('exit_at');r['net_cents']+=p.get('net_cents',0)
            result={'at':utcnow().isoformat(),'slug':slug,'game':c['game'],'league':c['league'],'state':state,
                    'accounts':accounts,'coverage':coverage,'blockers':blockers,'actions_across_variants':actions,
                    'fill_delay_seconds':{'count':len(fill_delays),'max':max(fill_delays,default=None)},
                    'timing_by_seconds':timing,'independent_probes':pl,'profitability_validated':False}
            path=output/'games'/slug;path.mkdir(exist_ok=True,parents=True);write_json(path/'report.json',result)
            lines=['# '+c['game'],'',f"Paper v3.3; {state}. Report at {result['at']}.",'',
                   '| Holding | Entries | Closed episodes | Realized net | Open contracts |',
                   '|---|---:|---:|---:|---:|']
            for h,a in accounts.items():lines.append(f"| {int(h)//60} min | {a['entries']} | {a['exits']} | ${a['realized_cents']/100:.2f} | {(a['position'] or {}).get('count',0)} |")
            lines+=['','Independent accounts; returns include modeled fees and slippage. Partial exits realize only their allocated cost basis. Unresolved positions have no invented payout.','',
                    '## Coverage and reasons','', '```json',json.dumps({'coverage':coverage,'blockers':blockers,'execution':actions,'fill_delays':result['fill_delay_seconds']},indent=2),'```','',
                    '## Signal timing','', '```json',json.dumps(timing,indent=2),'```','',
                    'Net markouts are hypothetical one-contract ask-to-bid round trips, not strategy P&L. Missing horizons are explicit; different delays and signals within one game are correlated.','',
                    '## Independent core-market probes','', '```json',json.dumps(pl,indent=2),'```','',
                    'GET RTT is a conservative latency proxy, not measured exchange order-routing latency. REST fallback has lower coverage than authenticated streaming. No profitability established.']
            (path/'report.md').write_text('\n'.join(lines)+'\n')
            summaries.append(result)
        write_json(output/'report.json',{'at':utcnow().isoformat(),'version':'sports_live_3.3','games':summaries})
    finally:db.close()
