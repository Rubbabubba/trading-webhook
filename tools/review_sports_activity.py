import json, sqlite3, gzip, hashlib
from pathlib import Path
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from opportunity_lab.sports_research_models import identity
ROOT=Path('sports_paper');now=datetime.now(timezone.utc);stamp=now.isoformat()
def snapshot(path):
 src=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=5);dst=sqlite3.connect(':memory:');src.backup(dst,pages=256,sleep=.01);src.close();return dst
lab=snapshot(ROOT/'market_lab_v11_20260910/laboratory.sqlite3');probes=defaultdict(list)
for raw, in lab.execute('select state from probes'):
 p=json.loads(raw);probes[p['game']].append(p)
lab.close()
notifications=json.loads((ROOT/'research_20260910/reviews/notifications.json').read_text())
rawdb=sqlite3.connect((ROOT/'expansion_20260910/capture.sqlite3').resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
summary=[]
for version in ['research_20260910','research_v11_20260912']:
 base=ROOT/version;reviews=base/'reviews';reviews.mkdir(exist_ok=True)
 db=snapshot(base/'paper.sqlite3')
 db.execute('create index if not exists review_action_sample on actions(sample_id)')
 for slug,cfgraw,anchor,memory,state in db.execute('select * from games').fetchall():
  cfg=json.loads(cfgraw)
  completed=state=='completed' and slug not in notifications['games']
  active=state=='running' and now-datetime.fromisoformat(cfg['kickoff'])>=timedelta(minutes=15)
  if not(completed or active):continue
  counts=Counter();blockers=Counter();marketfails=Counter();acts=defaultdict(Counter);reasons=defaultdict(Counter);examples=[];times=[]
  for at,raw in db.execute('select at,observation from samples where slug=? order by id',(slug,)):
   o=json.loads(raw);m=o['model'];times.append(at)
   if m.get('state')=='in' and not m.get('completed'):
    counts['in_play']+=1;blockers.update(m['blockers'])
    if not m['blockers'] and m['probabilities']:counts['valid_model']+=1
    if not m['blockers'] and m['probabilities'] and any(q['valid'] for q in o['markets'].values()):counts['valid_model_and_market']+=1
    for side,q in o['markets'].items():
     if not q['valid']:marketfails[side]+=1
  for at,h,raw in db.execute('select s.at,a.horizon,a.detail from samples s join actions a on a.sample_id=s.id where s.slug=? order by s.id',(slug,)):
   for a in json.loads(raw):
    acts[str(h)][a['action']]+=1
    if a.get('reason'):reasons[str(h)][str(a['reason'])]+=1
    for outcome, reason in a.get('outcomes', {}).items(): reasons[str(h)]['outcome_'+outcome+': '+str(reason)]+=1
    if a['action'] in ['buy','sell','settle','stage_buy','cancel_buy'] and len(examples)<12:examples.append({'at':at,'horizon':h,**a})
  accounts={str(h):json.loads(a) for h,a in db.execute('select horizon,state from accounts where slug=?',(slug,))}
  pl=defaultdict(lambda:Counter())
  for p in probes[slug]:
   k=p['family']+'/'+str(p['horizon']);pl[k]['entries']+=bool(p.get('entry_at'));pl[k]['exits']+=bool(p.get('exit_at'));pl[k]['net_cents']+=p.get('net_cents',0);pl[k]['unresolved']+=bool(p.get('entry_at')) and not p.get('exit_at');pl[k][p['state']]+=1
  evidence={'at':stamp,'game':cfg['game'],'slug':slug,'version':version,'state':state,'window':[times[0] if times else None,times[-1] if times else None],'samples':len(times),'coverage':counts,'blockers':blockers,'invalid_market_outcomes':marketfails,'actions':acts,'action_reasons':reasons,'accounts':accounts,'examples':examples,'laboratory_by_family_horizon':pl}
  if completed:
   rawcounts=Counter();gaps=defaultdict(list);final=None
   rows=rawdb.execute('select endpoint,metadata,sha256,body_gzip from responses where game in (?,?) order by id',(slug,cfg['market_event'])).fetchall()
   for endpoint,meta,digest,compressed in rows:
    rawcounts['payloads']+=1;meta=json.loads(meta)
    rawcounts['failed_transport']+=bool(meta.get('error')) or meta.get('status')!=200
    gaps[endpoint].append(datetime.fromisoformat(meta['received_at']))
    try:
     data=gzip.decompress(compressed)
     if hashlib.sha256(data).hexdigest()!=digest:rawcounts['hash_mismatches']+=1;continue
     data=json.loads(data)
     if endpoint=='summary':
      c=(data.get('header',{}).get('competitions') or [{}])[0]
      if identity(c,cfg) and c.get('status',{}).get('type',{}).get('completed'):
       final=', '.join(x.get('team',{}).get('displayName',x.get('id','?'))+' '+str(x.get('score','?')) for x in c['competitors'])
    except (ValueError,KeyError,TypeError,OSError):rawcounts['unreadable_payloads']+=1
   rawcounts['max_endpoint_gap_seconds']=max(((b-a).total_seconds() for ts in gaps.values() for a,b in zip(ts,ts[1:])),default=0)
   evidence['capture_validation']=rawcounts;evidence['recorded_final']=final
  if counts['valid_model']==0:cause='No valid model observations: inadequate signal coverage, not evidence of absent edge.'
  elif any(a['entries'] for a in accounts.values()):cause='Strategy fills recorded; assess completed episodes after costs separately from unresolved positions.'
  else:cause='Some valid signals but no fills. Review stage/cancel counts and cost/edge rejection reasons below; stale/inconsistent observations also reduce coverage.'
  evidence['finding']=cause
  name=slug if completed else slug+'_activity'
  (reviews/(name+'_evidence.json')).write_text(json.dumps(evidence,indent=2)+'\n')
  lines=['# '+cfg['game'],'',('Completed game review' if completed else 'In-game activity diagnostic')+' at '+stamp+'. Version '+version+'.', '', 'Recorded final: '+str(evidence.get('recorded_final','Game ongoing; no final assigned.')), '',cause,'',f"Coverage: {counts['valid_model']} valid model observations, {counts['valid_model_and_market']} with at least one valid market, out of {counts['in_play']} in-play observations; {len(times)} total samples.",'','| Holding | Entries | Exits | Realized net | Fees | Slippage | Drawdown | Open |','|---|---:|---:|---:|---:|---:|---:|---|']
  for h,a in accounts.items():lines.append(f"| {int(h)//60} min | {a['entries']} | {a['exits']} | ${a['realized_cents']/100:.2f} | ${a['fees_cents']/100:.2f} | ${a['slippage_cents']/100:.2f} | ${a['drawdown_cents']/100:.2f} | {bool(a['position'])} |")
  lines+=['','Independent hypothetical accounts; do not pool variants. Net includes modeled costs. No real orders or invented settlements.','', '## Signal blockers','', '```json',json.dumps(blockers.most_common(8),indent=2),'```','','## Execution decisions','', '```json',json.dumps({'counts':acts,'reasons':reasons,'examples':examples},indent=2),'```','','## Separate scheduled quote probes','', '| Market family / seconds | Entries | Exits | Closed net | Unresolved |','|---|---:|---:|---:|---:|']
  for key,p in sorted(pl.items()):lines.append(f"| {key} | {p['entries']} | {p['exits']} | ${p['net_cents']/100:.2f} | {p['unresolved']} |")
  lines+=['','Probes are correlated execution/price-movement experiments, not model-driven trades or evidence of profit. No probes for a family means missing coverage, not zero return.']
  if completed:lines+=['','## Raw capture validation','', '```json',json.dumps(evidence['capture_validation'],indent=2),'```','Hashes check stored-byte integrity, not feed truth. Gaps are within each endpoint; independent reference agreement and latency percentiles remain unvalidated.','', 'Operational version 1.1 now evaluates future games prospectively. Do not tune thresholds on this completed game or backfill trades.']
  (reviews/(name+'.md')).write_text('\n'.join(lines)+'\n')
  if completed:
   capture=ROOT/'expansion_20260910/reviews';capture.mkdir(exist_ok=True)
   (capture/(slug+'.md')).write_text('# '+cfg['game']+' — capture validation\n\nRecorded final: '+str(final)+'\n\nCapture only; no trading P&L.\n\n```json\n'+json.dumps(evidence['capture_validation'],indent=2)+'\n```\n\nIdentities verified against frozen paper configuration before reading final. Hash integrity does not establish feed truth; gaps and model exclusions limit conclusions.\n')
  summary.append({'slug':slug,'game':cfg['game'],'completed':completed,'version':version,'coverage':counts,'blockers':blockers.most_common(3),'actions':acts,'reasons':{h:r.most_common(4) for h,r in reasons.items()},'probe_entries':sum(p['entries'] for p in pl.values()),'final':evidence.get('recorded_final')})
 db.close()
rawdb.close()
Path('sports_paper/rolling_review_20260910/activity_latest.json').write_text(json.dumps(summary,indent=2))
print(json.dumps({'reviewed_games':len(summary),'completed':sum(g['completed'] for g in summary),'report':'sports_paper/rolling_review_20260910/activity_latest.json'}))
