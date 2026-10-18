from pathlib import Path
import sqlite3,json,gzip,hashlib
from fractions import Fraction
root=Path('/var/data/kalshi-demo-v9')
def ro(name):
 d=sqlite3.connect((root/name).resolve().as_uri()+'?mode=ro',uri=True);d.execute('BEGIN');return d
def digest(v):return hashlib.sha256(v).hexdigest()
c=ro('stream-shadow-comparison-v1.sqlite3');rows=c.execute('SELECT epoch,at,ticker,event,source_sha,input_sha,input_json FROM comparison_inputs ORDER BY id').fetchall()
a=ro('stream-source-v3.sqlite3');wanted={r[4] for r in rows};states={};found={};last={}
for epoch,at,h,blob in a.execute('SELECT connection,received,raw_sha,data FROM messages ORDER BY id'):
 p=json.loads(gzip.decompress(blob));m=p['msg'];kind=p['type'];t=m.get('market_ticker');key=(epoch,t)
 if kind=='orderbook_snapshot':
  if 'yes_dollars_fp' not in m:states.pop(key,None)
  else:states[key]={s:{Fraction(x):Fraction(n) for x,n in m[s+'_dollars_fp'] if Fraction(n)>0} for s in ('yes','no')}
 elif kind=='orderbook_delta':
  assert key in states
  side=m['side'];price=Fraction(m['price_dollars']);n=states[key][side].get(price,Fraction(0))+Fraction(m['delta_fp']);assert n>=0
  if n:states[key][side][price]=n
  else:states[key][side].pop(price,None)
 if h in wanted:found[h]=(epoch,at,p,{s:dict(v) for s,v in states[key].items()})
for epoch,at,t,event,h,ih,raw in rows:
 assert digest(raw.encode())==ih
 v=json.loads(raw);se,sa,p,b=found[h]
 assert se==epoch and sa==at and v['epoch']==epoch and v['source_message_sha256']==h
 assert v['sid']==p['sid'] and v['seq']==p['seq'] and p['msg']['market_ticker']==t
 assert v['metadata']['event_ticker']==event and v['metadata']['ticker']==t
 assert v['metadata_independently_current'] is False and v['frame']['received_at']==at
 for side in ('yes','no'):
  actual={Fraction(x):Fraction(n) for x,n in v['frame']['orderbook_fp'][side+'_dollars']}
  assert actual==dict(sorted(b[side].items(),reverse=True)[:20])
 assert (epoch,t) not in last or at-last[(epoch,t)]>=1
 last[(epoch,t)]=at
print(json.dumps(dict(read_only=True,accepted_inputs=len(rows),raw_source_links=len(found),independent_fraction_ladders_match=True,source_hashes_and_epochs_match=True,one_second_input_spacing=True,actual_orders=0,profitability_proven=False),sort_keys=True))
a.close();c.close()
