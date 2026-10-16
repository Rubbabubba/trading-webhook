"""Independent bounded archive auditor; standalone stdlib, read-only SQLite."""
import sqlite3,json,gzip,hashlib,math,time
from decimal import Decimal
from datetime import datetime
from collections import Counter
PATH="/var/data/kalshi-demo-v9/continuing-capture-v1.sqlite3"
def canonical(x):return json.dumps(x,sort_keys=True,separators=(",",":"),allow_nan=False)
def run():
 c=sqlite3.connect("file:"+PATH+"?mode=ro",uri=True);c.execute("BEGIN")
 protocol,started=c.execute("select detail,started_at from protocol where id=1").fetchone();protocol=json.loads(protocol)
 assert protocol["environment"]=="demo" and protocol["execution_enabled"] is False and protocol["fill_assumed"] is False
 blobs={};raw_total=0
 for identity,kind,size,digest,compressed in c.execute("select id,kind,raw_bytes,compressed_sha,data from blobs"):
  assert hashlib.sha256(compressed).hexdigest()==digest
  raw=gzip.decompress(compressed);raw_total+=len(raw)
  assert raw_total<=20000000 and len(raw)==size and hashlib.sha256(kind.encode()+b"\0"+raw).hexdigest()==identity
  blobs[identity]=json.loads(raw)
 quotes={};usable=0;events=set();invalid_lifecycle=0
 for identity,ticker,event,at,valid,mb,bb,receipt in c.execute("select * from quotes order by observed_at,id"):
  assert len(quotes)<5000
  r=json.loads(receipt);m=blobs[mb];book=blobs[bb];clocks=r["clocks"]
  assert all(type(v) in (float,int) and math.isfinite(v) for v in clocks) and clocks==sorted(clocks) and clocks[0]>=started and clocks[-1]==at
  assert m["ticker"]==r["ticker"]==ticker and m["event_ticker"]==r["event_id"]==event and m["market_type"]=="binary"
  assert hashlib.sha256(canonical(dict(receipt=r,metadata=m,sides=book)).encode()).hexdigest()==identity
  best=[];classified=True
  for side in ("yes","no"):
   rows=book[side];assert isinstance(rows,list) and len(rows)<=20
   parsed=[(Decimal(p),Decimal(n)) for p,n in rows]
   assert all(p.is_finite() and n.is_finite() and 0<p<1 and 0<=n<=1000000000 for p,n in parsed)
   assert len({p for p,n in parsed})==len(parsed)
   classified=classified and any(n>=1 for p,n in parsed)
   best.append(max((p for p,n in parsed if n>0),default=None))
  assert bool(valid)==classified
  if all(p is not None for p in best):assert sum(best)<1
  if m.get("close_time") and at>=datetime.fromisoformat(m["close_time"].replace("Z","+00:00")).timestamp():invalid_lifecycle+=1
  quotes[identity]=dict(ticker=ticker,event=event,at=at,usable=bool(valid));usable+=bool(valid);events.add(event)
 polls={r[0]:r for r in c.execute("select id,quote_id,attempted_at,state,detail from polls")};pages={}
 for identity,pid,at,blob,detail in c.execute("select * from page_receipts"):
  assert pid in polls and polls[pid][1] in quotes
  d=json.loads(detail);q=quotes[polls[pid][1]];payload=blobs[blob];params=d["params"]
  assert params["ticker"]==q["ticker"] and params["limit"]==100
  assert q["at"]+30<=d["started_at"]<=d["received_at"]==at<=q["at"]+120
  assert hashlib.sha256(canonical(dict(poll=pid,params=params,started=d["started_at"],received=at,payload=payload)).encode()).hexdigest()==identity
  pages[identity]=(q,payload,d)
 ages=Counter();sides=Counter();blocks=Counter();n=0;trade_events=set()
 for identity,ticker,event,at,economic,page in c.execute("select * from trades order by executed_at,id"):
  n+=1;assert n<=2000
  q,payload,d=pages[page];t=json.loads(economic)
  assert ticker==q["ticker"]==t["ticker"] and event==q["event"] and identity==t["trade_id"]
  assert at==datetime.fromisoformat(t["created_time"].replace("Z","+00:00")).timestamp() and q["at"]<at<=q["at"]+30
  assert any(all(row.get(k)==v for k,v in t.items()) for row in payload["trades"])
  yes,no,size=[Decimal(str(t[k])) for k in ("yes_price_dollars","no_price_dollars","count_fp")]
  assert all(v.is_finite() for v in (yes,no,size)) and yes+no==1 and 0<=yes<=1 and 0<size<=1000000000
  age=at-q["at"]
  for bound in (2,5,30):ages[str(bound)]+=int(age<=bound)
  sides[str(t.get("taker_side"))]+=1;blocks[str(t.get("is_block_trade"))]+=1;trade_events.add(event)
 out=dict(schema="continuing_capture_independent_integrity_audit_v1",audited_at=datetime.now().astimezone().isoformat(),source_protocol_sha256=hashlib.sha256(canonical(protocol).encode()).hexdigest(),source_started_at=started,blobs_checked=len(blobs),raw_bytes_checked=raw_total,quotes_checked=len(quotes),usable_quotes=usable,events=len(events),quotes_after_returned_close=invalid_lifecycle,polls_checked=len(polls),pages_checked=len(pages),public_trades_checked=n,trade_events=len(trade_events),trades_with_prior_quote_at_most_seconds=dict(ages),taker_sides=dict(sides),block_flags=dict(blocks),known_limitations=["Only selected30-second windows; not continuous coverage","Second-resolution execution clocks can overlap fractional receipt boundary","Displayed prior depth and public trades do not establish our queue or fill","No fees, strategy returns, settlements or executable activation assessed"],result="source_integrity_consistent_only",profitability_proven=False,actual_orders=0,actual_fills=0)
 c.close();print(json.dumps(out,separators=(",",":")))
if __name__=="__main__":run()
