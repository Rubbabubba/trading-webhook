"""Exact raw hash and independent Fraction-based full-prefix reconstruction audit."""
import gzip,hashlib,json
from pathlib import Path
from fractions import Fraction
from opportunity_lab.kalshi_demo_stream_source import Books
ROOT=Path(__file__).parent

def run():
 result=json.loads((ROOT/"result.json").read_bytes());compressed=(ROOT/"capture.jsonl.gz").read_bytes()
 assert hashlib.sha256(compressed).hexdigest()==result["compressed_sha256"]
 raw=gzip.decompress(compressed);assert len(raw)==result["raw_bytes"] and hashlib.sha256(raw).hexdigest()==result["raw_lines_sha256"]
 rows=[json.loads(line) for line in raw.splitlines()];assert len(rows)==result["messages"]
 decoder=Books(result["tickers"]);reference={};last={};checked=0;digest=hashlib.sha256()
 for record in rows:
  payload=record["payload"];message=payload["msg"];kind=payload["type"]
  decoder.apply(payload,record["received_at"])
  if kind not in {"orderbook_snapshot","orderbook_delta"}:continue
  sid,seq=payload["sid"],payload["seq"]
  if sid in last:assert seq==last[sid]+1
  last[sid]=seq;ticker=message["market_ticker"]
  if kind=="orderbook_snapshot":
   reference[ticker]={side:{Fraction(p):Fraction(n) for p,n in message[side+"_dollars_fp"] if Fraction(n)>0} for side in ("yes","no")}
  else:
   side=message["side"];price=Fraction(message["price_dollars"]);size=reference[ticker][side].get(price,Fraction(0))+Fraction(message["delta_fp"])
   assert size>=0
   if size:reference[ticker][side][price]=size
   else:reference[ticker][side].pop(price,None)
  for name,state in reference.items():
   for side,values in state.items():assert values=={Fraction(p):Fraction(n) for p,n in decoder.books[name][side].items()}
  frame=sorted((name,side,str(p),str(n)) for name,state in reference.items() for side,values in state.items() for p,n in values.items())
  digest.update(json.dumps(frame,separators=(",",":")).encode());checked+=1
 out=dict(schema="demo_stream_independent_prefix_audit_v1",raw_capture_sha256=result["compressed_sha256"],reconstructed_states_checked=checked,book_subscription_final_sequences=last,markets=len(reference),reconstructed_state_chain_sha256=digest.hexdigest(),public_trades=0,market_data_only=True,actual_fills=0,profitability_proven=False,result="captured sequence prefix and full book reconstruction consistent; trade/activation coverage unverified")
 (ROOT/"independent-audit.json").write_text(json.dumps(out,indent=2),encoding="utf8");print(json.dumps(out,indent=2))
if __name__=="__main__":run()
