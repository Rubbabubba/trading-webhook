"""One bounded authenticated Demo market-data session; no order interface."""
import asyncio,base64,gzip,hashlib,json,os,re,sqlite3,time
from pathlib import Path
from collections import Counter
URL="wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
ROOT=Path("/var/data/kalshi-demo-v9")
ARCHIVE="stream-feasibility-v1-20261016.jsonl.gz"
MAX_BYTES=2097152
ALLOWED={"subscribed","orderbook_snapshot","orderbook_delta","trade","ok","error","unsubscribed"}

def target_list(db_path):
 c=sqlite3.connect("file:"+str(db_path)+"?mode=ro",uri=True)
 try:r=c.execute("select detail from settings where name=?",("sampling_window",)).fetchone()
 finally:c.close()
 if not r:raise ValueError("no_sampling_window")
 tickers=json.loads(r[0]).get("tickers",[])
 if not 1<=len(tickers)<=8 or len(set(tickers))!=len(tickers) or any(not isinstance(t,str) or not re.fullmatch(r"[A-Z0-9_.-]{1,160}",t) for t in tickers):raise ValueError("invalid_targets")
 return tickers

def public_record(payload,received_at):
 if not isinstance(payload,dict) or payload.get("type") not in ALLOWED:raise ValueError("unexpected_message_type")
 def clean(x):
  if isinstance(x,dict):return {k:clean(v) for k,v in x.items() if k not in {"client_order_id","user_id","account_id","api_key","signature"}}
  if isinstance(x,list):return [clean(v) for v in x]
  return x
 sanitized=clean(payload)
 return dict(received_at=received_at,payload=sanitized,owner_fields_redacted=sanitized!=payload)

class Sink:
 def __init__(self,path):
  self.f=gzip.open(path,"xb");self.bytes=0;self.messages=0;self.counts=Counter();self.digest=hashlib.sha256()
 def add(self,record):
  raw=(json.dumps(record,sort_keys=True,separators=(",",":"),allow_nan=False)+"\n").encode()
  if self.messages>=10000 or self.bytes+len(raw)>MAX_BYTES:raise ValueError("capture_capacity")
  self.f.write(raw);self.digest.update(raw);self.bytes+=len(raw);self.messages+=1;self.counts[record["payload"]["type"]]+=1
 def close(self):self.f.close()

def headers():
 from cryptography.hazmat.primitives import hashes,serialization
 from cryptography.hazmat.primitives.asymmetric import padding
 key_id=os.environ.get("KALSHI_DEMO_API_KEY_ID");key_path=os.environ.get("KALSHI_DEMO_PRIVATE_KEY_PATH")
 if not key_id or not key_path:raise ValueError("demo_credentials_missing")
 key=serialization.load_pem_private_key(Path(key_path).read_bytes(),password=None)
 stamp=str(int(time.time()*1000));sig=key.sign((stamp+"GET/trade-api/ws/v2").encode(),padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.DIGEST_LENGTH),hashes.SHA256())
 return {"KALSHI-ACCESS-KEY":key_id,"KALSHI-ACCESS-TIMESTAMP":stamp,"KALSHI-ACCESS-SIGNATURE":base64.b64encode(sig).decode()}

async def capture(root=ROOT):
 from websockets.asyncio.client import connect
 class NoRedirect(connect):
  def process_redirect(self,exc):return exc
 # Only this literal Demo host may receive the existing Demo handshake credentials.
 tickers=target_list(root/"worker.sqlite3");path=root/ARCHIVE
 result=dict(schema="demo_book_trade_stream_feasibility_v1",endpoint=URL,tickers=tickers,started_at=time.time(),connection_attempts=1,connected=False,execution_enabled=False,actual_orders=0,actual_fills=0,profitability_proven=False,continuous_coverage_proven=False)
 sink=Sink(path)
 try:
  async with NoRedirect(URL,additional_headers=headers(),open_timeout=10,close_timeout=2,ping_interval=20,ping_timeout=10,max_size=262144,max_queue=32,compression=None) as ws:
   result["connected"]=True;result["connected_at"]=time.time()
   await ws.send(json.dumps({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta","trade"],"market_tickers":tickers}}))
   deadline=time.monotonic()+60
   while time.monotonic()<deadline:
    try:raw=await asyncio.wait_for(ws.recv(),timeout=max(.001,deadline-time.monotonic()))
    except asyncio.TimeoutError:break
    if not isinstance(raw,str) or len(raw.encode())>262144:raise ValueError("invalid_message_size")
    payload=json.loads(raw);record=public_record(payload,time.time());sink.add(record)
    if payload["type"]=="error":
     code=payload.get("msg",{}).get("code");result["server_error_code"]=code if type(code) is int else None
     result["state"]="server_rejected";break
   result.setdefault("state","observed")
 except Exception as exc:
  result["state"]="transport_or_protocol_blocked";result["error_class"]=type(exc).__name__
  response=getattr(exc,"response",None);result["http_status"]=getattr(response,"status_code",None)
  if isinstance(exc,ValueError) and str(exc) in {"capture_capacity","unexpected_message_type","invalid_message_size","demo_credentials_missing"}:result["bounded_reason"]=str(exc)
 finally:
  sink.close();result.update(completed_at=time.time(),messages=sink.messages,raw_bytes=sink.bytes,message_types=dict(sink.counts),raw_lines_sha256=sink.digest.hexdigest(),compressed_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),compressed_bytes=path.stat().st_size,archive=str(path))
 receipt=root/"stream-feasibility-v1-20261016-receipt.json"
 with receipt.open("x",encoding="utf8") as f:json.dump(result,f,indent=2)
 print(json.dumps(result,separators=(",",":")),flush=True)
 return result
if __name__=="__main__":asyncio.run(capture())
