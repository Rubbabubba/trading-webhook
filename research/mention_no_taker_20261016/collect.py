"""Bounded public collection and outcome-blind event/contract selection."""
import hashlib,json,time,urllib.request,urllib.error
from pathlib import Path
from datetime import datetime,timezone
from urllib.parse import urlencode
ROOT=Path(__file__).parent
BASE="https://external-api.kalshi.com/trade-api/v2"
FIELDS=["ticker","event_ticker","open_time","close_time","expected_expiration_time","expiration_time","occurrence_datetime","market_type","rules_primary","rules_secondary"]
def ts(s):return datetime.fromisoformat(s.replace("Z","+00:00")).timestamp() if s else None
def rank(s):return hashlib.sha256(("mention-v1|"+s).encode()).hexdigest()
def choose(markets):
    start=ts("2026-07-01T00:00:00Z");end=ts("2026-08-11T00:00:00Z");events={}
    for raw in markets:
        m={k:raw.get(k) for k in FIELDS}
        if m["market_type"]!="binary" or not m["ticker"] or not m["event_ticker"]:continue
        close=ts(m["close_time"])
        if close is None or not start<=close<end:continue
        events.setdefault(m["event_ticker"],{})[m["ticker"]]=m
    return [min(events[e].values(),key=lambda m:rank(m["ticker"])) for e in sorted(events,key=rank)[:120]]
class Collector:
    def __init__(self):
        self.cache=ROOT/"cache";self.cache.mkdir(exist_ok=True);self.n=0;self.existing=len(list(self.cache.glob("*.json")));self.next_at=0
    def get(self,path,params):
        u=BASE+path+"?"+urlencode(params);key=hashlib.sha256(u.encode()).hexdigest();rp=self.cache/(key+".json");bp=self.cache/(key+".body")
        if rp.exists():
            r=json.loads(rp.read_bytes());assert r["url"]==u
            if r.get("sha256"):assert hashlib.sha256(bp.read_bytes()).hexdigest()==r["sha256"]
            return r,json.loads(bp.read_bytes()) if r.get("status")==200 else {}
        if self.n+self.existing>=180:raise ValueError("request_capacity")
        self.n+=1;time.sleep(max(0,self.next_at-time.monotonic()));r=dict(url=u,captured_at=datetime.now(timezone.utc).isoformat(),method="GET",authenticated=False)
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"bounded-mention-cost-screen/1"}),timeout=20) as f:b=f.read(10000001);r["status"]=f.status
            bp.write_bytes(b);r.update(bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
            if len(b)>10000000:raise ValueError("response_capacity")
        except urllib.error.HTTPError as e:
            b=e.read(10000001);bp.write_bytes(b);r.update(status=e.code,error="HTTPError",bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
        except (OSError,ValueError) as e:r.update(status=None,error=type(e).__name__)
        finally:self.next_at=time.monotonic()+.5
        rp.write_text(json.dumps(r,indent=2),encoding="utf8")
        if r.get("status") in [400,401,403,429]:raise ValueError("external_access_stop")
        return r,json.loads(bp.read_bytes()) if r.get("status")==200 else {}
def run():
    spec=json.loads((ROOT/"registration.json").read_bytes());c=Collector();markets=[];discovery=[]
    for s in spec["series"]:
        cursor="";pages=[]
        for page in range(2):
            params=dict(series_ticker=s,limit=1000)
            if cursor:params["cursor"]=cursor
            r,d=c.get("/historical/markets",params);pages.append(r);markets.extend(d.get("markets",[]));cursor=d.get("cursor","")
            if not cursor or r.get("status")!=200:break
        discovery.append(dict(series=s,pages=pages,incomplete=bool(cursor) or any(r.get("status")!=200 for r in pages)))
        print(json.dumps(dict(series=s,rows=len(markets),requests=c.n)),flush=True)
    selected=choose(markets)
    manifest=dict(registration_sha256=hashlib.sha256((ROOT/"registration.json").read_bytes()).hexdigest(),discovery=discovery,raw_market_rows=len(markets),selected=selected,selected_before_candles=True)
    mp=ROOT/"selection.json"
    if mp.exists():assert json.loads(mp.read_bytes())==manifest
    else:mp.write_text(json.dumps(manifest,indent=2),encoding="utf8")
    (ROOT/"labels.json").write_text(json.dumps({m["ticker"]:m for m in markets if m.get("ticker") in {x["ticker"] for x in selected}},indent=2),encoding="utf8")
    candles={}
    for m in selected:
        if c.n+c.existing>=180:break
        anchors=[ts(m.get(k)) for k in ["close_time","expected_expiration_time","occurrence_datetime"] if m.get(k)]
        t=int(min(anchors)-86400);end=t//60*60
        r,d=c.get("/historical/markets/"+m["ticker"]+"/candlesticks",dict(start_ts=end-60,end_ts=end,period_interval=1));candles[m["ticker"]]=r
    # Raw metadata retained separately; evaluator selection reads only projected manifest.
    (ROOT/"labels.json").write_text(json.dumps({m["ticker"]:m for m in markets if m.get("ticker") in {x["ticker"] for x in selected}},indent=2),encoding="utf8")
    out=dict(new_requests=c.n,candle_receipts=candles,selected=len(selected),discovery_complete=not any(x["incomplete"] for x in discovery))
    (ROOT/"collection.json").write_text(json.dumps(out,indent=2),encoding="utf8");print(json.dumps(dict(selected=len(selected),requests=c.n,complete=out["discovery_complete"])),flush=True)
if __name__=="__main__":run()
