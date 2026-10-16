"""Independent raw-response audit; does not import screen or collector."""
import hashlib,json,math
from decimal import Decimal,ROUND_CEILING
from pathlib import Path
from datetime import datetime
ROOT=Path(__file__).parent

def time(s):return datetime.fromisoformat(s.replace("Z","+00:00")).timestamp()
def run():
    spec=json.loads((ROOT/"registration.json").read_bytes());selection=json.loads((ROOT/"selection.json").read_bytes());result=json.loads((ROOT/"result.json").read_bytes());collection=json.loads((ROOT/"collection.json").read_bytes())
    assert hashlib.sha256((ROOT/"registration.json").read_bytes()).hexdigest()==result["registration_sha256"]==selection["registration_sha256"]
    raw={};candles={};checked=0;failed=0
    for f in (ROOT/"cache").glob("*.json"):
        r=json.loads(f.read_bytes())
        if r["status"]!=200:failed+=1;continue
        b=f.with_suffix(".body").read_bytes();assert hashlib.sha256(b).hexdigest()==r["sha256"];checked+=1;d=json.loads(b)
        if "markets" in d:
            for m in d["markets"]:raw[m["ticker"]]=m
        if "candlesticks" in d:candles[d["ticker"]]=d["candlesticks"]
    rank=lambda s:hashlib.sha256(("mention-v1|"+s).encode()).hexdigest()
    eligible={}
    for m in raw.values():
        if m.get("market_type")!="binary" or not m.get("event_ticker"):continue
        if not time("2026-07-01T00:00:00Z")<=time(m["close_time"])<time("2026-08-11T00:00:00Z"):continue
        eligible.setdefault(m["event_ticker"],[]).append(m)
    selected=[min(eligible[e],key=lambda m:rank(m["ticker"]))["ticker"] for e in sorted(eligible,key=rank)[:120]]
    assert selected==[m["ticker"] for m in selection["selected"]]==[x["ticker"] for x in result["records"]]
    sums=dict(gross_cents=0,net_benchmark_cents=0,net_extra2_cents=0,net_extra5_cents=0);n=0
    for x in result["records"]:
        m=raw[x["ticker"]];anchor=min(time(m[k]) for k in ["close_time","expected_expiration_time","occurrence_datetime"] if m.get(k));decision=anchor-86400;end=int(decision)//60*60
        if "decision_ts" in x:assert x["decision_ts"]==decision and x["candle_end"]==end
        if "gross_cents" not in x:continue
        assert time(m["open_time"])<=decision
        c=next(c for c in candles[m["ticker"]] if c["end_period_ts"]==end)
        bid=Decimal(c["yes_bid"]["close"]);ask=Decimal(c["yes_ask"]["close"]);p=1-bid
        assert 0<bid<=ask<1 and ask-bid<=Decimal(".05") and Decimal(".2")<=p<=Decimal(".8")
        cost=int(p*100);fee=int((Decimal(".07")*p*(1-p)*100).to_integral_value(rounding=ROUND_CEILING))
        assert m["result"] in ["yes","no"]
        gross=(100 if m["result"]=="no" else 0)-cost
        expected=dict(gross_cents=gross,net_benchmark_cents=gross-fee,net_extra2_cents=gross-fee-2,net_extra5_cents=gross-fee-5)
        for k,v in expected.items():assert x[k]==v;sums[k]+=v
        n+=1
    assert n==result["complete"] and sums==result["conditional_analytical_totals_cents"]
    out=dict(schema="mention_independent_raw_audit_v1",successful_responses_hashed=checked,preserved_rejected_requests=failed,selected_contracts_reproduced=len(selected),complete_conditional_paths_checked=n,totals_cents=sums,result_sha256=hashlib.sha256((ROOT/"result.json").read_bytes()).hexdigest(),result="internal consistency passed; cost-stressed screen failed and execution unverified",profitability_proven=False)
    (ROOT/"independent-audit.json").write_text(json.dumps(out,indent=2),encoding="utf8");print(json.dumps(out,indent=2))
if __name__=="__main__":run()
