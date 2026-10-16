"""Conditional candle-cost screen; no depth or actual execution inference."""
import json,hashlib,random,math
from pathlib import Path
from decimal import Decimal,ROUND_CEILING
from collections import Counter,defaultdict
from datetime import datetime,timezone
from .collect import choose,ts,ROOT

def fee(p):return int((Decimal(".07")*p*(1-p)*100).to_integral_value(rounding=ROUND_CEILING))
def entry(m,candles):
    anchors=[ts(m.get(k)) for k in ["close_time","expected_expiration_time","occurrence_datetime"] if m.get(k)]
    if not anchors:return dict(status="missing_anchor")
    decision=min(anchors)-86400;end=int(decision)//60*60
    base=dict(decision_utc=datetime.fromtimestamp(decision,timezone.utc).isoformat(),decision_ts=decision,candle_end=end)
    if not m.get("open_time") or ts(m["open_time"])>decision:return dict(base,status="not_open_at_decision")
    matches=[c for c in candles if c.get("end_period_ts")==end]
    if len(matches)!=1:return dict(base,status="missing_or_ambiguous_exact_completed_minute")
    c=matches[0]
    try:
        bid=Decimal(str(c["yes_bid"]["close"]));ask=Decimal(str(c["yes_ask"]["close"]))
        if not bid.is_finite() or not ask.is_finite() or not 0<bid<=ask<1:return dict(base,status="invalid_quote")
    except (KeyError,ValueError,ArithmeticError):return dict(base,status="missing_quote")
    if ask-bid>Decimal(".05"):return dict(base,status="spread_rejected")
    p=1-bid
    if not Decimal(".20")<=p<=Decimal(".80"):return dict(base,status="price_rejected")
    if p*100!=(p*100).to_integral_value():return dict(base,status="non_cent_quote")
    return dict(base,status="conditional_candidate",no_ask_cents=int(p*100),fee_cents=fee(p),yes_bid=str(bid),yes_ask=str(ask),actual_fill_proven=False)

def run():
    manifest=json.loads((ROOT/"selection.json").read_bytes());collection=json.loads((ROOT/"collection.json").read_bytes());labels=json.loads((ROOT/"labels.json").read_bytes())
    assert manifest["registration_sha256"]==hashlib.sha256((ROOT/"registration.json").read_bytes()).hexdigest()
    records=[];raw=[]
    for f in (ROOT/"cache").glob("*.json"):
        r=json.loads(f.read_bytes())
        if r.get("status")==200:
            b=f.with_suffix(".body").read_bytes();assert hashlib.sha256(b).hexdigest()==r["sha256"]
            d=json.loads(b)
            if "/historical/markets?" in r["url"]:raw.extend(d.get("markets",[]))
    assert choose(raw)==manifest["selected"]
    for m in manifest["selected"]:
        r=collection["candle_receipts"].get(m["ticker"])
        if not r or r.get("status")!=200:x=dict(status="missing_transport")
        else:
            key=hashlib.sha256(r["url"].encode()).hexdigest();d=json.loads((ROOT/"cache"/(key+".body")).read_bytes());x=entry(m,d.get("candlesticks",[]))
        x.update(ticker=m["ticker"],event_ticker=m["event_ticker"],series=m["ticker"].split("-")[0])
        if x["status"]=="conditional_candidate":
            result=labels[m["ticker"]].get("result");x["official_result"]=result
            if result not in ["yes","no"]:x["status"]="unresolved_candidate"
            else:
                gross=(100 if result=="no" else 0)-x["no_ask_cents"]
                x.update(gross_cents=gross,net_benchmark_cents=gross-x["fee_cents"],net_extra2_cents=gross-x["fee_cents"]-2,net_extra5_cents=gross-x["fee_cents"]-5)
        records.append(x)
    complete=[x for x in records if "net_extra2_cents" in x];dates=defaultdict(list)
    for x in complete:dates[x["decision_utc"][:10]].append(x["net_extra2_cents"])
    rng=random.Random(20261016);clusters=list(dates.values());boots=[]
    if clusters:
        for _ in range(10000):
            draws=[rng.choice(clusters) for _ in clusters];boots.append(sum(map(sum,draws))/sum(map(len,draws)))
    lower=sorted(boots)[int(len(boots)*.025)] if boots else None
    totals={k:sum(x[k] for x in complete) if complete else None for k in ["gross_cents","net_benchmark_cents","net_extra2_cents","net_extra5_cents"]}
    gates=dict(min100complete=len(complete)>=100,min100events=len({x["event_ticker"] for x in complete})>=100,min30dates=len(dates)>=30,min5series=len({x["series"] for x in complete})>=5,discovery_complete=collection["discovery_complete"],positive_extra2=bool(complete) and totals["net_extra2_cents"]>0,positive_extra5=bool(complete) and totals["net_extra5_cents"]>0,positive_date_cluster_lcb=lower is not None and lower>0)
    out=dict(schema="mention_no_taker_conditional_cost_screen_v1",registration_sha256=manifest["registration_sha256"],selected=len(records),complete=len(complete),distinct_dates=len(dates),distinct_events=len({x["event_ticker"] for x in complete}),distinct_series=len({x["series"] for x in complete}),status_counts=dict(Counter(x["status"] for x in records)),conditional_analytical_totals_cents=totals,date_cluster_extra2_mean_lower_95pct_cents=lower,statistical_gates=gates,statistical_screen_pass=all(gates.values()),execution_qualified=False,profitability_proven=False,actual_orders=0,actual_fills=0,limitations=["Candle quote depth, receipt age and activation latency unverified", "Historical series fees and metadata vintage unverified", "Current series registry can omit dead series", "Source-informed hypothesis, no claim independent of authors historical sample", "Conditional outcomes are not broker fills or realized PnL"],records=records)
    (ROOT/"result.json").write_text(json.dumps(out,indent=2),encoding="utf8");print(json.dumps({k:v for k,v in out.items() if k!="records"},indent=2))
if __name__=="__main__":run()
