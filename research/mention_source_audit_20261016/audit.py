"""Independent static-source audit. Never imports or runs provider code."""
import ast, hashlib, json
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
ROOT=Path(__file__).parent

def source_predicate(no_bid, trade_no, taker):
    # Exact numeric predicate independently transcribed from pinned fills.py line 138.
    return taker == "yes" and 0 <= trade_no-no_bid <= 1

def necessary_cross(no_bid, trade_no, taker):
    # A posted NO buy at n is a YES sell at 100-n.
    # YES taker print must reach that ask; equality alone does not prove queue fill.
    return taker == "yes" and 100-trade_no >= 100-no_bid

def maker_fee_cents(quantity, no_cents):
    p=Decimal(no_cents)/100
    return int((Decimal("0.0175")*quantity*p*(1-p)*100).to_integral_value(rounding=ROUND_CEILING))

def run():
    receipts=[]
    for i in range(12):
        r=json.loads((ROOT/f"{i}.json").read_bytes());b=(ROOT/f"{i}.body").read_bytes()
        assert r["status"]==200 and hashlib.sha256(b).hexdigest()==r["sha256"]
        receipts.append(r)
    tree=ast.parse((ROOT/"9.body").read_text(encoding="utf8"))
    expected="t.taker_side == 'yes' and 0 <= t.no_price - fill_price <= FILL_TOL"
    conditions=[ast.unparse(n.test) for n in ast.walk(tree) if isinstance(n,ast.If)]
    assert expected in conditions, conditions
    cases=[]
    for trade_no in [34,35,36]:
        cases.append(dict(no_bid=35,equivalent_yes_ask=65,trade_no=trade_no,trade_yes=100-trade_no,
            source_counts_price_match=source_predicate(35,trade_no,"yes"),
            necessary_price_cross=necessary_cross(35,trade_no,"yes"),
            actual_fill_proven=False))
    r=dict(schema="mention_source_static_audit_v1",source_revision=json.loads((ROOT/"0.body").read_bytes())["sha"],
        public_responses_checked=len(receipts),official_mention_series=len(json.loads((ROOT/"1.body").read_bytes())["series"]),
        provider_code_executed=False,counterexamples=cases,
        fee_example=dict(contracts=1,price_cents=50,unrounded_source_fee_cents=float(Decimal("0.0175")*Decimal(".5")*Decimal(".5")*100),rounded_benchmark_fee_cents=maker_fee_cents(1,50),historical_series_specific_fee_verified=False),
        additional_limits=["Entry query uses final market volume, not volume known at decision", "Nearest trade within a symmetric entry window can occur after decision", "Fallback 50-percent book share is assumed, not our queue or broker evidence", "Source repository intentionally excludes underlying database", "Multiple words from same speech are correlated, not independent events", "Maker fee rounding/series schedule and receipt latency require independent verification"],
        no_new_historical_prices_or_outcomes=True,pnl=None,profitability_proven=False,promotion_qualified=False)
    (ROOT/"audit-result.json").write_text(json.dumps(r,indent=2),encoding="utf8")
    print(json.dumps(r,indent=2))
if __name__=="__main__":run()
