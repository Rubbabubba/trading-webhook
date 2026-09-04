"""Standalone, non-executing FastAPI surface for Opportunity Lab."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from operator_auth import operator_authorized
from opportunity_lab.catalog import candidate_catalog
from opportunity_lab.coinbase_market_data import check_cfm_read_access, credentials_configured, fetch_product_candles, get_fee_schedule, list_perpetual_products
from opportunity_lab.crypto_basis import BasisInputs, backtest_funding, evaluate_basis
from opportunity_lab.crypto_market_data import fetch_crypto_bars
from opportunity_lab.crypto_regime import crypto_research_suite
from opportunity_lab.cross_exchange_crypto import collect_cross_exchange
from opportunity_lab.funding_reconstruction import reconstruct_hourly_funding
from opportunity_lab.kalshi_market_data import (fetch_open_events, fetch_recent_trades, fetch_settled_series_markets,
                                                rank_event_dislocations)
from opportunity_lab.odds_arbitrage import OutcomeQuote, american_to_decimal, scan_arbitrage
from opportunity_lab.prediction_market_making import screen_market_making
from opportunity_lab.triangular_crypto import collect_triangular
from opportunity_lab.weather_value import collect_dallas_weather
from opportunity_lab.weather_backtest import historical_dallas_backtest, walk_forward_dallas
from opportunity_lab.store import (configured as store_configured, cross_exchange_scoreboard, triangular_scoreboard,
                                   kalshi_scoreboard, recent_runs, save_cross_exchange_scans, save_kalshi_scan,
                                   reconcile_weather_settlements, save_triangular_scan, save_weather_scan,
                                   weather_scoreboard, market_making_scoreboard)


APP_VERSION = "opportunity-lab-web-v5"
app = FastAPI(title="Opportunity Lab", docs_url=None, redoc_url=None)


@app.middleware("http")
async def protect_diagnostics(request: Request, call_next):
    protected = request.url.path.startswith("/diagnostics/") or request.url.path == "/dashboard/opportunity-lab"
    if protected and not operator_authorized(request.headers, os.getenv("OPPORTUNITY_ADMIN_SECRET", "")):
        return JSONResponse(status_code=401, content={"detail": "operator authentication required"}, headers={"WWW-Authenticate": 'Basic realm="Opportunity Lab"'})
    response = await call_next(request)
    if protected:
        response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/")
def root() -> dict:
    return {"ok": True, "service": "opportunity_lab", "version": APP_VERSION, "candidate_count": 8, "execution_enabled": False, "live_submission": False}


@app.get("/health")
def health() -> dict:
    return {"ok": True, "service": "opportunity_lab", "version": APP_VERSION, "active_candidate": "sports_prediction_arb", "mode": "research", "execution_enabled": False}


@app.get("/diagnostics/opportunity_lab/catalog")
def catalog() -> dict:
    return {"ok": True, "candidates": candidate_catalog()}


@app.get("/dashboard/opportunity-lab", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    return HTMLResponse("""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Opportunity Lab</title><style>
body{font-family:system-ui;background:#0b1020;color:#edf2ff;margin:0;padding:28px}main{max-width:1100px;margin:auto}
.card{background:#151d33;border:1px solid #2a385c;border-radius:12px;padding:20px;margin:16px 0}button,input,select,textarea{font:inherit;padding:9px;margin:4px;background:#202c49;color:#fff;border:1px solid #52658e;border-radius:6px}button{cursor:pointer;background:#3157c8}textarea{display:block;width:calc(100% - 26px);min-height:150px;font-family:ui-monospace,monospace}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#080c18;padding:16px;border-radius:8px;max-height:65vh;overflow:auto}.muted{color:#aebbd7}.ok{color:#71e6a0}.bad{color:#ff8c8c}
</style></head><body><main><h1>Opportunity Lab</h1><p class="muted">Research only · execution hard-disabled</p>
<section class="card"><h2>Crypto research</h2><label>Symbol <select id="symbol"><option>BTC/USD</option><option>ETH/USD</option></select></label><label>Days <input id="days" type="number" min="90" max="3650" value="730"></label><label>Timeframe <select id="timeframe"><option>1Hour</option><option>4Hour</option><option>1Day</option></select></label><button id="run">Run research</button><span id="status" class="muted"></span></section>
<section class="card"><h2>Funding/basis calculator</h2><p class="muted">Positive funding is paid to the short. Prices must be executable spot ask and derivative bid.</p><label>Spot ask <input id="spot" type="number" value="100000"></label><label>Derivative bid <input id="derivative" type="number" value="100500"></label><label>Funding bps / interval <input id="funding" type="number" step="0.01" value="1"></label><label>Hold hours <input id="hold" type="number" value="168"></label><label>Capital $ <input id="capital" type="number" value="1000"></label><button id="basis">Evaluate basis</button></section>
<section class="card"><h2>CDE funding reconstruction</h2><p class="muted">Research proxy from aligned completed hourly CDE-future and Coinbase-spot candles. No orders or balances.</p><label>Market <select id="carryMarket"><option>BTC</option><option>ETH</option></select></label><label>Days <input id="carryDays" type="number" min="7" max="365" value="365"></label><label>Primary round-trip cost (bps) <input id="carryCost" type="number" min="0" max="1000" step="0.1" value="139"></label><button id="carryRun">Reconstruct funding</button></section>
<section class="card"><h2>Sports / prediction arbitrage scanner</h2><p class="muted">Enter one best quote for every mutually exclusive outcome. Confirm rules only after checking settlement terms, overtime treatment, void rules, limits, and currency. Commission is a decimal fraction of winnings (for example, 0.02 = 2%).</p><label>Bankroll $ <input id="arbBankroll" type="number" min="1" step="0.01" value="1000"></label><label>Minimum profit $ <input id="arbMinProfit" type="number" min="0" step="0.01" value="1"></label><label><input id="arbRules" type="checkbox"> Market and settlement rules confirmed compatible</label><textarea id="arbQuotes">[
  {"outcome":"Home","venue":"Book A","odds_format":"american","odds":110,"max_stake":1000,"commission_rate":0},
  {"outcome":"Away","venue":"Book B","odds_format":"american","odds":110,"max_stake":1000,"commission_rate":0}
]</textarea><button id="arbRun">Scan opportunity</button></section>
<section class="card"><h2>Live prediction-market discovery</h2><p class="muted">Unauthenticated Kalshi public data only. Results are gross price-dislocation candidates, not approved trades; fees, complete outcome coverage, account eligibility, and jurisdiction remain blockers.</p><label>Category <select id="kalshiCategory"><option value="">All</option><option>Sports</option><option>Politics</option><option>Economics</option><option>Crypto</option></select></label><label>Pages <input id="kalshiPages" type="number" min="1" max="3" value="1"></label><button id="kalshiRun">Scan live markets</button><button id="kalshiSave">Scan and save</button></section>
<section class="card"><h2>Prediction-market maker simulator</h2><p class="muted">Models queue-clearing fills from public trades, one-sided inventory, next-quote marking, and maker fees. It does not place orders.</p><label>Pages <input id="makerPages" type="number" min="1" max="3" value="1"></label><label>Quote size <input id="makerSize" type="number" min="0.01" step="0.01" value="10"></label><label>Maker fee coefficient <input id="makerFee" type="number" min="0" max="1" step="0.0001" value="0.0175"></label><button id="makerRun">Run maker screen</button><button id="makerEvidence">Load replay evidence</button></section>
<section class="card"><h2>Cross-exchange crypto monitor</h2><p class="muted">Public Coinbase and Kraken order books. Sweeps executable depth and deducts conservative taker fees. No orders, balances, or credentials.</p><label>Market <select id="crossSymbol"><option>BTC</option><option>ETH</option></select></label><label>Maximum per-leg notional $ <input id="crossNotional" type="number" min="10" max="100000" value="1000"></label><button id="crossRun">Compare venues</button></section>
<section class="card"><h2>Triangular crypto monitor</h2><p class="muted">Public Kraken BTC/USD, ETH/USD, and ETH/BTC books. Models both three-leg cycles with depth and a fee on every leg.</p><label>Starting USD <input id="triangleCapital" type="number" min="10" max="100000" value="1000"></label><button id="triangleRun">Scan both cycles</button></section>
<section class="card"><h2>Dallas weather-value research</h2><p class="muted">NWS DFW hourly forecast proxy versus active Kalshi Dallas high/low contracts. Research is blocked from eligibility until forecast and settlement-source errors are calibrated.</p><label>Assumed forecast error σ°F <input id="weatherSigma" type="number" min="0.5" max="10" step="0.1" value="2.5"></label><button id="weatherRun">Score Dallas weather</button></section>
<section class="card"><h2>Historical Dallas weather backtest</h2><p class="muted">Archived 24-hour-prior GFS forecasts versus settled Kalshi high/low markets, priced from the latest hourly candle no later than Dallas midnight. Research only.</p><label>Days <input id="weatherHistoryDays" type="number" min="7" max="90" value="30"></label><label>Assumed forecast error σ°F <input id="weatherHistorySigma" type="number" min="0.5" max="10" step="0.1" value="2.5"></label><label>Minimum edge after fee <input id="weatherHistoryEdge" type="number" min="0" max="0.5" step="0.01" value="0.05"></label><button id="weatherHistoryRun">Run historical backtest</button><button id="weatherWalkRun">Run 60-day walk-forward</button></section>
<section class="card"><h2>Profitability scoreboard</h2><p class="muted">Fee-adjusted evidence from durable Kalshi observations. The verdict is mechanical; it cannot enable execution.</p><button id="scoreboardRun">Load 72-hour scoreboard</button></section>
<section class="card"><h2>Result</h2><button id="copyResult" type="button">Copy result</button><span id="copyStatus" class="muted"></span><pre id="result">Choose a market and run the research suite.</pre></section>
<script>
const post=async(path,body)=>{const s=document.getElementById('status'),r=document.getElementById('result');s.textContent=' Running…';s.className='muted';try{const response=await fetch(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});const data=await response.json();if(!response.ok)throw new Error(JSON.stringify(data));r.textContent=JSON.stringify(data,null,2);s.textContent=' Complete';s.className='ok'}catch(error){r.textContent=String(error);s.textContent=' Failed';s.className='bad'}};
document.getElementById('run').onclick=()=>post('/diagnostics/opportunity_lab/backtest/crypto',{symbol:document.getElementById('symbol').value,days:Number(document.getElementById('days').value),timeframe:document.getElementById('timeframe').value});
document.getElementById('basis').onclick=()=>post('/diagnostics/opportunity_lab/basis/evaluate',{spot_ask:Number(document.getElementById('spot').value),derivative_bid:Number(document.getElementById('derivative').value),funding_rate_bps:Number(document.getElementById('funding').value),holding_hours:Number(document.getElementById('hold').value),available_capital:Number(document.getElementById('capital').value),spot_ask_size:1000000000,derivative_bid_size:1000000000});
document.getElementById('carryRun').onclick=()=>post('/diagnostics/opportunity_lab/coinbase/reconstruct-funding',{market:document.getElementById('carryMarket').value,days:Number(document.getElementById('carryDays').value),total_cost_bps:Number(document.getElementById('carryCost').value),cost_scenarios_bps:[139,149,260]});
document.getElementById('arbRun').onclick=()=>{try{post('/diagnostics/opportunity_lab/arbitrage/scan',{quotes:JSON.parse(document.getElementById('arbQuotes').value),bankroll:Number(document.getElementById('arbBankroll').value),minimum_profit:Number(document.getElementById('arbMinProfit').value),stake_increment:.01,rules_compatible:document.getElementById('arbRules').checked})}catch(error){document.getElementById('result').textContent='Invalid quote JSON: '+String(error)}};
document.getElementById('kalshiRun').onclick=()=>post('/diagnostics/opportunity_lab/kalshi/scan',{category:document.getElementById('kalshiCategory').value,pages:Number(document.getElementById('kalshiPages').value),limit:200});
document.getElementById('kalshiSave').onclick=()=>post('/diagnostics/opportunity_lab/kalshi/scan',{category:document.getElementById('kalshiCategory').value,pages:Number(document.getElementById('kalshiPages').value),limit:200,persist:true});
document.getElementById('makerRun').onclick=()=>post('/diagnostics/opportunity_lab/kalshi/market-making',{pages:Number(document.getElementById('makerPages').value),limit:200,quote_size:Number(document.getElementById('makerSize').value),maker_fee_coefficient:Number(document.getElementById('makerFee').value)});
document.getElementById('makerEvidence').onclick=async()=>{const s=document.getElementById('status'),r=document.getElementById('result');s.textContent=' Loading…';try{const response=await fetch('/diagnostics/opportunity_lab/kalshi/market-making/evidence?hours=720');const data=await response.json();if(!response.ok)throw new Error(JSON.stringify(data));r.textContent=JSON.stringify(data,null,2);s.textContent=' Complete';s.className='ok'}catch(error){r.textContent=String(error);s.textContent=' Failed';s.className='bad'}};
document.getElementById('crossRun').onclick=()=>post('/diagnostics/opportunity_lab/cross-exchange/scan',{symbol:document.getElementById('crossSymbol').value,max_notional:Number(document.getElementById('crossNotional').value)});
document.getElementById('triangleRun').onclick=()=>post('/diagnostics/opportunity_lab/triangular/scan',{starting_usd:Number(document.getElementById('triangleCapital').value)});
document.getElementById('weatherRun').onclick=()=>post('/diagnostics/opportunity_lab/weather/dallas',{sigma_f:Number(document.getElementById('weatherSigma').value)});
document.getElementById('weatherHistoryRun').onclick=()=>post('/diagnostics/opportunity_lab/weather/dallas/backtest',{days:Number(document.getElementById('weatherHistoryDays').value),sigma_f:Number(document.getElementById('weatherHistorySigma').value),minimum_edge:Number(document.getElementById('weatherHistoryEdge').value),lead_days:1});
document.getElementById('weatherWalkRun').onclick=()=>post('/diagnostics/opportunity_lab/weather/dallas/walk-forward',{days:60,lead_days:1});
document.getElementById('scoreboardRun').onclick=async()=>{const s=document.getElementById('status'),r=document.getElementById('result');s.textContent=' Loading…';try{const response=await fetch('/diagnostics/opportunity_lab/scoreboard?hours=72');const data=await response.json();if(!response.ok)throw new Error(JSON.stringify(data));r.textContent=JSON.stringify(data,null,2);s.textContent=' Complete';s.className='ok'}catch(error){r.textContent=String(error);s.textContent=' Failed';s.className='bad'}};
document.getElementById('copyResult').onclick=async()=>{const status=document.getElementById('copyStatus');try{await navigator.clipboard.writeText(document.getElementById('result').textContent);status.textContent=' Copied';status.className='ok'}catch(error){status.textContent=' Copy failed—select the result manually';status.className='bad'}};
</script></main></body></html>""", headers={"Cache-Control": "no-store"})


@app.post("/diagnostics/opportunity_lab/backtest/crypto")
def crypto_backtest(body: dict) -> dict:
    symbol = str(body.get("symbol") or "BTC/USD").strip().upper()
    if symbol not in {"BTC/USD", "ETH/USD"}:
        raise HTTPException(status_code=400, detail="initial research universe is BTC/USD and ETH/USD")
    days = max(90, min(3650, int(body.get("days") or 730)))
    timeframe = str(body.get("timeframe") or "1Hour")
    if timeframe not in {"1Hour", "4Hour", "1Day"}:
        raise HTTPException(status_code=400, detail="timeframe must be 1Hour, 4Hour, or 1Day")
    end = datetime.now(timezone.utc)
    bars, transport = fetch_crypto_bars([symbol], start=end - timedelta(days=days), end=end, timeframe=timeframe)
    if transport.get("error"):
        raise HTTPException(status_code=502, detail={"transport": transport})
    if transport.get("truncated"):
        raise HTTPException(status_code=502, detail={"error": "historical_data_truncated", "transport": transport})
    return {"ok": True, "symbol": symbol, "requested_days": days, "transport": transport, "research": crypto_research_suite(bars.get(symbol, [])), "execution_enabled": False}


@app.post("/diagnostics/opportunity_lab/basis/evaluate")
def basis_evaluate(body: dict) -> dict:
    allowed = BasisInputs.__dataclass_fields__.keys()
    try:
        inputs = BasisInputs(**{key: body[key] for key in allowed if key in body})
        return {"ok": True, "evaluation": evaluate_basis(inputs)}
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/diagnostics/opportunity_lab/basis/backtest")
def basis_backtest(body: dict) -> dict:
    try:
        result = backtest_funding(
            body.get("funding_rates_bps") or [],
            entry_basis_bps=float(body.get("entry_basis_bps") or 0),
            exit_basis_bps=float(body.get("exit_basis_bps") or 0),
            total_cost_bps=float(body.get("total_cost_bps") or 72),
            derivative_leverage=float(body.get("derivative_leverage") or 1),
        )
        return {"ok": True, "backtest": result}
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/diagnostics/opportunity_lab/coinbase/status")
def coinbase_status() -> dict:
    products, transport = list_perpetual_products()
    if transport.get("error"):
        raise HTTPException(status_code=502, detail={"configured": credentials_configured(), "transport": transport})
    return {
        "ok": True,
        "configured": True,
        "authenticated": bool(transport.get("authenticated")),
        "perpetual_product_count": len(products),
        "products": products,
        "cfm_entitlement": check_cfm_read_access(),
        "transport": transport,
        "account_balances_returned": False,
        "execution_enabled": False,
    }


@app.post("/diagnostics/opportunity_lab/coinbase/reconstruct-funding")
def coinbase_reconstruct_funding(body: dict) -> dict:
    market = str(body.get("market") or "BTC").upper()
    mapping = {"BTC": ("BIP-20DEC30-CDE", "BTC-USD"), "ETH": ("ETP-20DEC30-CDE", "ETH-USD")}
    if market not in mapping:
        raise HTTPException(status_code=400, detail="market must be BTC or ETH")
    days = max(7, min(365, int(body.get("days") or 90)))
    total_cost_bps = max(0.0, min(1000.0, float(body.get("total_cost_bps") or 72.0)))
    raw_scenarios = body.get("cost_scenarios_bps") or [total_cost_bps]
    try:
        cost_scenarios = list(dict.fromkeys(max(0.0, min(1000.0, float(value))) for value in raw_scenarios))[:10]
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="cost_scenarios_bps must be a list of numbers") from exc
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    start = end - timedelta(days=days)
    future_id, spot_id = mapping[market]
    futures, future_transport = fetch_product_candles(future_id, start=start, end=end)
    spots, spot_transport = fetch_product_candles(spot_id, start=start, end=end)
    if future_transport.get("error") or spot_transport.get("error"):
        raise HTTPException(status_code=502, detail={"future_transport": future_transport, "spot_transport": spot_transport})
    if future_transport.get("truncated") or spot_transport.get("truncated"):
        raise HTTPException(status_code=502, detail={"error": "historical_data_truncated", "future_transport": future_transport, "spot_transport": spot_transport})
    primary = reconstruct_hourly_funding(futures, spots, total_cost_bps=total_cost_bps)
    scenario_results = [reconstruct_hourly_funding(futures, spots, total_cost_bps=cost) for cost in cost_scenarios]
    return {
        "ok": True,
        "market": market,
        "future_product_id": future_id,
        "spot_product_id": spot_id,
        "requested_days": days,
        "future_transport": future_transport,
        "spot_transport": spot_transport,
        "completed_candles_only": True,
        "reconstruction": primary,
        "cost_scenarios": [{
            "total_cost_bps": result.get("total_cost_bps"),
            "net_pnl_bps": result.get("net_pnl_bps"),
            "return_on_fully_collateralized_capital_pct": result.get("return_on_fully_collateralized_capital_pct"),
            "annualized_return_on_fully_collateralized_capital_pct": result.get("annualized_return_on_fully_collateralized_capital_pct"),
            "profitable": result.get("profitable"),
        } for result in scenario_results],
        "execution_enabled": False,
    }


@app.get("/diagnostics/opportunity_lab/coinbase/fees")
def coinbase_fees() -> dict:
    schedules, transports = get_fee_schedule()
    if not schedules:
        raise HTTPException(status_code=502, detail={"error": "coinbase_fee_schedule_unavailable", "transport": transports})
    return {
        "ok": True,
        "fee_schedules": schedules,
        "rates_are_decimal": True,
        "balances_volumes_and_identifiers_returned": False,
        "execution_enabled": False,
    }


@app.post("/diagnostics/opportunity_lab/arbitrage/scan")
def arbitrage_scan(body: dict) -> dict:
    try:
        quotes = []
        for row in body.get("quotes") or []:
            odds_format = str(row.get("odds_format") or "decimal").lower()
            if odds_format not in {"american", "decimal"}:
                raise ValueError("odds_format must be american or decimal")
            raw_odds = float(row.get("odds"))
            decimal_odds = american_to_decimal(raw_odds) if odds_format == "american" else raw_odds
            quotes.append(OutcomeQuote(
                outcome=str(row.get("outcome") or ""),
                venue=str(row.get("venue") or "unknown"),
                decimal_odds=decimal_odds,
                max_stake=float(row.get("max_stake") or 0),
                commission_rate=float(row.get("commission_rate") or 0),
            ))
        result = scan_arbitrage(
            quotes,
            bankroll=float(body.get("bankroll") or 0),
            stake_increment=float(body.get("stake_increment") or .01),
            minimum_profit=float(body.get("minimum_profit") or .01),
            rules_compatible=body.get("rules_compatible") is True,
        )
        return {"ok": True, "scan": result}
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/diagnostics/opportunity_lab/kalshi/scan")
def kalshi_scan(body: dict) -> dict:
    limit = max(1, min(200, int(body.get("limit") or 200)))
    pages = max(1, min(3, int(body.get("pages") or 1)))
    category = str(body.get("category") or "").strip()
    events, transport = fetch_open_events(limit=limit, pages=pages)
    if transport.get("error"):
        raise HTTPException(status_code=502, detail={"transport": transport})
    scan = rank_event_dislocations(events, category=category)
    scan["market_making"] = screen_market_making(events)
    persistence = save_kalshi_scan(scan, transport) if body.get("persist") is True else {"configured": store_configured(), "saved": False}
    return {"ok": True, "transport": transport, "scan": scan, "persistence": persistence, "execution_enabled": False}


@app.post("/diagnostics/opportunity_lab/kalshi/market-making")
def kalshi_market_making(body: dict) -> dict:
    limit = max(1, min(200, int(body.get("limit") or 200)))
    pages = max(1, min(3, int(body.get("pages") or 1)))
    quote_size = max(0.01, min(10000.0, float(body.get("quote_size") or 10)))
    maker_fee_coefficient = max(0.0, min(1.0, float(body.get("maker_fee_coefficient") if body.get("maker_fee_coefficient") is not None else 0.0175)))
    events, transport = fetch_open_events(limit=limit, pages=pages)
    if transport.get("error"):
        raise HTTPException(status_code=502, detail={"transport": transport})
    return {"ok": True, "transport": transport,
            "market_making": screen_market_making(events, quote_size=quote_size, maker_fee_coefficient=maker_fee_coefficient),
            "execution_enabled": False}


@app.get("/diagnostics/opportunity_lab/kalshi/market-making/evidence")
def kalshi_market_making_evidence(hours: int = 720) -> dict:
    return {"ok": True, "evidence": market_making_scoreboard(hours=hours), "execution_enabled": False}


@app.post("/diagnostics/opportunity_lab/cross-exchange/scan")
def cross_exchange_scan(body: dict) -> dict:
    symbol = str(body.get("symbol") or "BTC").upper()
    if symbol not in {"BTC", "ETH"}:
        raise HTTPException(status_code=400, detail="symbol must be BTC or ETH")
    maximum = max(10.0, min(100000.0, float(body.get("max_notional") or 1000)))
    result = collect_cross_exchange(symbol, max_notional=maximum)
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result)
    return result


@app.post("/diagnostics/opportunity_lab/triangular/scan")
def triangular_scan(body: dict) -> dict:
    capital = max(10.0, min(100000.0, float(body.get("starting_usd") or 1000)))
    result = collect_triangular(starting_usd=capital)
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result)
    return result


@app.post("/diagnostics/opportunity_lab/weather/dallas")
def weather_dallas(body: dict) -> dict:
    sigma = max(.5, min(10.0, float(body.get("sigma_f") or 2.5)))
    result = collect_dallas_weather(sigma_f=sigma)
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result)
    return result


@app.post("/diagnostics/opportunity_lab/weather/dallas/backtest")
def weather_dallas_backtest(body: dict) -> dict:
    days = max(7, min(90, int(body.get("days") or 30)))
    sigma = max(.5, min(10.0, float(body.get("sigma_f") or 2.5)))
    lead_days = max(1, min(7, int(body.get("lead_days") or 1)))
    minimum_edge = max(0.0, min(.5, float(body.get("minimum_edge") if body.get("minimum_edge") is not None else .05)))
    result = historical_dallas_backtest(days=days, sigma_f=sigma, lead_days=lead_days, minimum_edge=minimum_edge)
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result)
    return result


@app.post("/diagnostics/opportunity_lab/weather/dallas/walk-forward")
def weather_dallas_walk_forward(body: dict) -> dict:
    days = max(21, min(90, int(body.get("days") or 60)))
    lead_days = max(1, min(7, int(body.get("lead_days") or 1)))
    result = walk_forward_dallas(days=days, lead_days=lead_days)
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result)
    return result


@app.post("/worker/opportunity-lab/collect-kalshi")
def collect_kalshi(body: dict) -> dict:
    expected = (os.getenv("OPPORTUNITY_WORKER_SECRET") or "").strip()
    if not expected or str(body.get("worker_secret") or "") != expected:
        raise HTTPException(status_code=401, detail="invalid opportunity worker secret")
    pages = max(1, min(10, int(body.get("pages") or 10)))
    limit = max(1, min(200, int(body.get("limit") or 200)))
    events, transport = fetch_open_events(limit=limit, pages=pages)
    if transport.get("error"):
        raise HTTPException(status_code=502, detail={"transport": transport})
    scan = rank_event_dislocations(events)
    scan["market_making"] = screen_market_making(events)
    trades, trade_transport = fetch_recent_trades(min_ts=int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp()))
    if trade_transport.get("error"):
        raise HTTPException(status_code=502, detail={"trade_transport": trade_transport})
    scan["_public_trades"] = trades
    cross_exchange = [collect_cross_exchange(symbol, max_notional=1000) for symbol in ("BTC", "ETH")]
    cross_persistence = save_cross_exchange_scans(cross_exchange)
    triangular = collect_triangular(starting_usd=1000)
    triangular_persistence = save_triangular_scan(triangular)
    weather = collect_dallas_weather(sigma_f=2.5)
    weather_persistence = save_weather_scan(weather)
    settled_weather, settlement_transports = [], {}
    for series_ticker in ("KXHIGHTDAL", "KXLOWTDAL"):
        rows, settled_transport = fetch_settled_series_markets(
            series_ticker, min_settled_ts=int((datetime.now(timezone.utc) - timedelta(days=14)).timestamp()))
        settled_weather.extend(rows); settlement_transports[series_ticker] = settled_transport
    weather_calibration = reconcile_weather_settlements(settled_weather)
    return {"ok": True, "transport": transport, "scan_summary": {
        "events_received": scan["events_received"], "candidate_count": scan["candidate_count"],
        "price_dislocation_count": scan["price_dislocation_count"],
        "mutually_exclusive_no_pair_count": scan["mutually_exclusive_no_pair_count"],
        "closest_no_pair_count": scan["closest_no_pair_count"],
        "market_making_market_count": scan["market_making"]["market_count"],
        "market_making_conservative_positive_count": scan["market_making"]["conservative_positive_count"],
    }, "trade_transport": trade_transport, "persistence": save_kalshi_scan(scan, transport),
        "cross_exchange": [{"symbol": row["symbol"], "ok": row["ok"],
                            "best_direction": row.get("scan", {}).get("best_direction")} for row in cross_exchange],
        "cross_exchange_persistence": cross_persistence,
        "triangular": {"ok": triangular["ok"], "best_cycle": triangular.get("scan", {}).get("best_cycle")},
        "triangular_persistence": triangular_persistence,
        "weather": {"ok": weather["ok"], "event_count": weather.get("event_count"),
                    "best_candidate": weather.get("best_candidate")},
        "weather_persistence": weather_persistence, "weather_settlement_transports": settlement_transports,
        "weather_calibration": weather_calibration, "execution_enabled": False}


@app.get("/diagnostics/opportunity_lab/kalshi/history")
def kalshi_history(limit: int = 50) -> dict:
    return {"ok": True, "database_configured": store_configured(), **recent_runs(limit), "execution_enabled": False}


@app.get("/diagnostics/opportunity_lab/scoreboard")
def profitability_scoreboard(hours: int = 72) -> dict:
    return {"ok": True, "candidates": candidate_catalog(), "kalshi": kalshi_scoreboard(hours=hours),
            "cross_exchange_crypto": cross_exchange_scoreboard(hours=hours),
            "triangular_crypto": triangular_scoreboard(hours=hours),
            "prediction_market_making": market_making_scoreboard(hours=max(hours, 720)),
            "weather_prediction_value": weather_scoreboard(hours=hours), "execution_enabled": False}
