"""Standalone, non-executing FastAPI surface for Opportunity Lab."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from operator_auth import operator_authorized
from opportunity_lab.catalog import candidate_catalog
from opportunity_lab.coinbase_market_data import check_cfm_read_access, credentials_configured, fetch_product_candles, list_perpetual_products
from opportunity_lab.crypto_basis import BasisInputs, backtest_funding, evaluate_basis
from opportunity_lab.crypto_market_data import fetch_crypto_bars
from opportunity_lab.crypto_regime import crypto_research_suite
from opportunity_lab.funding_reconstruction import reconstruct_hourly_funding


APP_VERSION = "opportunity-lab-web-v2"
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
    return {"ok": True, "service": "opportunity_lab", "version": APP_VERSION, "candidate_count": 7, "execution_enabled": False, "live_submission": False}


@app.get("/health")
def health() -> dict:
    return {"ok": True, "service": "opportunity_lab", "version": APP_VERSION, "active_candidate": "crypto_basis", "mode": "research", "execution_enabled": False}


@app.get("/diagnostics/opportunity_lab/catalog")
def catalog() -> dict:
    return {"ok": True, "candidates": candidate_catalog()}


@app.get("/dashboard/opportunity-lab", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    return HTMLResponse("""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Opportunity Lab</title><style>
body{font-family:system-ui;background:#0b1020;color:#edf2ff;margin:0;padding:28px}main{max-width:1100px;margin:auto}
.card{background:#151d33;border:1px solid #2a385c;border-radius:12px;padding:20px;margin:16px 0}button,input,select{font:inherit;padding:9px;margin:4px;background:#202c49;color:#fff;border:1px solid #52658e;border-radius:6px}button{cursor:pointer;background:#3157c8}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#080c18;padding:16px;border-radius:8px;max-height:65vh;overflow:auto}.muted{color:#aebbd7}.ok{color:#71e6a0}.bad{color:#ff8c8c}
</style></head><body><main><h1>Opportunity Lab</h1><p class="muted">Research only · execution hard-disabled</p>
<section class="card"><h2>Crypto research</h2><label>Symbol <select id="symbol"><option>BTC/USD</option><option>ETH/USD</option></select></label><label>Days <input id="days" type="number" min="90" max="3650" value="730"></label><label>Timeframe <select id="timeframe"><option>1Hour</option><option>4Hour</option><option>1Day</option></select></label><button id="run">Run research</button><span id="status" class="muted"></span></section>
<section class="card"><h2>Funding/basis calculator</h2><p class="muted">Positive funding is paid to the short. Prices must be executable spot ask and derivative bid.</p><label>Spot ask <input id="spot" type="number" value="100000"></label><label>Derivative bid <input id="derivative" type="number" value="100500"></label><label>Funding bps / interval <input id="funding" type="number" step="0.01" value="1"></label><label>Hold hours <input id="hold" type="number" value="168"></label><label>Capital $ <input id="capital" type="number" value="1000"></label><button id="basis">Evaluate basis</button></section>
<section class="card"><h2>CDE funding reconstruction</h2><p class="muted">Research proxy from aligned hourly CDE-future and Coinbase-spot candles. No orders or balances.</p><label>Market <select id="carryMarket"><option>BTC</option><option>ETH</option></select></label><label>Days <input id="carryDays" type="number" min="7" max="365" value="90"></label><label>Total round-trip cost (bps) <input id="carryCost" type="number" min="0" max="1000" step="0.1" value="72"></label><button id="carryRun">Reconstruct funding</button></section>
<section class="card"><h2>Result</h2><pre id="result">Choose a market and run the research suite.</pre></section>
<script>const post=async(path,body)=>{const s=document.getElementById('status'),r=document.getElementById('result');s.textContent=' Running…';s.className='muted';try{const response=await fetch(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});const data=await response.json();if(!response.ok)throw new Error(JSON.stringify(data));r.textContent=JSON.stringify(data,null,2);s.textContent=' Complete';s.className='ok'}catch(error){r.textContent=String(error);s.textContent=' Failed';s.className='bad'}};document.getElementById('run').onclick=()=>post('/diagnostics/opportunity_lab/backtest/crypto',{symbol:document.getElementById('symbol').value,days:Number(document.getElementById('days').value),timeframe:document.getElementById('timeframe').value});document.getElementById('basis').onclick=()=>post('/diagnostics/opportunity_lab/basis/evaluate',{spot_ask:Number(document.getElementById('spot').value),derivative_bid:Number(document.getElementById('derivative').value),funding_rate_bps:Number(document.getElementById('funding').value),holding_hours:Number(document.getElementById('hold').value),available_capital:Number(document.getElementById('capital').value),spot_ask_size:1000000000,derivative_bid_size:1000000000});document.getElementById('carryRun').onclick=()=>post('/diagnostics/opportunity_lab/coinbase/reconstruct-funding',{market:document.getElementById('carryMarket').value,days:Number(document.getElementById('carryDays').value),total_cost_bps:Number(document.getElementById('carryCost').value)});</script></main></body></html>""", headers={"Cache-Control": "no-store"})


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
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    future_id, spot_id = mapping[market]
    futures, future_transport = fetch_product_candles(future_id, start=start, end=end)
    spots, spot_transport = fetch_product_candles(spot_id, start=start, end=end)
    if future_transport.get("error") or spot_transport.get("error"):
        raise HTTPException(status_code=502, detail={"future_transport": future_transport, "spot_transport": spot_transport})
    if future_transport.get("truncated") or spot_transport.get("truncated"):
        raise HTTPException(status_code=502, detail={"error": "historical_data_truncated", "future_transport": future_transport, "spot_transport": spot_transport})
    return {
        "ok": True,
        "market": market,
        "future_product_id": future_id,
        "spot_product_id": spot_id,
        "requested_days": days,
        "future_transport": future_transport,
        "spot_transport": spot_transport,
        "reconstruction": reconstruct_hourly_funding(futures, spots, total_cost_bps=total_cost_bps),
        "execution_enabled": False,
    }
