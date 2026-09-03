"""Standalone, non-executing FastAPI surface for Opportunity Lab."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from operator_auth import operator_authorized
from opportunity_lab.catalog import candidate_catalog
from opportunity_lab.crypto_market_data import fetch_crypto_bars
from opportunity_lab.crypto_regime import crypto_research_suite


APP_VERSION = "opportunity-lab-web-v1"
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
    return {"ok": True, "service": "opportunity_lab", "version": APP_VERSION, "active_candidate": "crypto_regime", "mode": "research", "execution_enabled": False}


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
<section class="card"><h2>Result</h2><pre id="result">Choose a market and run the research suite.</pre></section>
<script>document.getElementById('run').onclick=async()=>{const s=document.getElementById('status'),r=document.getElementById('result');s.textContent=' Running…';s.className='muted';r.textContent='Fetching bars and running walk-forward tests…';try{const response=await fetch('/diagnostics/opportunity_lab/backtest/crypto',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({symbol:document.getElementById('symbol').value,days:Number(document.getElementById('days').value),timeframe:document.getElementById('timeframe').value})});const data=await response.json();if(!response.ok)throw new Error(JSON.stringify(data));r.textContent=JSON.stringify(data,null,2);s.textContent=' Complete';s.className='ok'}catch(error){r.textContent=String(error);s.textContent=' Failed';s.className='bad'}};</script></main></body></html>""", headers={"Cache-Control": "no-store"})


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
    return {"ok": True, "symbol": symbol, "requested_days": days, "transport": transport, "research": crypto_research_suite(bars.get(symbol, [])), "execution_enabled": False}
