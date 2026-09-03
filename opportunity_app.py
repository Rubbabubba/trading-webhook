"""Standalone, non-executing FastAPI surface for Opportunity Lab."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from operator_auth import operator_authorized
from opportunity_lab.catalog import candidate_catalog
from opportunity_lab.crypto_market_data import fetch_crypto_bars
from opportunity_lab.crypto_regime import crypto_research_suite


APP_VERSION = "opportunity-lab-web-v1"
app = FastAPI(title="Opportunity Lab", docs_url=None, redoc_url=None)


@app.middleware("http")
async def protect_diagnostics(request: Request, call_next):
    if request.url.path.startswith("/diagnostics/") and not operator_authorized(request.headers, os.getenv("OPPORTUNITY_ADMIN_SECRET", "")):
        return JSONResponse(status_code=401, content={"detail": "operator authentication required"}, headers={"WWW-Authenticate": 'Basic realm="Opportunity Lab"'})
    response = await call_next(request)
    if request.url.path.startswith("/diagnostics/"):
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
