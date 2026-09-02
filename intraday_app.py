"""Production FastAPI entry point for the isolated regime-intraday system."""

from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from intraday_defaults import apply_intraday_defaults

apply_intraday_defaults()

from operator_auth import operator_authorized
from regime_intraday_api import build_regime_intraday_router
from regime_intraday_runtime import RegimeIntradayRuntime
from route_catalog import build_route_catalog


APP_VERSION = "regime-intraday-web-v1"
runtime = RegimeIntradayRuntime()
app = FastAPI(title="Regime Intraday Trading System", docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["GET", "POST"], allow_headers=["*"])


@app.middleware("http")
async def protect_operator_surfaces(request: Request, call_next):
    path = request.url.path
    protected = (path.startswith("/diagnostics/") and path != "/diagnostics/route_catalog") or path.startswith("/dashboard/")
    if protected and not operator_authorized(request.headers, os.getenv("ADMIN_SECRET", "")):
        return JSONResponse(status_code=401, content={"detail": "operator authentication required"}, headers={"WWW-Authenticate": 'Basic realm="Trading Operator"'})
    response = await call_next(request)
    if protected:
        response.headers["Cache-Control"] = "no-store"
    return response


def _html(content: str) -> HTMLResponse:
    return HTMLResponse(content=content, headers={"Cache-Control": "no-store"})


app.include_router(build_regime_intraday_router(
    get_scan=lambda: dict(runtime.last_scan), refresh_scan=runtime.scan, get_ledger_payload=runtime.ledger_payload,
    get_readiness_payload=runtime.readiness_payload, get_dashboard_payload=runtime.dashboard_payload,
    html_response=_html, replay=runtime.replay, scan_worker=runtime.scan_worker,
    paper_roundtrip=runtime.paper_roundtrip, paper_reconcile=runtime.paper_reconcile, paper_close=runtime.paper_close,
))


@app.get("/")
def root() -> dict:
    return {"ok": True, "service": "regime-intraday", "version": APP_VERSION, "paper_only": True, "live_submission": False}


@app.get("/health")
def health() -> dict:
    ledger = runtime.ledger_payload()
    cfg = runtime.config()
    return {
        "ok": True, "service": "regime-intraday", "version": APP_VERSION, "strategy_mode": "regime_intraday",
        "paper_only": True, "live_trading_enabled": False, "live_submission": False,
        "systems": {"regime_intraday": {"status": "paper_validation", "broker_mode": "paper", "live_entries_enabled": False,
                    "regime_inputs": list(cfg.symbols), "trade_symbols": list(cfg.trade_symbols),
                    "latest_regime": dict(runtime.last_scan.get("regime") or {}).get("name"),
                    "paper_order_count": dict(ledger.get("summary") or {}).get("paper_order_count", 0), "dashboard": "/dashboard/intraday"}},
    }


@app.get("/diagnostics/route_catalog")
def route_catalog() -> dict:
    return build_route_catalog(app.routes, archived_routes=[])
