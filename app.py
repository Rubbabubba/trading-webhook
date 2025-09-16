# app.py — Equities Webhook/Scanner
# Version: 2025-09-16-EXEC-GATELOCK-R2
# - S1–S4 execute live by default (no ?dry=1 needed)
# - Market gate blocks execution when market is closed (no force override)
# - /diag/gate and /diag/clock are bullet-proof (never 500)
# - Strategy runner adapts to varying scan() signatures (now, market, symbols, params)
# - Direct Alpaca HTTP fallback for clock & orders if Market API shape changes
# - Inline Dashboard at /dashboard (no template files needed)

from __future__ import annotations

import os
import sys
import json
import inspect
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable

import requests
from flask import Flask, request, jsonify, Response

# ----------------------------
# App & constants
# ----------------------------
APP_VERSION = "2025-09-16-EXEC-GATELOCK-R2"
S3_OPENING_RANGE_VER = "1.0.0"
S4_RSI_PB_VER = "1.0.1"

app = Flask(__name__)


# ----------------------------
# Market environment + object
# ----------------------------
def _env_flag(name: str, default: str = "on") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "on", "yes")

def _load_market_env() -> Dict[str, Any]:
    env = {
        "api_key": os.getenv("ALPACA_API_KEY", ""),
        "secret_key": os.getenv("ALPACA_SECRET_KEY", ""),
        "trading_base": os.getenv("ALPACA_TRADING_BASE", "https://paper-api.alpaca.markets"),
        "data_base": os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets"),
        "feed": os.getenv("ALPACA_DATA_FEED", "iex"),
        "market_gate": _env_flag("MARKET_GATE", "on"),
    }
    # Emit a simple structured log so you can confirm at boot
    try:
        print(json.dumps({
            "event": "market_env_loaded",
            "trading_base": env["trading_base"],
            "data_base": env["data_base"],
            "feed": env["feed"],
            "market_gate": "on" if env["market_gate"] else "off",
        }), flush=True)
    except Exception:
        pass
    return env

class _FallbackMarket:
    """Minimal Market facade in case your services.market API changes."""
    def __init__(self, api_key: str, secret_key: str, trading_base: str, data_base: str, feed: str, market_gate: bool):
        self.api_key = api_key
        self.secret_key = secret_key
        self.trading_base = trading_base.rstrip("/")
        self.data_base = data_base.rstrip("/")
        self.feed = feed
        self.market_gate = market_gate

    # Provide headers() and properties your code expects
    def headers(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Content-Type": "application/json",
        }

# Try to build a Market object from services.market if available
def _build_market() -> Any:
    env = _load_market_env()
    try:
        from services.market import Market  # your repo class
    except Exception:
        # services.market not importable — use fallback
        return _FallbackMarket(**env)

    # Try common construction patterns
    try:
        if hasattr(Market, "from_env") and callable(getattr(Market, "from_env")):
            return Market.from_env()
    except Exception:
        pass

    try:
        # Try kwargs init
        return Market(
            api_key=env["api_key"],
            secret_key=env["secret_key"],
            trading_base=env["trading_base"],
            data_base=env["data_base"],
            feed=env["feed"],
            market_gate=env["market_gate"],
        )
    except Exception:
        # Last resort
        return _FallbackMarket(**env)

market = _build_market()


# ----------------------------
# Alpaca HTTP helpers (fallback-safe)
# ----------------------------
def _auth_headers() -> Dict[str, str]:
    try:
        if hasattr(market, "headers") and callable(market.headers):
            h = market.headers()
            if isinstance(h, dict) and "APCA-API-KEY-ID" in h:
                return h
    except Exception:
        pass
    return {
        "APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY", ""),
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY", ""),
        "Content-Type": "application/json",
    }

def _trading_base() -> str:
    return getattr(market, "trading_base", os.getenv("ALPACA_TRADING_BASE", "https://paper-api.alpaca.markets")).rstrip("/")

def _data_base() -> str:
    return getattr(market, "data_base", os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets")).rstrip("/")

def _alpaca_get(path: str, params: Optional[dict] = None, timeout: int = 15) -> requests.Response:
    url = f"{_trading_base()}/{path.lstrip('/')}"
    return requests.get(url, headers=_auth_headers(), params=params, timeout=timeout)

def _alpaca_post(path: str, payload: dict, timeout: int = 15) -> requests.Response:
    url = f"{_trading_base()}/{path.lstrip('/')}"
    return requests.post(url, headers=_auth_headers(), data=json.dumps(payload), timeout=timeout)


# ----------------------------
# Clock + Market Gate (never raise)
# ----------------------------
def _get_clock() -> Dict[str, Any]:
    """
    Returns a dict like:
    {"is_open": bool, "next_open": str, "next_close": str, "timestamp": str}
    Uses Market methods if present; falls back to raw HTTP; never raises.
    """
    # Try Market.get_clock() / Market.clock()
    try:
        for attr in ("get_clock", "clock"):
            if hasattr(market, attr) and callable(getattr(market, attr)):
                c = getattr(market, attr)()
                if isinstance(c, dict):
                    return {
                        "is_open": bool(c.get("is_open", False)),
                        "next_open": str(c.get("next_open", "")),
                        "next_close": str(c.get("next_close", "")),
                        "timestamp": str(c.get("timestamp", "")),
                    }
                # Object with attributes
                return {
                    "is_open": bool(getattr(c, "is_open", False)),
                    "next_open": str(getattr(c, "next_open", "")),
                    "next_close": str(getattr(c, "next_close", "")),
                    "timestamp": str(getattr(c, "timestamp", "")),
                }
    except Exception:
        pass

    # Raw HTTP fallback
    try:
        r = _alpaca_get("/v2/clock", params=None, timeout=10)
        r.raise_for_status()
        data = r.json()
        return {
            "is_open": bool(data.get("is_open", False)),
            "next_open": data.get("next_open", ""),
            "next_close": data.get("next_close", ""),
            "timestamp": data.get("timestamp", ""),
        }
    except Exception as e:
        return {
            "is_open": False,
            "next_open": "",
            "next_close": "",
            "timestamp": "",
            "error": f"clock_fetch_failed:{type(e).__name__}",
        }

def _gate_on() -> bool:
    return bool(getattr(market, "market_gate", True))

def _gate_decision() -> str:
    if not _gate_on():
        return "bypass"
    clock = _get_clock()
    return "open" if clock.get("is_open") is True else "closed"

def _gate_allows_execution() -> bool:
    return _gate_decision() == "open"


# ----------------------------
# Strategy import helpers (soft)
# ----------------------------
def _import_strategy(symbol: str) -> Optional[type]:
    """
    Import strategy class by conventional symbol name ('S1','S2','S3','S4').
    Returns a class or None; we will handle None gracefully.
    """
    candidates = [
        # common places
        f"strategies.{symbol.lower()}",
        f"services.strategies.{symbol.lower()}",
        f"services.strategies.{symbol}",
        f"{symbol.lower()}",
        f"{symbol}",
    ]
    for modname in candidates:
        try:
            mod = __import__(modname, fromlist=["*"])
        except Exception:
            continue
        # Pick the first class in the module that starts with the symbol name
        for name in dir(mod):
            if name.upper().startswith(symbol.upper()):
                obj = getattr(mod, name)
                if inspect.isclass(obj):
                    return obj
    return None


# ----------------------------
# Execution helpers
# ----------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _order_payload_from_plan(plan: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalizes a 'plan' dict into an Alpaca /v2/orders payload.
    Expected keys in plan (flexible): symbol, side, qty, type, time_in_force, limit_price, stop_price, trail_percent, trail_price, extended_hours
    """
    if not isinstance(plan, dict):
        return None
    symbol = plan.get("symbol")
    side = plan.get("side", "buy")
    qty = plan.get("qty") or plan.get("quantity") or 0
    otype = str(plan.get("type", "market")).lower()
    tif = plan.get("time_in_force", "day")
    if not symbol or not qty:
        return None

    payload = {
        "symbol": symbol,
        "side": side,
        "qty": str(qty),
        "type": otype,
        "time_in_force": tif,
    }
    # Optional fields
    if otype == "limit" and plan.get("limit_price") is not None:
        payload["limit_price"] = str(plan["limit_price"])
    if otype in ("stop", "stop_limit") and plan.get("stop_price") is not None:
        payload["stop_price"] = str(plan["stop_price"])
    if otype == "stop_limit" and plan.get("limit_price") is not None:
        payload["limit_price"] = str(plan["limit_price"])
    if otype == "trailing_stop":
        if plan.get("trail_percent") is not None:
            payload["trail_percent"] = str(plan["trail_percent"])
        if plan.get("trail_price") is not None:
            payload["trail_price"] = str(plan["trail_price"])
    if plan.get("extended_hours") is not None:
        payload["extended_hours"] = bool(plan["extended_hours"])
    if plan.get("client_order_id"):
        payload["client_order_id"] = str(plan["client_order_id"])
    return payload

def _submit_orders(plans: List[Dict[str, Any]]) -> Dict[str, Any]:
    submitted = []
    errors = []
    for plan in plans or []:
        payload = _order_payload_from_plan(plan)
        if not payload:
            errors.append({"plan": plan, "error": "invalid_plan"})
            continue
        try:
            r = _alpaca_post("/v2/orders", payload)
            if r.status_code >= 400:
                errors.append({"plan": plan, "status": r.status_code, "body": r.text})
            else:
                submitted.append(r.json())
        except Exception as e:
            errors.append({"plan": plan, "error": str(e)})
    return {"submitted": submitted, "errors": errors}


# ----------------------------
# Strategy runner (signature-tolerant)
# ----------------------------
def _call_scan(strat: Any, params: Dict[str, Any], market_obj: Any, symbols: List[str]) -> List[Dict[str, Any]]:
    """
    Calls strat.scan with whatever signature it supports.
    Supported forms:
      scan()
      scan(now)
      scan(now, market)
      scan(now, market, symbols)
      scan(market)
      scan(market, symbols)
      scan(symbols)
      scan(params=...), scan(now=..., market=..., symbols=...)
    """
    now = datetime.now(timezone.utc)

    if hasattr(strat, "scan") and callable(strat.scan):
        sig = inspect.signature(strat.scan)
        kwargs = {}
        for name, p in sig.parameters.items():
            if name == "now":
                kwargs["now"] = now
            elif name == "market":
                kwargs["market"] = market_obj
            elif name == "symbols":
                kwargs["symbols"] = symbols
            elif name == "params":
                kwargs["params"] = params
            else:
                # if strategy expects any other optional params present in query
                if name in params:
                    kwargs[name] = params[name]

        try:
            # Try keyword call first
            return list(strat.scan(**kwargs) or [])
        except TypeError:
            # Fallback: positional best-effort
            candidate_args: List[Any] = []
            for name in ["now", "market", "symbols", "params"]:
                if name in kwargs:
                    candidate_args.append(kwargs[name])
            return list(strat.scan(*candidate_args) or [])

    # No scan method? no plans
    return []

def run_strategy(symbol: str, params: Dict[str, Any], default_symbols: List[str], dry: bool) -> Dict[str, Any]:
    # Gate: *always* enforced; cannot be bypassed
    if not _gate_allows_execution():
        return {"ok": True, "skipped": "market_closed"}

    # Load strategy class if available
    StratCls = _import_strategy(symbol)
    symbols = params.get("symbols")
    if isinstance(symbols, str):
        symbols = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbols:
        symbols = default_symbols

    # Instantiate strategy (tolerant to ctor shapes)
    strat = None
    if StratCls is not None:
        try:
            # Try most likely signatures
            try:
                strat = StratCls(market=market, params=params)
            except TypeError:
                try:
                    strat = StratCls(market=market)
                except TypeError:
                    try:
                        strat = StratCls(params=params)
                    except TypeError:
                        strat = StratCls()
        except Exception:
            strat = None

    plans: List[Dict[str, Any]] = []
    if strat is not None:
        try:
            plans = _call_scan(strat, params=params, market_obj=market, symbols=symbols)
        except Exception as e:
            return {"ok": False, "error": f"scan_failed:{type(e).__name__}", "trace": str(e)}

    # If no plans, return quickly
    if not plans:
        # Structured log like your earlier prints
        try:
            print(json.dumps({"event": "scan_done", "system": symbol, "triggers": 0, "ts": int(datetime.now().timestamp())}), flush=True)
            print(json.dumps({"event": "orders_submitted", "system": symbol, "submitted": 0, "ts": int(datetime.now().timestamp())}), flush=True)
        except Exception:
            pass
        # Still include symbol list where relevant
        payload = {"dry": bool(dry)} if symbol in ("S3", "S4") else {}
        payload.update({
            "ok": True,
            "results": [],
            "system": symbol if symbol in ("S3", "S4") else symbol,
        })
        if symbol == "S3":
            payload["version"] = S3_OPENING_RANGE_VER
            payload["symbols"] = symbols
        if symbol == "S4":
            payload["version"] = S4_RSI_PB_VER
            payload["symbols"] = symbols
        if symbol in ("S1", "S2"):
            payload["dry_run"] = bool(dry)
            payload["executed"] = 0
        return payload

    # If plans exist and dry-run requested, return plans but do not execute
    if dry:
        return {
            "ok": True,
            "dry": True,
            "plans": plans,
            "system": symbol,
            **({"version": S3_OPENING_RANGE_VER, "symbols": symbols} if symbol == "S3" else {}),
            **({"version": S4_RSI_PB_VER, "symbols": symbols} if symbol == "S4" else {}),
        }

    # Live execution
    result = _submit_orders(plans)
    submitted_ct = len(result.get("submitted", []))
    try:
        print(json.dumps({"event": "scan_done", "system": symbol, "triggers": len(plans), "ts": int(datetime.now().timestamp())}), flush=True)
        print(json.dumps({"event": "orders_submitted", "system": symbol, "submitted": submitted_ct, "ts": int(datetime.now().timestamp())}), flush=True)
    except Exception:
        pass

    # Normalize response
    if symbol in ("S3", "S4"):
        return {
            "dry": False,
            "results": result.get("submitted", []),
            "errors": result.get("errors", []),
            "system": "OPENING_RANGE" if symbol == "S3" else "RSI_PB",
            "version": S3_OPENING_RANGE_VER if symbol == "S3" else S4_RSI_PB_VER,
            "symbols": symbols,
        }
    else:
        return {
            "ok": True,
            "dry_run": False,
            "executed": submitted_ct,
            "results": result.get("submitted", []),
            "errors": result.get("errors", []),
            "system": symbol,
        }


# ----------------------------
# Routes — Diagnostics & Versions
# ----------------------------
@app.get("/health/versions")
def health_versions():
    return jsonify({
        "app": APP_VERSION,
        "S3_OPENING_RANGE": S3_OPENING_RANGE_VER,
        "S4_RSI_PB": S4_RSI_PB_VER,
    })

@app.get("/diag/alpaca")
def diag_alpaca():
    try:
        return jsonify({
            "trading_base": _trading_base(),
            "data_base": _data_base(),
            "feed": getattr(market, "feed", os.getenv("ALPACA_DATA_FEED", "iex")),
            "market_gate": "on" if _gate_on() else "off",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.get("/diag/clock")
def diag_clock():
    try:
        return jsonify(_get_clock())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.get("/diag/gate")
def diag_gate():
    try:
        clock = _get_clock()
        decision = _gate_decision()
        return jsonify({"gate_on": _gate_on(), "decision": decision, "clock": clock})
    except Exception as e:
        return jsonify({"gate_on": _gate_on(), "decision": "closed", "clock": {"is_open": False}, "error": str(e)})


# ----------------------------
# Routes — Orders & Positions (read)
# ----------------------------
@app.get("/orders/recent")
def orders_recent():
    try:
        # Use query param ?status=all/closed/open to filter
        status = request.args.get("status", "all")
        params = {"status": status, "limit": request.args.get("limit", "200")}
        r = _alpaca_get("/v2/orders", params=params)
        if r.status_code >= 400:
            return Response(r.text, status=r.status_code, content_type="application/json")
        return Response(r.text, content_type="application/json")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.get("/positions")
def positions():
    try:
        r = _alpaca_get("/v2/positions")
        if r.status_code >= 400:
            return Response(r.text, status=r.status_code, content_type="application/json")
        return Response(r.text, content_type="application/json")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# Simple test order endpoint
@app.post("/test/order")
def test_order():
    try:
        symbol = request.args.get("symbol", "SPY").upper()
        qty = int(request.args.get("qty", "1"))
        side = request.args.get("side", "buy")
        otype = request.args.get("type", "market")
        tif = request.args.get("tif", "day")
        payload = {"symbol": symbol, "qty": str(qty), "side": side, "type": otype, "time_in_force": tif}
        r = _alpaca_post("/v2/orders", payload)
        if r.status_code >= 400:
            return Response(r.text, status=r.status_code, content_type="application/json")
        return Response(r.text, content_type="application/json")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ----------------------------
# Routes — Strategy Scans
# ----------------------------
def _bool_arg(name: str, default: bool = False) -> bool:
    v = request.args.get(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "on", "yes")

def _collect_params() -> Dict[str, Any]:
    params = {}
    for k, v in request.args.items():
        params[k] = v
    return params

@app.get("/scan/s1")
def scan_s1():
    params = _collect_params()
    dry = _bool_arg("dry", False)
    # S1 defaults
    default_symbols = params.get("symbols")
    if isinstance(default_symbols, str):
        default_symbols = [s.strip().upper() for s in default_symbols.split(",") if s.strip()]
    if not default_symbols:
        default_symbols = ["SPY"]
    result = run_strategy("S1", params=params, default_symbols=default_symbols, dry=dry)
    return jsonify(result)

@app.get("/scan/s2")
def scan_s2():
    params = _collect_params()
    dry = _bool_arg("dry", False)
    # S2 defaults — allow watchlist via ?symbols=...
    default_symbols = params.get("symbols")
    if isinstance(default_symbols, str):
        default_symbols = [s.strip().upper() for s in default_symbols.split(",") if s.strip()]
    if not default_symbols:
        default_symbols = ["TSLA", "NVDA", "COIN"]
    result = run_strategy("S2", params=params, default_symbols=default_symbols, dry=dry)
    return jsonify(result)

@app.post("/scan/s3")
def scan_s3():
    params = _collect_params()
    dry = _bool_arg("dry", False)
    symbols = params.get("symbols", "SPY")
    default_symbols = [s.strip().upper() for s in str(symbols).split(",") if s.strip()]
    result = run_strategy("S3", params=params, default_symbols=default_symbols, dry=dry)
    return jsonify(result)

@app.post("/scan/s4")
def scan_s4():
    params = _collect_params()
    dry = _bool_arg("dry", False)
    symbols = params.get("symbols", "SPY,QQQ")
    default_symbols = [s.strip().upper() for s in str(symbols).split(",") if s.strip()]
    result = run_strategy("S4", params=params, default_symbols=default_symbols, dry=dry)
    return jsonify(result)


# ----------------------------
# Dashboard (inline HTML)
# ----------------------------
DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Equities Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root {
    --bg: #0b0f14;
    --panel: #121821;
    --text: #e6edf3;
    --muted: #8aa0b4;
    --ok: #2ecc71;
    --warn: #f1c40f;
    --err: #e74c3c;
    --accent: #4aa3ff;
    --chip: #1b2430;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", "Apple Color Emoji", "Segoe UI Emoji"; background: var(--bg); color: var(--text); }
  header { padding: 16px 20px; background: linear-gradient(180deg, #0e131a 0%, #0b0f14 100%); border-bottom: 1px solid #1a2330; display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
  h1 { margin: 0; font-size: 18px; letter-spacing: .4px; font-weight: 600; }
  .muted { color: var(--muted); }
  .grid { display: grid; gap: 16px; padding: 16px; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); }
  .card { background: var(--panel); border: 1px solid #1a2330; border-radius: 12px; padding: 16px; }
  .card h2 { margin: 0 0 12px; font-size: 16px; letter-spacing: .3px; }
  .row { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
  .chips { display: flex; gap: 8px; flex-wrap: wrap; }
  .chip { background: var(--chip); border: 1px solid #1f2a38; color: var(--text); border-radius: 999px; padding: 6px 10px; font-size: 12px; }
  .ok { color: var(--ok); }
  .warn { color: var(--warn); }
  .err { color: var(--err); }
  button, .btn { cursor: pointer; background: #162335; color: var(--text); border: 1px solid #233248; padding: 8px 12px; border-radius: 8px; font-size: 13px; }
  button:hover, .btn:hover { background: #1b2a40; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 8px; border-bottom: 1px solid #1a2330; text-align: left; }
  th { color: var(--muted); font-weight: 500; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
  .right { text-align: right; }
  .small { font-size: 12px; color: var(--muted); }
  .notice { background: #0f1520; border: 1px solid #203049; padding: 10px 12px; border-radius: 10px; font-size: 13px; }
</style>
</head>
<body>
<header>
  <div>
    <h1>Equities Dashboard <span class="small muted mono" id="appVersion"></span></h1>
    <div class="small muted">Live execution is <strong id="gateState">checking…</strong></div>
  </div>
  <div class="row">
    <button onclick="refreshAll()">Refresh</button>
    <a class="btn" href="/health/versions">Versions</a>
    <a class="btn" href="/diag/alpaca">Alpaca</a>
  </div>
</header>

<div class="grid">
  <div class="card">
    <h2>Market Gate</h2>
    <div id="gateCard" class="notice">Loading…</div>
    <div class="chips" style="margin-top:10px;">
      <span class="chip">/diag/gate</span>
      <span class="chip">/diag/clock</span>
    </div>
  </div>

  <div class="card">
    <h2>Quick Actions</h2>
    <div class="row">
      <button onclick="triggerScan('S3')">Scan S3 (Opening Range)</button>
      <button onclick="triggerScan('S4')">Scan S4 (RSI Pullback)</button>
    </div>
    <div class="row" style="margin-top:10px;">
      <button onclick="triggerScan('S1')">Scan S1</button>
      <button onclick="triggerScan('S2')">Scan S2</button>
    </div>
    <div class="small muted" id="scanResult" style="margin-top:10px;"></div>
  </div>

  <div class="card" style="grid-column: 1 / -1;">
    <h2>Recent Orders</h2>
    <div class="row" style="margin-bottom:8px;">
      <button onclick="loadOrders('all')">All</button>
      <button onclick="loadOrders('open')">Open</button>
      <button onclick="loadOrders('closed')">Closed</button>
    </div>
    <div id="ordersTable">Loading…</div>
  </div>

  <div class="card" style="grid-column: 1 / -1;">
    <h2>Positions</h2>
    <div id="positionsTable">Loading…</div>
  </div>
</div>

<script>
async function jfetch(url, opts={}) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error("HTTP "+r.status);
  const ct = r.headers.get("content-type") || "";
  if (ct.includes("application/json")) return r.json();
  return r.text();
}

function esc(s){ return (s==null?"":String(s)).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

async function refreshGate() {
  try {
    const gate = await jfetch('/diag/gate');
    const v = await jfetch('/health/versions');
    document.getElementById('appVersion').textContent = "v" + esc(v.app);
    const open = gate.decision === 'open';
    document.getElementById('gateState').textContent = open ? 'OPEN' : (gate.decision || 'closed');
    document.getElementById('gateState').className = open ? 'ok' : 'warn';

    const clk = gate.clock || {};
    const lines = [
      `<div><strong>Gate:</strong> ${esc(gate.gate_on ? 'on' : 'off')} — <strong>Decision:</strong> ${esc(gate.decision)}</div>`,
      `<div><strong>Clock:</strong> is_open=${esc(clk.is_open)} · next_open=${esc(clk.next_open)} · next_close=${esc(clk.next_close)}</div>`
    ];
    document.getElementById('gateCard').innerHTML = lines.join('');
  } catch (e) {
    document.getElementById('gateCard').innerHTML = `<span class="err">Failed to load gate</span>`;
  }
}

async function loadOrders(status='all') {
  try {
    const rows = await jfetch(`/orders/recent?status=${encodeURIComponent(status)}&limit=200`);
    const arr = Array.isArray(rows) ? rows : [];
    if (arr.length === 0) {
      document.getElementById('ordersTable').innerHTML = '<div class="muted">No orders</div>';
      return;
    }
    let html = '<table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Type</th><th>Status</th><th class="right">Filled</th></tr></thead><tbody>';
    for (const o of arr.slice(0,200)) {
      html += `<tr>
        <td class="mono small">${esc(o.submitted_at || o.created_at || '')}</td>
        <td class="mono">${esc(o.symbol || '')}</td>
        <td>${esc(o.side || '')}</td>
        <td>${esc(o.qty || '')}</td>
        <td>${esc(o.type || '')}</td>
        <td>${esc(o.status || '')}</td>
        <td class="right">${esc(o.filled_qty || '0')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    document.getElementById('ordersTable').innerHTML = html;
  } catch (e) {
    document.getElementById('ordersTable').innerHTML = `<div class="err">Failed to load orders</div>`;
  }
}

async function loadPositions() {
  try {
    const rows = await jfetch('/positions');
    const arr = Array.isArray(rows) ? rows : [];
    if (arr.length === 0) {
      document.getElementById('positionsTable').innerHTML = '<div class="muted">No positions</div>';
      return;
    }
    let html = '<table><thead><tr><th>Symbol</th><th>Side</th><th>Qty</th><th class="right">Market Value</th><th class="right">Unrealized P/L</th></tr></thead><tbody>';
    for (const p of arr) {
      html += `<tr>
        <td class="mono">${esc(p.symbol || '')}</td>
        <td>${esc(p.side || '')}</td>
        <td>${esc(p.qty || '')}</td>
        <td class="right mono">$${esc(p.market_value || '0')}</td>
        <td class="right mono">$${esc(p.unrealized_pl || '0')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    document.getElementById('positionsTable').innerHTML = html;
  } catch (e) {
    document.getElementById('positionsTable').innerHTML = `<div class="err">Failed to load positions</div>`;
  }
}

async function triggerScan(which) {
  try {
    const isPost = which === 'S3' || which === 'S4';
    const url = which === 'S3' ? '/scan/s3?dry=0'
              : which === 'S4' ? '/scan/s4?dry=0'
              : which === 'S1' ? '/scan/s1?dry=0'
              : '/scan/s2?dry=0';
    const payload = isPost ? { method: 'POST' } : {};
    const res = await jfetch(url, payload);
    document.getElementById('scanResult').textContent = JSON.stringify(res);
    // After a scan, refresh orders
    loadOrders('all');
  } catch (e) {
    document.getElementById('scanResult').textContent = 'Scan failed';
  }
}

function refreshAll() {
  refreshGate();
  loadOrders('all');
  loadPositions();
}

window.addEventListener('load', () => {
  refreshAll();
  setInterval(refreshGate, 30_000);
});
</script>
</body>
</html>
"""

@app.get("/dashboard")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")


# ----------------------------
# Root
# ----------------------------
@app.get("/")
def index_root():
    # Keep this 404 (as in your logs), or redirect to /dashboard if you prefer:
    # return redirect("/dashboard")
    return Response("Not Found", status=404)


# ----------------------------
# Gunicorn entrypoint
# ----------------------------
if __name__ == "__main__":
    # Local run: FLASK runs on 0.0.0.0:10000 to match Render
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
