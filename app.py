# app.py — Equities Webhook/Scanner
# Version: 2025-09-15-EXEC-GATELOCK-06
# - Robust strategy runner that adapts to S1–S4 scan signatures
# - Market gate blocks execution when market is closed (no force override)
# - /scan endpoints never 500 on strategy errors; return {ok:false,error,trace}
# - Diagnostics (/diag/*), orders (/orders/*), dashboard, and health versions

import os, json, traceback, uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable

from flask import Flask, request, jsonify, Response

# ---------- Flexible imports (supports flat or packaged layouts) ----------
def _try_import(*choices):
    last_err = None
    for modpath, names in choices:
        try:
            m = __import__(modpath, fromlist=names)
            return tuple(getattr(m, n) for n in names)
        except Exception as e:
            last_err = e
            continue
    raise last_err

# Market service (HTTP clients for Alpaca data/trading)
Market, = _try_import(
    ("market", ("Market",)),
    ("services.market", ("Market",)),
    ("core.market", ("Market",)),
)

# Strategies
try:
    S1Scanner, = _try_import(("s1", ("S1Scanner",)))
except Exception:
    S1Scanner = None

try:
    S2Scanner, = _try_import(("s2", ("S2Scanner",)))
except Exception:
    S2Scanner = None

S3OpeningRange, = _try_import(
    ("s3", ("S3OpeningRange",)),
    ("strategies.s3", ("S3OpeningRange",)),
)

S4RSIPullback, = _try_import(
    ("s4", ("S4RSIPullback",)),
    ("strategies.s4", ("S4RSIPullback",)),
)

# Optional helpers
try:
    orders_api, = _try_import(("orders", ("OrdersAPI",)))
except Exception:
    orders_api = None

try:
    perf_api, = _try_import(("performance", ("PerformanceAPI",)))
except Exception:
    perf_api = None

# Alpaca helper (for clock/account shortcuts)
try:
    from alpaca import AlpacaHTTP
except Exception:
    AlpacaHTTP = None

app = Flask(__name__)

# ---------- Global singletons ----------
# Build Market instance, supporting both Market() and Market.from_env()
if hasattr(Market, "from_env"):
    market = Market.from_env()
else:
    market = Market()

# When available, create order/performance helpers bound to the same market/session
ORDERS = orders_api(market) if orders_api else None
PERF = perf_api(market) if perf_api else None

APP_VERSION = os.getenv("APP_VERSION", "2025-09-15-EXEC-GATELOCK-06")
S3_VERSION = getattr(S3OpeningRange, "VERSION", "1.0.0")
S4_VERSION = getattr(S4RSIPullback, "VERSION", "1.0.1")

DEFAULT_S1_SYMBOLS = os.getenv("S1_SYMBOLS", "SPY,QQQ").split(",")
DEFAULT_S2_SYMBOLS = os.getenv("S2_SYMBOLS", "TSLA,NVDA,COIN").split(",")
DEFAULT_S4_SYMBOLS = os.getenv("S4_SYMBOLS", "SPY,QQQ").split(",")

# Gate toggle (on by default)
GATE_ON = os.getenv("MARKET_GATE", "on").lower() in ("1","true","on","yes")

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _parse_bool(q: Optional[str], default: bool=False) -> bool:
    if q is None:
        return default
    return q.lower() in ("1","true","yes","on")

def _symbols_from_query(defaults: List[str]) -> List[str]:
    s = request.args.get("symbols")
    if not s:
        return defaults
    return [t.strip().upper() for t in s.split(",") if t.strip()]

def _gate_decision() -> Dict[str, Any]:
    clk = market.clock()
    is_open = bool(clk.get("is_open", False))
    decision = "open" if (not GATE_ON or is_open) else "closed"
    return {"gate_on": GATE_ON, "decision": decision, "clock": clk}

def _exec_allowed() -> bool:
    gd = _gate_decision()
    return gd["decision"] == "open"

def _json(data: Dict[str, Any], status: int=200) -> Response:
    return app.response_class(
        response=json.dumps(data, default=str),
        status=status,
        mimetype="application/json"
    )

def _safe_call(fn: Callable[[], Dict[str, Any]]) -> Response:
    try:
        payload = fn()
        # Always 200 to keep cron green; embed ok flag
        return _json(payload, status=200)
    except Exception as e:
        trace = traceback.format_exc(limit=6)
        return _json({"ok": False, "error": str(e), "trace": trace}, status=200)

# ---------- Strategy runner ----------
def run_strategy(strategy_cls, *, params: Optional[Dict[str, Any]]=None,
                 symbols: Optional[List[str]]=None, dry: bool=False) -> Dict[str, Any]:
    params = params or {}
    now = _now()
    # Instantiate
    strat = strategy_cls(market=market, params=params, now=now, dry=dry)
    # Pick symbols default
    if symbols is None:
        if strategy_cls is S4RSIPullback:
            symbols = DEFAULT_S4_SYMBOLS
        elif strategy_cls is S3OpeningRange:
            symbols = ["SPY"]
        else:
            symbols = DEFAULT_S1_SYMBOLS

    # Introspect scan signature and call appropriately
    scan_fn = getattr(strat, "scan")
    try:
        import inspect
        sig = inspect.signature(scan_fn)
        if len(sig.parameters) == 0:
            plans = scan_fn()  # S1/S2 old style
        else:
            # Pass the trio most strategies expect
            plans = scan_fn(now=now, market=market, symbols=symbols)
    except TypeError:
        # Fallback: try positional trio
        plans = scan_fn(now, market, symbols)

    # Optionally execute
    executed = 0
    if not dry and _exec_allowed() and hasattr(strat, "execute"):
        try:
            executed = strat.execute(plans) or 0
        except Exception:
            app.logger.exception("execute() failed")
    return {
        "ok": True,
        "dry": dry,
        "executed": executed,
        "plans": plans if isinstance(plans, list) else [],
        "system": getattr(strat, "SYSTEM", getattr(strategy_cls, "__name__", "STRAT")),
        "symbols": symbols,
        "ts": now.isoformat(),
    }

# ---------- Routes ----------
@app.get("/")
def index():
    return _json({"ok": False, "error": "Not Found", "tip": "See /dashboard or /health/versions"}, status=404)

@app.get("/health/versions")
def versions():
    return _json({"app": APP_VERSION, "S3_OPENING_RANGE": S3_VERSION, "S4_RSI_PB": S4_VERSION})

# Diagnostics
@app.get("/diag/alpaca")
def diag_alpaca():
    base_trade = market.trading_base
    base_data = market.data_base
    feed = market.feed
    return _json({"trading_base": base_trade, "data_base": base_data, "feed": feed, "market_gate": "on" if GATE_ON else "off"})

@app.get("/diag/clock")
def diag_clock():
    return _json(market.clock())

@app.get("/diag/gate")
def diag_gate():
    return _json(_gate_decision())

# Orders & positions (best effort; ignore if helpers missing)
@app.get("/orders/recent")
def orders_recent():
    if ORDERS and hasattr(ORDERS, "list_recent_orders"):
        return _json(ORDERS.list_recent_orders())
    # Fallback via market
    try:
        return _json(market.list_orders(status="all", limit=50))
    except Exception as e:
        return _json({"ok": False, "error": str(e)}, status=200)

@app.get("/positions")
def positions():
    try:
        return _json(market.list_positions())
    except Exception as e:
        return _json({"ok": False, "error": str(e)}, status=200)

# S1
@app.get("/scan/s1")
def scan_s1():
    dry = _parse_bool(request.args.get("dry"), default=False)
    tf = request.args.get("tf", "1")
    mode = request.args.get("mode", "either")
    params = {
        "tf": tf,
        "mode": mode,
        "band": float(request.args.get("band", "0.6")),
        "vol": _parse_bool(request.args.get("vol"), True),
        "vmult": float(request.args.get("vmult", "1.0")),
    }
    def _run():
        if not dry and not _exec_allowed():
            return {"ok": True, "skipped": "market_closed"}
        if S1Scanner is None:
            return {"ok": False, "error": "S1 module not available"}
        return run_strategy(S1Scanner, params=params, dry=dry)
    return _safe_call(_run)

# S2
@app.get("/scan/s2")
def scan_s2():
    dry = _parse_bool(request.args.get("dry"), default=False)
    tf = request.args.get("tf", "30")
    mode = request.args.get("mode", "either")
    symbols = _symbols_from_query(DEFAULT_S2_SYMBOLS)
    params = {"tf": tf, "mode": mode}
    def _run():
        if not dry and not _exec_allowed():
            return {"ok": True, "skipped": "market_closed"}
        if S2Scanner is None:
            return {"ok": False, "error": "S2 module not available"}
        return run_strategy(S2Scanner, params=params, symbols=symbols, dry=dry)
    return _safe_call(_run)

# S3 (Opening Range)
@app.post("/scan/s3")
def scan_s3():
    dry = _parse_bool(request.args.get("dry"), default=False)
    symbols = ["SPY"]
    params = {}
    def _run():
        if not dry and not _exec_allowed():
            return {"ok": True, "skipped": "market_closed"}
        return run_strategy(S3OpeningRange, params=params, symbols=symbols, dry=dry)
    return _safe_call(_run)

# S4 (RSI Pullback)
@app.post("/scan/s4")
def scan_s4():
    dry = _parse_bool(request.args.get("dry"), default=False)
    symbols = _symbols_from_query(DEFAULT_S4_SYMBOLS)
    params = {
        "rsi_len": int(request.args.get("rsi_len", "14")),
        "pb_std": float(request.args.get("pb_std", "1.0")),
        "timeframe": request.args.get("timeframe", "1Min"),
        "limit": int(request.args.get("limit", "400")),
    }
    def _run():
        if not dry and not _exec_allowed():
            return {"ok": True, "skipped": "market_closed"}
        return run_strategy(S4RSIPullback, params=params, symbols=symbols, dry=dry)
    return _safe_call(_run)

# Simple HTML dashboard (minimal; real UI may live elsewhere)
@app.get("/dashboard")
def dashboard():
    gate = _gate_decision()
    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Equities Webhook Dashboard</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 2rem; }}
    code, pre {{ background:#111; color:#0f0; padding:.5rem; border-radius:6px; display:block; }}
    .cards {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(260px,1fr)); gap:1rem; }}
    .card {{ border:1px solid #ddd; border-radius:8px; padding:1rem; }}
    h1 {{ margin-top:0; }}
    table {{ width:100%; border-collapse:collapse; }}
    th, td {{ text-align:left; padding:.35rem .5rem; border-bottom:1px solid #eee; }}
    .okay {{ color:#0a7; }} .warn {{ color:#c60; }} .bad {{ color:#c00; }}
  </style>
</head>
<body>
  <h1>Equities Webhook</h1>
  <div class="cards">
    <div class="card">
      <h3>Versions</h3>
      <table>
        <tr><th>App</th><td>{APP_VERSION}</td></tr>
        <tr><th>S3</th><td>{S3_VERSION}</td></tr>
        <tr><th>S4</th><td>{S4_VERSION}</td></tr>
      </table>
    </div>
    <div class="card">
      <h3>Alpaca</h3>
      <table>
        <tr><th>Trading</th><td>{market.trading_base}</td></tr>
        <tr><th>Data</th><td>{market.data_base}</td></tr>
        <tr><th>Feed</th><td>{market.feed}</td></tr>
      </table>
    </div>
    <div class="card">
      <h3>Gate</h3>
      <pre>{json.dumps(gate, indent=2)}</pre>
    </div>
  </div>

  <h3>Quick links</h3>
  <ul>
    <li><a href="/health/versions">/health/versions</a></li>
    <li><a href="/diag/alpaca">/diag/alpaca</a></li>
    <li><a href="/diag/clock">/diag/clock</a></li>
    <li><a href="/diag/gate">/diag/gate</a></li>
    <li><a href="/orders/recent">/orders/recent</a></li>
    <li><a href="/positions">/positions</a></li>
  </ul>
</body>
</html>
""".strip()
    return Response(html, status=200, mimetype="text/html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
