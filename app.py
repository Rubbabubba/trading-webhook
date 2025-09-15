# app.py — Equities Webhook/Scanner
# Version: 2025-09-15-EXEC-GATELOCK-ORDERS-01
# - S1–S4 wired with default live execution (no dry flag needed)
# - Market gate blocks execution when market is closed (no override)
# - New order endpoints: POST /orders/submit and POST /test/order
# - Diagnostics (/diag/*), versions (/health/versions), orders/positions, dashboard HTML

import os
import json
import time
import importlib
from datetime import datetime, timezone
from flask import Flask, request, jsonify, Response, redirect, render_template_string

# --- Services & Strategies -------------------------------------------------------

from services.market import Market  # must exist (we added earlier)

# Strategy imports are tolerant to naming differences
def _import_strategy(mod_name, class_candidates, version_attr="__version__"):
    try:
        mod = importlib.import_module(mod_name)
    except Exception:
        return None, None
    cls = None
    for name in class_candidates:
        cls = getattr(mod, name, None)
        if cls:
            break
    ver = getattr(mod, version_attr, None)
    return (cls, ver)

# S1 & S2: your legacy strategies (class/function names may vary)
S1_CLS, S1_VERSION = _import_strategy(
    "strategies.s1",
    ["StrategyS1", "S1Strategy", "S1", "ScannerS1"]
)
S2_CLS, S2_VERSION = _import_strategy(
    "strategies.s2",
    ["StrategyS2", "S2Strategy", "S2", "ScannerS2"]
)

# S3 Opening Range
S3_CLS, S3_VERSION = _import_strategy(
    "strategies.s3",
    ["S3OpeningRange", "OpeningRange", "StrategyS3"]
)
# S4 RSI Pullback
S4_CLS, S4_VERSION = _import_strategy(
    "strategies.s4",
    ["S4RSIPullback", "RSIPullback", "StrategyS4"]
)

# Fallback version labels if modules provided but no version attr
S1_VERSION = S1_VERSION or "unknown"
S2_VERSION = S2_VERSION or "unknown"
S3_VERSION = S3_VERSION or "unknown"
S4_VERSION = S4_VERSION or "unknown"

# --- App / Market ----------------------------------------------------------------

app = Flask(__name__)
market = Market.from_env()  # reads keys + endpoints + feed from env

MARKET_GATE_DEFAULT = os.getenv("MARKET_GATE", "on").lower()  # "on"|"off"
GATE_ON = MARKET_GATE_DEFAULT != "off"

APP_VERSION = os.getenv("APP_VERSION", "2025-09-15-EXEC-GATELOCK-ORDERS-01")

# --- Helpers ---------------------------------------------------------------------

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _gate_decision():
    """
    Decide if trading is allowed right now.
    Returns: {"gate_on": bool, "decision": "open"|"closed", "clock": {...}}
    """
    if not GATE_ON:
        return {"gate_on": False, "decision": "open", "clock": {"is_open": True}}

    try:
        clock = market.get_clock()  # {"is_open": bool, "next_open": "...", "next_close": "...", "timestamp": "..."}
        is_open = bool(clock.get("is_open"))
        return {"gate_on": True, "decision": "open" if is_open else "closed", "clock": clock}
    except Exception as e:
        # Fail-safe: closed if we cannot confirm open
        return {
            "gate_on": True,
            "decision": "closed",
            "clock": {"error": str(e), "is_open": False}
        }

def _gate_block_if_closed():
    decision = _gate_decision()
    if decision["gate_on"] and decision["decision"] == "closed":
        return jsonify(ok=True, skipped="market_closed", gate=decision), 200
    return None  # no block

def _parse_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "on")

def _json_log(**kw):
    # small JSON line logger to stdout
    try:
        kw.setdefault("ts", int(time.time()))
        print(json.dumps(kw, separators=(",", ":")))
    except Exception:
        pass

# Generic strategy runner
def run_strategy(strategy_cls, params=None, dry=False):
    """
    Instantiate strategy, call generate_plans() if available, else scan().
    Then (if not dry) send to market.execute_plans(...) to place orders.
    Returns a dict with "plans" and "results" (submitted orders, if any).
    """
    if strategy_cls is None:
        return {"ok": True, "plans": [], "results": [], "note": "strategy class not found"}

    params = params or {}
    strat = strategy_cls(market=market, **params) if "market" in strategy_cls.__init__.__code__.co_varnames else strategy_cls(**params)

    # get plans
    plans = []
    if hasattr(strat, "generate_plans") and callable(getattr(strat, "generate_plans")):
        plans = strat.generate_plans()
    elif hasattr(strat, "scan") and callable(getattr(strat, "scan")):
        plans = strat.scan()
    else:
        # Cannot get plans
        return {"ok": True, "plans": [], "results": [], "note": "strategy has no generate_plans/scan"}

    # If dry, do not place orders
    if dry:
        return {"ok": True, "plans": plans, "results": []}

    # Live execution through Market executor
    if not plans:
        return {"ok": True, "plans": [], "results": []}

    try:
        results = market.execute_plans(plans)
        return {"ok": True, "plans": plans, "results": results}
    except Exception as e:
        return {"ok": False, "plans": plans, "error": str(e), "results": []}

# --- Root / Docs -----------------------------------------------------------------

@app.get("/")
def root():
    return Response(
        "Not Found. Try /dashboard, /health/versions, /diag/gate, /scan/s1|s2 (GET), /scan/s3|s4 (POST).",
        status=404,
        mimetype="text/plain",
    )

# --- Health / Diagnostics ---------------------------------------------------------

@app.get("/health/versions")
def health_versions():
    return jsonify({
        "S3_OPENING_RANGE": S3_VERSION,
        "S4_RSI_PB": S4_VERSION,
        "S1": S1_VERSION,
        "S2": S2_VERSION,
        "app": APP_VERSION
    })

@app.get("/diag/gate")
def diag_gate():
    return jsonify(_gate_decision())

@app.get("/diag/clock")
def diag_clock():
    try:
        return jsonify(market.get_clock())
    except Exception as e:
        return jsonify(error=str(e)), 200

@app.get("/diag/alpaca")
def diag_alpaca():
    base_info = market.get_alpaca_bases()  # {"trading_base": "...", "data_base": "...", "feed": "..."}
    base_info["market_gate"] = "on" if GATE_ON else "off"
    return jsonify(base_info)

# --- Orders & Positions -----------------------------------------------------------

@app.get("/orders/open")
def orders_open():
    try:
        return jsonify(market.get_open_orders())
    except Exception as e:
        return jsonify(error=str(e)), 200

@app.get("/orders/recent")
def orders_recent():
    try:
        return jsonify(market.get_orders_recent())
    except Exception as e:
        return jsonify(error=str(e)), 200

@app.get("/positions")
def positions_get():
    try:
        return jsonify(market.get_positions())
    except Exception as e:
        return jsonify(error=str(e)), 200

# Submit an order directly (for testing exec path)
def _submit_order_from_request():
    """
    Accepts query string OR JSON body.
    Required: symbol, qty
    Optional: side=buy|sell, type=market|limit|stop|stop_limit, time_in_force=day|gtc,
              limit, stop
    """
    payload = {}
    if request.is_json:
        try:
            payload = request.get_json(force=True) or {}
        except Exception:
            payload = {}

    symbol = request.args.get("symbol") or payload.get("symbol")
    qty    = request.args.get("qty")    or payload.get("qty")
    side   = (request.args.get("side")  or payload.get("side") or "buy").lower()
    typ    = (request.args.get("type")  or payload.get("type") or "market").lower()
    tif    = (request.args.get("time_in_force") or payload.get("time_in_force") or "day").lower()

    limit_price = request.args.get("limit") or payload.get("limit")
    stop_price  = request.args.get("stop")  or payload.get("stop")

    if not symbol or not qty:
        return jsonify(ok=False, error="symbol and qty are required"), 400

    # Enforce gate here too
    block = _gate_block_if_closed()
    if block:
        return block

    try:
        qty_i = int(qty)
        limit_f = float(limit_price) if limit_price is not None else None
        stop_f  = float(stop_price)  if stop_price  is not None else None

        order = market.submit_order(
            symbol=symbol.upper(),
            qty=qty_i,
            side=side,
            type=typ,
            time_in_force=tif,
            limit_price=limit_f,
            stop_price=stop_f,
        )
        return jsonify(ok=True, order=order)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 200

@app.post("/orders/submit")
def orders_submit():
    return _submit_order_from_request()

@app.post("/test/order")
def test_order_alias():
    return _submit_order_from_request()

# --- Scans (S1–S4) ---------------------------------------------------------------

@app.get("/scan/s1")
def scan_s1():
    # Gate first
    block = _gate_block_if_closed()
    if block:
        return block

    # Params (legacy S1 knobs)
    tf   = request.args.get("tf", type=int)       # timeframe minutes
    mode = request.args.get("mode", default="either")
    band = request.args.get("band", type=float)
    vol  = _parse_bool(request.args.get("vol"), default=False)
    vmul = request.args.get("vmult", type=float)
    slope = request.args.get("slope", type=float)

    dry = _parse_bool(request.args.get("dry"), default=False)  # default live

    params = {}
    if tf is not None:     params["tf"] = tf
    if mode is not None:   params["mode"] = mode
    if band is not None:   params["band"] = band
    if vol is not None:    params["vol"] = vol
    if vmul is not None:   params["vmult"] = vmul
    if slope is not None:  params["slope"] = slope

    result = run_strategy(S1_CLS, params=params, dry=dry)
    # legacy shape for quick reading
    triggers = len(result.get("plans") or [])
    submitted = len(result.get("results") or [])
    _json_log(event="scan_done", system="S1", triggers=triggers)
    _json_log(event="orders_submitted", system="S1", submitted=submitted)
    return jsonify({
        "ok": result.get("ok", True),
        "dry_run": dry,
        "system": "S1",
        "results": result.get("results") or [],
        "triggers": triggers,
        "submitted": submitted
    })

@app.get("/scan/s2")
def scan_s2():
    # Gate first
    block = _gate_block_if_closed()
    if block:
        return block

    # Params passthrough (light)
    dry = _parse_bool(request.args.get("dry"), default=False)
    params = {}
    for key in ("symbols", "tf", "mode"):
        if key in request.args:
            params[key] = request.args.get(key)

    # Convert comma symbols to list if needed
    if "symbols" in params and isinstance(params["symbols"], str):
        params["symbols"] = [s.strip().upper() for s in params["symbols"].split(",") if s.strip()]

    result = run_strategy(S2_CLS, params=params, dry=dry)
    triggers = len(result.get("plans") or [])
    submitted = len(result.get("results") or [])
    _json_log(event="scan_done", system="S2", triggers=triggers)
    _json_log(event="orders_submitted", system="S2", submitted=submitted)
    return jsonify({
        "ok": result.get("ok", True),
        "dry_run": dry,
        "system": "S2",
        "results": result.get("results") or [],
        "triggers": triggers,
        "submitted": submitted
    })

@app.post("/scan/s3")
def scan_s3():
    # Gate first
    block = _gate_block_if_closed()
    if block:
        return block

    dry = _parse_bool(request.args.get("dry"), default=False)
    # Symbols & config can come from env defaults in the strategy or be overridden here
    params = {}
    if "symbols" in request.args:
        params["symbols"] = [s.strip().upper() for s in request.args.get("symbols", "").split(",") if s.strip()]

    result = run_strategy(S3_CLS, params=params, dry=dry)
    return jsonify({
        "ok": result.get("ok", True),
        "dry": dry,
        "system": "OPENING_RANGE",
        "version": S3_VERSION,
        "symbols": params.get("symbols") or getattr(S3_CLS, "DEFAULT_SYMBOLS", ["SPY"]),
        "results": result.get("results") or [],
        "plans": result.get("plans") or []
    })

@app.post("/scan/s4")
def scan_s4():
    # Gate first
    block = _gate_block_if_closed()
    if block:
        return block

    dry = _parse_bool(request.args.get("dry"), default=False)
    params = {}
    if "symbols" in request.args:
        params["symbols"] = [s.strip().upper() for s in request.args.get("symbols", "").split(",") if s.strip()]

    result = run_strategy(S4_CLS, params=params, dry=dry)
    return jsonify({
        "ok": result.get("ok", True),
        "dry": dry,
        "system": "RSI_PB",
        "version": S4_VERSION,
        "symbols": params.get("symbols") or getattr(S4_CLS, "DEFAULT_SYMBOLS", ["SPY", "QQQ"]),
        "results": result.get("results") or [],
        "plans": result.get("plans") or []
    })

# --- Performance (light stubs so dashboard renders) --------------------------------

@app.get("/performance")
def performance_summary():
    # Minimal fast summary stub; you can wire to your real P&L tracker
    days = int(request.args.get("days", 7))
    include_trades = _parse_bool(request.args.get("include_trades"), default=False)
    data = {
        "from": days,
        "equity": market.get_equity_fast(),    # should exist in services/market.py (or return None)
        "pnl_total": market.get_pnl_fast(days) # same note as above
    }
    if include_trades:
        try:
            data["trades"] = market.get_orders_recent()
        except Exception:
            data["trades"] = []
    return jsonify(data)

@app.get("/performance/daily")
def performance_daily():
    days = int(request.args.get("days", 7))
    return jsonify(market.get_daily_pnl_fast(days))

# --- Dashboard -------------------------------------------------------------------

_DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Equities Scanner — Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root { --bg:#0f172a; --fg:#e5e7eb; --sub:#94a3b8; --card:#111827; --good:#10b981; --warn:#f59e0b; --bad:#ef4444; }
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial}
    header{padding:24px 20px;border-bottom:1px solid #1f2937}
    h1{margin:0;font-size:22px}
    main{padding:20px;display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
    .card{background:var(--card);border:1px solid #1f2937;border-radius:16px;padding:16px;box-shadow:0 1px 0 #0006}
    .row{display:flex;gap:8px;flex-wrap:wrap;margin:6px 0}
    .pill{padding:4px 8px;border-radius:999px;background:#0b1220;border:1px solid #1f2937;color:var(--sub)}
    code{background:#0b1220;border:1px solid #1f2937;padding:2px 6px;border-radius:8px}
    button, a.btn{cursor:pointer;border:1px solid #334155;background:#0b1220;color:var(--fg);padding:8px 10px;border-radius:10px}
    .ok{color:var(--good)} .warn{color:var(--warn)} .bad{color:var(--bad)}
    table{width:100%;border-collapse:collapse} th,td{padding:6px 8px;border-bottom:1px solid #1f2937}
    footer{padding:24px 20px;color:var(--sub)}
  </style>
</head>
<body>
<header>
  <h1>Equities Scanner — Dashboard <span class="pill">app {{app_version}}</span></h1>
</header>
<main>
  <section class="card">
    <h3>Market Gate</h3>
    <div id="gate">Loading…</div>
    <div class="row">
      <button onclick="hit('/diag/gate')">Refresh</button>
      <a class="btn" href="/diag/clock" target="_blank">Clock</a>
      <a class="btn" href="/diag/alpaca" target="_blank">Alpaca</a>
    </div>
  </section>

  <section class="card">
    <h3>Versions</h3>
    <div id="vers">Loading…</div>
    <div class="row">
      <button onclick="hit('/health/versions')">Refresh</button>
    </div>
  </section>

  <section class="card">
    <h3>Quick Scans</h3>
    <div class="row">
      <button onclick="hit('/scan/s1?tf=1&mode=either&band=0.6&vol=true&vmult=1.0&dry=1')">S1 dry 1m either</button>
      <button onclick="hit('/scan/s1?tf=1&mode=trend&slope=0.02&vol=true&vmult=1.0&dry=1')">S1 dry trend</button>
      <button onclick="post('/scan/s3?dry=1')">S3 dry</button>
      <button onclick="post('/scan/s4?dry=1')">S4 dry</button>
    </div>
    <pre id="scanout" style="white-space:pre-wrap;"></pre>
  </section>

  <section class="card">
    <h3>Orders & Positions</h3>
    <div class="row">
      <button onclick="hit('/orders/open')">Open Orders</button>
      <button onclick="hit('/orders/recent')">Recent Orders</button>
      <button onclick="hit('/positions')">Positions</button>
    </div>
    <pre id="ordpos" style="white-space:pre-wrap;"></pre>
  </section>
</main>

<footer>
  <small>All strategies default to live execution (no <code>dry</code> flag required). Market gate is enforced for all routes.</small>
</footer>

<script>
async function hit(path) {
  const el = path.startsWith('/diag') ? document.getElementById('gate')
           : path.startsWith('/health') ? document.getElementById('vers')
           : path.startsWith('/orders') || path.startsWith('/positions') ? document.getElementById('ordpos')
           : document.getElementById('scanout');
  el.textContent = 'Loading…';
  const r = await fetch(path);
  const t = await r.text();
  try { el.textContent = JSON.stringify(JSON.parse(t), null, 2); }
  catch { el.textContent = t; }
}
async function post(path) {
  const el = document.getElementById('scanout');
  el.textContent = 'Loading…';
  const r = await fetch(path, {method:'POST'});
  const t = await r.text();
  try { el.textContent = JSON.stringify(JSON.parse(t), null, 2); }
  catch { el.textContent = t; }
}
hit('/diag/gate'); hit('/health/versions');
</script>
</body>
</html>
"""

@app.get("/dashboard")
def dashboard():
    return render_template_string(_DASHBOARD_HTML, app_version=APP_VERSION)

# --- Gunicorn entry ---------------------------------------------------------------

if __name__ == "__main__":
    # Local run: FLASK_ENV=development flask run -p 8000
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
