# app.py — Equities Webhook/Scanner
# Version: 2025-09-12-EXEC-GATELOCK
# - S1–S4 wired with default live execution (no dry flag needed)
# - Market gate blocks execution when market is closed (no force override)
# - Diagnostics, performance, and a full dashboard HTML

import os, sys, json, time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from flask import Flask, request, jsonify, make_response

# ===== Core config/utilities =====
from core.config import (
    APP_VERSION, CURRENT_STRATEGIES, ALPACA_BASE,
    MAX_DAYS_PERF, HEADERS, SESSION
)
from core.utils import _iso, _scrub_nans, only_current_strats
from core.performance import (
    fetch_trade_activities, fetch_client_order_ids,
    compute_performance, bucket_daily, _load_symbol_system_map
)
from core.alpaca import is_market_open_now
from core.signals import process_signal, load_strategy_config

# ===== Legacy scanners =====
from strategies.s1 import scan_s1_symbols
from strategies.s2 import scan_s2_symbols

# Optional versions for S1/S2 (if present in your modules)
try:
    from strategies.s1 import __version__ as S1_VERSION
except Exception:
    S1_VERSION = "n/a"
try:
    from strategies.s2 import __version__ as S2_VERSION
except Exception:
    S2_VERSION = "n/a"

# ===== Strategy base / plan =====
from strategies.base import OrderPlan

# ===== New scanners (S3/S4) =====
from strategies.s3 import S3OpeningRange, __version__ as S3_VERSION
from strategies.s4 import S4RSIPullback, __version__ as S4_VERSION

# ===== Execution + market abstraction =====
try:
    from services.market import Market
    from services.alpaca_exec import execute_orderplan_batch
    _SERVICES_AVAILABLE = True
except Exception:
    Market = None
    execute_orderplan_batch = None
    _SERVICES_AVAILABLE = False

# Diagnostics import (aliased)
try:
    from services.market import (
        ALPACA_TRADING_BASE as M_TRADING_BASE,
        ALPACA_DATA_BASE as M_DATA_BASE,
        ALPACA_FEED as M_FEED,
        HEADERS as M_HEADERS,
        SESSION as M_SESSION,
    )
    _DIAG_OK = True
except Exception:
    _DIAG_OK = False
    M_TRADING_BASE = M_DATA_BASE = M_FEED = None
    M_HEADERS = M_SESSION = None

app = Flask(__name__)

# ===== Helpers =====
def _to_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")

def _respond(payload, status=200, version_header: str | None = None):
    resp = make_response(jsonify(payload), status)
    if version_header:
        resp.headers["X-Strategy-Version"] = version_header
    return resp

def _market_gate_is_on() -> bool:
    # You can disable the gate via env for testing:
    # MARKET_GATE=off  (but there is no querystring bypass)
    return os.getenv("MARKET_GATE", "on").strip().lower() not in ("off", "0", "false")

def _gate_or_skip():
    """Return a Flask response if market is closed and gate is on, else None."""
    if not _market_gate_is_on():
        return None
    if not is_market_open_now():
        # Explicitly mark skip with a header for log-based debugging
        resp = jsonify({"ok": True, "skipped": "market_closed"})
        out = make_response(resp, 200)
        out.headers["X-Gate"] = "market_closed"
        return out
    return None

def _default(v, alt):  # env helper
    return os.getenv(v, alt)

def _log(event, **fields):
    fields["event"] = event
    fields["ts"] = int(time.time())
    print(json.dumps(fields), file=sys.stdout, flush=True)

# Build OrderPlans from S1/S2 "triggers"
def _plans_from_triggers(triggers, market, *, system_name: str,
                         default_side: str, tp_pct: float, sl_pct: float,
                         risk_per_trade: float) -> List[OrderPlan]:
    bp = max(float(market.buying_power()), 0.0)
    risk_dollars = bp * risk_per_trade
    plans: List[OrderPlan] = []
    for t in (triggers or []):
        sym = (t.get("symbol") or t.get("sym") or "").strip().upper()
        if not sym:
            continue
        side = (t.get("side") or default_side).lower()
        try:
            qty = int(t.get("qty") or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            qty = market.position_sizer(sym, risk_dollars, sl_pct)
        if qty <= 0:
            continue
        plans.append(OrderPlan(
            symbol=sym, side=side, qty=qty, tp_pct=tp_pct, sl_pct=sl_pct,
            meta={"system": system_name, "trigger": t}
        ))
    return plans

# Unified runner for S3/S4 (default executes live)
def _run_strategy(strategy_cls, system_name: str, system_version: str, env_prefix: str):
    # Gate strictly enforced (no force bypass)
    gate = _gate_or_skip()
    if gate is not None:
        return gate

    dry = _to_bool(request.args.get("dry"), default=False)

    if not _SERVICES_AVAILABLE:
        if dry:
            return _respond({
                "system": system_name, "version": system_version, "dry": True,
                "symbols": os.getenv(f"{env_prefix}_SYMBOLS") or os.getenv("EQUITY_SYMBOLS", "SPY"),
                "plans": [], "note": "services missing — dry run only"
            }, version_header=f"{env_prefix}/{system_version}")
        return _respond({
            "error": "services_unavailable",
            "message": "services.market/alpaca_exec not found; live/paper disabled."
        }, status=503)

    try:
        symbols = (os.getenv(f"{env_prefix}_SYMBOLS") or os.getenv("EQUITY_SYMBOLS", "SPY")).split(",")
        symbols = [s.strip().upper() for s in symbols if s.strip()]
        mkt = Market()
        strat = strategy_cls(mkt)
        plans = strat.scan(mkt.now(), mkt, symbols)
        _log("plans_built", system=system_name, plans=len(plans), symbols=symbols)

        if dry:
            return _respond({
                "system": system_name, "version": system_version, "dry": True,
                "symbols": symbols, "plans": [p.__dict__ for p in plans]
            }, version_header=f"{env_prefix}/{system_version}")

        results = execute_orderplan_batch(plans, system=system_name)
        _log("orders_submitted", system=system_name,
             submitted=len([r for r in results if r.get("status") == "submitted"]))
        return _respond({
            "system": system_name, "version": system_version, "dry": False,
            "symbols": symbols, "results": results
        }, version_header=f"{env_prefix}/{system_version}")
    except Exception as e:
        _log("strategy_error", system=system_name, error=str(e))
        return _respond({"ok": False, "error": str(e)})

# ===== Health =====
@app.get("/health")
def health():
    return jsonify({
        "ok": True, "service": "equities-webhook",
        "version": APP_VERSION, "time": int(time.time())
    }), 200

@app.get("/health/versions")
def health_versions():
    return jsonify({
        "app": APP_VERSION,
        "S1": S1_VERSION, "S2": S2_VERSION,
        "S3_OPENING_RANGE": S3_VERSION,
        "S4_RSI_PB": S4_VERSION
    }), 200

# ===== Webhook =====
@app.post("/webhook")
def webhook():
    data = request.get_json(force=True, silent=True) or {}
    status, out = process_signal(data)
    return jsonify(out), status

# ===== Performance / Reporting =====
@app.get("/performance")
def performance():
    try:
        days_s = request.args.get("days", "7")
        fast   = request.args.get("fast", "0") == "1"
        include_trades = request.args.get("include_trades", "0") == "1"
        try:
            days = max(1, int(days_s))
        except Exception:
            days = 7
        days = min(days, MAX_DAYS_PERF)

        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)
        start_iso = _iso(start_dt); end_iso = _iso(end_dt)

        acts = fetch_trade_activities(start_iso, end_iso)
        order_ids = [a.get("order_id") for a in acts if a.get("order_id")]
        cid_map = {} if fast else fetch_client_order_ids(order_ids)
        sym_map = _load_symbol_system_map()
        perf = compute_performance(acts, cid_map, sym_map)

        out = {
            "ok": True, "version": APP_VERSION, "current_strategies": CURRENT_STRATEGIES,
            "days": days, "total_realized_pnl": perf["total_pnl"],
            "by_strategy": only_current_strats(perf["by_strategy"]),
            "equity": perf["equity"],
            "note": "Realized P&L from Alpaca fills (FIFO). Use fast=1 to skip order lookups.",
        }
        if include_trades:
            out["trades"] = perf["trades"]
        return jsonify(_scrub_nans(out)), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

@app.get("/performance/daily")
def performance_daily():
    try:
        days_s = request.args.get("days", "30")
        fast   = request.args.get("fast", "0") == "1"
        try:
            days = max(1, int(days_s))
        except Exception:
            days = 30
        days = min(days, MAX_DAYS_PERF)

        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)
        start_iso = _iso(start_dt); end_iso = _iso(end_dt)

        acts = fetch_trade_activities(start_iso, end_iso)
        order_ids = [a.get("order_id") for a in acts if a.get("order_id")]
        cid_map = {} if fast else fetch_client_order_ids(order_ids)
        sym_map = _load_symbol_system_map()
        perf = compute_performance(acts, cid_map, sym_map)
        daily = bucket_daily(perf["trades"])
        for row in daily:
            bs = row.get("by_strategy", {}) or {}
            row["by_strategy"] = {k: bs.get(k, 0.0) for k in CURRENT_STRATEGIES}

        return jsonify(_scrub_nans({
            "ok": True, "version": APP_VERSION,
            "current_strategies": CURRENT_STRATEGIES, "days": days, "daily": daily
        })), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

# ===== Alpaca proxies =====
@app.get("/positions")
def positions():
    try:
        r = SESSION.get(f"{ALPACA_BASE}/v2/positions", headers=HEADERS, timeout=6)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/orders/open")
def orders_open():
    try:
        r = SESSION.get(
            f"{ALPACA_BASE}/v2/orders",
            headers=HEADERS,
            params={"status": "open", "limit": 200, "nested": "false"},
            timeout=6,
        )
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

@app.get("/orders/recent")
def orders_recent():
    try:
        r = SESSION.get(
            f"{ALPACA_BASE}/v2/orders",
            headers=HEADERS,
            params={"status": "all", "limit": 50, "nested": "false"},
            timeout=6,
        )
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

# ===== Config snapshot =====
@app.get("/config")
def config():
    def cfg_for(system):
        c = load_strategy_config(system)
        return {
            "qty": c.qty, "order_type": c.order_type, "tif": c.tif,
            "tp_pct": c.tp_pct, "sl_pct": c.sl_pct
        }
    out = {
        "version": APP_VERSION,
        "alpaca_base": ALPACA_BASE,
        "current_strategies": CURRENT_STRATEGIES,
        "systems": {
            # Examples — adapt to your own systems
            "SPY_VWAP_EMA20": cfg_for("SPY_VWAP_EMA20"),
            "SMA10D_MACD": cfg_for("SMA10D_MACD"),
        }
    }
    return jsonify(out), 200

# ===== Dashboard (full HTML) =====
@app.get("/dashboard")
def dashboard():
    try:
        html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Equities Webhook Dashboard — {APP_VERSION}</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
:root {{
  --bg:#0f172a; --panel:#111827; --muted:#94a3b8; --fg:#e5e7eb; --ok:#22c55e; --warn:#f59e0b; --err:#ef4444; --link:#60a5fa;
  --accent:#3b82f6;
}}
* {{ box-sizing:border-box }}
html,body {{ margin:0; padding:0; background:var(--bg); color:var(--fg); font:14px/1.5 system-ui,Segoe UI,Roboto,Arial }}
a {{ color:var(--link); text-decoration:none }}
a:hover {{ text-decoration:underline }}
.container {{ max-width:1200px; margin:0 auto; padding:20px }}
h1 {{ font-size:22px; margin:0 0 10px }}
h2 {{ font-size:16px; color:var(--muted); margin:0 0 12px }}
.grid {{ display:grid; gap:16px; grid-template-columns: repeat(12, 1fr); }}
.card {{ background:var(--panel); border:1px solid #1f2937; border-radius:12px; padding:16px; }}
.col-4 {{ grid-column: span 4; }}
.col-6 {{ grid-column: span 6; }}
.col-8 {{ grid-column: span 8; }}
.col-12 {{ grid-column: span 12; }}
.kv {{ display:grid; grid-template-columns: 160px 1fr; gap:6px 12px; align-items:center }}
.kv div:nth-child(odd) {{ color:var(--muted) }}
code, pre {{ background:#0b1220; border:1px solid #1f2937; border-radius:8px; padding:10px; white-space:pre-wrap; word-break:break-word }}
button {{
  background:var(--accent); color:#fff; border:0; padding:10px 12px; border-radius:10px; cursor:pointer; font-weight:600;
}}
button.secondary {{ background:#374151 }}
.btnrow {{ display:flex; gap:8px; flex-wrap:wrap }}
.badge {{ display:inline-block; padding:2px 8px; border-radius:999px; border:1px solid #374151; color:#cbd5e1 }}
.ok {{ color:var(--ok) }} .warn {{ color:var(--warn) }} .err {{ color:var(--err) }}
.tbl {{ width:100%; border-collapse:collapse; font-size:13px }}
.tbl th, .tbl td {{ padding:8px 10px; border-bottom:1px solid #1f2937; text-align:left }}
.tbl th {{ color:#a5b4fc; font-weight:600; background:#0b1220 }}
.small {{ font-size:12px; color:var(--muted) }}
footer {{ margin-top:20px; color:var(--muted) }}
</style>
</head>
<body>
<div class="container">
  <h1>Equities Webhook Dashboard <span class="badge">{APP_VERSION}</span></h1>
  <div class="grid">
    <div class="card col-4">
      <h2>Health & Versions</h2>
      <div id="versions" class="kv small">Loading…</div>
    </div>
    <div class="card col-4">
      <h2>Alpaca</h2>
      <div id="alpaca" class="kv small">Loading…</div>
    </div>
    <div class="card col-4">
      <h2>Market Clock</h2>
      <div id="clock" class="kv small">Loading…</div>
    </div>

    <div class="card col-12">
      <h2>Run Scanners</h2>
      <div class="btnrow">
        <button onclick="run('GET','/scan/s1')">Run S1 (GET)</button>
        <button onclick="run('GET','/scan/s2')">Run S2 (GET)</button>
        <button onclick="run('POST','/scan/s3')">Run S3 (POST)</button>
        <button onclick="run('POST','/scan/s4')">Run S4 (POST)</button>
        <button class="secondary" onclick="refreshPerf()">Refresh Performance</button>
      </div>
      <div class="small" style="margin-top:8px">
        Note: Execution is blocked when market is closed (gate enforced). Use <code>?dry=1</code> to simulate.
      </div>
      <pre id="out" style="margin-top:12px; max-height:320px; overflow:auto">Waiting…</pre>
    </div>

    <div class="card col-6">
      <h2>Performance (last 30 days)</h2>
      <table class="tbl" id="perf">
        <thead><tr><th>Day</th><th>P&L</th><th>By Strategy (current)</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="card col-6">
      <h2>Open/Recent Orders</h2>
      <div class="btnrow">
        <button onclick="loadOpen()">Open Orders</button>
        <button class="secondary" onclick="loadRecent()">Recent Orders</button>
        <button class="secondary" onclick="loadPositions()">Positions</button>
      </div>
      <pre id="orders" style="margin-top:12px; max-height:320px; overflow:auto">Waiting…</pre>
    </div>
  </div>
  <footer>
    <div class="small">© {datetime.now().year} — Service version {APP_VERSION}</div>
  </footer>
</div>
<script>
const q = (s) => document.querySelector(s);
const fmt = (v) => (typeof v === 'object' ? JSON.stringify(v, null, 2) : String(v));
function setKV(el, obj) {{
  const target = document.getElementById(el);
  if (!obj) {{ target.textContent = "n/a"; return; }}
  target.innerHTML = Object.entries(obj).map(([k,v]) => `<div>${{k}}</div><div><code>${{fmt(v)}}</code></div>`).join("");
}}

async function fetchJSON(path, options={{}}) {{
  const r = await fetch(path, options);
  const txt = await r.text();
  try {{ return [r.status, JSON.parse(txt)]; }} catch {{ return [r.status, {{ raw: txt }}]; }}
}}

async function loadVersions() {{
  const [st, j] = await fetchJSON('/health/versions');
  setKV('versions', j);
}}
async function loadAlpaca() {{
  const [st, j] = await fetchJSON('/diag/alpaca');
  setKV('alpaca', j);
}}
async function loadClock() {{
  const [st, j] = await fetchJSON('/diag/clock');
  setKV('clock', j);
}}
async function loadOpen() {{
  const [st, j] = await fetchJSON('/orders/open');
  q('#orders').textContent = fmt(j);
}}
async function loadRecent() {{
  const [st, j] = await fetchJSON('/orders/recent');
  q('#orders').textContent = fmt(j);
}}
async function loadPositions() {{
  const [st, j] = await fetchJSON('/positions');
  q('#orders').textContent = fmt(j);
}}

async function refreshPerf() {{
  const [st1, daily] = await fetchJSON('/performance/daily?days=30&fast=1');
  const tb = q('#perf tbody'); tb.innerHTML = '';
  if (!daily || !daily.daily) return;
  for (const row of daily.daily) {{
    const tr = document.createElement('tr');
    const by = Object.entries(row.by_strategy||{{}}).map(([k,v])=>`${{k}}: ${{(v||0).toFixed(2)}}`).join(', ');
    tr.innerHTML = `<td>${{row.day}}</td><td>${{(row.pnl||0).toFixed(2)}}</td><td>${{by}}</td>`;
    tb.appendChild(tr);
  }}
}}

async function run(method, path) {{
  q('#out').textContent = "Running " + method + " " + path + " …";
  const opt = {{ method }};
  const [st, j] = await fetchJSON(path + (path.includes('?') ? '' : '?dry=0'), opt);
  q('#out').textContent = "(" + st + ")\\n" + fmt(j);
  loadOpen();
}}

loadVersions(); loadAlpaca(); loadClock(); refreshPerf();
</script>
</body></html>"""
        resp = make_response(html, 200)
        resp.headers["Content-Type"] = "text/html; charset=utf-8"
        return resp
    except Exception as e:
        return f"<pre>dashboard render error: {e}</pre>", 500

# ===== Diagnostics =====
@app.get("/diag/alpaca")
def diag_alpaca():
    if not _DIAG_OK:
        return jsonify({"ok": False, "error": "diagnostics unavailable"}), 500
    return jsonify({
        "trading_base": M_TRADING_BASE,
        "data_base": M_DATA_BASE,
        "feed": M_FEED,
        "market_gate": "on" if _market_gate_is_on() else "off",
    }), 200

@app.get("/diag/clock")
def diag_clock():
    if not _DIAG_OK:
        return jsonify({"ok": False, "error": "diagnostics unavailable"}), 500
    r = M_SESSION.get(f"{M_TRADING_BASE}/v2/clock", headers=M_HEADERS, timeout=6)
    return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type", "application/json")})

@app.get("/diag/routes")
def diag_routes():
    routes = sorted([f"{','.join(sorted(r.methods))} {r.rule}" for r in app.url_map.iter_rules()])
    return jsonify(routes), 200

# ===== S1 (exec by default; gate enforced) =====
@app.route("/scan/s1", methods=["GET"])
def scan_s1_route():
    try:
        gate = _gate_or_skip()
        if gate is not None:
            return gate

        # Defaults via env; override with query
        symbols = (_default("S1_SYMBOLS", "SPY")).split(",")
        tf      = request.args.get("tf", _default("S1_TF_DEFAULT", "1"))
        mode    = request.args.get("mode", _default("S1_MODE_DEFAULT", "either"))
        band    = float(request.args.get("band", _default("S1_BAND_DEFAULT", "0.6")))
        slope   = float(request.args.get("slope", _default("S1_SLOPE_MIN", "0.02")))
        rth     = _to_bool(request.args.get("rth"), default=_to_bool(_default("S1_USE_RTH_DEFAULT","true"), True))
        vol     = _to_bool(request.args.get("vol"), default=_to_bool(_default("S1_USE_VOL_DEFAULT","true"), True))
        vmult   = float(request.args.get("vmult", _default("S1_VOL_MULT_DEFAULT", "1.0")))
        dry     = _to_bool(request.args.get("dry"), default=False)  # default live

        scan = scan_s1_symbols(
            symbols, tf=tf, mode=mode, band=band, slope_min=slope,
            use_rth=rth, use_vol=vol, vol_mult=vmult, dry_run=True
        )
        trigs = scan.get("triggers", []) or []
        _log("scan_done", system="S1", triggers=len(trigs))

        if not _SERVICES_AVAILABLE:
            return jsonify({"ok": False, "error": "services_unavailable"}), 503

        mkt = Market()
        plans = _plans_from_triggers(
            trigs, mkt,
            system_name="S1",
            default_side=_default("S1_DEFAULT_SIDE", "buy"),
            tp_pct=float(_default("S1_TP_PCT", "0.006")),
            sl_pct=float(_default("S1_SL_PCT", "0.003")),
            risk_per_trade=float(_default("S1_RISK_PER_TRADE", "0.003")),
        )

        if dry:
            return jsonify({
                "ok": True, "system": "S1", "dry_run": True,
                "checked": scan.get("checked", []),
                "triggers": trigs,
                "plans": [p.__dict__ for p in plans]
            }), 200

        results = execute_orderplan_batch(plans, system="S1")
        _log("orders_submitted", system="S1",
             submitted=len([r for r in results if r.get("status") == "submitted"]))
        return jsonify({
            "ok": True, "system": "S1", "dry_run": False,
            "executed": len([r for r in results if r.get("status") == "submitted"]),
            "results": results
        }), 200

    except Exception as e:
        _log("route_error", system="S1", error=str(e))
        return jsonify({"ok": False, "error": str(e)}), 200

# ===== S2 (exec by default; gate enforced) =====
@app.route("/scan/s2", methods=["GET"])
def scan_s2_route():
    try:
        gate = _gate_or_skip()
        if gate is not None:
            return gate

        default_syms = _default("S2_WHITELIST", "TSLA,NVDA,COIN,SPY,QQQ")
        symbols = request.args.get("symbols", default_syms).split(",")
        tf      = request.args.get("tf", _default("S2_TF_DEFAULT", "30"))
        mode    = request.args.get("mode", _default("S2_MODE_DEFAULT", "either"))
        rth     = _to_bool(request.args.get("rth"), default=_to_bool(_default("S2_USE_RTH_DEFAULT","true"), True))
        vol     = _to_bool(request.args.get("vol"), default=_to_bool(_default("S2_USE_VOL_DEFAULT","true"), True))
        dry     = _to_bool(request.args.get("dry"), default=False)  # default live

        scan = scan_s2_symbols(symbols, tf=tf, mode=mode, use_rth=rth, use_vol=vol, dry_run=True)
        trigs = scan.get("triggers", []) or []
        _log("scan_done", system="S2", triggers=len(trigs))

        if not _SERVICES_AVAILABLE:
            return jsonify({"ok": False, "error": "services_unavailable"}), 503
        mkt = Market()
        plans = _plans_from_triggers(
            trigs, mkt,
            system_name="S2",
            default_side=_default("S2_DEFAULT_SIDE", "buy"),
            tp_pct=float(_default("S2_TP_PCT", "0.006")),
            sl_pct=float(_default("S2_SL_PCT", "0.003")),
            risk_per_trade=float(_default("S2_RISK_PER_TRADE", "0.003")),
        )

        if dry:
            return jsonify({
                "ok": True, "system": "S2", "dry_run": True,
                "checked": scan.get("checked", []),
                "triggers": trigs,
                "plans": [p.__dict__ for p in plans]
            }), 200

        results = execute_orderplan_batch(plans, system="S2")
        _log("orders_submitted", system="S2",
             submitted=len([r for r in results if r.get("status") == "submitted"]))
        return jsonify({
            "ok": True, "system": "S2", "dry_run": False,
            "executed": len([r for r in results if r.get("status") == "submitted"]),
            "results": results
        }), 200

    except Exception as e:
        _log("route_error", system="S2", error=str(e))
        return jsonify({"ok": False, "error": str(e)}), 200

# ===== S3/S4 (default live; gate enforced) =====
@app.route("/scan/s3", methods=["GET", "POST"])
def scan_s3_route():
    return _run_strategy(S3OpeningRange, "OPENING_RANGE", S3_VERSION, "S3")

@app.route("/scan/s4", methods=["GET", "POST"])
def scan_s4_route():
    return _run_strategy(S4RSIPullback, "RSI_PB", S4_VERSION, "S4")

# ===== Entrypoint =====
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
