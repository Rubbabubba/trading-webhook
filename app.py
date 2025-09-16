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

from flask import Response

@app.get("/dashboard")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")

DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Equities Trading Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  :root{
    --bg:#0f1221; --panel:#141836; --muted:#9aa3b2; --text:#e8ecf2; --good:#0ecb81; --bad:#ff5b5b; --accent:#7aa2ff;
    --chip:#1b2148; --chip-br:#283063;
  }
  *{box-sizing:border-box}
  html,body{margin:0;padding:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,Segoe UI,Roboto,Arial,sans-serif}
  a{color:var(--accent);text-decoration:none}
  .wrap{max-width:1200px;margin:32px auto;padding:0 16px}
  header{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px}
  h1{font-size:22px;margin:0}
  .sub{font-size:12px;color:var(--muted)}
  .grid{display:grid;gap:16px}
  @media (min-width:900px){ .grid.cols-3{grid-template-columns:2fr 1fr 1fr} .grid.cols-2{grid-template-columns:2fr 1fr} }
  .card{background:var(--panel);border:1px solid #1f2552;border-radius:14px;padding:16px}
  .card h2{font-size:14px;margin:0 0 12px 0;color:#c7cff7;letter-spacing:.2px}
  .row{display:flex;gap:10px;flex-wrap:wrap}
  .stat{background:var(--chip);border:1px solid var(--chip-br);border-radius:12px;padding:10px 12px;display:inline-flex;flex-direction:column;gap:4px;min-width:120px}
  .stat .k{font-size:11px;color:var(--muted)}
  .stat .v{font-weight:700}
  .btn{background:#1b4bff22;border:1px solid #365cff;border-radius:10px;color:#cdd6ff;padding:8px 12px;font-weight:600;cursor:pointer}
  .btn:disabled{opacity:.5;cursor:not-allowed}
  .btn.alt{background:#1a2748;border-color:#2a3b7d}
  .btn.row{display:inline-flex;align-items:center;gap:8px}
  .btns{display:flex;flex-wrap:wrap;gap:8px}
  table{width:100%;border-collapse:collapse}
  th,td{padding:8px;border-bottom:1px solid #1f2552;font-size:12px}
  th{text-align:left;color:#b7c0e6;font-weight:600}
  .pill{font-size:11px;padding:3px 8px;border-radius:999px;border:1px solid var(--chip-br);background:var(--chip);color:#cfd6ff;display:inline-block}
  .ok{color:var(--good)} .bad{color:var(--bad)}
  .muted{color:var(--muted)}
  .note{font-size:12px;color:var(--muted);margin-top:6px}
  .log{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;background:#0d1124;border:1px solid #1e2450;border-radius:10px;padding:10px;height:150px;overflow:auto;font-size:12px;white-space:pre-wrap}
  .tag{background:#1e2655;border:1px solid #2f3a7a;border-radius:8px;padding:4px 8px;color:#b7c0e6;font-size:11px}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>Equities Trading Dashboard</h1>
      <div class="sub">Live scans (S1–S4), orders, positions, and performance</div>
    </div>
    <div class="row">
      <span id="appVersion" class="tag">app: —</span>
      <span id="s3v" class="tag">S3: —</span>
      <span id="s4v" class="tag">S4: —</span>
    </div>
  </header>

  <div class="grid cols-3">
    <section class="card">
      <h2>Status</h2>
      <div class="row" style="margin-bottom:10px">
        <div class="stat"><span class="k">Market Gate</span><span class="v" id="gateDecision">—</span></div>
        <div class="stat"><span class="k">Clock</span><span class="v"><span id="isOpen">—</span> <span class="muted">(now)</span></span></div>
        <div class="stat"><span class="k">Next Open</span><span class="v" id="nextOpen">—</span></div>
        <div class="stat"><span class="k">Next Close</span><span class="v" id="nextClose">—</span></div>
      </div>
      <div class="row">
        <div class="stat"><span class="k">Feed</span><span class="v" id="feed">—</span></div>
        <div class="stat"><span class="k">Trading Host</span><span class="v" id="tbase">—</span></div>
        <div class="stat"><span class="k">Data Host</span><span class="v" id="dbase">—</span></div>
      </div>
      <div class="note">Gate must be <strong>open</strong> for executions to submit.</div>
    </section>

    <section class="card">
      <h2>Controls</h2>
      <div class="btns" style="margin-bottom:10px">
        <button class="btn" onclick="scan('s1','tf=1&mode=trend&slope=0.02&vol=true&vmult=1.0')">Run S1 Trend (1m)</button>
        <button class="btn" onclick="scan('s1','tf=5&mode=reversion&band=0.8')">Run S1 Reversion (5m)</button>
        <button class="btn" onclick="scan('s1','tf=1&mode=either&band=0.6&vol=true&vmult=1.0')">Run S1 Either (1m)</button>
        <button class="btn alt" onclick="scan('s2','')">Run S2</button>
        <button class="btn alt" onclick="post('/scan/s3?dry=0')">Run S3</button>
        <button class="btn alt" onclick="post('/scan/s4?dry=0')">Run S4</button>
      </div>
      <div class="btns">
        <button class="btn" onclick="refreshAll()">Refresh All</button>
      </div>
      <div class="note">Scans call your live endpoints with <code>dry=0</code>. Execution still respects the gate.</div>
    </section>

    <section class="card">
      <h2>Equity (30d)</h2>
      <canvas id="pnlChart" height="160"></canvas>
      <div class="note">Pulled from <code>/performance/daily?days=30</code>.</div>
    </section>
  </div>

  <div class="grid cols-2" style="margin-top:16px">
    <section class="card">
      <h2>Recent Orders</h2>
      <table id="ordersTbl">
        <thead><tr>
          <th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Type</th><th>Status</th><th>Filled</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </section>

    <section class="card">
      <h2>Positions</h2>
      <table id="posTbl">
        <thead><tr>
          <th>Symbol</th><th>Qty</th><th>Side</th><th>Avg</th><th>Market</th><th>Unrealized</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </section>
  </div>

  <section class="card" style="margin-top:16px">
    <h2>Logs</h2>
    <div id="log" class="log"></div>
  </section>
</div>

<script>
const $ = (sel)=>document.querySelector(sel);
const log = (msg)=>{ const el=$("#log"); const ts=new Date().toLocaleTimeString(); el.textContent += "["+ts+"] "+msg+"\\n"; el.scrollTop=el.scrollHeight; };

async function jget(url, opts={}){
  try{
    const r = await fetch(url, {headers:{'Accept':'application/json'}, ...opts});
    if(!r.ok) throw new Error(r.status+" "+r.statusText);
    return await r.json();
  }catch(e){ log("GET "+url+" -> "+e.message); throw e; }
}
async function post(url){
  try{
    const r = await fetch(url, {method:"POST", headers:{'Accept':'application/json'}});
    const t = await r.text();
    log("POST "+url+" -> "+t.slice(0,200));
    return t;
  }catch(e){ log("POST "+url+" -> "+e.message); throw e; }
}
async function scan(system, query){
  const url = `/scan/${system}?dry=0${query?("&"+query):""}`;
  await post(url);
  // lightweight refresh after scan
  setTimeout(()=>{ loadOrders(); loadPositions(); }, 1200);
}

function fmtUSD(x){ if(x===null||x===undefined||isNaN(x)) return "—"; return new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:2}).format(+x); }
function fmtQty(x){ return (x===null||x===undefined)?"—":(+x).toLocaleString(); }
function td(v, cls=""){ return `<td class="${cls}">${v}</td>`; }

let pnlChart=null;
function drawLineChart(labels, data){
  const ctx = document.getElementById('pnlChart').getContext('2d');
  if(pnlChart){ pnlChart.destroy(); }
  pnlChart = new Chart(ctx, {
    type:'line',
    data:{ labels, datasets:[{ label:'Equity', data, tension:0.25 }] },
    options:{ plugins:{legend:{display:false}}, scales:{x:{ticks:{maxRotation:0,minRotation:0}}} }
  });
}

async function loadVersions(){
  const v = await jget('/health/versions');
  $('#appVersion').textContent = 'app: '+(v.app||'—');
  $('#s3v').textContent = 'S3: '+(v.S3_OPENING_RANGE||'—');
  $('#s4v').textContent = 'S4: '+(v.S4_RSI_PB||'—');
}
async function loadAlpaca(){
  const d = await jget('/diag/alpaca');
  $('#feed').textContent = d.feed || '—';
  $('#tbase').textContent = (d.trading_base||'—').replace('https://','');
  $('#dbase').textContent = (d.data_base||'—').replace('https://','');
}
async function loadGate(){
  try{
    const g = await jget('/diag/gate');
    $('#gateDecision').textContent = (g.decision||'—') + (g.gate_on?' (on)':' (off)');
    const c = g.clock||{};
    $('#isOpen').textContent = c.is_open ? 'OPEN' : 'CLOSED';
    $('#nextOpen').textContent = c.next_open || '—';
    $('#nextClose').textContent = c.next_close || '—';
  }catch(e){
    $('#gateDecision').textContent = 'error';
  }
}
async function loadOrders(){
  const rows = await jget('/orders/recent');
  const tb = $('#ordersTbl tbody');
  tb.innerHTML = rows.slice(0,50).map(o=>{
    const t = (o.submitted_at||o.created_at||'').replace('Z','').replace('T',' ');
    const filled = (o.filled_qty||'0') + ' / ' + (o.qty||o.order_class_qty||'');
    const side = (o.side||'').toUpperCase();
    const cls = side==='BUY'?'ok':'bad';
    return `<tr>
      ${td(t)}${td(o.symbol||'—')}
      ${td(side, cls)}${td(o.qty||o.order_class_qty||'—')}
      ${td((o.type||'').toUpperCase())}${td((o.status||'').toUpperCase())}
      ${td(filled)}
    </tr>`;
  }).join('');
}
async function loadPositions(){
  const rows = await jget('/positions');
  const tb = $('#posTbl tbody');
  tb.innerHTML = rows.map(p=>{
    const side = (p.side||'').toUpperCase();
    const cls = side==='LONG'?'ok':'bad';
    const unreal = +(p.unrealized_pl||p.unrealized_intraday_pl||0);
    const ucls = unreal>=0?'ok':'bad';
    return `<tr>
      ${td(p.symbol||'—')}
      ${td(p.qty||'—')}
      ${td(side, cls)}
      ${td(fmtUSD(p.avg_entry_price))}
      ${td(fmtUSD(p.current_price))}
      ${td(fmtUSD(unreal), ucls)}
    </tr>`;
  }).join('');
}
async function loadPnl(){
  const d = await jget('/performance/daily?days=30&fast=1');
  const labels = (d||[]).map(x=>x.date||x.day||'').slice(-30);
  const vals = (d||[]).map(x=>+x.equity||0).slice(-30);
  drawLineChart(labels, vals);
}

async function refreshAll(){
  log('Refreshing dashboard…');
  await Promise.allSettled([loadVersions(), loadAlpaca(), loadGate(), loadOrders(), loadPositions(), loadPnl()]);
  log('Done.');
}
window.addEventListener('load', ()=>{
  refreshAll();
  setInterval(()=>{ loadGate(); loadOrders(); loadPositions(); }, 60000);
});
</script>
</body>
</html>"""
.strip()
    return Response(html, status=200, mimetype="text/html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
