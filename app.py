# app.py — Equities Webhook/Scanner
# Version: 2025-09-15-EXEC-GATELOCK+S4-CALLFIX
# - Fix: S3/S4 runner now passes (now, market, symbols) to .scan(...)
# - Gate: market gate still prevents live execution when RTH is closed (override with ?force=1)
# - Diagnostics, performance, proxies, and full dashboard HTML

import os, time, json
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, make_response

# ===== Core config/utilities from your codebase =====
from core.config import (
    APP_VERSION, CURRENT_STRATEGIES, ALPACA_BASE,
    S2_WHITELIST, S2_TF_DEFAULT, S2_MODE_DEFAULT,
    S2_USE_RTH_DEFAULT, S2_USE_VOL_DEFAULT,
    MAX_DAYS_PERF, HEADERS, SESSION
)
from core.utils import _iso, _canon_timeframe, _scrub_nans, only_current_strats
from core.performance import (
    fetch_trade_activities, fetch_client_order_ids,
    compute_performance, bucket_daily, _load_symbol_system_map
)
from core.alpaca import is_market_open_now

# Webhook processor (kept)
from core.signals import process_signal, load_strategy_config

# ===== Legacy scanners (kept) =====
from strategies.s1 import scan_s1_symbols
from strategies.s2 import scan_s2_symbols

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

# Diagnostics import (aliased; used by /diag/* routes)
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
    # Toggle via env: MARKET_GATE=off (or 0/false) to disable the RTH gate globally.
    return os.getenv("MARKET_GATE", "on").strip().lower() not in ("off", "0", "false")

def _env_list(key: str, default_csv: str) -> list[str]:
    csv = os.getenv(key) or os.getenv("EQUITY_SYMBOLS", default_csv)
    return [s.strip().upper() for s in csv.split(",") if s.strip()]

# Build OrderPlans from S1/S2 "triggers"
def _plans_from_triggers(triggers, market, *, system_name: str,
                         default_side: str, tp_pct: float, sl_pct: float,
                         risk_per_trade: float) -> list[OrderPlan]:
    bp = max(float(market.buying_power()), 0.0)
    risk_dollars = bp * risk_per_trade
    plans = []
    for t in (triggers or []):
        sym = (t.get("symbol") or t.get("sym") or "").strip().upper()
        if not sym:
            continue
        side = (t.get("side") or default_side).lower()  # "buy"/"sell"
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

# Unified runner for S3/S4 (default executes live; pass ?dry=1 for dry)
def _run_strategy(strategy_cls, system_name: str, system_version: str, env_prefix: str):
    # Gate: if on and not forced, skip live execution when market closed
    if _market_gate_is_on() and request.args.get("force", "0") != "1":
        if not is_market_open_now():
            return _respond({"ok": True, "skipped": "market_closed"})

    dry = _to_bool(request.args.get("dry"), default=False)

    if not _SERVICES_AVAILABLE:
        # Safe behavior if services are missing: allow dry only
        if dry:
            return _respond({
                "system": system_name, "version": system_version, "dry": True,
                "symbols": os.getenv(f"{env_prefix}_SYMBOLS") or os.getenv("EQUITY_SYMBOLS", "SPY"),
                "plans": [], "note": "services missing — dry run only"
            }, version_header=f"{env_prefix}/{system_version}")
        return _respond({
            "ok": False,
            "message": "services.market/alpaca_exec not found; live/paper disabled."
        }, status=503)

    try:
        symbols = _env_list(f"{env_prefix}_SYMBOLS", "SPY")
        mkt = Market()
        strat = strategy_cls(mkt)
        # >>> FIX: always pass (now, market, symbols) <<<
        plans = strat.scan(mkt.now(), mkt, symbols)

        if dry:
            return _respond({
                "system": system_name, "version": system_version, "dry": True,
                "symbols": symbols, "plans": [p.__dict__ for p in plans]
            }, version_header=f"{env_prefix}/{system_version}")

        results = execute_orderplan_batch(plans, system=system_name)
        return _respond({
            "system": system_name, "version": system_version, "dry": False,
            "symbols": symbols, "results": results
        }, version_header=f"{env_prefix}/{system_version}")
    except Exception as e:
        return _respond({"ok": False, "error": str(e)}, status=500)

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
        "S3_OPENING_RANGE": S3_VERSION,
        "S4_RSI_PB": S4_VERSION
    }), 200

# ===== Webhook (unchanged) =====
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
        r = SESSION.get(f"{ALPACA_BASE}/positions", headers=HEADERS, timeout=4)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/orders/open")
def orders_open():
    try:
        r = SESSION.get(f"{ALPACA_BASE}/orders?status=open&limit=200&nested=false", headers=HEADERS, timeout=4)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/orders/recent")
def orders_recent():
    try:
        r = SESSION.get(f"{ALPACA_BASE}/orders?status=all&limit=200&nested=false", headers=HEADERS, timeout=6)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# Optional: quick manual test order endpoint (paper/live respects your keys)
@app.post("/test/order")
def test_order():
    try:
        symbol = (request.args.get("symbol") or "SPY").upper()
        qty    = request.args.get("qty","1")
        side   = request.args.get("side","buy").lower()
        otype  = request.args.get("type","market").lower()
        payload = {"symbol": symbol, "qty": qty, "side": side, "type": otype, "time_in_force": "day"}
        r = SESSION.post(f"{ALPACA_BASE}/orders", headers=HEADERS, json=payload, timeout=6)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ===== Diagnostics =====
@app.get("/diag/alpaca")
def diag_alpaca():
    try:
        return jsonify({
            "trading_base": M_TRADING_BASE,
            "data_base": M_DATA_BASE,
            "feed": M_FEED,
            "market_gate": "on" if _market_gate_is_on() else "off",
        }), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/clock")
def diag_clock():
    try:
        r = M_SESSION.get(f"{M_TRADING_BASE}/v2/clock", headers=M_HEADERS, timeout=5)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/gate")
def diag_gate():
    try:
        r = M_SESSION.get(f"{M_TRADING_BASE}/v2/clock", headers=M_HEADERS, timeout=5)
        clock = r.json() if r.ok else {"error": r.text}
        gate_on = _market_gate_is_on()
        decision = "open" if (not gate_on or (clock.get("is_open") is True)) else "closed"
        return jsonify({"gate_on": gate_on, "decision": decision, "clock": clock}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ===== Scanners =====
@app.get("/scan/s1")
def scan_s1():
    # Examples: /scan/s1?tf=1&mode=either&band=0.6&vol=true&vmult=1.0&dry=0
    dry = _to_bool(request.args.get("dry"), default=False)
    tf  = request.args.get("tf", "1")
    mode = request.args.get("mode", "either")
    slope = request.args.get("slope")  # optional trend slope
    band = request.args.get("band")    # optional reversion band
    use_vol = _to_bool(request.args.get("vol"), default=S2_USE_VOL_DEFAULT)
    vmult = float(request.args.get("vmult", "1.0"))
    symbols = _env_list("S1_SYMBOLS", "SPY,QQQ")
    try:
        mkt = Market() if _SERVICES_AVAILABLE else None
        # run legacy S1 scanner (your function handles tf/mode variants)
        res = scan_s1_symbols(symbols, tf=tf, mode=mode, slope=slope, band=band,
                              use_vol=use_vol, vol_mult=vmult, dry_run=dry)
        if not dry and _SERVICES_AVAILABLE:
            # if scanner returns raw triggers, turn them into plans and execute
            cfg = load_strategy_config("SPY_VWAP_EMA20")  # use your S1 system config
            plans = _plans_from_triggers(
                res.get("triggers"), mkt,
                system_name="SPY_VWAP_EMA20",
                default_side="buy",
                tp_pct=cfg.tp_pct, sl_pct=cfg.sl_pct,
                risk_per_trade=float(os.getenv("S1_RISK_PER_TRADE","0.003"))
            )
            results = execute_orderplan_batch(plans, system="SPY_VWAP_EMA20")
            return jsonify({"ok": True, "system": "S1", "executed": len(results), "results": results}), 200
        return jsonify({"ok": True, "system": "S1", "dry_run": dry, "results": res.get("triggers", [])}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/scan/s2")
def scan_s2():
    # Examples: /scan/s2?symbols=TSLA,NVDA,COIN&tf=30&mode=either&dry=0
    dry = _to_bool(request.args.get("dry"), default=False)
    tf = request.args.get("tf", S2_TF_DEFAULT)
    mode = request.args.get("mode", S2_MODE_DEFAULT)
    use_rth = _to_bool(request.args.get("use_rth"), default=S2_USE_RTH_DEFAULT)
    use_vol = _to_bool(request.args.get("use_vol"), default=S2_USE_VOL_DEFAULT)
    symbols = request.args.get("symbols")
    symbols = [s.strip().upper() for s in (symbols.split(",") if symbols else S2_WHITELIST)]
    try:
        res = scan_s2_symbols(symbols, tf=tf, mode=mode, use_rth=use_rth, use_vol=use_vol, dry_run=dry)
        return jsonify({"ok": True, "system": "S2", "dry_run": dry, "results": res.get("triggers", [])}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/scan/s3")
def scan_s3():
    return _run_strategy(S3OpeningRange, "OPENING_RANGE", S3_VERSION, "S3")

@app.post("/scan/s4")
def scan_s4():
    return _run_strategy(S4RSIPullback, "RSI_PB", S4_VERSION, "S4")

# ===== Dashboard (dark mode) =====
@app.get("/dashboard")
def dashboard():
    try:
        html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Trading Dashboard — Dark</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root { --bg:#0b0f14; --panel:#0f172a; --border:#1f2937; --ink:#e2e8f0; --muted:#94a3b8; --grid:#334155; --pos-h:142; --neg-h:0; }
    html, body { background: var(--bg); color: var(--ink); }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
    h1 { margin: 0 0 6px 0; } h2 { margin: 0 0 12px 0; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.35); }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid var(--border); }
    th { color: var(--muted); font-weight: 600; }
    .muted { color: var(--muted); font-size: 12px; }
    .cal-wrap { display: grid; gap: 12px; }
    .cal-head { display: flex; align-items: center; gap: 8px; justify-content: space-between; }
    .cal-title { font-weight: 700; font-size: 18px; }
    .cal-grid { display: grid; grid-template-columns: repeat(7,1fr); gap: 6px; }
    .dow { color: var(--muted); text-align: center; font-weight: 700; font-size: 12px; margin-top: 4px; }
    .cell { border: 1px dashed var(--grid); border-radius: 8px; min-height: 78px; padding: 6px; display: grid; align-content: start; gap: 6px; }
    .date { font-size: 12px; color: var(--muted); }
    .pl { font-weight: 700; font-size: 14px; }
    .legend { display: flex; align-items: center; gap: 8px; color: var(--muted); font-size: 12px; }
    .legend .swatch { width: 80px; height: 10px; background: linear-gradient(90deg, hsla(var(--neg-h),70%,45%,0.6), hsla(var(--pos-h),70%,45%,0.6)); border-radius: 999px; border: 1px solid var(--border); }
    @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <h1>Trading Dashboard</h1>
  <div class="muted">Origin: <code id="origin"></code> • Version: <code>%%APP_VERSION%%</code></div>
  <br/>

  <div class="card cal-wrap">
    <div class="cal-head">
      <div class="cal-title">Monthly P&L Calendar: <span id="calMonth"></span></div>
      <div class="cal-ctl">
        <button id="prevBtn" title="Previous month">&#x276E;</button>
        <button id="todayBtn">Today</button>
        <button id="nextBtn" title="Next month">&#x276F;</button>
      </div>
    </div>
    <div class="legend">
      <span>Loss</span><span class="swatch"></span><span>Gain</span>
      <span class="muted">— intensity scales with |P&L|</span>
    </div>
    <div class="cal-grid" id="dowRow"></div>
    <div class="cal-grid" id="calGrid"></div>
    <div class="muted" id="calTotals"></div>
  </div>

  <br/>

  <div class="grid">
    <div class="card">
      <h2>Summary (last 7 days)</h2>
      <div id="summary"></div>
      <h3>By Strategy</h3>
      <table id="byStrategy">
        <thead><tr><th>Strategy</th><th>Trades</th><th>Win %</th><th>Realized P&L ($)</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="card">
      <h2>Daily P&L (last 30 days)</h2>
      <canvas id="dailyChart" height="200"></canvas>
    </div>

    <div class="card">
      <h2>Equity Curve (realized)</h2>
      <canvas id="equityChart" height="200"></canvas>
    </div>

    <div class="card">
      <h2>Recent Realized Trades</h2>
      <table id="trades">
        <thead><tr><th>Exit Time (UTC)</th><th>System</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <script>
    const origin = window.location.origin;
    document.getElementById('origin').textContent = origin;

    async function getJSON(url) {
      const r = await fetch(url);
      const t = await r.text();
      try { return JSON.parse(t); } catch { return { ok:false, error: t || 'parse error' }; }
    }
    function fmt(n) { n = Number(n||0); return (n>=0?'+':'') + n.toFixed(2); }

    const DOW = ['Su','Mo','Tu','We','Th','Fr','Sa'];
    function ymd(d) { return d.toISOString().slice(0,10); }
    function endOfMonth(d) { return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth()+1, 0)); }
    function startOfMonth(d) { return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1)); }
    function colorForPL(v, maxAbs) {
      if (!v) return null;
      const a = Math.min(1, Math.abs(v)/Math.max(1, maxAbs));
      const alpha = 0.15 + a*0.45;
      const hue = v >= 0 ? 142 : 0;
      return `hsla(${hue}, 70%, 45%, ${alpha})`;
    }

    (function() {
      const dowRow = document.getElementById('dowRow');
      DOW.forEach(d => { const el = document.createElement('div'); el.className = 'dow'; el.textContent = d; dowRow.appendChild(el); });
    })();

    (async () => {
      const perfAll = await getJSON(origin + '/performance?days=30&include_trades=1&fast=1');
      const daily   = await getJSON(origin + '/performance/daily?days=30&fast=1');

      if (perfAll.ok === false) {
        document.body.insertAdjacentHTML('beforeend', '<pre style="color:#fca5a5;">Dashboard error: ' + (perfAll.error||'unknown') + '</pre>');
        return;
      }
      if (daily.ok === false) {
        document.body.insertAdjacentHTML('beforeend', '<pre style="color:#fca5a5;">Dashboard error: ' + (daily.error||'unknown') + '</pre>');
        return;
      }

      const STRATS = perfAll.current_strategies || [];
      const sum = {
        pnl: Number(perfAll.total_realized_pnl||0),
        equity: Number(perfAll.equity||0),
      };

      // Summary + By Strategy table
      const by = perfAll.by_strategy || {};
      const tbody = document.querySelector('#byStrategy tbody');
      tbody.innerHTML = '';
      STRATS.forEach(k => {
        const s = by[k] || { trades: 0, win_rate: 0, pnl: 0 };
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${k}</td><td>${s.trades||0}</td><td>${(Number(s.win_rate||0)*100).toFixed(1)}%</td><td>${(Number(s.pnl||0)).toFixed(2)}</td>`;
        tbody.appendChild(tr);
      });

      document.getElementById('summary').innerHTML =
        `<div class="muted">Equity (realized):</div><div class="pl" style="margin-bottom:8px;">$${sum.equity.toFixed(2)}</div>` +
        `<div class="muted">Total P&L (last ${perfAll.days} days):</div><div class="pl">${fmt(sum.pnl)}</div>`;

      // Calendar
      const drows = daily.daily || [];
      const byDay = {};
      let maxAbs = 0;
      drows.forEach(d => { byDay[d.date] = d.total_pnl; maxAbs = Math.max(maxAbs, Math.abs(d.total_pnl||0)); });

      const today = new Date();
      let month = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), 1));

      function drawMonth(m) {
        document.getElementById('calGrid').innerHTML = '';
        document.getElementById('calMonth').textContent = m.toLocaleString('en-US', {month:'long', year:'numeric', timeZone:'UTC'});
        const start = startOfMonth(m);
        const end = endOfMonth(m);
        const firstDow = start.getUTCDay();
        const grid = document.getElementById('calGrid');
        // headers
        ['Su','Mo','Tu','We','Th','Fr','Sa'].forEach(d => {
          const el = document.createElement('div'); el.className = 'dow'; el.textContent = d; grid.appendChild(el);
        });
        // blanks
        for (let i=0;i<firstDow;i++){ grid.appendChild(document.createElement('div')); }
        // days
        for (let d=1; d<=end.getUTCDate(); d++) {
          const cell = document.createElement('div'); cell.className = 'cell';
          const dt = new Date(Date.UTC(m.getUTCFullYear(), m.getUTCMonth(), d));
          const key = ymd(dt);
          const v = Number(byDay[key]||0);
          const sw = document.createElement('div');
          sw.style.height = '24px';
          const col = colorForPL(v, maxAbs);
          if (col) { cell.style.background = col; }
          cell.innerHTML = `<div class="date">${key}</div><div class="pl">${fmt(v)}</div>`;
          grid.appendChild(cell);
        }
      }
      drawMonth(month);

      document.getElementById('prevBtn').onclick = () => { month = new Date(Date.UTC(month.getUTCFullYear(), month.getUTCMonth()-1, 1)); drawMonth(month); };
      document.getElementById('todayBtn').onclick = () => { month = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), 1)); drawMonth(month); };
      document.getElementById('nextBtn').onclick = () => { month = new Date(Date.UTC(month.getUTCFullYear(), month.getUTCMonth()+1, 1)); drawMonth(month); };

      // Charts
      const dailyCtx = document.getElementById('dailyChart').getContext('2d');
      const equityCtx = document.getElementById('equityChart').getContext('2d');
      new Chart(dailyCtx, { type:'bar', data:{ labels: drows.map(d=>d.date), datasets:[{ label:'Daily P&L', data: drows.map(d=>d.total_pnl) }] } });
      new Chart(equityCtx, { type:'line', data:{ labels: perfAll.trades ? perfAll.trades.map(t=>t.exit_time) : [], datasets:[{ label:'Equity (realized)', data: (perfAll.equity_series||[]) }] } });
    })();
  </script>
</body>
</html>
""".replace("%%APP_VERSION%%", APP_VERSION)
        return make_response(html, 200, {"Content-Type": "text/html; charset=utf-8"})
    except Exception as e:
        return make_response(f"<pre>dashboard error: {e}</pre>", 500)

# Default 404 (Render health check often does HEAD /)
@app.route("/", methods=["GET","HEAD"])
def root_404():
    return make_response(jsonify({"ok": False, "error": "not found", "version": APP_VERSION}), 404)
