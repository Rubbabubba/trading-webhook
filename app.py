# app.py — modular Equities Webhook/Scanner
# Version: 2025-09-10-05

import os, time, json
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, make_response

from core.config import (APP_VERSION, CURRENT_STRATEGIES, ALPACA_BASE,
                         S2_WHITELIST, S2_TF_DEFAULT, S2_MODE_DEFAULT,
                         S2_USE_RTH_DEFAULT, S2_USE_VOL_DEFAULT,
                         MAX_DAYS_PERF, HEADERS, SESSION)
from core.utils import _iso, _canon_timeframe, _scrub_nans, only_current_strats
from core.performance import (fetch_trade_activities, fetch_client_order_ids,
                              compute_performance, bucket_daily, _load_symbol_system_map)
from core.alpaca import is_market_open_now
from core.signals import process_signal, load_strategy_config
from strategies.s2 import scan_s2_symbols

app = Flask(__name__)

# ===== Routes =====
@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "equities-webhook", "version": APP_VERSION, "time": int(time.time())}), 200

@app.post("/webhook")
def webhook():
    """Accept external alerts (or tests) and pass to the shared processor."""
    data = request.get_json(force=True, silent=True) or {}
    status, out = process_signal(data)
    return jsonify(out), status

@app.get("/performance")
def performance():
    """Realized P&L from Alpaca fills (FIFO)."""
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
        start_iso = _iso(start_dt)
        end_iso   = _iso(end_dt)

        acts = fetch_trade_activities(start_iso, end_iso)
        order_ids = [a.get("order_id") for a in acts if a.get("order_id")]
        cid_map = {} if fast else fetch_client_order_ids(order_ids)
        sym_map = _load_symbol_system_map()
        perf = compute_performance(acts, cid_map, sym_map)

        out = {
            "ok": True,
            "version": APP_VERSION,
            "current_strategies": CURRENT_STRATEGIES,
            "days": days,
            "total_realized_pnl": perf["total_pnl"],
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
    """Daily buckets of realized P&L (UTC), with per-strategy splits."""
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

        return jsonify(_scrub_nans({"ok": True, "version": APP_VERSION, "current_strategies": CURRENT_STRATEGIES, "days": days, "daily": daily})), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

@app.get("/positions")
def positions():
    """Proxy Alpaca positions (JSON)."""
    try:
        r = SESSION.get(f"{ALPACA_BASE}/positions", headers=HEADERS, timeout=4)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/orders/open")
def orders_open():
    """Proxy Alpaca open orders (JSON)."""
    try:
        r = SESSION.get(f"{ALPACA_BASE}/orders?status=open&limit=200&nested=false", headers=HEADERS, timeout=4)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/config")
def config():
    """Effective runtime config snapshot (JSON)."""
    def cfg_for(system):
        from core.signals import load_strategy_config
        c = load_strategy_config(system)
        return {"qty": c.qty, "order_type": c.order_type, "tif": c.tif, "tp_pct": c.tp_pct, "sl_pct": c.sl_pct}
    out = {
        "version": APP_VERSION,
        "alpaca_base": ALPACA_BASE,
        "current_strategies": CURRENT_STRATEGIES,
        "safety": {
            "cancel_open_orders_before_plain": os.getenv("CANCEL_OPEN_ORDERS_BEFORE_PLAIN","true"),
            "attach_oco_on_plain": os.getenv("ATTACH_OCO_ON_PLAIN","true"),
            "oco_tif": os.getenv("OCO_TIF","gtc"),
        },
        "caps": {
            "MAX_OPEN_POSITIONS": os.getenv("MAX_OPEN_POSITIONS","5"),
            "MAX_POSITIONS_PER_SYMBOL": os.getenv("MAX_POSITIONS_PER_SYMBOL","1"),
            "MAX_POSITIONS_PER_STRATEGY": os.getenv("MAX_POSITIONS_PER_STRATEGY","3"),
        },
        "systems": {
            "SPY_VWAP_EMA20": cfg_for("SPY_VWAP_EMA20"),
            "SMA10D_MACD": cfg_for("SMA10D_MACD"),
        }
    }
    return jsonify(out), 200

@app.get("/dashboard")
def dashboard():
    """Dark-mode HTML dashboard with Monthly P&L Calendar, Summary, Charts, Trades."""
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
    .cal-ctl button { padding: 6px 10px; border-radius: 8px; border: 1px solid var(--border); background: #0b1220; color: var(--ink); cursor: pointer; }
    .cal-grid { display: grid; grid-template-columns: repeat(7, 1fr); gap: 8px; }
    .dow { text-align:center; font-size:12px; color: var(--muted); margin-bottom: -6px; }
    .day { min-height: 82px; border: 1px solid var(--border); border-radius: 10px; padding: 6px 8px; position: relative; background: #0b1220; display:flex; flex-direction:column; justify-content:flex-end; }
    .day .date { position:absolute; top:6px; right:8px; font-size:12px; color: var(--muted); }
    .day .pl { font-weight:700; font-size:14px; color: var(--ink); }
    .day .trades { font-size:12px; color: var(--muted); }
    .day.today { outline: 2px dashed #8b5cf6; outline-offset: 2px; }
    .legend { display:flex; gap:10px; align-items:center; font-size:12px; color: var(--muted); }
    .swatch { width: 48px; height: 10px; border-radius: 6px; background: linear-gradient(90deg, hsl(var(--neg-h),80%,55%), #273041, hsl(var(--pos-h),70%,45%)); border:1px solid var(--border); }
    @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <h1>Trading Dashboard</h1>
  <div class="muted">Origin: <code id="origin"></code> • Version: <code>%%APP_VERSION%%</code></div>
  <br/>

  <div class="card cal-wrap">
    <div class="cal-head">
      <div class="cal-title">Monthly P&amp;L Calendar: <span id="calMonth"></span></div>
      <div class="cal-ctl">
        <button id="prevBtn" title="Previous month">&#x276E;</button>
        <button id="todayBtn">Today</button>
        <button id="nextBtn" title="Next month">&#x276F;</button>
      </div>
    </div>
    <div class="legend">
      <span>Loss</span><span class="swatch"></span><span>Gain</span>
      <span class="muted">— intensity scales with |P&amp;L|</span>
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
        <thead><tr><th>Strategy</th><th>Trades</th><th>Win %</th><th>Realized P&amp;L ($)</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="card">
      <h2>Daily P&amp;L (last 30 days)</h2>
      <canvas id="dailyChart" height="200"></canvas>
    </div>

    <div class="card">
      <h2>Equity Curve (realized)</h2>
      <canvas id="equityChart" height="200"></canvas>
    </div>

    <div class="card">
      <h2>Recent Realized Trades</h2>
      <table id="trades">
        <thead><tr><th>Exit Time (UTC)</th><th>System</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&amp;L</th></tr></thead>
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

      const STRATS = perfAll.current_strategies || ["SPY_VWAP_EMA20","SMA10D_MACD"];

      const dayPNL = new Map();
      (daily.daily || []).forEach(d => dayPNL.set(d.date, d.pnl));
      const dayTrades = new Map();
      (perfAll.trades || []).forEach(tr => {
        const day = String(tr.exit_time).slice(0,10);
        dayTrades.set(day, (dayTrades.get(day) || 0) + 1);
      });

      const maxAbs = Math.max(1, ...Array.from(dayPNL.values()).map(v => Math.abs(v)));

      let current = new Date();
      function setMonthLabel(dt) {
        const m = dt.toLocaleString('en-US', { month: 'long', timeZone: 'UTC' });
        const y = dt.getUTCFullYear();
        document.getElementById('calMonth').textContent = m + ' ' + y;
      }

      function renderCalendar(dt) {
        const grid = document.getElementById('calGrid');
        grid.innerHTML = '';
        const start = startOfMonth(dt);
               const end = endOfMonth(dt);
        const firstDow = start.getUTCDay();
        const daysInMonth = end.getUTCDate();

        for (let i=0; i<firstDow; i++) grid.appendChild(document.createElement('div'));

        let totalMonth = 0; let totalTrades = 0;

        for (let d=1; d<=daysInMonth; d++) {
          const cDate = new Date(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), d));
          const key = ymd(cDate);
          const pnl = dayPNL.get(key) || 0;
          const trades = dayTrades.get(key) || 0;
          const cell = document.createElement('div');
          cell.className = 'day';
          const bg = colorForPL(pnl, maxAbs);
          if (bg) cell.style.background = bg;
          if (ymd(cDate) === ymd(new Date())) cell.classList.add('today');

          cell.innerHTML = `
            <div class="date">${d}</div>
            <div class="pl">$${Number(pnl||0).toFixed(2)}</div>
            <div class="trades">${trades} trades</div>
          `;
          grid.appendChild(cell);

          totalMonth += pnl; totalTrades += trades;
        }

        const footer = document.getElementById('calTotals');
        footer.textContent = 'Month total: $' + totalMonth.toFixed(2) + ' across ' + totalTrades + ' trades';
        setMonthLabel(dt);
      }

      renderCalendar(current);
      document.getElementById('prevBtn').onclick = () => { current = new Date(Date.UTC(current.getUTCFullYear(), current.getUTCMonth()-1, 1)); renderCalendar(current); };
      document.getElementById('nextBtn').onclick = () => { current = new Date(Date.UTC(current.getUTCFullYear(), current.getUTCMonth()+1, 1)); renderCalendar(current); };
      document.getElementById('todayBtn').onclick = () => { current = new Date(); renderCalendar(current); };

      // Summary (7d)
      const perf7 = await getJSON(origin + '/performance?days=7&include_trades=1&fast=1');
      if (perf7.ok !== false) {
        const tbody = document.querySelector('#byStrategy tbody');
        document.getElementById('summary').innerHTML = '<div>Total realized P&amp;L: <b>$' + Number(perf7.total_realized_pnl||0).toFixed(2) + '</b></div>';
        tbody.innerHTML = '';
        (perf7.current_strategies || STRATS).forEach((name) => {
          const v = (perf7.by_strategy || {})[name] || { trades:0, win_rate:0, realized_pnl:0 };
          const tr = document.createElement('tr');
          tr.innerHTML = '<td>'+name+'</td><td>'+(v.trades||0)+'</td><td>'+(v.win_rate||0)+'%</td><td>'+Number(v.realized_pnl||0).toFixed(2)+'</td>';
          tbody.appendChild(tr);
        });

        // Equity chart
        const eq = perf7.equity || [];
        const eqLabels = eq.map(x => x.time);
        const eqData = eq.map(x => x.equity);
        new Chart(document.getElementById('equityChart').getContext('2d'), {
          type: 'line',
          data: { labels: eqLabels, datasets: [{ label: 'Equity ($)', data: eqData }] },
          options: { responsive: true, plugins: { legend: { display: false } } }
        });

        // Daily (30d)
        const dLabels = (daily.daily||[]).map(x => x.date);
        const dData = (daily.daily||[]).map(x => x.pnl);
        new Chart(document.getElementById('dailyChart').getContext('2d'), {
          type: 'bar',
          data: { labels: dLabels, datasets: [{ label: 'Daily P&L ($)', data: dData }] },
          options: { responsive: true, plugins: { legend: { display: false } } }
        });

        // Trades table (last 25)
        const tBody = document.querySelector('#trades tbody');
        (perf7.trades || []).slice(-25).forEach(t => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${t.exit_time}</td>
            <td>${t.system}</td>
            <td>${t.symbol}</td>
            <td>${t.side}</td>
            <td>$${Number(t.entry_price).toFixed(2)}</td>
            <td>$${Number(t.exit_price).toFixed(2)}</td>
            <td>${fmt(Number(t.pnl))}</td>`;
          tBody.appendChild(tr);
        });
      }
    })();
  </script>
</body>
</html>
        """
        html = html.replace("%%APP_VERSION%%", APP_VERSION)
        resp = make_response(html, 200)
        resp.headers["Content-Type"] = "text/html; charset=utf-8"
        return resp
    except Exception as e:
        return f"<pre>dashboard render error: {e}</pre>", 500

@app.get("/scan/s1")
def scan_s1_route():
    """
    Server-side scanner for Strategy 1 (VWAP x EMA20; long entries).
    Query params:
      symbols=CSV      (default: env S1_SYMBOLS, usually "SPY")
      tf=1|5           (default: env S1_TF_DEFAULT, "1")
      mode=reversion|trend|either  (default: env S1_MODE_DEFAULT, "either")
      band=float       (default: env S1_BAND_DEFAULT, 0.6)
      slope=float      (default: env S1_SLOPE_MIN, 0.0)
      rth=true|false   (default: env S1_USE_RTH_DEFAULT, true)
      vol=true|false   (default: env S1_USE_VOL_DEFAULT, false)
      vmult=float      (default: env S1_VOL_MULT_DEFAULT, 1.0)
      dry=1|0          (default: 1)
      force=1          (optional) bypass market-open gate
    """
    try:
        from core.config import (
            S1_SYMBOLS, S1_TF_DEFAULT, S1_MODE_DEFAULT, S1_BAND_DEFAULT, S1_SLOPE_MIN,
            S1_USE_RTH_DEFAULT, S1_USE_VOL_DEFAULT, S1_VOL_MULT_DEFAULT
        )
        from core.alpaca import is_market_open_now

        if request.args.get("force", "0") != "1":
            if not is_market_open_now():
                return jsonify({"ok": True, "skipped": "market_closed"}), 200

        symbols = request.args.get("symbols", S1_SYMBOLS).split(",")
        tf      = request.args.get("tf", S1_TF_DEFAULT)
        mode    = request.args.get("mode", S1_MODE_DEFAULT)
        band    = float(request.args.get("band", str(S1_BAND_DEFAULT)))
        slope   = float(request.args.get("slope", str(S1_SLOPE_MIN)))
        rth     = request.args.get("rth", str(S1_USE_RTH_DEFAULT)).lower() == "true"
        vol     = request.args.get("vol", str(S1_USE_VOL_DEFAULT)).lower() == "true"
        vmult   = float(request.args.get("vmult", str(S1_VOL_MULT_DEFAULT)))
        dry     = request.args.get("dry", "1") == "1"

        scan = scan_s1_symbols(
            symbols, tf=tf, mode=mode, band=band, slope_min=slope,
            use_rth=rth, use_vol=vol, vol_mult=vmult, dry_run=dry
        )

        res = {
            "ok": True,
            "timeframe": scan.get("timeframe"),
            "mode": mode,
            "band": band,
            "slope_min": slope,
            "use_rth": rth,
            "use_vol": vol,
            "vmult": vmult,
            "dry_run": dry,
            "checked": scan.get("checked", []),
            "triggers": scan.get("triggers", []),
        }
        return jsonify(res), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

@app.get("/scan/s2")
def scan_s2_route():
    """
    Server-side scanner for Strategy 2 (MACD x SMA10 with daily trend filter).

    Query params:
      symbols=CSV            (default: env S2_WHITELIST)
      tf=60|30|120|day       (default: env S2_TF_DEFAULT)
      mode=strict|either     (default: env S2_MODE_DEFAULT)
      rth=true|false         (default: env S2_USE_RTH_DEFAULT)
      vol=true|false         (default: env S2_USE_VOL_DEFAULT)
      dry=1|0                (default: 1 = dry-run only)
      force=1                (optional) bypass market-open gate
    """
    try:
        if request.args.get("force", "0") != "1":
            if not is_market_open_now():
                return jsonify({"ok": True, "skipped": "market_closed"}), 200

        default_syms = os.getenv("S2_WHITELIST", S2_WHITELIST)
        symbols = request.args.get("symbols", default_syms).split(",")
        tf      = request.args.get("tf", S2_TF_DEFAULT)
        mode    = request.args.get("mode", S2_MODE_DEFAULT)
        rth     = (request.args.get("rth", S2_USE_RTH_DEFAULT).lower() == "true")
        vol     = (request.args.get("vol", S2_USE_VOL_DEFAULT).lower() == "true")
        dry     = request.args.get("dry", "1") == "1"

        scan = scan_s2_symbols(symbols, tf=tf, mode=mode, use_rth=rth, use_vol=vol, dry_run=dry)

        res = {
            "ok": True,
            "timeframe": _canon_timeframe(tf),
            "mode": mode,
            "use_rth": rth,
            "use_vol": vol,
            "dry_run": dry,
            "checked": scan.get("checked", []),
            "triggers": [],
        }

        if dry:
            res["triggers"] = scan.get("triggers", [])
            return jsonify(res), 200

        res["triggers"] = scan.get("triggers", [])
        return jsonify(res), 200

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)