# app.py
import os, json, math, time, uuid, sys, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# -------------------------
# Logging helper
# -------------------------
def log(msg):
    print(msg, file=sys.stdout, flush=True)

# -------------------------
# ENV / CONFIG
# -------------------------
BASE = os.getenv("ALPACA_BASE", "https://paper-api.alpaca.markets").rstrip("/")
if BASE.endswith("/v2"):
    BASE = BASE[:-3]  # avoid /v2/v2

KEY  = os.getenv("ALPACA_KEY", "")
SEC  = os.getenv("ALPACA_SECRET", "")

WHITELIST = [s.strip().upper() for s in os.getenv(
    "WHITELIST",
    "SPY,QQQ,TSLA,NVDA,COIN,GOOGL,META,MSFT,AMZN,AAPL"
).split(",")]

DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))
try:
    QTY_MAP = json.loads(os.getenv("QTY_MAP", "{}"))
except Exception:
    QTY_MAP = {}

DAILY_TARGET = float(os.getenv("DAILY_TARGET", "200"))    # stop new entries after +$200 on the day
DAILY_STOP   = float(os.getenv("DAILY_STOP", "-100"))     # stop new entries after -$100 on the day
MAX_OPEN_ORDERS_PER_SYMBOL = int(os.getenv("MAX_OPEN_ORDERS_PER_SYMBOL", "1"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "2"))

WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")

# Notion (optional)
NOTION_TOKEN     = os.getenv("NOTION_TOKEN", "")
NOTION_DB_TRADES = os.getenv("NOTION_DB_TRADES", "")
NOTION_API       = "https://api.notion.com/v1"
NOTION_VERSION   = "2022-06-28"

# Strategy configs
STRATEGY = {
    # Strategy #1 (from your setup)
    "SPY_VWAP_EMA20": {
        "stop_pct": 0.003,      # 0.30%
        "tp_pct":   0.006,      # 0.60%
        "time_window": ("10:00","12:00"),  # ET window
        "rth_only": True,
        "whitelist": ["SPY"]
    },
    # Strategy #2 (your SMA + MACD + Midnight line)
    "SMA10D_MACD": {
        "stop_pct": 0.005,      # 0.50%
        "tp_pct":   0.010,      # 1.00%
        "time_window": None,    # whole RTH
        "rth_only": True,
        "whitelist": ["SPY","QQQ","NVDA","TSLA","COIN","GOOGL","META","MSFT","AMZN","AAPL"]
    }
}

# -------------------------
# Alpaca helpers
# -------------------------
def headers():
    return {
        "APCA-API-KEY-ID": KEY,
        "APCA-API-SECRET-KEY": SEC,
        "Content-Type": "application/json"
    }

def alpaca_get(path):
    r = requests.get(f"{BASE}{path}", headers=headers(), timeout=15)
    if not r.ok:
        try:
            err = r.json()
        except Exception:
            err = r.text
        log(f"ALPACA_GET_ERROR {path} status={r.status_code} body={err}")
    r.raise_for_status()
    return r.json()

def alpaca_post(path, body):
    r = requests.post(f"{BASE}{path}", headers=headers(), data=json.dumps(body), timeout=15)
    if not r.ok:
        # Log full error body so we can see exact Alpaca reason (e.g., duplicate client_order_id)
        try:
            err = r.json()
        except Exception:
            err = r.text
        log(f"ALPACA_POST_ERROR {path} status={r.status_code} body={err} sent={body}")
    r.raise_for_status()
    return r.json()

def get_account():
    return alpaca_get("/v2/account")

def list_positions():
    try:
        return alpaca_get("/v2/positions")
    except Exception as e:
        log(f"list_positions error: {e}")
        return []

def list_open_orders(symbol=None):
    path = "/v2/orders?status=open&limit=500"
    if symbol:
        path += f"&symbols={symbol}"
    try:
        return alpaca_get(path)
    except Exception as e:
        log(f"list_open_orders error: {e}")
        return []

def list_closed_orders(after_iso=None):
    if after_iso is None:
        after_iso = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = f"/v2/orders?status=closed&limit=500&after={after_iso}"
    return alpaca_get(path)

def market_is_open_et():
    now_et = datetime.now(ZoneInfo("America/New_York")).time()
    open_t  = datetime.strptime("09:30","%H:%M").time()
    close_t = datetime.strptime("16:00","%H:%M").time()
    return open_t <= now_et <= close_t

def within_time_window(system_name: str) -> bool:
    cfg = STRATEGY.get(system_name, {})
    window = cfg.get("time_window")
    if not window:
        return True
    start, end = window
    now_et = datetime.now(ZoneInfo("America/New_York")).time()
    return datetime.strptime(start,"%H:%M").time() <= now_et <= datetime.strptime(end,"%H:%M").time()

def qty_for(symbol: str) -> int:
    try:
        return int(QTY_MAP.get(symbol, DEFAULT_QTY))
    except Exception:
        return DEFAULT_QTY

def round_px(px: float) -> float:
    return float(f"{px:.2f}")

def place_bracket(symbol: str, side: str, qty: int, entry_price: float, tp_pct: float, stop_pct: float, system: str):
    # Make client_order_id truly unique to avoid 422 on duplicates
    cli_id = f"{system[:18]}-{symbol}-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}"

    if side == "buy":
        tp = round_px(entry_price * (1.0 + tp_pct))
        sl = round_px(entry_price * (1.0 - stop_pct))
    else:
        tp = round_px(entry_price * (1.0 - tp_pct))
        sl = round_px(entry_price * (1.0 + stop_pct))

    body = {
        "symbol": symbol,
        "qty": qty,
        "side": side,
        "type": "market",
        "time_in_force": "day",
        "order_class": "bracket",
        "take_profit": {"limit_price": str(tp)},
        "stop_loss":   {"stop_price":  str(sl)},
        "client_order_id": cli_id
    }
    return alpaca_post("/v2/orders", body)

# -------------------------
# Notion (optional)
# -------------------------
def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }

def notion_find_by_order_id(order_id: str):
    if not (NOTION_TOKEN and NOTION_DB_TRADES):
        return None
    url = f"{NOTION_API}/databases/{NOTION_DB_TRADES}/query"
    body = {
        "page_size": 1,
        "filter": {"property": "OrderId", "rich_text": {"equals": order_id}}
    }
    r = requests.post(url, headers=notion_headers(), data=json.dumps(body), timeout=15)
    r.raise_for_status()
    res = r.json()
    if res.get("results"):
        return res["results"][0]
    return None

def notion_upsert_trade(order: dict):
    if not (NOTION_TOKEN and NOTION_DB_TRADES):
        return {"ok": False, "skipped": True, "reason": "Notion env not configured"}

    order_id = order.get("id")
    if not order_id:
        return {"ok": False, "skipped": True, "reason": "Order missing id"}

    if notion_find_by_order_id(order_id):
        return {"ok": True, "skipped": True, "reason": "Already logged"}

    sym   = order.get("symbol")
    side  = order.get("side")
    qty   = float(order.get("filled_qty", "0") or 0)
    avgpx = float(order.get("filled_avg_price", "0") or 0)
    filled_at = order.get("filled_at") or order.get("updated_at") or datetime.utcnow().isoformat()+"Z"
    cli_id = order.get("client_order_id", "")
    system = cli_id.split("-")[0] if "-" in cli_id else ""

    props = {
        "Name": {"title": [{"text": {"content": f"{sym} {side.upper()} {int(qty)}"}}]},
        "Date": {"date": {"start": filled_at}},
        "Ticker": {"rich_text": [{"text": {"content": sym or ""}}]},
        "Side": {"rich_text": [{"text": {"content": side or ""}}]},
        "Qty": {"number": qty},
        "AvgPrice": {"number": avgpx},
        "OrderId": {"rich_text": [{"text": {"content": order_id}}]},
        "ClientOrderId": {"rich_text": [{"text": {"content": cli_id}}]},
        "Strategy": {"rich_text": [{"text": {"content": system}}]}
    }

    url = f"{NOTION_API}/pages"
    body = {"parent": {"database_id": NOTION_DB_TRADES}, "properties": props}
    r = requests.post(url, headers=notion_headers(), data=json.dumps(body), timeout=15)
    r.raise_for_status()
    return {"ok": True, "skipped": False}

# -------------------------
# Routes
# -------------------------
@app.route("/", methods=["GET"])
def health():
    return "OK", 200

@app.route("/status", methods=["GET"])
def status():
    acct = {}
    try:
        acct = get_account()
    except Exception as e:
        acct = {"error": str(e)}
    return jsonify({
        "ok": True,
        "base": BASE,
        "whitelist": WHITELIST,
        "default_qty": DEFAULT_QTY,
        "qty_map": QTY_MAP,
        "daily_target": DAILY_TARGET,
        "daily_stop": DAILY_STOP,
        "acct": acct,
        "notion_enabled": bool(NOTION_TOKEN and NOTION_DB_TRADES)
    }), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True) or {}
    log(f"WEBHOOK IN: {data}")

    # Auth
    if WEBHOOK_TOKEN:
        header_token = request.headers.get("X-Webhook-Token")
        body_token = data.get("token")
        if WEBHOOK_TOKEN not in (header_token, body_token):
            log("SKIP: Unauthorized (token mismatch)")
            return jsonify(ok=False, error="Unauthorized"), 401

    system = str(data.get("system", "unknown"))
    side_in = str(data.get("side", "")).lower()  # long/short/buy/sell
    symbol  = str(data.get("ticker", "")).upper()
    price   = data.get("price", None)

    if not KEY or not SEC:
        return jsonify(ok=False, error="Missing ALPACA_KEY/ALPACA_SECRET"), 400
    if not symbol:
        return jsonify(ok=False, error="Missing 'ticker'"), 400

    # Strategy-level whitelist (if present); else fall back to global WHITELIST
    strat_wl = STRATEGY.get(system, {}).get("whitelist")
    effective_wl = set((strat_wl or WHITELIST))
    if symbol not in effective_wl:
        log(f"SKIP: {symbol} not in whitelist for {system}")
        return jsonify(ok=True, skipped=True, reason=f"{symbol} not in whitelist"), 200

    if side_in not in ("long","short","buy","sell"):
        return jsonify(ok=False, error="side must be LONG/SHORT/BUY/SELL"), 400

    cfg = STRATEGY.get(system, STRATEGY["SMA10D_MACD"])

    # Time gates
    if cfg.get("rth_only", True) and not market_is_open_et():
        log("SKIP: Market closed (RTH only)")
        return jsonify(ok=True, skipped=True, reason="Market closed (RTH only)"), 200
    if not within_time_window(system):
        log("SKIP: Outside strategy time window")
        return jsonify(ok=True, skipped=True, reason="Outside strategy time window"), 200

    # Daily P&L gate (equity vs last_equity)
    acct = get_account()
    equity = float(acct.get("equity", 0))
    last_equity = float(acct.get("last_equity", equity))
    pnl = equity - last_equity
    if pnl >= DAILY_TARGET or pnl <= DAILY_STOP:
        log(f"SKIP: Daily PnL {pnl} limits {DAILY_STOP}/{DAILY_TARGET}")
        return jsonify(ok=True, skipped=True, reason="Daily P&L limit reached", pnl=pnl), 200

    # Exposure gates
    positions = list_positions()
    if len(positions) >= MAX_POSITIONS:
        log("SKIP: Max positions reached")
        return jsonify(ok=True, skipped=True, reason="Max positions reached"), 200

    # Only count parent orders (ignore bracket child legs which have parent_order_id)
    open_orders = [o for o in list_open_orders(symbol=symbol) if not o.get("parent_order_id")]
    if len(open_orders) >= MAX_OPEN_ORDERS_PER_SYMBOL:
        log("SKIP: Open parent order exists for symbol")
        return jsonify(ok=True, skipped=True, reason="Open order exists for symbol"), 200

    side = "buy" if side_in in ("long","buy") else "sell"
    if price is None:
        return jsonify(ok=False, error="Missing 'price' in payload"), 400
    entry_px = float(price)
    qty = qty_for(symbol)

    # Place order with robust error handling
    try:
        log(f"PLACE: {symbol} {side} qty={qty} @ {entry_px} system={system}")
        order = place_bracket(
            symbol=symbol,
            side=side,
            qty=qty,
            entry_price=entry_px,
            tp_pct=cfg["tp_pct"],
            stop_pct=cfg["stop_pct"],
            system=system
        )
        log(f"ORDER OK: {order.get('id')} class={order.get('order_class')}")
        return jsonify(ok=True, system=system, order=order), 200
    except requests.HTTPError as e:
        # r.raise_for_status() already logged details in alpaca_post
        log(f"ORDER REJECTED: {e}")
        return jsonify(ok=False, error="alpaca_rejected", detail=str(e)), 400
    except Exception as e:
        log(f"ORDER ERROR: {e}")
        return jsonify(ok=False, error="unknown_error", detail=str(e)), 500

# -------------------------
# Performance (JSON + HTML)
# -------------------------
def _compute_performance(days: int):
    after_iso = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    orders = list_closed_orders(after_iso=after_iso)

    by_symbol = {}
    for o in orders:
        if o.get("status") != "filled":
            continue
        sym   = o.get("symbol")
        side  = o.get("side")
        try:
            qty   = float(o.get("filled_qty", "0") or 0)
            avgpx = float(o.get("filled_avg_price", "0") or 0)
        except Exception:
            qty, avgpx = 0.0, 0.0
        if qty == 0:
            continue

        d = by_symbol.setdefault(sym, {"buy_qty":0,"buy_value":0.0,"sell_qty":0,"sell_value":0.0,"orders":[]})
        if side == "buy":
            d["buy_qty"]   += qty
            d["buy_value"] += qty * avgpx
        else:
            d["sell_qty"]   += qty
            d["sell_value"] += qty * avgpx
        d["orders"].append({
            "id": o.get("id"),
            "submitted_at": o.get("submitted_at"),
            "filled_at": o.get("filled_at"),
            "side": side,
            "qty": qty,
            "avg_price": avgpx,
            "order_class": o.get("order_class"),
            "status": o.get("status")
        })

    total_pnl = 0.0
    wins = 0
    losses = 0
    details = []
    for sym, d in by_symbol.items():
        matched_qty = min(d["buy_qty"], d["sell_qty"])
        realized = 0.0
        if matched_qty > 0 and d["buy_qty"] > 0 and d["sell_qty"] > 0:
            avg_buy  = d["buy_value"] / d["buy_qty"]
            avg_sell = d["sell_value"] / d["sell_qty"]
            realized = (avg_sell - avg_buy) * matched_qty
        total_pnl += realized
        if realized > 0: wins += 1
        elif realized < 0: losses += 1
        details.append({
            "symbol": sym,
            "avg_buy": round(d["buy_value"]/d["buy_qty'], 4) if d["buy_qty"] else None,
            "avg_sell": round(d["sell_value"]/d["sell_qty'], 4) if d["sell_qty"] else None,
            "matched_qty": matched_qty,
            "realized_pnl": round(realized, 2),
            "orders": d["orders"][-5:]
        })

    total_trades = wins + losses
    winrate = round(100 * wins / total_trades, 2) if total_trades else 0.0
    acct = {}
    try:
        acct = get_account()
    except Exception as e:
        acct = {"error": str(e)}
    positions = list_positions()

    return {
        "ok": True,
        "window_days": days,
        "equity": acct.get("equity"),
        "last_equity": acct.get("last_equity"),
        "realized_pnl_est": round(total_pnl, 2),
        "winrate_pct_est": winrate,
        "wins": wins, "losses": losses,
        "symbols": details,
        "open_positions": positions
    }

@app.route("/performance.json", methods=["GET"])
def performance_json():
    try:
        days = int(request.args.get("days", "14"))
    except Exception:
        days = 14
    data = _compute_performance(days)
    return jsonify(data), 200

@app.route("/performance", methods=["GET"])
def performance_html():
    try:
        days = int(request.args.get("days", "14"))
    except Exception:
        days = 14
    perf = _compute_performance(days)

    pnl = perf.get("realized_pnl_est", 0)
    pnl_color = "#16a34a" if (pnl or 0) >= 0 else "#dc2626"

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Performance (last {days} day(s))</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color: #111; }}
    .card {{ border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin-bottom:16px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ text-align: left; padding: 8px 6px; border-bottom: 1px solid #f3f4f6; }}
    .muted {{ color: #6b7280; }}
  </style>
</head>
<body>
  <h1>Performance</h1>
  <div class="muted">Window: last {days} day(s)</div>

  <div class="card">
    <div><strong>Equity:</strong> {perf.get("equity")} &nbsp; <span class="muted">(Prev: {perf.get("last_equity")})</span></div>
    <div><strong>Realized P&amp;L (est):</strong> <span style="color:{pnl_color}">{perf.get("realized_pnl_est")}</span></div>
    <div><strong>Win rate (est):</strong> {perf.get("winrate_pct_est")}% &nbsp; <span class="muted">({perf.get("wins")} W / {perf.get("losses")} L)</span></div>
  </div>

  <div class="card">
    <h3>By Symbol</h3>
    <table>
      <thead><tr><th>Symbol</th><th>Avg Buy</th><th>Avg Sell</th><th>Matched Qty</th><th>Realized P&amp;L (est)</th></tr></thead>
      <tbody>
        {''.join(f"<tr><td>{d['symbol']}</td><td>{d['avg_buy']}</td><td>{d['avg_sell']}</td><td>{d['matched_qty']}</td><td>{d['realized_pnl']}</td></tr>" for d in perf.get("symbols", []))}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h3>Open Positions</h3>
    <table>
      <thead><tr><th>Symbol</th><th>Qty</th><th>Avg Entry</th><th>Unrealized P&amp;L</th></tr></thead>
      <tbody>
        {''.join(f"<tr><td>{p.get('symbol')}</td><td>{p.get('qty')}</td><td>{p.get('avg_entry_price')}</td><td>{p.get('unrealized_pl')}</td></tr>" for p in perf.get("open_positions", []))}
      </tbody>
    </table>
  </div>

  <div class="muted">Need JSON? Use <code>/performance.json?days={days}</code>.</div>
</body>
</html>
"""
    return Response(html, mimetype="text/html")

# -------------------------
# Sync to Notion (optional)
# -------------------------
@app.route("/sync_trades", methods=["POST", "GET"])
def sync_trades():
    if not (NOTION_TOKEN and NOTION_DB_TRADES):
        return jsonify(ok=False, error="Notion env not configured"), 400
    try:
        minutes = int(request.args.get("minutes", "60"))
    except Exception:
        minutes = 60
    after_iso = (datetime.utcnow() - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    orders = list_closed_orders(after_iso=after_iso)

    created = 0
    skipped = 0
    for o in orders:
        if o.get("status") != "filled":
            continue
        res = notion_upsert_trade(o)
        if res.get("ok") and not res.get("skipped"):
            created += 1
        else:
            skipped += 1

    return jsonify(ok=True, created=created, skipped=skipped, window_minutes=minutes), 200

# -------------------------
# Minimal dashboard (optional)
# -------------------------
@app.route("/dashboard", methods=["GET"])
def dashboard():
    try:
        days = int(request.args.get("days", "7"))
    except Exception:
        days = 7

    with app.test_request_context(f"/performance.json?days={days}"):
        perf_resp = performance_json()
    perf = perf_resp.get_json()

    if not perf.get("ok"):
        return Response(f"<h2>Error</h2><pre>{perf}</pre>", mimetype="text/html")

    equity = perf.get("equity")
    last_eq = perf.get("last_equity")
    pnl = perf.get("realized_pnl_est")
    winrate = perf.get("winrate_pct_est")
    symbols = perf.get("symbols", [])
    positions = perf.get("open_positions", [])
    pnl_color = "#16a34a" if (pnl or 0) >= 0 else "#dc2626"

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Trading Dashboard</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color: #111; }}
    .card {{ border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin-bottom:16px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ text-align: left; padding: 8px 6px; border-bottom: 1px solid #f3f4f6; }}
    .muted {{ color: #6b7280; }}
  </style>
</head>
<body>
  <h1>Trading Dashboard</h1>
  <div class="muted">Window: last {days} day(s)</div>

  <div class="card">
    <div><strong>Equity:</strong> {equity} &nbsp; <span class="muted">(Prev: {last_eq})</span></div>
    <div><strong>Realized P&amp;L (est):</strong> <span style="color:{pnl_color}">{pnl}</span></div>
    <div><strong>Win rate (est):</strong> {winrate}%</div>
  </div>

  <div class="card">
    <h3>By Symbol</h3>
    <table>
      <thead><tr><th>Symbol</th><th>Avg Buy</th><th>Avg Sell</th><th>Matched Qty</th><th>Realized P&amp;L (est)</th></tr></thead>
      <tbody>
        {''.join(f"<tr><td>{d['symbol']}</td><td>{d['avg_buy']}</td><td>{d['avg_sell']}</td><td>{d['matched_qty']}</td><td>{d['realized_pnl']}</td></tr>" for d in symbols)}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h3>Open Positions</h3>
    <table>
      <thead><tr><th>Symbol</th><th>Qty</th><th>Avg Entry</th><th>Unrealized P&amp;L</th></tr></thead>
      <tbody>
        {''.join(f"<tr><td>{p.get('symbol')}</td><td>{p.get('qty')}</td><td>{p.get('avg_entry_price')}</td><td>{p.get('unrealized_pl')}</td></tr>" for p in positions)}
      </tbody>
    </table>
  </div>

  <div class="muted">JSON view: <code>/performance.json?days={days}</code></div>
</body>
</html>
"""
    return Response(html, mimetype="text/html")

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")))
