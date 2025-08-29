# app.py
import os, json, math, time, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# =========================
# ENV / CONFIG
# =========================
BASE = os.getenv("ALPACA_BASE", "https://paper-api.alpaca.markets").rstrip("/")
if BASE.endswith("/v2"):
    BASE = BASE[:-3]

KEY  = os.getenv("ALPACA_KEY", "")
SEC  = os.getenv("ALPACA_SECRET", "")

WHITELIST = [s.strip().upper() for s in os.getenv("WHITELIST", "SPY,QQQ,TSLA,NVDA").split(",")]
DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))

# per-symbol overrides, e.g. {"SPY":5,"QQQ":2}
try:
    QTY_MAP = json.loads(os.getenv("QTY_MAP", "{}"))
except Exception:
    QTY_MAP = {}

DAILY_TARGET = float(os.getenv("DAILY_TARGET", "200"))
DAILY_STOP   = float(os.getenv("DAILY_STOP", "-100"))
MAX_OPEN_ORDERS_PER_SYMBOL = int(os.getenv("MAX_OPEN_ORDERS_PER_SYMBOL", "1"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "1"))

WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")

# Notion config (optional but recommended)
NOTION_TOKEN     = os.getenv("NOTION_TOKEN", "")
NOTION_DB_TRADES = os.getenv("NOTION_DB_TRADES", "")  # Performance Tracker DB ID
NOTION_API       = "https://api.notion.com/v1"
NOTION_VERSION   = "2022-06-28"

# Per-strategy defaults (percent TP/SL and time window). Times are ET.
STRATEGY = {
    "SPY_VWAP_EMA20": {  # Strategy #6
        "stop_pct": 0.003,
        "tp_pct":   0.006,
        "time_window": ("10:00", "12:00"),  # ET
        "rth_only": True
    },
    "SMA10D_MACD": {     # Strategy #2
        "stop_pct": 0.005,
        "tp_pct":   0.010,
        "time_window": None,
        "rth_only": True
    }
}

# =========================
# HELPERS: Alpaca
# =========================
def headers():
    return {"APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SEC, "Content-Type": "application/json"}

def alpaca_get(path):
    r = requests.get(f"{BASE}{path}", headers=headers(), timeout=15)
    r.raise_for_status()
    return r.json()

def alpaca_post(path, body):
    r = requests.post(f"{BASE}{path}", headers=headers(), data=json.dumps(body), timeout=15)
    r.raise_for_status()
    return r.json()

def get_account():
    return alpaca_get("/v2/account")

def list_positions():
    try:
        return alpaca_get("/v2/positions")
    except Exception:
        return []

def list_open_orders(symbol=None):
    path = "/v2/orders?status=open"
    if symbol:
        path += f"&symbols={symbol}"
    try:
        return alpaca_get(path)
    except Exception:
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
    # Build a client_order_id that embeds the system name + epoch seconds
    cli_id = f"{system[:18]}-{int(time.time())}"  # Alpaca limit is 48 chars; keep it short

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

# =========================
# HELPERS: Notion
# =========================
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
        "filter": {
            "property": "OrderId",
            "rich_text": {"equals": order_id}
        }
    }
    r = requests.post(url, headers=notion_headers(), data=json.dumps(body), timeout=15)
    r.raise_for_status()
    res = r.json()
    if res.get("results"):
        return res["results"][0]
    return None

def notion_upsert_trade(order: dict):
    """Create a Notion row for a filled order if not exists."""
    if not (NOTION_TOKEN and NOTION_DB_TRADES):
        return {"ok": False, "skipped": True, "reason": "Notion env not configured"}

    order_id = order.get("id")
    if not order_id:
        return {"ok": False, "skipped": True, "reason": "Order missing id"}

    exists = notion_find_by_order_id(order_id)
    if exists:
        return {"ok": True, "skipped": True, "reason": "Already logged"}

    # Extract fields
    sym   = order.get("symbol")
    side  = order.get("side")
    qty   = float(order.get("filled_qty", "0") or 0)
    avgpx = float(order.get("filled_avg_price", "0") or 0)
    filled_at = order.get("filled_at") or order.get("updated_at") or datetime.utcnow().isoformat()+"Z"
    cli_id = order.get("client_order_id", "")
    # try to recover strategy from client_order_id prefix "SYSTEM-epoch"
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

# =========================
# ROUTES
# =========================
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

    # Auth: accept header OR body token
    if WEBHOOK_TOKEN:
        header_token = request.headers.get("X-Webhook-Token")
        body_token = data.get("token")
        if WEBHOOK_TOKEN not in (header_token, body_token):
            return jsonify(ok=False, error="Unauthorized"), 401

    system = str(data.get("system", "unknown"))
    side_in = str(data.get("side", "")).lower()     # long/short/buy/sell
    symbol  = str(data.get("ticker", "")).upper()
    price   = data.get("price", None)

    if not KEY or not SEC:
        return jsonify(ok=False, error="Missing ALPACA_KEY/ALPACA_SECRET"), 400
    if symbol not in WHITELIST:
        return jsonify(ok=True, skipped=True, reason=f"{symbol} not in whitelist"), 200
    if side_in not in ("long","short","buy","sell"):
        return jsonify(ok=False, error="side must be LONG/SHORT/BUY/SELL"), 400

    cfg = STRATEGY.get(system, STRATEGY["SMA10D_MACD"])

    # Time gates
    if cfg.get("rth_only", True) and not market_is_open_et():
        return jsonify(ok=True, skipped=True, reason="Market closed (RTH only)"), 200
    if not within_time_window(system):
        return jsonify(ok=True, skipped=True, reason="Outside strategy time window"), 200

    # Daily P&L gate
    acct = get_account()
    equity = float(acct.get("equity", 0))
    last_equity = float(acct.get("last_equity", equity))
    pnl = equity - last_equity
    if pnl >= DAILY_TARGET or pnl <= DAILY_STOP:
        return jsonify(ok=True, skipped=True, reason="Daily P&L limit reached", pnl=pnl), 200

    # Exposure gates
    positions = list_positions()
    if len(positions) >= MAX_POSITIONS:
        return jsonify(ok=True, skipped=True, reason="Max positions reached"), 200
    open_orders = list_open_orders(symbol=symbol)
    if len(open_orders) >= MAX_OPEN_ORDERS_PER_SYMBOL:
        return jsonify(ok=True, skipped=True, reason="Open order exists for symbol"), 200

    # side & qty
    side = "buy" if side_in in ("long","buy") else "sell"
    qty  = qty_for(symbol)

    if price is None:
        return jsonify(ok=False, error="Missing 'price' in payload"), 400
    entry_px = float(price)

    # Place bracket order; include system in client_order_id for later Notion mapping
    order = place_bracket(
        symbol=symbol,
        side=side,
        qty=qty,
        entry_price=entry_px,
        tp_pct=cfg["tp_pct"],
        stop_pct=cfg["stop_pct"],
        system=system
    )
    return jsonify(ok=True, system=system, order=order), 200

# ----- PERFORMANCE: JSON + HTML -----
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
            "avg_buy": round(d["buy_value"]/d["buy_qty"], 4) if d["buy_qty"] else None,
            "avg_sell": round(d["sell_value"]/d["sell_qty"], 4) if d["sell_qty"] else None,
            "matched_qty": matched_qty,
            "realized_pnl": round(realized, 2),
            "orders": d["orders"][-5:]
        })

    total_trades = wins + losses
    winrate = round(100 * wins / total_trades, 2) if total_trades else 0.0
    acct = get_account()
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
    """HTML view (use ?days=N)."""
    try:
        days = int(request.args.get("days", "14"))
    except Exception:
        days = 14
    perf = _compute_performance(days)

    pnl = perf.get("realized_pnl_est", 0)
    pnl_color = "#16a34a" if pnl >= 0 else "#dc2626"

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
    <div><strong>Realized P&amp;L (est):</strong> <span style="color:{pnl_color}">{pnl}</span></div>
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

# ----- SYNC TO NOTION -----
@app.route("/sync_trades", methods=["POST", "GET"])
def sync_trades():
    """Fetch closed orders from the last ?minutes (default 60) and log fills to Notion."""
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

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")))
