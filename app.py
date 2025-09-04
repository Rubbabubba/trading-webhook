# app.py
import os
import sys
import json
import time
import uuid
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# ---------- tiny logger ----------
def log(msg: str) -> None:
    print(msg, file=sys.stdout, flush=True)

# ---------- ENV / CONFIG ----------
BASE = os.getenv("ALPACA_BASE", "https://paper-api.alpaca.markets").rstrip("/")
if BASE.endswith("/v2"):
    BASE = BASE[:-3]  # normalize if someone pasted .../v2

KEY = os.getenv("ALPACA_KEY", "")
SEC = os.getenv("ALPACA_SECRET", "")

# global whitelist fallback (used if a strategy doesn't define its own)
WHITELIST = [s.strip().upper() for s in os.getenv(
    "WHITELIST",
    "SPY,QQQ,TSLA,NVDA,COIN,GOOGL,META,MSFT,AMZN,AAPL"
).split(",") if s.strip()]

try:
    DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))
except Exception:
    DEFAULT_QTY = 1

try:
    QTY_MAP = json.loads(os.getenv("QTY_MAP", "{}"))
except Exception:
    QTY_MAP = {}

try:
    DAILY_TARGET = float(os.getenv("DAILY_TARGET", "200"))
    DAILY_STOP   = float(os.getenv("DAILY_STOP", "-100"))
except Exception:
    DAILY_TARGET = 200.0
    DAILY_STOP   = -100.0

try:
    MAX_OPEN_ORDERS_PER_SYMBOL = int(os.getenv("MAX_OPEN_ORDERS_PER_SYMBOL", "1"))
    MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "2"))
except Exception:
    MAX_OPEN_ORDERS_PER_SYMBOL = 1
    MAX_POSITIONS = 2

WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")

# Notion (optional)
NOTION_TOKEN     = os.getenv("NOTION_TOKEN", "")
NOTION_DB_TRADES = os.getenv("NOTION_DB_TRADES", "")
NOTION_API       = "https://api.notion.com/v1"
NOTION_VERSION   = "2022-06-28"

# Strategy configs
STRATEGY = {
    # Strategy 1 (example): SPY VWAP + EMA20 with a mid-morning window and tighter brackets
    "SPY_VWAP_EMA20": {
        "stop_pct": 0.003,   # 0.3%
        "tp_pct":   0.006,   # 0.6%
        "time_window": ("10:00", "12:00"),  # ET
        "rth_only": True,
        "whitelist": ["SPY"],
    },
    # Strategy 2: SMA10D + MACD + Midnight bias (broad set of tickers)
    "SMA10D_MACD": {
        "stop_pct": 0.005,   # 0.5%
        "tp_pct":   0.010,   # 1.0%
        "time_window": None, # full RTH
        "rth_only": True,
        "whitelist": ["SPY","QQQ","NVDA","TSLA","COIN","GOOGL","META","MSFT","AMZN","AAPL"],
    },
}

# ---------- Alpaca helpers ----------
def _alpaca_headers():
    return {
        "APCA-API-KEY-ID": KEY,
        "APCA-API-SECRET-KEY": SEC,
        "Content-Type": "application/json"
    }

def alpaca_get(path: str):
    r = requests.get(f"{BASE}{path}", headers=_alpaca_headers(), timeout=15)
    if not r.ok:
        try:
            err = r.json()
        except Exception:
            err = r.text
        log(f"ALPACA_GET_ERROR path={path} status={r.status_code} body={err}")
    r.raise_for_status()
    return r.json()

def alpaca_post(path: str, body: dict):
    r = requests.post(f"{BASE}{path}", headers=_alpaca_headers(), data=json.dumps(body), timeout=15)
    if not r.ok:
        try:
            err = r.json()
        except Exception:
            err = r.text
        log(f"ALPACA_POST_ERROR path={path} status={r.status_code} body={err} sent={body}")
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

def list_open_orders(symbol: str | None = None):
    path = "/v2/orders?status=open&limit=500"
    if symbol:
        path += f"&symbols={symbol}"
    try:
        return alpaca_get(path)
    except Exception as e:
        log(f"list_open_orders error: {e}")
        return []

def list_closed_orders(after_iso: str | None = None):
    if after_iso is None:
        after_iso = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = f"/v2/orders?status=closed&limit=500&after={after_iso}"
    return alpaca_get(path)

def market_is_open_et() -> bool:
    # simple RTH check
    now_et = datetime.now(ZoneInfo("America/New_York")).time()
    open_t  = datetime.strptime("09:30", "%H:%M").time()
    close_t = datetime.strptime("16:00", "%H:%M").time()
    return open_t <= now_et <= close_t

def within_time_window(system_name: str) -> bool:
    cfg = STRATEGY.get(system_name, {})
    window = cfg.get("time_window")
    if not window:
        return True
    start, end = window
    now_et = datetime.now(ZoneInfo("America/New_York")).time()
    return (datetime.strptime(start, "%H:%M").time()
            <= now_et
            <= datetime.strptime(end, "%H:%M").time())

def qty_for(symbol: str) -> int:
    try:
        return int(QTY_MAP.get(symbol, DEFAULT_QTY))
    except Exception:
        return DEFAULT_QTY

def round_px(px: float) -> float:
    return float(f"{px:.2f}")

def place_bracket(symbol: str, side: str, qty: int, entry_price: float, tp_pct: float, stop_pct: float, system: str):
    # unique client id to avoid duplicate 422 errors
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
        "client_order_id": cli_id,
    }
    return alpaca_post("/v2/orders", body)

# ---------- Notion (optional) ----------
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
    return res.get("results", [None])[0]

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
    try:
        qty   = float(order.get("filled_qty", "0") or 0)
        avgpx = float(order.get("filled_avg_price", "0") or 0)
    except Exception:
        qty, avgpx = 0.0, 0.0
    filled_at = order.get("filled_at") or order.get("updated_at") or (datetime.utcnow().isoformat() + "Z")
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
        "Strategy": {"rich_text": [{"text": {"content": system}}]},
    }
    url = f"{NOTION_API}/pages"
    body = {"parent": {"database_id": NOTION_DB_TRADES}, "properties": props}
    r = requests.post(url, headers=notion_headers(), data=json.dumps(body), timeout=15)
    r.raise_for_status()
    return {"ok": True, "skipped": False}

# ---------- routes ----------
@app.route("/", methods=["GET"])
def health():
    return "OK", 200

@app.route("/status", methods=["GET"])
def status():
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
        "notion_enabled": bool(NOTION_TOKEN and NOTION_DB_TRADES),
    }), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True) or {}
    log(f"WEBHOOK IN: {data}")

    # auth
    if WEBHOOK_TOKEN:
        header_token = request.headers.get("X-Webhook-Token")
        body_token = data.get("token")
        if WEBHOOK_TOKEN not in (header_token, body_token):
            log("SKIP: Unauthorized (token mismatch)")
            return jsonify(ok=False, error="Unauthorized"), 401

    system = str(data.get("system", "unknown"))
    side_in = str(data.get("side", "")).lower()
    symbol = str(data.get("ticker", "")).upper()
    price = data.get("price", None)

    if not KEY or not SEC:
        return jsonify(ok=False, error="Missing ALPACA_KEY/ALPACA_SECRET"), 400
    if not symbol:
        return jsonify(ok=False, error="Missing 'ticker'"), 400

    # whitelist (strategy-specific overrides if present)
    strat_wl = STRATEGY.get(system, {}).get("whitelist")
    effective_wl = set(strat_wl or WHITELIST)
    if symbol not in effective_wl:
        log(f"SKIP: {symbol} not in whitelist for {system}")
        return jsonify(ok=True, skipped=True, reason=f"{symbol} not in whitelist"), 200

    if side_in not in ("long", "short", "buy", "sell"):
        return jsonify(ok=False, error="side must be LONG/SHORT/BUY/SELL"), 400

    cfg = STRATEGY.get(system, STRATEGY["SMA10D_MACD"])

    # time gates
    if cfg.get("rth_only", True) and not market_is_open_et():
        log("SKIP: Market closed (RTH only)")
        return jsonify(ok=True, skipped=True, reason="Market closed (RTH only)"), 200
    if not within_time_window(system):
        log("SKIP: Outside strategy time window")
        return jsonify(ok=True, skipped=True, reason="Outside strategy time window"), 200

    # daily P&L gate
    acct = get_account()
    try:
        equity = float(acct.get("equity", 0))
        last_equity = float(acct.get("last_equity", equity))
    except Exception:
        equity = 0.0
        last_equity = 0.0
    pnl = equity - last_equity
    if pnl >= DAILY_TARGET or pnl <= DAILY_STOP:
        log(f"SKIP: Daily PnL {pnl} limits {DAILY_STOP}/{DAILY_TARGET}")
        return jsonify(ok=True, skipped=True, reason="Daily P&L limit reached", pnl=pnl), 200

    # exposure gates
    positions = list_positions()
    if len(positions) >= MAX_POSITIONS:
        log("SKIP: Max positions reached")
        return jsonify(ok=True, skipped=True, reason="Max positions reached"), 200

    # ignore OCO children; only count parent orders per symbol
    open_orders = [o for o in list_open_orders(symbol=symbol) if not o.get("parent_order_id")]
    if len(open_orders) >= MAX_OPEN_ORDERS_PER_SYMBOL:
        log("SKIP: Open parent order exists for symbol")
        return jsonify(ok=True, skipped=True, reason="Open order exists for symbol"), 200

    side = "buy" if side_in in ("long", "buy") else "sell"
    if price is None:
        return jsonify(ok=False, error="Missing 'price' in payload"), 400

    entry_px = float(price)
    qty = qty_for(symbol)

    # place order
    try:
        log(f"PLACE: {symbol} {side} qty={qty} @ {entry_px} system={system}")
        order = place_bracket(
            symbol=symbol,
            side=side,
            qty=qty,
            entry_price=entry_px,
            tp_pct=cfg["tp_pct"],
            stop_pct=cfg["stop_pct"],
            system=system,
        )
        log(f"ORDER OK: {order.get('id')} class={order.get('order_class')}")
        return jsonify(ok=True, system=system, order=order), 200
    except requests.HTTPError as e:
        log(f"ORDER REJECTED: {e}")
        # include alpaca error in response for easier debugging
        try:
            err = e.response.json()
        except Exception:
            err = str(e)
        return jsonify(ok=False, error="alpaca_rejected", detail=err), 400
    except Exception as e:
        log(f"ORDER ERROR: {e}")
        return jsonify(ok=False, error="unknown_error", detail=str(e)), 500

# ---------- PERFORMANCE ----------
def _compute_performance(days: int) -> dict:
    after_iso = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    orders = list_closed_orders(after_iso=after_iso)

    by_symbol = {}
    fills = []  # NEW: flat list of each filled order

    for o in orders:
        if o.get("status") != "filled":
            continue

        sym = o.get("symbol")
        side = o.get("side")
        try:
            qty = float(o.get("filled_qty", "0") or 0)
            avgpx = float(o.get("filled_avg_price", "0") or 0)
        except Exception:
            qty, avgpx = 0.0, 0.0
        if qty == 0:
            continue

        # collect the fill row
        fills.append({
            "filled_at": o.get("filled_at") or o.get("updated_at") or o.get("submitted_at"),
            "symbol": sym,
            "side": side,
            "qty": qty,
            "avg_price": avgpx,
            "order_class": o.get("order_class"),
            "id": o.get("id"),
            "client_order_id": o.get("client_order_id")
        })

        d = by_symbol.setdefault(sym, {
            "buy_qty": 0.0, "buy_value": 0.0,
            "sell_qty": 0.0, "sell_value": 0.0,
            "orders": []
        })
        if side == "buy":
            d["buy_qty"] += qty
            d["buy_value"] += qty * avgpx
        else:
            d["sell_qty"] += qty
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

    # simple realized PnL per symbol via average-price matching
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
        if realized > 0:
            wins += 1
        elif realized < 0:
            losses += 1

        details.append({
            "symbol": sym,
            "avg_buy": round(d["buy_value"]/d["buy_qty"], 4) if d["buy_qty"] else None,
            "avg_sell": round(d["sell_value"]/d["sell_qty"], 4) if d["sell_qty"] else None,
            "matched_qty": matched_qty,
            "realized_pnl": round(realized, 2),
            "orders": d["orders"][-5:]
        })

    total_trades = wins + losses
    winrate = round(100.0 * wins / total_trades, 2) if total_trades else 0.0

    try:
        acct = get_account()
    except Exception as e:
        acct = {"error": str(e)}

    positions = list_positions()

    # Sort fills newest-first for nicer display
    try:
        fills.sort(key=lambda r: (r.get("filled_at") or ""), reverse=True)
    except Exception:
        pass

    return {
        "ok": True,
        "window_days": days,
        "equity": acct.get("equity"),
        "last_equity": acct.get("last_equity"),
        "realized_pnl_est": round(total_pnl, 2),
        "winrate_pct_est": winrate,
        "wins": wins,
        "losses": losses,
        "symbols": details,
        "open_positions": positions,
        "fills": fills  # NEW
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
        days = int(request.args.get("days", "7"))
    except Exception:
        days = 7

    perf = _compute_performance(days)
    pnl = perf.get("realized_pnl_est", 0.0)
    pnl_color = "#16a34a" if (pnl or 0.0) >= 0.0 else "#dc2626"

    # by-symbol rows
    rows_html = ""
    for d in perf.get("symbols", []):
        rows_html += (
            "<tr>"
            "<td>" + str(d.get("symbol")) + "</td>"
            "<td>" + str(d.get("avg_buy")) + "</td>"
            "<td>" + str(d.get("avg_sell")) + "</td>"
            "<td>" + str(d.get("matched_qty")) + "</td>"
            "<td>" + str(d.get("realized_pnl")) + "</td>"
            "</tr>"
        )

    # open positions rows
    pos_html = ""
    for p in perf.get("open_positions", []):
        pos_html += (
            "<tr>"
            "<td>" + str(p.get("symbol")) + "</td>"
            "<td>" + str(p.get("qty")) + "</td>"
            "<td>" + str(p.get("avg_entry_price")) + "</td>"
            "<td>" + str(p.get("unrealized_pl")) + "</td>"
            "</tr>"
        )

    # filled orders (flat list) rows
    fills_html = ""
    for f in perf.get("fills", []):
        fills_html += (
            "<tr>"
            "<td>" + str(f.get("filled_at")) + "</td>"
            "<td>" + str(f.get("symbol")) + "</td>"
            "<td>" + str(f.get("side")).upper() + "</td>"
            "<td>" + str(f.get("qty")) + "</td>"
            "<td>" + str(f.get("avg_price")) + "</td>"
            "<td>" + str(f.get("order_class")) + "</td>"
            "<td style='max-width:260px;overflow-wrap:anywhere'>" + str(f.get("client_order_id")) + "</td>"
            "</tr>"
        )

    html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Performance</title>"
        "<style>"
        "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px;color:#111}"
        ".card{border:1px solid #e5e7eb;border-radius:12px;padding:16px;margin-bottom:16px}"
        "table{border-collapse:collapse;width:100%}"
        "th,td{text-align:left;padding:8px 6px;border-bottom:1px solid #f3f4f6;vertical-align:top}"
        ".muted{color:#6b7280}"
        ".pill{display:inline-block;padding:2px 6px;border-radius:999px;border:1px solid #e5e7eb;font-size:12px}"
        "</style></head><body>"
        "<h1>Performance</h1>"
        "<div class='card'>"
        "<div><strong>Window:</strong> last " + str(days) + " day(s)</div>"
        "<div><strong>Equity:</strong> " + str(perf.get("equity")) + " "
        "<span class='muted'>(Prev: " + str(perf.get("last_equity")) + ")</span></div>"
        "<div><strong>Realized P&amp;L (est):</strong> "
        "<span style='color:" + pnl_color + "'>" + str(pnl) + "</span></div>"
        "<div><strong>Win rate (est):</strong> "
        + str(perf.get("winrate_pct_est")) + "% "
        "<span class='muted'>(" + str(perf.get("wins")) + " W / " + str(perf.get("losses")) + " L)</span></div>"
        "</div>"

        "<div class='card'>"
        "<h3>By Symbol</h3>"
        "<table><thead><tr><th>Symbol</th><th>Avg Buy</th><th>Avg Sell</th>"
        "<th>Matched Qty</th><th>Realized P&amp;L (est)</th></tr></thead>"
        "<tbody>" + rows_html + "</tbody></table>"
        "</div>"

        "<div class='card'>"
        "<h3>Open Positions</h3>"
        "<table><thead><tr><th>Symbol</th><th>Qty</th><th>Avg Entry</th><th>Unrealized P&amp;L</th></tr></thead>"
        "<tbody>" + pos_html + "</tbody></table>"
        "</div>"

        "<div class='card'>"
        "<h3>Filled Orders (last " + str(days) + " day(s))</h3>"
        "<table><thead><tr>"
        "<th>Filled At (UTC)</th><th>Symbol</th><th>Side</th><th>Qty</th>"
        "<th>Avg Price</th><th>Order Class</th><th>Client Order ID</th>"
        "</tr></thead>"
        "<tbody>" + fills_html + "</tbody></table>"
        "</div>"

        "<div class='muted'>JSON: <code>/performance.json?days=" + str(days) + "</code></div>"
        "</body></html>"
    )
    return Response(html, mimetype="text/html")

@app.route("/dashboard", methods=["GET"])
def dashboard():
    # just reuse the performance HTML (7-day default if not passed)
    return performance_html()

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

# ---------- main ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
