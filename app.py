import os, json, requests, math
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+
from flask import Flask, request, jsonify

app = Flask(__name__)

# ---- ENV CONFIG ----
BASE = os.getenv("ALPACA_BASE", "https://paper-api.alpaca.markets").rstrip("/")
if BASE.endswith("/v2"):
    BASE = BASE[:-3]  # strip accidental /v2

KEY  = os.getenv("ALPACA_KEY", "")
SEC  = os.getenv("ALPACA_SECRET", "")

# Symbol controls
WHITELIST = [s.strip().upper() for s in os.getenv("WHITELIST", "SPY,QQQ,TSLA,NVDA").split(",")]
DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "10"))
QTY_MAP = {}
try:
    # Optional JSON like: {"SPY":10,"QQQ":8,"TSLA":1,"NVDA":2}
    QTY_MAP = json.loads(os.getenv("QTY_MAP", "{}"))
except Exception:
    QTY_MAP = {}

# Risk controls
DAILY_TARGET = float(os.getenv("DAILY_TARGET", "500"))   # stop new entries if >= +$500 on the day
DAILY_STOP   = float(os.getenv("DAILY_STOP", "-250"))    # stop new entries if <= -$250 on the day
MAX_OPEN_ORDERS_PER_SYMBOL = int(os.getenv("MAX_OPEN_ORDERS_PER_SYMBOL", "1"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "4"))

# Security (optional)
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")  # set this and send header X-Webhook-Token to restrict access

# Strategy-specific defaults (percent stops/targets)
# You can tune these later.
STRATEGY = {
    "SPY_VWAP_EMA20": {  # Strategy #6 (time window 10:00–12:00 ET)
        "stop_pct": 0.003,   # 0.30%
        "tp_pct":   0.006,   # 0.60%
        "time_window": ("10:00", "12:00"),  # ET
        "rth_only": True
    },
    "SMA10D_MACD": {   # Strategy #2 (allow all RTH)
        "stop_pct": 0.005,   # 0.50%
        "tp_pct":   0.010,   # 1.00%
        "time_window": None, # no extra window
        "rth_only": True
    }
}

# ---- HELPERS ----
def headers():
    return {"APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SEC, "Content-Type": "application/json"}

def alpaca_get(path):
    r = requests.get(f"{BASE}{path}", headers=headers(), timeout=10)
    r.raise_for_status()
    return r.json()

def alpaca_post(path, body):
    r = requests.post(f"{BASE}{path}", headers=headers(), data=json.dumps(body), timeout=10)
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

def market_is_open_et():
    # This very simple guard uses current ET time.
    # For production, you can hit Alpaca calendar/clock, but this avoids data subs.
    now_et = datetime.now(ZoneInfo("America/New_York")).time()
    return (now_et >= datetime.strptime("09:30","%H:%M").time() and
            now_et <= datetime.strptime("16:00","%H:%M").time())

def within_time_window(system_name):
    cfg = STRATEGY.get(system_name, {})
    window = cfg.get("time_window")
    if not window:
        return True
    start, end = window  # "HH:MM"
    now_et = datetime.now(ZoneInfo("America/New_York")).time()
    return (now_et >= datetime.strptime(start,"%H:%M").time() and
            now_et <= datetime.strptime(end,"%H:%M").time())

def qty_for(symbol):
    return int(QTY_MAP.get(symbol, DEFAULT_QTY))

def round_px(px):
    # 2 decimals is fine for US equities
    return float(f"{px:.2f}")

def place_bracket(symbol, side, qty, entry_price, tp_pct, stop_pct):
    # side: "buy" or "sell"
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
        "stop_loss":   {"stop_price":  str(sl)}
    }
    return alpaca_post("/v2/orders", body)

# ---- ROUTES ----
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
        "acct": acct
    }), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    # Get body first so we can check token in body too
    data = request.get_json(force=True, silent=True) or {}

    # Accept token from header OR body
    if WEBHOOK_TOKEN:
        header_token = request.headers.get("X-Webhook-Token")
        body_token = data.get("token")
        if WEBHOOK_TOKEN not in (header_token, body_token):
            return jsonify(ok=False, error="Unauthorized"), 401



    data = request.get_json(force=True, silent=True) or {}

    system = str(data.get("system", "unknown"))
    side_in = str(data.get("side", "")).lower()     # long/short/buy/sell
    symbol  = str(data.get("ticker", "")).upper()
    price   = data.get("price", None)

    if not KEY or not SEC:
        return jsonify(ok=False, error="Missing ALPACA_KEY/ALPACA_SECRET"), 400
    if symbol not in WHITELIST:
        return jsonify(ok=False, error=f"{symbol} not in whitelist"), 400
    if side_in not in ("long","short","buy","sell"):
        return jsonify(ok=False, error="side must be LONG/SHORT/BUY/SELL"), 400

    # Strategy gating
    cfg = STRATEGY.get(system, STRATEGY["SMA10D_MACD"])  # default to conservative config
    if cfg.get("rth_only", True) and not market_is_open_et():
        return jsonify(ok=True, skipped=True, reason="Market closed (RTH only)"), 200
    if not within_time_window(system):
        return jsonify(ok=True, skipped=True, reason="Outside strategy time window"), 200

    # Daily P&L gate
    acct = get_account()
    equity = float(acct["equity"])
    last_equity = float(acct.get("last_equity", acct["equity"]))
    pnl = equity - last_equity
    if pnl >= DAILY_TARGET or pnl <= DAILY_STOP:
        return jsonify(ok=True, skipped=True, reason="Daily P&L limit reached", pnl=pnl), 200

    # Position / orders gate
    open_positions = list_positions()
    if len(open_positions) >= MAX_POSITIONS:
        return jsonify(ok=True, skipped=True, reason="Max positions reached"), 200
    open_orders = list_open_orders(symbol=symbol)
    if len(open_orders) >= MAX_OPEN_ORDERS_PER_SYMBOL:
        return jsonify(ok=True, skipped=True, reason="Open order exists for symbol"), 200

    # Side & quantity
    side = "buy" if side_in in ("long","buy") else "sell"
    qty = qty_for(symbol)

    # Entry price anchor: prefer TradingView's {{close}} if present
    if price is None:
        # As a fallback, you could query quotes here, but that may require a data plan.
        # We'll require price in the message to avoid data dependency.
        return jsonify(ok=False, error="Missing 'price' in payload"), 400
    entry_px = float(price)

    # Place bracket order
    order = place_bracket(
        symbol=symbol,
        side=side,
        qty=qty,
        entry_price=entry_px,
        tp_pct=cfg["tp_pct"],
        stop_pct=cfg["stop_pct"]
    )

    return jsonify(ok=True, system=system, order=order), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")))
    