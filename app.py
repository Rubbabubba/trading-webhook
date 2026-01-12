import os
from flask import Flask, request, jsonify

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
from alpaca.trading.requests import TakeProfitRequest, StopLossRequest

app = Flask(__name__)

# -----------------------------
# Environment
# -----------------------------
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

APCA_KEY = os.getenv("APCA_API_KEY_ID", "").strip()
APCA_SECRET = os.getenv("APCA_API_SECRET_KEY", "").strip()
APCA_PAPER = os.getenv("APCA_PAPER", "true").lower() == "true"

# Risk controls (safe defaults for $500)
RISK_DOLLARS = float(os.getenv("RISK_DOLLARS", "3"))          # dollars risked per trade
STOP_PCT = float(os.getenv("STOP_PCT", "0.003"))              # 0.30%
TAKE_PCT = float(os.getenv("TAKE_PCT", "0.006"))              # 0.60%
MIN_QTY = float(os.getenv("MIN_QTY", "0.01"))                 # fractional shares
MAX_QTY = float(os.getenv("MAX_QTY", "1.5"))                  # keep small for $500
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"     # if true: don't place orders

# Only allow these symbols (safety)
ALLOWED_SYMBOLS = set(s.strip().upper() for s in os.getenv("ALLOWED_SYMBOLS", "SPY").split(","))

if not APCA_KEY or not APCA_SECRET:
    # Render will still boot, but webhook will error until env vars are set
    pass

trading_client = TradingClient(APCA_KEY, APCA_SECRET, paper=APCA_PAPER)

# -----------------------------
# Helpers
# -----------------------------
def _bad(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code

def compute_qty(price: float) -> float:
    """
    Risk-based position size using percentage stop:
    risk_per_share = price * STOP_PCT
    qty = RISK_DOLLARS / risk_per_share
    """
    if price <= 0:
        raise ValueError("Price must be > 0")

    risk_per_share = price * STOP_PCT
    if risk_per_share <= 0:
        raise ValueError("STOP_PCT must be > 0")

    qty = RISK_DOLLARS / risk_per_share

    # Bound and round to 2 decimals for fractional shares
    qty = max(qty, MIN_QTY)
    qty = min(qty, MAX_QTY)
    qty = round(qty, 2)

    return qty

def build_bracket_prices(price: float, side: str):
    """
    For MARKET entries, we compute bracket prices off the alert's price.
    Long:
      stop = price * (1 - STOP_PCT)
      take = price * (1 + TAKE_PCT)
    Short:
      stop = price * (1 + STOP_PCT)
      take = price * (1 - TAKE_PCT)
    """
    if side == "buy":
        stop_price = round(price * (1 - STOP_PCT), 2)
        take_price = round(price * (1 + TAKE_PCT), 2)
    else:
        stop_price = round(price * (1 + STOP_PCT), 2)
        take_price = round(price * (1 - TAKE_PCT), 2)

    # Basic sanity
    if stop_price <= 0 or take_price <= 0:
        raise ValueError("Computed bracket prices invalid")

    return stop_price, take_price

# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "trading-webhook", "paper": APCA_PAPER})

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}

    # 1) Validate secret
    incoming_secret = (data.get("secret") or "").strip()
    if WEBHOOK_SECRET:
        if incoming_secret != WEBHOOK_SECRET:
            return _bad("Invalid secret", 401)

    # 2) Validate payload
    symbol = (data.get("symbol") or "").upper().strip()
    side = (data.get("side") or "").lower().strip()  # "buy" or "sell"
    signal = (data.get("signal") or "").strip()
    price_raw = data.get("price")

    if not symbol or symbol not in ALLOWED_SYMBOLS:
        return _bad(f"Symbol not allowed. Got '{symbol}'. Allowed: {sorted(ALLOWED_SYMBOLS)}")

    if side not in ("buy", "sell"):
        return _bad("side must be 'buy' or 'sell'")

    if side == "sell" and not ALLOW_SHORT:
        # For $500 and novice mode: default is long-only
        return jsonify({"ok": True, "ignored": True, "reason": "Shorts disabled", "signal": signal})

    try:
        price = float(price_raw)
    except Exception:
        return _bad("Missing/invalid 'price'. Send TradingView {{close}} in alert JSON.")

    if price <= 0:
        return _bad("price must be > 0")

    # 3) Compute qty + brackets
    try:
        qty = compute_qty(price)
        stop_price, take_price = build_bracket_prices(price, side)
    except Exception as e:
        return _bad(str(e), 400)

    # 4) Place order (or dry run)
    order_payload = {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "type": "market",
        "time_in_force": "day",
        "order_class": "bracket",
        "take_profit": {"limit_price": take_price},
        "stop_loss": {"stop_price": stop_price},
        "signal": signal,
        "paper": APCA_PAPER,
        "dry_run": DRY_RUN,
    }

    if DRY_RUN:
        return jsonify({"ok": True, "submitted": False, "dry_run": True, "order": order_payload})

    try:
        req = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=take_price),
            stop_loss=StopLossRequest(stop_price=stop_price),
        )
        order = trading_client.submit_order(req)
        return jsonify({"ok": True, "submitted": True, "order_id": order.id, "order": order_payload})
    except Exception as e:
        return _bad(f"Alpaca submit failed: {e}", 500)

# Render/Gunicorn entrypoint expects `app`
