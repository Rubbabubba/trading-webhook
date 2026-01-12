import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, TakeProfitRequest, StopLossRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# ENV
# -----------------------------
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

APCA_KEY = os.getenv("APCA_API_KEY_ID", "").strip()
APCA_SECRET = os.getenv("APCA_API_SECRET_KEY", "").strip()
APCA_PAPER = os.getenv("APCA_PAPER", "true").lower() == "true"

# $500-safe defaults
RISK_DOLLARS = float(os.getenv("RISK_DOLLARS", "3"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.003"))   # used only when bracket is possible
TAKE_PCT = float(os.getenv("TAKE_PCT", "0.006"))   # used only when bracket is possible

MIN_QTY = float(os.getenv("MIN_QTY", "0.01"))
MAX_QTY = float(os.getenv("MAX_QTY", "1.50"))

ALLOW_SHORT = os.getenv("ALLOW_SHORT", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# IMPORTANT: For your account size, keep simple orders ON.
FORCE_SIMPLE = os.getenv("FORCE_SIMPLE", "true").lower() == "true"

ALLOWED_SYMBOLS = set(s.strip().upper() for s in os.getenv("ALLOWED_SYMBOLS", "SPY").split(","))

trading_client = TradingClient(APCA_KEY, APCA_SECRET, paper=APCA_PAPER)
data_client = StockHistoricalDataClient(APCA_KEY, APCA_SECRET)


# -----------------------------
# Helpers
# -----------------------------
def compute_qty(price: float) -> float:
    """
    Risk-based sizing using percent stop approximation.
    With $500, this will often be fractional shares.
    """
    if price <= 0:
        raise ValueError("Price must be > 0")

    risk_per_share = price * STOP_PCT
    if risk_per_share <= 0:
        raise ValueError("STOP_PCT must be > 0")

    qty = RISK_DOLLARS / risk_per_share
    qty = max(qty, MIN_QTY)
    qty = min(qty, MAX_QTY)
    return round(qty, 2)


def get_base_price(symbol: str) -> float:
    req = StockLatestTradeRequest(symbol_or_symbols=[symbol])
    latest = data_client.get_stock_latest_trade(req)
    px = float(latest[symbol].price)
    if px <= 0:
        raise ValueError("Latest trade price invalid")
    return px


def is_whole_share(qty: float) -> bool:
    # qty like 1.00, 2.00, etc.
    return abs(qty - round(qty)) < 1e-9 and qty >= 1.0


def clamp_bracket_prices(base_price: float, side: str, stop_price: float, take_price: float) -> tuple[float, float]:
    tick = 0.01

    if side == "buy":
        min_take = round(base_price + tick, 2)
        max_stop = round(base_price - tick, 2)
        take_price = max(take_price, min_take)
        stop_price = min(stop_price, max_stop)
    else:
        max_take = round(base_price - tick, 2)
        min_stop = round(base_price + tick, 2)
        take_price = min(take_price, max_take)
        stop_price = max(stop_price, min_stop)

    if stop_price <= 0 or take_price <= 0:
        raise ValueError("Invalid bracket prices after clamp")

    return stop_price, take_price


def bracket_prices_from_base(base_price: float, side: str) -> tuple[float, float]:
    if side == "buy":
        stop_price = round(base_price * (1 - STOP_PCT), 2)
        take_price = round(base_price * (1 + TAKE_PCT), 2)
    else:
        stop_price = round(base_price * (1 + STOP_PCT), 2)
        take_price = round(base_price * (1 - TAKE_PCT), 2)
    return clamp_bracket_prices(base_price, side, stop_price, take_price)


# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "paper": APCA_PAPER,
        "allowed_symbols": sorted(ALLOWED_SYMBOLS),
        "force_simple": FORCE_SIMPLE
    }


@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()

    # Secret check
    if WEBHOOK_SECRET:
        if (data.get("secret") or "").strip() != WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Invalid secret")

    symbol = (data.get("symbol") or "").upper().strip()
    side = (data.get("side") or "").lower().strip()
    signal = (data.get("signal") or "").strip()

    if symbol not in ALLOWED_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Symbol not allowed: {symbol}")

    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="side must be 'buy' or 'sell'")

    if side == "sell" and not ALLOW_SHORT:
        return {"ok": True, "ignored": True, "reason": "Shorts disabled", "signal": signal}

    # Use Alpaca latest trade as base
    try:
        base_price = get_base_price(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get latest price: {e}")

    # Size order
    try:
        qty = compute_qty(base_price)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Decide order type:
    # - Fractional shares => MUST be simple order (Alpaca rule)
    # - Whole shares => can do bracket (optional)
    use_bracket = (not FORCE_SIMPLE) and is_whole_share(qty)

    payload = {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "base_price": round(base_price, 2),
        "signal": signal,
        "paper": APCA_PAPER,
        "dry_run": DRY_RUN,
        "order_mode": "bracket" if use_bracket else "simple"
    }

    if DRY_RUN:
        if use_bracket:
            stop_price, take_price = bracket_prices_from_base(base_price, side)
            payload["take_profit"] = take_price
            payload["stop_loss"] = stop_price
        return {"ok": True, "submitted": False, "dry_run": True, "order": payload}

    try:
        if use_bracket:
            stop_price, take_price = bracket_prices_from_base(base_price, side)
            order_req = MarketOrderRequest(
                symbol=symbol,
                qty=int(round(qty)),
                side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                order_class=OrderClass.BRACKET,
                take_profit=TakeProfitRequest(limit_price=take_price),
                stop_loss=StopLossRequest(stop_price=stop_price),
            )
        else:
            # Simple market order (fractional allowed)
            order_req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
            )

        order = trading_client.submit_order(order_req)
        return {"ok": True, "submitted": True, "order_id": str(order.id), "order": payload}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Alpaca submit failed: {e}")
