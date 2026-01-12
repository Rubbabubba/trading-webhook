import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, TakeProfitRequest, StopLossRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass

app = FastAPI()

# CORS (lets Hoppscotch / browsers send OPTIONS preflight cleanly)
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

# Safety defaults for $500
RISK_DOLLARS = float(os.getenv("RISK_DOLLARS", "3"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.003"))   # 0.30%
TAKE_PCT = float(os.getenv("TAKE_PCT", "0.006"))   # 0.60%
MIN_QTY = float(os.getenv("MIN_QTY", "0.01"))
MAX_QTY = float(os.getenv("MAX_QTY", "1.50"))

ALLOW_SHORT = os.getenv("ALLOW_SHORT", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

ALLOWED_SYMBOLS = set(s.strip().upper() for s in os.getenv("ALLOWED_SYMBOLS", "SPY").split(","))

trading_client = TradingClient(APCA_KEY, APCA_SECRET, paper=APCA_PAPER)


def compute_qty(price: float) -> float:
    risk_per_share = price * STOP_PCT
    if risk_per_share <= 0:
        raise ValueError("STOP_PCT must be > 0")
    qty = RISK_DOLLARS / risk_per_share
    qty = max(qty, MIN_QTY)
    qty = min(qty, MAX_QTY)
    return round(qty, 2)


def bracket_prices(price: float, side: str) -> tuple[float, float]:
    # returns (stop_price, take_profit_price)
    if side == "buy":
        stop_price = round(price * (1 - STOP_PCT), 2)
        take_price = round(price * (1 + TAKE_PCT), 2)
    else:
        stop_price = round(price * (1 + STOP_PCT), 2)
        take_price = round(price * (1 - TAKE_PCT), 2)
    if stop_price <= 0 or take_price <= 0:
        raise ValueError("Invalid bracket prices")
    return stop_price, take_price


@app.get("/health")
def health():
    return {"ok": True, "paper": APCA_PAPER, "allowed_symbols": sorted(ALLOWED_SYMBOLS)}


@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()

    # Secret check
    if WEBHOOK_SECRET:
        if (data.get("secret") or "").strip() != WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Invalid secret")

    symbol = (data.get("symbol") or "").upper().strip()
    side = (data.get("side") or "").lower().strip()  # buy/sell
    signal = (data.get("signal") or "").strip()

    if symbol not in ALLOWED_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Symbol not allowed: {symbol}")

    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="side must be 'buy' or 'sell'")

    if side == "sell" and not ALLOW_SHORT:
        return {"ok": True, "ignored": True, "reason": "Shorts disabled", "signal": signal}

    try:
        price = float(data.get("price"))
    except Exception:
        raise HTTPException(status_code=400, detail="Missing/invalid price. Send TradingView {{close}}.")

    qty = compute_qty(price)
    stop_price, take_price = bracket_prices(price, side)

    payload = {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "type": "market",
        "order_class": "bracket",
        "take_profit": take_price,
        "stop_loss": stop_price,
        "signal": signal,
        "paper": APCA_PAPER,
        "dry_run": DRY_RUN,
    }

    if DRY_RUN:
        return {"ok": True, "submitted": False, "dry_run": True, "order": payload}

    try:
        order_req = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=take_price),
            stop_loss=StopLossRequest(stop_price=stop_price),
        )
        order = trading_client.submit_order(order_req)
        return {"ok": True, "submitted": True, "order_id": str(order.id), "order": payload}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Alpaca submit failed: {e}")
