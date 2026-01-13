import os
import math
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

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

# Symbols
ALLOWED_SYMBOLS = set(s.strip().upper() for s in os.getenv("ALLOWED_SYMBOLS", "SPY").split(","))

# Risk / sizing
RISK_DOLLARS = float(os.getenv("RISK_DOLLARS", "3"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.003"))   # 0.30%
TAKE_PCT = float(os.getenv("TAKE_PCT", "0.006"))   # 0.60%
MIN_QTY = float(os.getenv("MIN_QTY", "0.01"))
MAX_QTY = float(os.getenv("MAX_QTY", "1.50"))

# Safety
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# For $500: keep simple orders on. (Fractional orders must be simple orders.)
FORCE_SIMPLE = os.getenv("FORCE_SIMPLE", "true").lower() == "true"

# Exit worker auth (optional but recommended)
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

# Exit timing
NY_TZ = ZoneInfo("America/New_York")
EOD_FLATTEN_TIME = os.getenv("EOD_FLATTEN_TIME", "15:55")  # HH:MM in NY time
EXIT_COOLDOWN_SEC = int(os.getenv("EXIT_COOLDOWN_SEC", "20"))  # prevent spam exits if called often

# Trading hours guard (optional)
ONLY_MARKET_HOURS = os.getenv("ONLY_MARKET_HOURS", "true").lower() == "true"
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Alpaca clients
trading_client = TradingClient(APCA_KEY, APCA_SECRET, paper=APCA_PAPER)
data_client = StockHistoricalDataClient(APCA_KEY, APCA_SECRET)

# -----------------------------
# In-memory trade plan state
# (good enough for now; later we can persist to Redis/DB)
# -----------------------------
TRADE_PLAN = {
    # symbol -> dict with entry/targets
    # "SPY": { "active": True, "side": "buy", "qty": 1.23, "entry_price": 500.0,
    #          "stop_price": 498.5, "take_price": 503.0, "signal": "...",
    #          "opened_at": "...", "last_exit_attempt_ts": 0 }
}


# -----------------------------
# Helpers
# -----------------------------
def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)


def parse_hhmm(hhmm: str) -> time:
    parts = hhmm.strip().split(":")
    return time(int(parts[0]), int(parts[1]))


def in_market_hours() -> bool:
    t = now_ny().time()
    return (t >= MARKET_OPEN) and (t <= MARKET_CLOSE)


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
    return abs(qty - round(qty)) < 1e-9 and qty >= 1.0


def build_trade_plan(symbol: str, side: str, qty: float, entry_price: float, signal: str) -> dict:
    """
    For now, we only manage LONG exits (buy then sell).
    If you enable shorts later, we’ll add symmetric logic.
    """
    if side != "buy":
        # short support is parked unless ALLOW_SHORT is enabled and we add exit logic
        return {
            "active": True,
            "side": side,
            "qty": qty,
            "entry_price": entry_price,
            "stop_price": None,
            "take_price": None,
            "signal": signal,
            "opened_at": now_ny().isoformat(),
            "last_exit_attempt_ts": 0,
        }

    stop_price = round(entry_price * (1 - STOP_PCT), 2)
    take_price = round(entry_price * (1 + TAKE_PCT), 2)

    return {
        "active": True,
        "side": side,
        "qty": qty,
        "entry_price": round(entry_price, 4),
        "stop_price": stop_price,
        "take_price": take_price,
        "signal": signal,
        "opened_at": now_ny().isoformat(),
        "last_exit_attempt_ts": 0,
    }


def submit_market_order(symbol: str, side: str, qty: float):
    order_req = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
    )
    return trading_client.submit_order(order_req)


def get_open_position_qty(symbol: str) -> float:
    """
    Returns absolute position qty if exists, else 0.
    """
    try:
        pos = trading_client.get_open_position(symbol)
        q = float(pos.qty)
        return abs(q)
    except Exception:
        return 0.0


def flatten_position(symbol: str) -> dict:
    """
    Close any open position in this symbol with a market order.
    """
    qty = get_open_position_qty(symbol)
    if qty <= 0:
        return {"flattened": False, "reason": "No open position"}

    if DRY_RUN:
        return {"flattened": False, "dry_run": True, "symbol": symbol, "qty": qty}

    order = submit_market_order(symbol, "sell", qty)
    return {"flattened": True, "symbol": symbol, "qty": qty, "order_id": str(order.id)}


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "trading-webhook", "paper": APCA_PAPER}


@app.get("/health")
def health():
    return {
        "ok": True,
        "paper": APCA_PAPER,
        "allowed_symbols": sorted(ALLOWED_SYMBOLS),
        "force_simple": FORCE_SIMPLE,
        "only_market_hours": ONLY_MARKET_HOURS,
        "eod_flatten_time_ny": EOD_FLATTEN_TIME,
        "active_plans": {k: v.get("active") for k, v in TRADE_PLAN.items()},
    }


@app.get("/state")
def state():
    # quick debug view
    return {"ok": True, "trade_plan": TRADE_PLAN}


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

    # Optional market hours guard
    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "ignored": True, "reason": "Outside market hours", "signal": signal}

    # One position at a time guard (simple & safe)
    existing_qty = get_open_position_qty(symbol)
    if existing_qty > 0:
        return {
            "ok": True,
            "ignored": True,
            "reason": f"Position already open ({existing_qty}). One-at-a-time guard enabled.",
            "signal": signal,
            "symbol": symbol,
        }

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

    # Fractional must be simple; with FORCE_SIMPLE we always do simple entries
    order_mode = "simple"

    payload = {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "base_price": round(base_price, 2),
        "signal": signal,
        "paper": APCA_PAPER,
        "dry_run": DRY_RUN,
        "order_mode": order_mode,
    }

    if DRY_RUN:
        # Create trade plan (but don't send order)
        TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)
        return {"ok": True, "submitted": False, "dry_run": True, "order": payload, "plan": TRADE_PLAN[symbol]}

    try:
        # Simple market order (fractional allowed)
        order = submit_market_order(symbol, side, qty)

        # Record plan using the base price (close enough for SPY; later we can pull fill price)
        TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)

        return {
            "ok": True,
            "submitted": True,
            "order_id": str(order.id),
            "order": payload,
            "plan": TRADE_PLAN[symbol],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Alpaca submit failed: {e}")


@app.post("/worker/exit")
async def worker_exit(req: Request):
    """
    Called by Render Cron every 30 seconds.
    Checks for active plan + open position and exits if stop/target/time-stop hit.
    """
    # Optional worker auth
    if WORKER_SECRET:
        body = await req.json()
        if (body.get("worker_secret") or "").strip() != WORKER_SECRET:
            raise HTTPException(status_code=401, detail="Invalid worker secret")
    else:
        # if no secret, still accept empty JSON
        _ = await req.json()

    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "skipped": True, "reason": "Outside market hours"}

    eod_t = parse_hhmm(EOD_FLATTEN_TIME)
    now = now_ny()
    now_t = now.time()

    results = []

    for symbol, plan in list(TRADE_PLAN.items()):
        if not plan.get("active"):
            continue

        # Must have a real open position to manage
        pos_qty = get_open_position_qty(symbol)
        if pos_qty <= 0:
            # No position: mark plan inactive (clean up)
            plan["active"] = False
            results.append({"symbol": symbol, "action": "plan_deactivated", "reason": "No open position"})
            continue

        # Cooldown to avoid repeated exits
        last_ts = int(plan.get("last_exit_attempt_ts") or 0)
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        if now_ts - last_ts < EXIT_COOLDOWN_SEC:
            results.append({"symbol": symbol, "action": "cooldown", "seconds_remaining": EXIT_COOLDOWN_SEC - (now_ts - last_ts)})
            continue

        # Time stop: flatten near end of day
        if now_t >= eod_t:
            plan["last_exit_attempt_ts"] = now_ts
            out = flatten_position(symbol)
            if out.get("flattened"):
                plan["active"] = False
                results.append({"symbol": symbol, "action": "flatten_eod", **out})
            else:
                results.append({"symbol": symbol, "action": "flatten_eod_failed_or_none", **out})
            continue

        # Only handling long exits right now
        if plan.get("side") != "buy":
            results.append({"symbol": symbol, "action": "skip", "reason": "Short exit logic parked"})
            continue

        stop_price = plan.get("stop_price")
        take_price = plan.get("take_price")

        # Get latest price
        try:
            px = get_base_price(symbol)
        except Exception as e:
            results.append({"symbol": symbol, "action": "error", "reason": f"latest_price_failed: {e}"})
            continue

        # Stop/target checks
        hit_stop = stop_price is not None and px <= float(stop_price)
        hit_take = take_price is not None and px >= float(take_price)

        if hit_stop or hit_take:
            plan["last_exit_attempt_ts"] = now_ts
            reason = "stop" if hit_stop else "target"
            out = flatten_position(symbol)
            if out.get("flattened"):
                plan["active"] = False
                results.append({"symbol": symbol, "action": f"exit_{reason}", "price": px, "stop": stop_price, "take": take_price, **out})
            else:
                results.append({"symbol": symbol, "action": f"exit_{reason}_failed_or_none", "price": px, "stop": stop_price, "take": take_price, **out})
        else:
            results.append({"symbol": symbol, "action": "hold", "price": px, "stop": stop_price, "take": take_price, "qty": pos_qty})

    return {"ok": True, "ts_ny": now.isoformat(), "results": results}
