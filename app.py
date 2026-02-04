import os
import threading
import hashlib
import time as _time
from collections import defaultdict
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

def getenv_any(*names: str, default: str = "") -> str:
    """Return the first non-empty env var value among names (stripped)."""
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

# -----------------------------
WEBHOOK_SECRET = getenv_any("WEBHOOK_SECRET", default="")

APCA_KEY = getenv_any("APCA_API_KEY_ID", "ALPACA_KEY_ID", "ALPACA_API_KEY_ID", default="")
APCA_SECRET = getenv_any("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET_KEY", default="")
APCA_PAPER = getenv_any("APCA_PAPER", "ALPACA_PAPER", default="true").lower() == "true"

# Symbols
ALLOWED_SYMBOLS = set(s.strip().upper() for s in os.getenv("ALLOWED_SYMBOLS", "SPY").split(",") if s.strip())

# Risk / sizing
RISK_DOLLARS = float(os.getenv("RISK_DOLLARS", "3"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.003"))  # 0.30%
TAKE_PCT = float(os.getenv("TAKE_PCT", "0.006"))  # 0.60%
MIN_QTY = float(os.getenv("MIN_QTY", "0.01"))
MAX_QTY = float(os.getenv("MAX_QTY", "1.50"))

# Safety
ALLOW_SHORT = os.getenv("ALLOW_SHORT", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# If true, allow reversing (close existing position then open opposite direction)
ALLOW_REVERSAL = os.getenv("ALLOW_REVERSAL", "true").lower() == "true"

# Exit worker auth (optional but recommended)
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

# Exit timing
NY_TZ = ZoneInfo("America/New_York")
EOD_FLATTEN_TIME = os.getenv("EOD_FLATTEN_TIME", "15:55")  # HH:MM in NY time
EXIT_COOLDOWN_SEC = int(os.getenv("EXIT_COOLDOWN_SEC", "20"))  # prevent spam exits if called often

# Trading hours guard
ONLY_MARKET_HOURS = os.getenv("ONLY_MARKET_HOURS", "true").lower() == "true"
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Optional: only accept known signals (comma-separated). Empty = accept all.
ALLOWED_SIGNALS = set(s.strip() for s in os.getenv("ALLOWED_SIGNALS", "").split(",") if s.strip())

# Alpaca clients

# Validate credentials early to avoid cryptic runtime errors
if not APCA_KEY or not APCA_SECRET:
    # Don't print secrets; only print which vars are missing.
    missing = []
    if not APCA_KEY:
        missing.append("APCA_API_KEY_ID (or ALPACA_KEY_ID/ALPACA_API_KEY_ID)")
    if not APCA_SECRET:
        missing.append("APCA_API_SECRET_KEY (or ALPACA_SECRET_KEY/ALPACA_API_SECRET_KEY)")
    raise RuntimeError("Missing Alpaca credentials: " + ", ".join(missing))

trading_client = TradingClient(APCA_KEY, APCA_SECRET, paper=APCA_PAPER)
data_client = StockHistoricalDataClient(APCA_KEY, APCA_SECRET)

# -----------------------------
# In-memory trade plan state
# (NOTE: Render restarts wipe this. We therefore also have an EOD safety
# flatten that uses Alpaca positions as the source of truth.)
# -----------------------------
TRADE_PLAN = {
    # symbol -> dict
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
    """Risk-based sizing using percent stop approximation."""
    if price <= 0:
        raise ValueError("Price must be > 0")
    risk_per_share = price * STOP_PCT
    if risk_per_share <= 0:
        raise ValueError("STOP_PCT must be > 0")
    qty = RISK_DOLLARS / risk_per_share
    qty = max(qty, MIN_QTY)
    qty = min(qty, MAX_QTY)
    return round(qty, 2)


def get_latest_price(symbol: str) -> float:
    req = StockLatestTradeRequest(symbol_or_symbols=[symbol])
    latest = data_client.get_stock_latest_trade(req)
    px = float(latest[symbol].price)
    if px <= 0:
        raise ValueError("Latest trade price invalid")
    return px


def get_position(symbol: str):
    """Returns (qty_signed, side_str) where side_str is 'long'/'short'. If none, (0,'flat')."""
    try:
        pos = trading_client.get_open_position(symbol)
        q = float(pos.qty)
        if q > 0:
            return q, "long"
        if q < 0:
            return q, "short"
        return 0.0, "flat"
    except Exception:
        return 0.0, "flat"


def submit_market_order(symbol: str, side: str, qty: float):
    order_req = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
    )
    return trading_client.submit_order(order_req)


def close_position(symbol: str) -> dict:
    """Closes any open position in `symbol` using a market order."""
    qty_signed, side = get_position(symbol)
    if qty_signed == 0:
        return {"closed": False, "reason": "No open position"}

    qty = abs(qty_signed)
    close_side = "sell" if qty_signed > 0 else "buy"

    if DRY_RUN:
        return {"closed": False, "dry_run": True, "symbol": symbol, "qty": qty, "close_side": close_side}

    order = submit_market_order(symbol, close_side, qty)
    return {"closed": True, "symbol": symbol, "qty": qty, "close_side": close_side, "order_id": str(order.id)}


def list_open_positions_allowed() -> list[dict]:
    """Returns open Alpaca positions restricted to ALLOWED_SYMBOLS."""
    out = []
    try:
        positions = trading_client.get_all_positions()
    except Exception:
        return out

    for p in positions:
        try:
            sym = str(p.symbol).upper()
            if sym not in ALLOWED_SYMBOLS:
                continue
            qty = float(p.qty)
            if qty == 0:
                continue
            out.append({"symbol": sym, "qty": qty})
        except Exception:
            continue
    return out


def build_trade_plan(symbol: str, side: str, qty: float, entry_price: float, signal: str) -> dict:
    """Build symmetric stop/take for long or short."""
    entry_price = float(entry_price)

    if side == "buy":
        stop_price = round(entry_price * (1 - STOP_PCT), 2)
        take_price = round(entry_price * (1 + TAKE_PCT), 2)
    else:
        # short: stop ABOVE entry, take BELOW entry
        stop_price = round(entry_price * (1 + STOP_PCT), 2)
        take_price = round(entry_price * (1 - TAKE_PCT), 2)

    return {
        "active": True,
        "side": side,  # 'buy' for long entries, 'sell' for short entries
        "qty": float(qty),
        "entry_price": round(entry_price, 4),
        "stop_price": stop_price,
        "take_price": take_price,
        "signal": signal,
        "opened_at": now_ny().isoformat(),
        "last_exit_attempt_ts": 0,
    }


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
        "allow_short": ALLOW_SHORT,
        "allow_reversal": ALLOW_REVERSAL,
        "only_market_hours": ONLY_MARKET_HOURS,
        "eod_flatten_time_ny": EOD_FLATTEN_TIME,
        "active_plans": {k: v.get("active") for k, v in TRADE_PLAN.items()},
    }


@app.get("/state")
def state():
    return {"ok": True, "trade_plan": TRADE_PLAN}
@app.post("/kill")
async def kill_switch_on(request: Request):
    _require_admin(request)
    global KILL_SWITCH
    KILL_SWITCH = True
    return {"ok": True, "kill_switch": KILL_SWITCH}

@app.post("/unkill")
async def kill_switch_off(request: Request):
    _require_admin(request)
    global KILL_SWITCH
    KILL_SWITCH = False
    return {"ok": True, "kill_switch": KILL_SWITCH}




@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()

    # Secret check
    if WEBHOOK_SECRET:
        if (data.get("secret") or "").strip() != WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Invalid secret")

    symbol = (data.get("symbol") or "").upper().strip()
    side = (data.get("side") or "").lower().strip()  # 'buy' or 'sell'
    signal = (data.get("signal") or "").strip()

    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")

    if symbol not in ALLOWED_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Symbol not allowed: {symbol}")

    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="side must be 'buy' or 'sell'")

    if ALLOWED_SIGNALS and signal not in ALLOWED_SIGNALS:
        raise HTTPException(status_code=400, detail=f"Signal not allowed: {signal}")

    if side == "sell" and not ALLOW_SHORT:
        # If shorts disabled, we treat sell as *close long if present*, otherwise ignore.
        qty_signed, pos_side = get_position(symbol)
        if qty_signed > 0:
            out = close_position(symbol)
            # deactivate any plan
            if symbol in TRADE_PLAN:
                TRADE_PLAN[symbol]["active"] = False
            return {"ok": True, "closed": True, "reason": "Shorts disabled; closed long", "signal": signal, "symbol": symbol, **out}
        return {"ok": True, "ignored": True, "reason": "Shorts disabled", "signal": signal, "symbol": symbol}

    # Optional market hours guard
    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "ignored": True, "reason": "Outside market hours", "signal": signal, "symbol": symbol}

    # Handle existing position (close or reversal)
    qty_signed, pos_side = get_position(symbol)
    if qty_signed != 0:
        desired_side = "long" if side == "buy" else "short"

        if desired_side == pos_side:
            return {
                "ok": True,
                "ignored": True,
                "reason": f"Position already open ({pos_side} {qty_signed}).",
                "signal": signal,
                "symbol": symbol,
            }

        # Opposite direction: close, then optionally open
        close_out = close_position(symbol)
        if symbol in TRADE_PLAN:
            TRADE_PLAN[symbol]["active"] = False

        if not close_out.get("closed"):
            return {"ok": True, "action": "reverse_close_failed", "signal": signal, "symbol": symbol, **close_out}

        if not ALLOW_REVERSAL:
            return {"ok": True, "action": "closed_opposite_position", "signal": signal, "symbol": symbol, **close_out}

        # fall through to open the new position

    # Entry sizing uses latest price
    try:
        base_price = get_latest_price(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get latest price: {e}")

    try:
        qty = compute_qty(base_price)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    payload = {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "base_price": round(base_price, 2),
        "signal": signal,
        "paper": APCA_PAPER,
        "dry_run": DRY_RUN,
    }

    if DRY_RUN:
        TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)
        return {"ok": True, "submitted": False, "dry_run": True, "order": payload, "plan": TRADE_PLAN[symbol]}

    try:
        order = submit_market_order(symbol, side, qty)
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
    """Called by background worker every N seconds."""

    body = await req.json()

    # Optional worker auth
    if WORKER_SECRET:
        if (body.get("worker_secret") or "").strip() != WORKER_SECRET:
            raise HTTPException(status_code=401, detail="Invalid worker secret")

    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "skipped": True, "reason": "Outside market hours"}

    eod_t = parse_hhmm(EOD_FLATTEN_TIME)
    now = now_ny()
    now_t = now.time()

    results = []

    # HARD SAFETY: at/after EOD time, flatten any open Alpaca positions in ALLOWED_SYMBOLS
    # (even if trade plans were lost due to service restart).
    if now_t >= eod_t:
        for p in list_open_positions_allowed():
            sym = p["symbol"]
            # cooldown not needed at EOD; just try
            out = close_position(sym)
            # deactivate any plan
            if sym in TRADE_PLAN:
                TRADE_PLAN[sym]["active"] = False
            results.append({"symbol": sym, "action": "flatten_eod", **out})

        return {"ok": True, "ts_ny": now.isoformat(), "results": results, "mode": "eod_flatten"}

    # Otherwise, manage active plans with stop/take
    for symbol, plan in list(TRADE_PLAN.items()):
        if not plan.get("active"):
            continue

        qty_signed, _pos_side = get_position(symbol)
        if qty_signed == 0:
            plan["active"] = False
            results.append({"symbol": symbol, "action": "plan_deactivated", "reason": "No open position"})
            continue

        # Cooldown to avoid repeated exits
        last_ts = int(plan.get("last_exit_attempt_ts") or 0)
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        if now_ts - last_ts < EXIT_COOLDOWN_SEC:
            results.append({"symbol": symbol, "action": "cooldown", "seconds_remaining": EXIT_COOLDOWN_SEC - (now_ts - last_ts)})
            continue

        try:
            px = get_latest_price(symbol)
        except Exception as e:
            results.append({"symbol": symbol, "action": "error", "reason": f"latest_price_failed: {e}"})
            continue

        stop_price = float(plan.get("stop_price"))
        take_price = float(plan.get("take_price"))
        side = plan.get("side")  # 'buy' (long) or 'sell' (short)

        if side == "buy":
            hit_stop = px <= stop_price
            hit_take = px >= take_price
        else:
            # short
            hit_stop = px >= stop_price
            hit_take = px <= take_price

        if hit_stop or hit_take:
            plan["last_exit_attempt_ts"] = now_ts
            reason = "stop" if hit_stop else "target"
            out = close_position(symbol)
            if out.get("closed"):
                plan["active"] = False
                results.append({"symbol": symbol, "action": f"exit_{reason}", "price": px, "stop": stop_price, "take": take_price, **out})
            else:
                results.append({"symbol": symbol, "action": f"exit_{reason}_failed_or_none", "price": px, "stop": stop_price, "take": take_price, **out})
        else:
            results.append({"symbol": symbol, "action": "hold", "price": px, "stop": stop_price, "take": take_price, "qty": qty_signed})

    return {"ok": True, "ts_ny": now.isoformat(), "results": results}
