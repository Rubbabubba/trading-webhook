import os
import hashlib
import traceback
import time as _time
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


# =============================
# App
# =============================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

NY_TZ = ZoneInfo("America/New_York")


# =============================
# Env helpers
# =============================
def getenv_any(*names: str, default: str = "") -> str:
    """Return the first non-empty env var value among names (stripped)."""
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)


def utc_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def parse_hhmm(hhmm: str) -> time:
    parts = hhmm.strip().split(":")
    return time(int(parts[0]), int(parts[1]))


# =============================
# ENV
# =============================
WEBHOOK_SECRET = getenv_any("WEBHOOK_SECRET", default="")

# Alpaca (support APCA_* + ALPACA_*)
APCA_KEY = getenv_any("APCA_API_KEY_ID", "ALPACA_KEY_ID", "ALPACA_API_KEY_ID", default="")
APCA_SECRET = getenv_any("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET_KEY", default="")
APCA_PAPER = getenv_any("APCA_PAPER", "ALPACA_PAPER", default="true").lower() == "true"

if not APCA_KEY or not APCA_SECRET:
    missing = []
    if not APCA_KEY:
        missing.append("APCA_API_KEY_ID (or ALPACA_KEY_ID/ALPACA_API_KEY_ID)")
    if not APCA_SECRET:
        missing.append("APCA_API_SECRET_KEY (or ALPACA_SECRET_KEY/ALPACA_API_SECRET_KEY)")
    raise RuntimeError("Missing Alpaca credentials: " + ", ".join(missing))

# Symbols
ALLOWED_SYMBOLS = set(
    s.strip().upper() for s in os.getenv("ALLOWED_SYMBOLS", "SPY").split(",") if s.strip()
)

# Risk / sizing
RISK_DOLLARS = float(os.getenv("RISK_DOLLARS", "3"))
STOP_PCT = float(os.getenv("STOP_PCT", "0.003"))  # 0.30%
TAKE_PCT = float(os.getenv("TAKE_PCT", "0.006"))  # 0.60%
MIN_QTY = float(os.getenv("MIN_QTY", "0.01"))
MAX_QTY = float(os.getenv("MAX_QTY", "1.50"))

# Safety / behavior
ALLOW_SHORT = env_bool("ALLOW_SHORT", "false")
ALLOW_REVERSAL = env_bool("ALLOW_REVERSAL", "true")
DRY_RUN = env_bool("DRY_RUN", "false")

# Market hours
ONLY_MARKET_HOURS = env_bool("ONLY_MARKET_HOURS", "true")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Exit worker
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()
EOD_FLATTEN_TIME = os.getenv("EOD_FLATTEN_TIME", "15:55")  # NY time
EXIT_COOLDOWN_SEC = int(os.getenv("EXIT_COOLDOWN_SEC", "20"))

# Idempotency
DEDUP_WINDOW_SEC = int(os.getenv("DEDUP_WINDOW_SEC", "90"))  # absorb retries

# Symbol lock
SYMBOL_LOCK_SEC = int(os.getenv("SYMBOL_LOCK_SEC", "180"))  # lock during entry/plan

# Daily stop + kill switch
DAILY_STOP_DOLLARS = float(os.getenv("DAILY_STOP_DOLLARS", "0"))  # e.g. 50 means stop at -50
KILL_SWITCH = env_bool("KILL_SWITCH", "false")  # can flip by env or /kill
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "").strip()  # protect /kill endpoints

# Optional allowed signals
ALLOWED_SIGNALS = set(
    s.strip() for s in os.getenv("ALLOWED_SIGNALS", "").split(",") if s.strip()
)

# =============================
# Alpaca clients
# =============================
trading_client = TradingClient(APCA_KEY, APCA_SECRET, paper=APCA_PAPER)
data_client = StockHistoricalDataClient(APCA_KEY, APCA_SECRET)


# =============================
# In-memory state
# =============================
TRADE_PLAN: dict[str, dict] = {}          # symbol -> plan dict
DEDUP_CACHE: dict[str, int] = {}          # dedup_key -> last_seen_utc_ts
SYMBOL_LOCKS: dict[str, int] = {}         # symbol -> lock_expiry_utc_ts


# =============================
# Logging helpers
# =============================
def log(msg: str, **kv):
    stamp = datetime.now(tz=timezone.utc).isoformat()
    if kv:
        extras = " ".join([f"{k}={repr(v)}" for k, v in kv.items()])
        print(f"{stamp} {msg} {extras}", flush=True)
    else:
        print(f"{stamp} {msg}", flush=True)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # This is the missing piece: you will now see the real traceback in Render logs.
    log("UNHANDLED_EXCEPTION", path=str(request.url.path), err=str(exc))
    traceback.print_exc()
    return JSONResponse(status_code=500, content={"ok": False, "error": "Internal Server Error"})


# =============================
# Core helpers
# =============================
def in_market_hours() -> bool:
    t = now_ny().time()
    return (t >= MARKET_OPEN) and (t <= MARKET_CLOSE)


def compute_qty(price: float) -> float:
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
    qty_signed, _side = get_position(symbol)
    if qty_signed == 0:
        return {"closed": False, "reason": "No open position"}

    qty = abs(qty_signed)
    close_side = "sell" if qty_signed > 0 else "buy"

    if DRY_RUN:
        return {"closed": False, "dry_run": True, "symbol": symbol, "qty": qty, "close_side": close_side}

    order = submit_market_order(symbol, close_side, qty)
    return {"closed": True, "symbol": symbol, "qty": qty, "close_side": close_side, "order_id": str(order.id)}


def list_open_positions_allowed() -> list[dict]:
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
    entry_price = float(entry_price)
    if side == "buy":
        stop_price = round(entry_price * (1 - STOP_PCT), 2)
        take_price = round(entry_price * (1 + TAKE_PCT), 2)
    else:
        stop_price = round(entry_price * (1 + STOP_PCT), 2)
        take_price = round(entry_price * (1 - TAKE_PCT), 2)

    return {
        "active": True,
        "side": side,
        "qty": float(qty),
        "entry_price": round(entry_price, 4),
        "stop_price": stop_price,
        "take_price": take_price,
        "signal": signal,
        "opened_at": now_ny().isoformat(),
        "last_exit_attempt_ts": 0,
    }


def cleanup_caches():
    now = utc_ts()
    # dedup
    for k in list(DEDUP_CACHE.keys()):
        if now - DEDUP_CACHE[k] > max(DEDUP_WINDOW_SEC * 2, 300):
            del DEDUP_CACHE[k]
    # symbol locks
    for sym in list(SYMBOL_LOCKS.keys()):
        if SYMBOL_LOCKS[sym] <= now:
            del SYMBOL_LOCKS[sym]


def dedup_key(payload: dict) -> str:
    raw = f"{payload.get('symbol','')}-{payload.get('side','')}-{payload.get('signal','')}-{payload.get('secret','')}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def is_symbol_locked(symbol: str) -> bool:
    now = utc_ts()
    exp = SYMBOL_LOCKS.get(symbol, 0)
    return exp > now


def lock_symbol(symbol: str):
    SYMBOL_LOCKS[symbol] = utc_ts() + SYMBOL_LOCK_SEC


def daily_pnl() -> float | None:
    """
    Best-effort daily P&L using Alpaca account equity change from last_equity.
    This is *not* perfect but is reliable enough as a safety brake.
    """
    try:
        acct = trading_client.get_account()
        equity = float(acct.equity)
        last_equity = float(acct.last_equity)
        return equity - last_equity
    except Exception:
        return None


def daily_stop_hit() -> bool:
    if DAILY_STOP_DOLLARS <= 0:
        return False
    pnl = daily_pnl()
    if pnl is None:
        return False
    return pnl <= -abs(DAILY_STOP_DOLLARS)


def require_admin(request: Request):
    if not ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="ADMIN_SECRET not set")
    got = request.headers.get("x-admin-secret", "").strip()
    if got != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Invalid admin secret")


def flatten_all(reason: str) -> list[dict]:
    results = []
    for p in list_open_positions_allowed():
        sym = p["symbol"]
        out = close_position(sym)
        if sym in TRADE_PLAN:
            TRADE_PLAN[sym]["active"] = False
        results.append({"symbol": sym, "action": "flatten", "reason": reason, **out})
    return results


# =============================
# Routes
# =============================
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
        "dedup_window_sec": DEDUP_WINDOW_SEC,
        "symbol_lock_sec": SYMBOL_LOCK_SEC,
        "daily_stop_dollars": DAILY_STOP_DOLLARS,
        "kill_switch": KILL_SWITCH,
        "active_plans": {k: v.get("active") for k, v in TRADE_PLAN.items()},
    }


@app.get("/state")
def state():
    return {"ok": True, "trade_plan": TRADE_PLAN, "symbol_locks": SYMBOL_LOCKS}


@app.post("/kill")
async def kill_on(request: Request):
    global KILL_SWITCH
    require_admin(request)
    KILL_SWITCH = True
    log("KILL_SWITCH_ON")
    return {"ok": True, "kill_switch": KILL_SWITCH}


@app.post("/unkill")
async def kill_off(request: Request):
    global KILL_SWITCH
    require_admin(request)
    KILL_SWITCH = False
    log("KILL_SWITCH_OFF")
    return {"ok": True, "kill_switch": KILL_SWITCH}


@app.post("/webhook")
async def webhook(req: Request):
    cleanup_caches()

    # Parse body safely
    try:
        data = await req.json()
    except Exception as e:
        log("WEBHOOK_BAD_JSON", err=str(e))
        return JSONResponse(status_code=200, content={"ok": False, "rejected": True, "reason": "bad_json"})

    # Secret check
    if WEBHOOK_SECRET:
        if (data.get("secret") or "").strip() != WEBHOOK_SECRET:
            return JSONResponse(status_code=200, content={"ok": False, "rejected": True, "reason": "invalid_secret"})

    symbol = (data.get("symbol") or "").upper().strip()
    side = (data.get("side") or "").lower().strip()  # 'buy' or 'sell'
    signal = (data.get("signal") or "").strip()

    log("WEBHOOK_IN", symbol=symbol, side=side, signal=signal)

    if not symbol:
        return JSONResponse(status_code=200, content={"ok": False, "rejected": True, "reason": "symbol_required"})

    if symbol not in ALLOWED_SYMBOLS:
        return JSONResponse(status_code=200, content={"ok": False, "rejected": True, "reason": f"symbol_not_allowed:{symbol}"})

    if side not in ("buy", "sell"):
        return JSONResponse(status_code=200, content={"ok": False, "rejected": True, "reason": "side_must_be_buy_or_sell"})

    if ALLOWED_SIGNALS and signal not in ALLOWED_SIGNALS:
        return JSONResponse(status_code=200, content={"ok": False, "rejected": True, "reason": f"signal_not_allowed:{signal}"})

    # Kill switch / daily stop gates
    if KILL_SWITCH:
        out = flatten_all("kill_switch")
        return {"ok": False, "rejected": True, "reason": "kill_switch", "flatten": out}

    if daily_stop_hit():
        out = flatten_all("daily_stop_hit")
        return {"ok": False, "rejected": True, "reason": "daily_stop_hit", "flatten": out}

    # Market hours guard
    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "ignored": True, "reason": "outside_market_hours", "symbol": symbol, "signal": signal}

    # Shorts disabled behavior
    if side == "sell" and not ALLOW_SHORT:
        qty_signed, _pos_side = get_position(symbol)
        if qty_signed > 0:
            out = close_position(symbol)
            if symbol in TRADE_PLAN:
                TRADE_PLAN[symbol]["active"] = False
            return {"ok": True, "closed": True, "reason": "shorts_disabled_closed_long", "symbol": symbol, "signal": signal, **out}
        return {"ok": True, "ignored": True, "reason": "shorts_disabled", "symbol": symbol, "signal": signal}

    # Idempotency: absorb retries
    dk = dedup_key(data)
    last = DEDUP_CACHE.get(dk, 0)
    now = utc_ts()
    if last and (now - last) < DEDUP_WINDOW_SEC:
        return {"ok": True, "ignored": True, "reason": "dedup", "symbol": symbol, "signal": signal}
    DEDUP_CACHE[dk] = now

    # Symbol lock: prevent multiple overlapping entries
    # Also prevent entries if a plan is active or Alpaca shows an open position.
    if is_symbol_locked(symbol):
        return {"ok": True, "ignored": True, "reason": "symbol_locked", "symbol": symbol, "signal": signal}

    if TRADE_PLAN.get(symbol, {}).get("active"):
        return {"ok": True, "ignored": True, "reason": "plan_active", "symbol": symbol, "signal": signal}

    qty_signed, pos_side = get_position(symbol)
    if qty_signed != 0:
        desired_side = "long" if side == "buy" else "short"
        if desired_side == pos_side:
            return {"ok": True, "ignored": True, "reason": f"position_already_open:{pos_side}", "symbol": symbol, "signal": signal}

        # Opposite direction: close first, then optionally open
        close_out = close_position(symbol)
        if symbol in TRADE_PLAN:
            TRADE_PLAN[symbol]["active"] = False

        if not close_out.get("closed"):
            return {"ok": False, "rejected": True, "reason": "reverse_close_failed", "symbol": symbol, "signal": signal, **close_out}

        if not ALLOW_REVERSAL:
            return {"ok": True, "action": "closed_opposite_position", "symbol": symbol, "signal": signal, **close_out}

    # Lock symbol while we place the entry
    lock_symbol(symbol)

    # Price + sizing
    try:
        base_price = get_latest_price(symbol)
        qty = compute_qty(base_price)
    except Exception as e:
        return {"ok": False, "rejected": True, "reason": f"price_or_sizing_failed:{e}", "symbol": symbol, "signal": signal}

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

    # Submit order (do NOT throw 500 on broker rejections)
    try:
        order = submit_market_order(symbol, side, qty)
        TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)
        log("ORDER_SUBMITTED", symbol=symbol, side=side, qty=qty, order_id=str(order.id), signal=signal)
        return {"ok": True, "submitted": True, "order_id": str(order.id), "order": payload, "plan": TRADE_PLAN[symbol]}
    except Exception as e:
        log("ORDER_REJECTED", symbol=symbol, side=side, qty=qty, err=str(e), signal=signal)
        # Release lock faster on rejection
        SYMBOL_LOCKS[symbol] = utc_ts() + 5
        return {"ok": False, "rejected": True, "reason": f"alpaca_submit_failed:{e}", "order": payload}


@app.post("/worker/exit")
async def worker_exit(req: Request):
    cleanup_caches()

    body = {}
    try:
        body = await req.json()
    except Exception:
        body = {}

    # Optional worker auth
    if WORKER_SECRET:
        if (body.get("worker_secret") or "").strip() != WORKER_SECRET:
            raise HTTPException(status_code=401, detail="Invalid worker secret")

    # Kill switch / daily stop: flatten immediately
    if KILL_SWITCH:
        out = flatten_all("kill_switch")
        return {"ok": True, "mode": "kill_switch", "ts_ny": now_ny().isoformat(), "results": out}

    if daily_stop_hit():
        out = flatten_all("daily_stop_hit")
        return {"ok": True, "mode": "daily_stop_hit", "ts_ny": now_ny().isoformat(), "results": out}

    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "skipped": True, "reason": "outside_market_hours"}

    eod_t = parse_hhmm(EOD_FLATTEN_TIME)
    now = now_ny()
    now_t = now.time()
    results = []

    # EOD flatten: source of truth is Alpaca positions
    if now_t >= eod_t:
        for p in list_open_positions_allowed():
            sym = p["symbol"]
            out = close_position(sym)
            if sym in TRADE_PLAN:
                TRADE_PLAN[sym]["active"] = False
            results.append({"symbol": sym, "action": "flatten_eod", **out})
        return {"ok": True, "ts_ny": now.isoformat(), "results": results, "mode": "eod_flatten"}

    # Manage active plans with stop/take
    for symbol, plan in list(TRADE_PLAN.items()):
        if not plan.get("active"):
            continue

        qty_signed, _pos_side = get_position(symbol)
        if qty_signed == 0:
            plan["active"] = False
            results.append({"symbol": symbol, "action": "plan_deactivated", "reason": "no_open_position"})
            continue

        last_ts = int(plan.get("last_exit_attempt_ts") or 0)
        now_ts = utc_ts()
        if now_ts - last_ts < EXIT_COOLDOWN_SEC:
            results.append({"symbol": symbol, "action": "cooldown"})
            continue

        try:
            px = get_latest_price(symbol)
        except Exception as e:
            results.append({"symbol": symbol, "action": "error", "reason": f"latest_price_failed:{e}"})
            continue

        stop_price = float(plan.get("stop_price"))
        take_price = float(plan.get("take_price"))
        entry_side = plan.get("side")  # 'buy' (long) or 'sell' (short)

        if entry_side == "buy":
            hit_stop = px <= stop_price
            hit_take = px >= take_price
        else:
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
                results.append({"symbol": symbol, "action": f"exit_{reason}_failed", "price": px, **out})
        else:
            results.append({"symbol": symbol, "action": "hold", "price": px, "stop": stop_price, "take": take_price})

    return {"ok": True, "ts_ny": now.isoformat(), "results": results}
