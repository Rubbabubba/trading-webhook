import os
import threading
import hashlib
import time as _time
import logging
import traceback
from collections import defaultdict
from datetime import datetime, time, timezone, date
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


# =============================================================================
# Logging (critical so 500s show the real reason)
# =============================================================================
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("trading-webhook")


# =============================================================================
# App
# =============================================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_unhandled_exceptions(request: Request, call_next):
    """
    Ensure *every* unhandled exception prints a traceback + request body into Render logs.
    This fixes the 'POST /webhook 500' with no details problem.
    """
    try:
        return await call_next(request)
    except Exception:
        try:
            body = await request.body()
            body_str = body.decode("utf-8", errors="replace")
        except Exception:
            body_str = "<unable to read body>"

        logger.error("UNHANDLED EXCEPTION %s %s | body=%s", request.method, request.url.path, body_str)
        logger.error("TRACEBACK:\n%s", traceback.format_exc())
        raise


# =============================================================================
# ENV helpers
# =============================================================================
def getenv_any(*names: str, default: str = "") -> str:
    """Return the first non-empty env var value among names (stripped)."""
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def getenv_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


def getenv_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)


def getenv_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)


# =============================================================================
# ENV
# =============================================================================
WEBHOOK_SECRET = getenv_any("WEBHOOK_SECRET", default="")

# Alpaca (support APCA_* + ALPACA_*)
APCA_KEY = getenv_any("APCA_API_KEY_ID", "ALPACA_KEY_ID", "ALPACA_API_KEY_ID", default="")
APCA_SECRET = getenv_any("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET_KEY", default="")
APCA_PAPER = getenv_any("APCA_PAPER", "ALPACA_PAPER", default="true").lower() == "true"

# Symbols
ALLOWED_SYMBOLS = set(
    s.strip().upper()
    for s in os.getenv("ALLOWED_SYMBOLS", "SPY").split(",")
    if s.strip()
)

# Risk / sizing
RISK_DOLLARS = getenv_float("RISK_DOLLARS", "3")
STOP_PCT = getenv_float("STOP_PCT", "0.003")  # 0.30%
TAKE_PCT = getenv_float("TAKE_PCT", "0.006")  # 0.60%
MIN_QTY = getenv_float("MIN_QTY", "0.01")
MAX_QTY = getenv_float("MAX_QTY", "1.50")

# Safety
ALLOW_SHORT = getenv_bool("ALLOW_SHORT", "false")
DRY_RUN = getenv_bool("DRY_RUN", "false")
ALLOW_REVERSAL = getenv_bool("ALLOW_REVERSAL", "true")

# Kill switch (can be toggled at runtime via /kill, /unkill)
KILL_SWITCH = getenv_bool("KILL_SWITCH", "false")

# Admin secret for /kill /unkill (REQUIRED if you expose those endpoints)
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "").strip()

# Exit worker auth (optional but recommended)
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

# Exit timing
NY_TZ = ZoneInfo("America/New_York")
EOD_FLATTEN_TIME = os.getenv("EOD_FLATTEN_TIME", "15:55")  # HH:MM in NY time
EXIT_COOLDOWN_SEC = getenv_int("EXIT_COOLDOWN_SEC", "20")  # prevent spam exits

# Trading hours guard
ONLY_MARKET_HOURS = getenv_bool("ONLY_MARKET_HOURS", "true")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Optional: only accept known signals (comma-separated). Empty = accept all.
ALLOWED_SIGNALS = set(s.strip() for s in os.getenv("ALLOWED_SIGNALS", "").split(",") if s.strip())

# Idempotency
IDEMPOTENCY_WINDOW_SEC = getenv_int("IDEMPOTENCY_WINDOW_SEC", "90")  # absorb TV retries
IDEMPOTENCY_MAX_KEYS = getenv_int("IDEMPOTENCY_MAX_KEYS", "5000")

# Symbol lock / one-position-per-symbol guard
SYMBOL_LOCK_TTL_SEC = getenv_int("SYMBOL_LOCK_TTL_SEC", "900")  # 15m default

# Daily stop (intraday equity-based, best-effort)
DAILY_STOP_DOLLARS = getenv_float("DAILY_STOP_DOLLARS", "0")  # <=0 disables

# =============================================================================
# Alpaca clients
# =============================================================================
if not APCA_KEY or not APCA_SECRET:
    missing = []
    if not APCA_KEY:
        missing.append("APCA_API_KEY_ID (or ALPACA_KEY_ID/ALPACA_API_KEY_ID)")
    if not APCA_SECRET:
        missing.append("APCA_API_SECRET_KEY (or ALPACA_SECRET_KEY/ALPACA_API_SECRET_KEY)")
    raise RuntimeError("Missing Alpaca credentials: " + ", ".join(missing))

trading_client = TradingClient(APCA_KEY, APCA_SECRET, paper=APCA_PAPER)
data_client = StockHistoricalDataClient(APCA_KEY, APCA_SECRET)


# =============================================================================
# In-memory state (Render restarts wipe this; Alpaca positions are source of truth)
# =============================================================================
STATE_LOCK = threading.Lock()

TRADE_PLAN = {
    # symbol -> dict
}

# idempotency_key -> last_seen_epoch
DEDUPE = {}

# symbol -> lock info
SYMBOL_LOCKS = {
    # symbol: {"until": epoch, "reason": "..."}
}

# Intraday equity baseline (per NY date)
DAY_STATE = {
    # "date": "YYYY-MM-DD",
    # "start_equity": float
}


# =============================================================================
# Helpers
# =============================================================================
def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)


def ny_date_str() -> str:
    return now_ny().date().isoformat()


def parse_hhmm(hhmm: str) -> time:
    parts = hhmm.strip().split(":")
    return time(int(parts[0]), int(parts[1]))


def in_market_hours() -> bool:
    t = now_ny().time()
    return (t >= MARKET_OPEN) and (t <= MARKET_CLOSE)


def _require_admin(req: Request):
    if not ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="ADMIN_SECRET not set")
    provided = (req.headers.get("x-admin-secret") or "").strip()
    if provided != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Invalid admin secret")


def _dedupe_key(payload: dict) -> str:
    """
    Build a stable hash for TradingView retries.
    Use symbol/side/signal + (optional) price string if included.
    """
    sym = (payload.get("symbol") or "").upper().strip()
    side = (payload.get("side") or "").lower().strip()
    sig = (payload.get("signal") or "").strip()
    price = str(payload.get("price") or "").strip()  # TV sometimes includes it
    base = f"{sym}|{side}|{sig}|{price}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _dedupe_seen(key: str) -> bool:
    now_ts = int(_time.time())
    with STATE_LOCK:
        # prune a bit
        if len(DEDUPE) > IDEMPOTENCY_MAX_KEYS:
            # drop oldest ~20%
            items = sorted(DEDUPE.items(), key=lambda kv: kv[1])
            for k, _ in items[: max(1, len(items) // 5)]:
                DEDUPE.pop(k, None)

        # expire window
        # (light prune to keep memory small)
        cutoff = now_ts - (IDEMPOTENCY_WINDOW_SEC * 2)
        for k, ts in list(DEDUPE.items()):
            if ts < cutoff:
                DEDUPE.pop(k, None)

        ts = DEDUPE.get(key)
        if ts and (now_ts - ts) <= IDEMPOTENCY_WINDOW_SEC:
            return True

        DEDUPE[key] = now_ts
        return False


def _symbol_locked(symbol: str) -> bool:
    now_ts = int(_time.time())
    with STATE_LOCK:
        info = SYMBOL_LOCKS.get(symbol)
        if not info:
            return False
        if info.get("until", 0) <= now_ts:
            SYMBOL_LOCKS.pop(symbol, None)
            return False
        return True


def _lock_symbol(symbol: str, reason: str, ttl: int = SYMBOL_LOCK_TTL_SEC):
    until = int(_time.time()) + int(ttl)
    with STATE_LOCK:
        SYMBOL_LOCKS[symbol] = {"until": until, "reason": reason}


def _unlock_symbol(symbol: str):
    with STATE_LOCK:
        SYMBOL_LOCKS.pop(symbol, None)


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
        stop_price = round(entry_price * (1 + STOP_PCT), 2)
        take_price = round(entry_price * (1 - TAKE_PCT), 2)

    return {
        "active": True,
        "side": side,  # 'buy' or 'sell'
        "qty": float(qty),
        "entry_price": round(entry_price, 4),
        "stop_price": stop_price,
        "take_price": take_price,
        "signal": signal,
        "opened_at": now_ny().isoformat(),
        "last_exit_attempt_ts": 0,
    }


def _get_intraday_pnl() -> dict:
    """
    Best-effort intraday P&L estimate using account equity.
    - On first call each NY trading day, stores start_equity
    - PnL = current_equity - start_equity

    Note: process restarts reset baseline; still useful as a safety valve.
    """
    try:
        acct = trading_client.get_account()
        equity = float(acct.equity)
    except Exception as e:
        return {"ok": False, "error": f"account_fetch_failed: {e}"}

    today = ny_date_str()
    with STATE_LOCK:
        if DAY_STATE.get("date") != today:
            DAY_STATE["date"] = today
            DAY_STATE["start_equity"] = equity

        start = float(DAY_STATE.get("start_equity", equity))

    pnl = equity - start
    return {"ok": True, "date": today, "start_equity": start, "equity": equity, "pnl": pnl}


def _daily_stop_triggered() -> tuple[bool, dict]:
    if DAILY_STOP_DOLLARS <= 0:
        return False, {"enabled": False, "threshold": DAILY_STOP_DOLLARS}
    pnl_info = _get_intraday_pnl()
    if not pnl_info.get("ok"):
        # If we cannot compute, do NOT hard-block (but we log loudly).
        logger.warning("Daily PnL unavailable: %s", pnl_info.get("error"))
        return False, {"enabled": True, "threshold": DAILY_STOP_DOLLARS, **pnl_info}

    triggered = float(pnl_info["pnl"]) <= (-1.0 * float(DAILY_STOP_DOLLARS))
    return triggered, {"enabled": True, "threshold": DAILY_STOP_DOLLARS, **pnl_info}


def _flatten_all(reason: str) -> list[dict]:
    results = []
    for p in list_open_positions_allowed():
        sym = p["symbol"]
        out = close_position(sym)
        if sym in TRADE_PLAN:
            TRADE_PLAN[sym]["active"] = False
        _unlock_symbol(sym)
        results.append({"symbol": sym, "action": "flatten", "reason": reason, **out})
    return results


# =============================================================================
# Routes
# =============================================================================
@app.get("/")
def root():
    return {"ok": True, "service": "trading-webhook", "paper": APCA_PAPER}


@app.get("/health")
def health():
    pnl = _get_intraday_pnl()
    with STATE_LOCK:
        active_plans = {k: v.get("active") for k, v in TRADE_PLAN.items()}
        locks = {k: v for k, v in SYMBOL_LOCKS.items()}
    return {
        "ok": True,
        "paper": APCA_PAPER,
        "allowed_symbols": sorted(ALLOWED_SYMBOLS),
        "allow_short": ALLOW_SHORT,
        "allow_reversal": ALLOW_REVERSAL,
        "only_market_hours": ONLY_MARKET_HOURS,
        "eod_flatten_time_ny": EOD_FLATTEN_TIME,
        "kill_switch": KILL_SWITCH,
        "daily_stop_dollars": DAILY_STOP_DOLLARS,
        "pnl": pnl,
        "active_plans": active_plans,
        "symbol_locks": locks,
        "idempotency_window_sec": IDEMPOTENCY_WINDOW_SEC,
    }


@app.get("/state")
def state():
    with STATE_LOCK:
        return {
            "ok": True,
            "trade_plan": TRADE_PLAN,
            "symbol_locks": SYMBOL_LOCKS,
            "dedupe_keys": len(DEDUPE),
            "day_state": DAY_STATE,
            "kill_switch": KILL_SWITCH,
        }


@app.post("/kill")
async def kill_switch_on(request: Request):
    _require_admin(request)
    global KILL_SWITCH
    KILL_SWITCH = True
    # Optionally flatten immediately
    results = _flatten_all("kill_switch")
    return {"ok": True, "kill_switch": KILL_SWITCH, "flattened": results}


@app.post("/unkill")
async def kill_switch_off(request: Request):
    _require_admin(request)
    global KILL_SWITCH
    KILL_SWITCH = False
    return {"ok": True, "kill_switch": KILL_SWITCH}


@app.post("/webhook")
async def webhook(req: Request):
    """
    TradingView -> this endpoint

    Implements:
    - idempotency (dedupe retries)
    - symbol lock (one active position per symbol)
    - daily stop
    - kill switch
    - detailed error logging (middleware + explicit logs)
    """
    data = await req.json()

    # Idempotency first (absorb TV retries before doing anything expensive)
    dkey = _dedupe_key(data)
    if _dedupe_seen(dkey):
        return {"ok": True, "deduped": True}

    # Secret check
    if WEBHOOK_SECRET:
        if (data.get("secret") or "").strip() != WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Invalid secret")

    # Kill switch
    if KILL_SWITCH:
        return {"ok": True, "rejected": True, "reason": "kill_switch_enabled"}

    # Daily stop check
    hit_stop, pnl_info = _daily_stop_triggered()
    if hit_stop:
        # hard stop: flip kill switch ON and flatten
        global KILL_SWITCH
        KILL_SWITCH = True
        flattened = _flatten_all("daily_stop")
        return {
            "ok": True,
            "rejected": True,
            "reason": "daily_stop_triggered",
            "pnl": pnl_info,
            "kill_switch": KILL_SWITCH,
            "flattened": flattened,
        }

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

    # Trading hours guard
    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "ignored": True, "reason": "Outside market hours", "signal": signal, "symbol": symbol}

    # Symbol lock guard (prevents multi-strat churn / duplicate entries)
    # Lock applies to entry attempts; reversals (close then open) also go through.
    if _symbol_locked(symbol):
        with STATE_LOCK:
            info = SYMBOL_LOCKS.get(symbol, {})
        return {"ok": True, "ignored": True, "reason": "symbol_locked", "symbol": symbol, "lock": info}

    # If there is already an open Alpaca position, block new entry alerts (unless reversal is happening below)
    qty_signed, pos_side = get_position(symbol)

    # Shorts disabled behavior
    if side == "sell" and not ALLOW_SHORT:
        # Treat sell as close long if present
        if qty_signed > 0:
            _lock_symbol(symbol, "close_long_shorts_disabled", ttl=60)
            out = close_position(symbol)
            if symbol in TRADE_PLAN:
                TRADE_PLAN[symbol]["active"] = False
            _unlock_symbol(symbol)
            return {"ok": True, "closed": True, "reason": "Shorts disabled; closed long", "signal": signal, "symbol": symbol, **out}
        return {"ok": True, "ignored": True, "reason": "Shorts disabled", "signal": signal, "symbol": symbol}

    # If already positioned and the signal is same-direction -> ignore.
    if qty_signed != 0:
        desired_side = "long" if side == "buy" else "short"
        if desired_side == pos_side:
            # IMPORTANT: lock briefly to absorb flurries from TV on same candle
            _lock_symbol(symbol, "already_in_position", ttl=30)
            return {
                "ok": True,
                "ignored": True,
                "reason": f"Position already open ({pos_side} {qty_signed}).",
                "signal": signal,
                "symbol": symbol,
            }

        # Opposite direction: close existing position first
        _lock_symbol(symbol, "reversal_in_progress", ttl=120)
        close_out = close_position(symbol)
        if symbol in TRADE_PLAN:
            TRADE_PLAN[symbol]["active"] = False

        if not close_out.get("closed"):
            _unlock_symbol(symbol)
            return {"ok": True, "action": "reverse_close_failed", "signal": signal, "symbol": symbol, **close_out}

        if not ALLOW_REVERSAL:
            _unlock_symbol(symbol)
            return {"ok": True, "action": "closed_opposite_position", "signal": signal, "symbol": symbol, **close_out}

        # reversal allowed -> proceed to open new position below

    else:
        # No position open. If a plan is active, treat it as a lock (source of truth is Alpaca, but this helps churn).
        if TRADE_PLAN.get(symbol, {}).get("active"):
            _lock_symbol(symbol, "plan_active", ttl=60)
            return {"ok": True, "ignored": True, "reason": "plan_active", "signal": signal, "symbol": symbol}

    # Entry sizing uses latest price
    try:
        base_price = get_latest_price(symbol)
    except Exception:
        logger.error("latest_price_failed symbol=%s\n%s", symbol, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to get latest price")

    try:
        qty = compute_qty(base_price)
    except Exception as e:
        _unlock_symbol(symbol)
        raise HTTPException(status_code=400, detail=str(e))

    payload = {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "base_price": round(base_price, 2),
        "signal": signal,
        "paper": APCA_PAPER,
        "dry_run": DRY_RUN,
        "dedupe": dkey[:10],
    }

    if DRY_RUN:
        with STATE_LOCK:
            TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)
        _lock_symbol(symbol, "dry_run_plan", ttl=SYMBOL_LOCK_TTL_SEC)
        return {"ok": True, "submitted": False, "dry_run": True, "order": payload, "plan": TRADE_PLAN[symbol]}

    # Lock symbol while submitting to prevent race conditions
    _lock_symbol(symbol, "submit_in_progress", ttl=120)
    try:
        order = submit_market_order(symbol, side, qty)
        with STATE_LOCK:
            TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)
        # keep lock while plan is active
        _lock_symbol(symbol, "plan_active", ttl=SYMBOL_LOCK_TTL_SEC)
        return {
            "ok": True,
            "submitted": True,
            "order_id": str(order.id),
            "order": payload,
            "plan": TRADE_PLAN[symbol],
        }
    except Exception:
        logger.error("alpaca_submit_failed symbol=%s side=%s qty=%s\n%s", symbol, side, qty, traceback.format_exc())
        _unlock_symbol(symbol)
        raise HTTPException(status_code=500, detail="Alpaca submit failed")


@app.post("/worker/exit")
async def worker_exit(req: Request):
    """
    Called by background worker every N seconds.

    Implements:
    - kill switch flatten
    - daily stop flatten
    - EOD flatten
    - stop/take exits
    """
    body = await req.json()

    # Optional worker auth
    if WORKER_SECRET:
        if (body.get("worker_secret") or "").strip() != WORKER_SECRET:
            raise HTTPException(status_code=401, detail="Invalid worker secret")

    # If kill switch enabled -> flatten everything in allowed list
    if KILL_SWITCH:
        results = _flatten_all("kill_switch")
        return {"ok": True, "ts_ny": now_ny().isoformat(), "mode": "kill_switch", "results": results}

    # Daily stop enforcement from worker too (belt + suspenders)
    hit_stop, pnl_info = _daily_stop_triggered()
    if hit_stop:
        global KILL_SWITCH
        KILL_SWITCH = True
        results = _flatten_all("daily_stop")
        return {
            "ok": True,
            "ts_ny": now_ny().isoformat(),
            "mode": "daily_stop",
            "pnl": pnl_info,
            "kill_switch": KILL_SWITCH,
            "results": results,
        }

    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "skipped": True, "reason": "Outside market hours"}

    eod_t = parse_hhmm(EOD_FLATTEN_TIME)
    now = now_ny()
    now_t = now.time()

    results = []

    # HARD SAFETY: at/after EOD time, flatten any open Alpaca positions in ALLOWED_SYMBOLS
    if now_t >= eod_t:
        results = _flatten_all("eod_flatten")
        return {"ok": True, "ts_ny": now.isoformat(), "results": results, "mode": "eod_flatten"}

    # Manage active plans with stop/take
    for symbol, plan in list(TRADE_PLAN.items()):
        if not plan.get("active"):
            # unlock if we still had a lock
            continue

        qty_signed, _pos_side = get_position(symbol)
        if qty_signed == 0:
            plan["active"] = False
            _unlock_symbol(symbol)
            results.append({"symbol": symbol, "action": "plan_deactivated", "reason": "No open position"})
            continue

        # Cooldown to avoid repeated exits
        last_ts = int(plan.get("last_exit_attempt_ts") or 0)
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        if now_ts - last_ts < EXIT_COOLDOWN_SEC:
            results.append(
                {"symbol": symbol, "action": "cooldown", "seconds_remaining": EXIT_COOLDOWN_SEC - (now_ts - last_ts)}
            )
            continue

        try:
            px = get_latest_price(symbol)
        except Exception:
            results.append({"symbol": symbol, "action": "error", "reason": "latest_price_failed"})
            continue

        stop_price = float(plan.get("stop_price"))
        take_price = float(plan.get("take_price"))
        side = plan.get("side")  # 'buy' (long) or 'sell' (short)

        if side == "buy":
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
                _unlock_symbol(symbol)
                results.append(
                    {
                        "symbol": symbol,
                        "action": f"exit_{reason}",
                        "price": px,
                        "stop": stop_price,
                        "take": take_price,
                        **out,
                    }
                )
            else:
                results.append(
                    {
                        "symbol": symbol,
                        "action": f"exit_{reason}_failed_or_none",
                        "price": px,
                        "stop": stop_price,
                        "take": take_price,
                        **out,
                    }
                )
        else:
            results.append({"symbol": symbol, "action": "hold", "price": px, "stop": stop_price, "take": take_price, "qty": qty_signed})

    return {"ok": True, "ts_ny": now.isoformat(), "results": results}
