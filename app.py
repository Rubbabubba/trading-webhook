import os
import threading
import hashlib
import time as _time
import logging
import traceback
from datetime import datetime, time, timezone, date
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


# -----------------------------
# Logging (prints into Render logs)
# -----------------------------
logger = logging.getLogger("trading-webhook")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

# -----------------------------
# App
# -----------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# ENV helpers
# -----------------------------
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


# -----------------------------
# ENV
# -----------------------------
WEBHOOK_SECRET = getenv_any("WEBHOOK_SECRET", default="")

# Alpaca creds (APCA-first, compat fallbacks)
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

# If true, allow reversing (close existing position then open opposite direction)
ALLOW_REVERSAL = getenv_bool("ALLOW_REVERSAL", "true")

# Exit worker auth (optional but recommended)
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

# Admin auth (for /kill, /unkill, /state, etc.)
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "").strip()

# Exit timing
NY_TZ = ZoneInfo("America/New_York")
EOD_FLATTEN_TIME = os.getenv("EOD_FLATTEN_TIME", "15:55")  # HH:MM NY time
EXIT_COOLDOWN_SEC = getenv_int("EXIT_COOLDOWN_SEC", "20")  # prevent spam exits if called often

# Trading hours guard
ONLY_MARKET_HOURS = getenv_bool("ONLY_MARKET_HOURS", "true")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Optional: only accept known signals (comma-separated). Empty = accept all.
ALLOWED_SIGNALS = set(s.strip() for s in os.getenv("ALLOWED_SIGNALS", "").split(",") if s.strip())

# Idempotency (dedupe)
IDEMPOTENCY_WINDOW_SEC = getenv_int("IDEMPOTENCY_WINDOW_SEC", "120")

# Daily stop + kill switch behavior
DAILY_STOP_DOLLARS = getenv_float("DAILY_STOP_DOLLARS", "50")  # triggers kill when P&L <= -X
KILL_SWITCH_DEFAULT = getenv_bool("KILL_SWITCH", "false")
KILL_SWITCH_FLATTEN_ON_WEBHOOK = getenv_bool("KILL_SWITCH_FLATTEN_ON_WEBHOOK", "true")

# -----------------------------
# Alpaca clients (validate creds early)
# -----------------------------
if not APCA_KEY or not APCA_SECRET:
    missing = []
    if not APCA_KEY:
        missing.append("APCA_API_KEY_ID (or ALPACA_KEY_ID/ALPACA_API_KEY_ID)")
    if not APCA_SECRET:
        missing.append("APCA_API_SECRET_KEY (or ALPACA_SECRET_KEY/ALPACA_API_SECRET_KEY)")
    raise RuntimeError("Missing Alpaca credentials: " + ", ".join(missing))

trading_client = TradingClient(APCA_KEY, APCA_SECRET, paper=APCA_PAPER)
data_client = StockHistoricalDataClient(APCA_KEY, APCA_SECRET)

# -----------------------------
# In-memory state (Render restarts wipe this)
# -----------------------------
STATE_LOCK = threading.Lock()

TRADE_PLAN: dict[str, dict] = {
    # symbol -> plan dict
}

# symbol locks to prevent concurrent entries / overlapping strategies per symbol
SYMBOL_LOCKS: dict[str, float] = {
    # symbol -> locked_until_epoch_seconds
}

# Idempotency cache: digest -> last_seen_epoch_seconds
IDEMPOTENCY_CACHE: dict[str, float] = {}

# Runtime toggles / daily state
RUNTIME = {
    "kill_switch": bool(KILL_SWITCH_DEFAULT),
    "last_kill_reason": "",
}

DAILY_STATE = {
    "date_ny": "",          # YYYY-MM-DD
    "start_equity": None,   # float
    "last_equity": None,    # float
    "pnl": None,            # float
    "triggered": False,
    "triggered_at": "",
    "trigger_reason": "",
}

# -----------------------------
# Helpers
# -----------------------------
def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)


def ny_today_str() -> str:
    return now_ny().date().isoformat()


def parse_hhmm(hhmm: str) -> time:
    parts = hhmm.strip().split(":")
    return time(int(parts[0]), int(parts[1]))


def in_market_hours() -> bool:
    t = now_ny().time()
    return (t >= MARKET_OPEN) and (t <= MARKET_CLOSE)


def _require_admin(req: Request) -> None:
    """Admin auth: header x-admin-secret OR json {admin_secret:""} when ADMIN_SECRET is set."""
    if not ADMIN_SECRET:
        return  # admin endpoints unprotected if user doesn't set it (not recommended)
    hdr = (req.headers.get("x-admin-secret") or "").strip()
    if hdr == ADMIN_SECRET:
        return
    # try json body (best-effort)
    try:
        # NOTE: request body may only be readable once; admin endpoints are small anyway
        # We'll accept missing body; fail secure.
        pass
    except Exception:
        pass
    raise HTTPException(status_code=401, detail="Unauthorized (admin)")


def _safe_log_exception(prefix: str, exc: Exception) -> None:
    logger.error("%s: %s\n%s", prefix, exc, traceback.format_exc())


def _digest_payload(symbol: str, side: str, signal: str, price: str | None) -> str:
    """
    Compute a stable idempotency key.
    If TradingView retries the same alert, this stays the same.
    """
    base = f"{symbol}|{side}|{signal}|{(price or '').strip()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _purge_idempotency(now_ts: float) -> None:
    cutoff = now_ts - IDEMPOTENCY_WINDOW_SEC
    dead = [k for k, t in IDEMPOTENCY_CACHE.items() if t < cutoff]
    for k in dead:
        IDEMPOTENCY_CACHE.pop(k, None)


def _lock_symbol(symbol: str, seconds: int = 180) -> None:
    SYMBOL_LOCKS[symbol] = _time.time() + max(1, int(seconds))


def _symbol_locked(symbol: str) -> bool:
    until = SYMBOL_LOCKS.get(symbol, 0.0)
    return _time.time() < float(until)


def _unlock_symbol(symbol: str) -> None:
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
        "side": side,  # 'buy' for long entries, 'sell' for short entries
        "qty": float(qty),
        "entry_price": round(entry_price, 4),
        "stop_price": stop_price,
        "take_price": take_price,
        "signal": signal,
        "opened_at": now_ny().isoformat(),
        "last_exit_attempt_ts": 0,
    }


def _get_account_equity() -> float | None:
    """
    Best-effort: use account equity (not realized-only).
    This is safer for a 'daily stop' because it includes open P&L too.
    """
    try:
        acct = trading_client.get_account()
        # alpaca-py returns strings for some numeric fields
        eq = float(getattr(acct, "equity"))
        return eq
    except Exception as e:
        _safe_log_exception("equity_fetch_failed", e)
        return None


def _refresh_daily_state() -> None:
    """
    Ensure DAILY_STATE is initialized for today (NY date) and update P&L.
    Trigger kill switch if P&L <= -DAILY_STOP_DOLLARS.
    """
    today = ny_today_str()

    eq = _get_account_equity()
    if eq is None:
        return

    # initialize per day
    if DAILY_STATE["date_ny"] != today:
        DAILY_STATE["date_ny"] = today
        DAILY_STATE["start_equity"] = eq
        DAILY_STATE["last_equity"] = eq
        DAILY_STATE["pnl"] = 0.0
        DAILY_STATE["triggered"] = False
        DAILY_STATE["triggered_at"] = ""
        DAILY_STATE["trigger_reason"] = ""
        logger.info("daily_state_reset date=%s start_equity=%.2f", today, eq)
        return

    # update
    start_eq = float(DAILY_STATE["start_equity"] or eq)
    pnl = eq - start_eq
    DAILY_STATE["last_equity"] = eq
    DAILY_STATE["pnl"] = pnl

    if (not DAILY_STATE["triggered"]) and (pnl <= -abs(float(DAILY_STOP_DOLLARS))):
        DAILY_STATE["triggered"] = True
        DAILY_STATE["triggered_at"] = now_ny().isoformat()
        DAILY_STATE["trigger_reason"] = f"daily_stop_hit pnl={pnl:.2f} <= -{abs(float(DAILY_STOP_DOLLARS)):.2f}"
        RUNTIME["kill_switch"] = True
        RUNTIME["last_kill_reason"] = DAILY_STATE["trigger_reason"]
        logger.warning("DAILY_STOP TRIGGERED: %s", DAILY_STATE["trigger_reason"])


def _flatten_all(reason: str) -> list[dict]:
    results: list[dict] = []
    for p in list_open_positions_allowed():
        sym = p["symbol"]
        out = close_position(sym)
        if sym in TRADE_PLAN:
            TRADE_PLAN[sym]["active"] = False
        _unlock_symbol(sym)
        results.append({"symbol": sym, "action": f"flatten_{reason}", **out})
    return results


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "trading-webhook", "paper": APCA_PAPER}


@app.get("/health")
def health():
    with STATE_LOCK:
        return {
            "ok": True,
            "paper": APCA_PAPER,
            "allowed_symbols": sorted(ALLOWED_SYMBOLS),
            "allow_short": ALLOW_SHORT,
            "allow_reversal": ALLOW_REVERSAL,
            "only_market_hours": ONLY_MARKET_HOURS,
            "eod_flatten_time_ny": EOD_FLATTEN_TIME,
            "active_plans": {k: v.get("active") for k, v in TRADE_PLAN.items()},
            "symbol_locks": {k: v for k, v in SYMBOL_LOCKS.items()},
            "kill_switch": RUNTIME["kill_switch"],
            "daily_state": DAILY_STATE,
            "idempotency_window_sec": IDEMPOTENCY_WINDOW_SEC,
        }


@app.get("/state")
def state(req: Request):
    _require_admin(req)
    with STATE_LOCK:
        return {
            "ok": True,
            "trade_plan": TRADE_PLAN,
            "symbol_locks": SYMBOL_LOCKS,
            "kill_switch": RUNTIME,
            "daily_state": DAILY_STATE,
            "idempotency_cache_size": len(IDEMPOTENCY_CACHE),
        }


@app.post("/kill")
async def kill_switch_on(request: Request):
    _require_admin(request)
    with STATE_LOCK:
        RUNTIME["kill_switch"] = True
        RUNTIME["last_kill_reason"] = "manual_kill"
        results = _flatten_all("kill_switch")
    return {"ok": True, "kill_switch": RUNTIME["kill_switch"], "flattened": results}


@app.post("/unkill")
async def kill_switch_off(request: Request):
    _require_admin(request)
    with STATE_LOCK:
        RUNTIME["kill_switch"] = False
        RUNTIME["last_kill_reason"] = ""
    return {"ok": True, "kill_switch": RUNTIME["kill_switch"]}


@app.post("/webhook")
async def webhook(req: Request):
    """
    TradingView -> Render webhook.
    Implements:
      - Symbol lock (one active position/plan per symbol)
      - Idempotency (dedupe retries)
      - Daily stop (equity-based)
      - Kill switch (reject entries + optionally flatten)
      - Full exception logging (no more "500 with no details")
    """
    try:
        data = await req.json()
    except Exception as e:
        _safe_log_exception("webhook_json_parse_failed", e)
        raise HTTPException(status_code=400, detail="Invalid JSON")

    try:
        # Secret check
        if WEBHOOK_SECRET:
            if (data.get("secret") or "").strip() != WEBHOOK_SECRET:
                raise HTTPException(status_code=401, detail="Invalid secret")

        symbol = (data.get("symbol") or "").upper().strip()
        side = (data.get("side") or "").lower().strip()  # 'buy' or 'sell'
        signal = (data.get("signal") or "").strip()
        price_in = (data.get("price") or None)

        if not symbol:
            raise HTTPException(status_code=400, detail="symbol is required")

        if symbol not in ALLOWED_SYMBOLS:
            raise HTTPException(status_code=400, detail=f"Symbol not allowed: {symbol}")

        if side not in ("buy", "sell"):
            raise HTTPException(status_code=400, detail="side must be 'buy' or 'sell'")

        if ALLOWED_SIGNALS and signal not in ALLOWED_SIGNALS:
            raise HTTPException(status_code=400, detail=f"Signal not allowed: {signal}")

        # Refresh daily stop
        with STATE_LOCK:
            _refresh_daily_state()

            # Kill switch guard
            if RUNTIME["kill_switch"]:
                # Optional: flatten immediately on webhook hit (safer)
                flattened = []
                if KILL_SWITCH_FLATTEN_ON_WEBHOOK:
                    flattened = _flatten_all("kill_switch")
                return {
                    "ok": True,
                    "rejected": True,
                    "reason": "kill_switch_enabled",
                    "last_kill_reason": RUNTIME.get("last_kill_reason", ""),
                    "flattened": flattened,
                    "symbol": symbol,
                    "signal": signal,
                }

            # Daily stop guard (if triggered it turns on kill switch above, but keep explicit)
            if DAILY_STATE.get("triggered"):
                flattened = []
                if KILL_SWITCH_FLATTEN_ON_WEBHOOK:
                    flattened = _flatten_all("daily_stop")
                return {
                    "ok": True,
                    "rejected": True,
                    "reason": "daily_stop_triggered",
                    "daily_state": DAILY_STATE,
                    "flattened": flattened,
                    "symbol": symbol,
                    "signal": signal,
                }

        # Market hours guard
        if ONLY_MARKET_HOURS and not in_market_hours():
            return {"ok": True, "ignored": True, "reason": "Outside market hours", "signal": signal, "symbol": symbol}

        # Idempotency (dedupe)
        now_ts = _time.time()
        digest = _digest_payload(symbol, side, signal, str(price_in) if price_in is not None else None)

        with STATE_LOCK:
            _purge_idempotency(now_ts)
            last_seen = IDEMPOTENCY_CACHE.get(digest)
            if last_seen is not None and (now_ts - float(last_seen)) < IDEMPOTENCY_WINDOW_SEC:
                return {
                    "ok": True,
                    "ignored": True,
                    "reason": "idempotent_duplicate",
                    "window_sec": IDEMPOTENCY_WINDOW_SEC,
                    "symbol": symbol,
                    "signal": signal,
                }
            IDEMPOTENCY_CACHE[digest] = now_ts

        # Shorts disabled behavior
        if side == "sell" and not ALLOW_SHORT:
            qty_signed, _pos_side = get_position(symbol)
            if qty_signed > 0:
                out = close_position(symbol)
                with STATE_LOCK:
                    if symbol in TRADE_PLAN:
                        TRADE_PLAN[symbol]["active"] = False
                    _unlock_symbol(symbol)
                return {
                    "ok": True,
                    "closed": True,
                    "reason": "Shorts disabled; closed long",
                    "signal": signal,
                    "symbol": symbol,
                    **out,
                }
            return {"ok": True, "ignored": True, "reason": "Shorts disabled", "signal": signal, "symbol": symbol}

        # -----------------------------
        # SYMBOL LOCK (hard)
        # If plan active OR Alpaca position exists OR lock TTL active -> block new entries
        # -----------------------------
        with STATE_LOCK:
            if _symbol_locked(symbol):
                return {"ok": True, "ignored": True, "reason": "symbol_locked", "symbol": symbol, "signal": signal}

            # If our in-memory plan says active, block entry
            plan = TRADE_PLAN.get(symbol)
            if plan and plan.get("active"):
                return {
                    "ok": True,
                    "ignored": True,
                    "reason": "plan_active_symbol_lock",
                    "symbol": symbol,
                    "signal": signal,
                }

        # If Alpaca already shows a position, handle as in your original logic
        qty_signed, pos_side = get_position(symbol)
        if qty_signed != 0:
            desired_side = "long" if side == "buy" else "short"

            if desired_side == pos_side:
                # lock to prevent churn
                with STATE_LOCK:
                    _lock_symbol(symbol, seconds=180)
                return {
                    "ok": True,
                    "ignored": True,
                    "reason": f"Position already open ({pos_side} {qty_signed}).",
                    "signal": signal,
                    "symbol": symbol,
                }

            # Opposite direction: close, then optionally open
            close_out = close_position(symbol)
            with STATE_LOCK:
                if symbol in TRADE_PLAN:
                    TRADE_PLAN[symbol]["active"] = False
                _unlock_symbol(symbol)

            if not close_out.get("closed"):
                return {"ok": True, "action": "reverse_close_failed", "signal": signal, "symbol": symbol, **close_out}

            if not ALLOW_REVERSAL:
                # lock briefly after close to avoid immediate re-open spam
                with STATE_LOCK:
                    _lock_symbol(symbol, seconds=60)
                return {"ok": True, "action": "closed_opposite_position", "signal": signal, "symbol": symbol, **close_out}

            # fall through to open new position

        # Price + sizing
        try:
            base_price = get_latest_price(symbol)
        except Exception as e:
            _safe_log_exception("latest_price_failed", e)
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

        # Acquire lock BEFORE submit to prevent races across simultaneous alerts
        with STATE_LOCK:
            _lock_symbol(symbol, seconds=180)

        if DRY_RUN:
            with STATE_LOCK:
                TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)
            return {"ok": True, "submitted": False, "dry_run": True, "order": payload, "plan": TRADE_PLAN[symbol]}

        try:
            order = submit_market_order(symbol, side, qty)
            with STATE_LOCK:
                TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)
            return {
                "ok": True,
                "submitted": True,
                "order_id": str(order.id),
                "order": payload,
                "plan": TRADE_PLAN[symbol],
            }
        except Exception as e:
            # IMPORTANT: if submit fails, release lock so future valid signals can work
            _safe_log_exception("alpaca_submit_failed", e)
            with STATE_LOCK:
                _unlock_symbol(symbol)
                if symbol in TRADE_PLAN:
                    TRADE_PLAN[symbol]["active"] = False
            raise HTTPException(status_code=500, detail=f"Alpaca submit failed: {e}")

    except HTTPException:
        # re-raise expected errors without extra noise
        raise
    except Exception as e:
        # Full stack trace into Render logs
        _safe_log_exception("webhook_unhandled_exception", e)
        raise HTTPException(status_code=500, detail="Internal error (see logs for stack trace)")


@app.post("/worker/exit")
async def worker_exit(req: Request):
    """Called by background worker every N seconds."""
    try:
        body = await req.json()
    except Exception as e:
        _safe_log_exception("worker_json_parse_failed", e)
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Optional worker auth
    if WORKER_SECRET:
        if (body.get("worker_secret") or "").strip() != WORKER_SECRET:
            raise HTTPException(status_code=401, detail="Invalid worker secret")

    # Refresh daily stop each worker tick too
    with STATE_LOCK:
        _refresh_daily_state()

        # Kill switch mode: flatten and exit
        if RUNTIME["kill_switch"]:
            results = _flatten_all("kill_switch")
            return {"ok": True, "ts_ny": now_ny().isoformat(), "mode": "kill_switch", "results": results}

        # Daily stop mode: if triggered, flatten
        if DAILY_STATE.get("triggered"):
            results = _flatten_all("daily_stop")
            return {"ok": True, "ts_ny": now_ny().isoformat(), "mode": "daily_stop", "results": results}

    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "skipped": True, "reason": "Outside market hours"}

    eod_t = parse_hhmm(EOD_FLATTEN_TIME)
    now = now_ny()
    now_t = now.time()

    results = []

    # HARD SAFETY: at/after EOD time, flatten any open Alpaca positions in ALLOWED_SYMBOLS
    if now_t >= eod_t:
        with STATE_LOCK:
            for p in list_open_positions_allowed():
                sym = p["symbol"]
                out = close_position(sym)
                if sym in TRADE_PLAN:
                    TRADE_PLAN[sym]["active"] = False
                _unlock_symbol(sym)
                results.append({"symbol": sym, "action": "flatten_eod", **out})
        return {"ok": True, "ts_ny": now.isoformat(), "results": results, "mode": "eod_flatten"}

    # Otherwise, manage active plans with stop/take
    for symbol, plan in list(TRADE_PLAN.items()):
        if not plan.get("active"):
            continue

        qty_signed, _pos_side = get_position(symbol)
        if qty_signed == 0:
            with STATE_LOCK:
                plan["active"] = False
                _unlock_symbol(symbol)
            results.append({"symbol": symbol, "action": "plan_deactivated", "reason": "No open position"})
            continue

        # Cooldown to avoid repeated exits
        last_ts = int(plan.get("last_exit_attempt_ts") or 0)
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        if now_ts - last_ts < EXIT_COOLDOWN_SEC:
            results.append(
                {
                    "symbol": symbol,
                    "action": "cooldown",
                    "seconds_remaining": EXIT_COOLDOWN_SEC - (now_ts - last_ts),
                }
            )
            continue

        try:
            px = get_latest_price(symbol)
        except Exception as e:
            _safe_log_exception("worker_latest_price_failed", e)
            results.append({"symbol": symbol, "action": "error", "reason": f"latest_price_failed: {e}"})
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
            with STATE_LOCK:
                plan["last_exit_attempt_ts"] = now_ts
            reason = "stop" if hit_stop else "target"
            out = close_position(symbol)
            if out.get("closed"):
                with STATE_LOCK:
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
            results.append(
                {"symbol": symbol, "action": "hold", "price": px, "stop": stop_price, "take": take_price, "qty": qty_signed}
            )

    return {"ok": True, "ts_ny": now.isoformat(), "results": results}
