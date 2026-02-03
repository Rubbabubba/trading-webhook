import os
import time
import traceback
import hashlib
from datetime import datetime, timezone, date
from typing import Dict, Any, Optional

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, ClosePositionRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


# -----------------------------
# Config
# -----------------------------
APP_NAME = os.getenv("APP_NAME", "trading-webhook")

ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
# Paper vs Live:
# - For live: set ALPACA_PAPER="false" and use live keys
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "true").strip().lower() in ("1", "true", "yes", "y")

TV_SECRET = os.getenv("TV_SECRET", "")

ALLOWLIST = [s.strip().upper() for s in os.getenv("ALLOWLIST", "SPY,QQQ,NVDA,TSLA,AMD,META").split(",") if s.strip()]

# Risk / sizing
RISK_DOLLARS = float(os.getenv("RISK_DOLLARS", "25"))  # dollars at risk per trade (via stop distance)
STOP_PCT = float(os.getenv("STOP_PCT", "0.003"))       # 0.30%
TAKE_PCT = float(os.getenv("TAKE_PCT", "0.0045"))      # 0.45%
MIN_QTY = float(os.getenv("MIN_QTY", "1"))
MAX_QTY = float(os.getenv("MAX_QTY", "25"))

# Safety rails
SYMBOL_LOCK_SECONDS = int(os.getenv("SYMBOL_LOCK_SECONDS", "3600"))  # lock while in position/plan
IDEMPOTENCY_WINDOW_SECONDS = int(os.getenv("IDEMPOTENCY_WINDOW_SECONDS", "10"))
DAILY_STOP_DOLLARS = float(os.getenv("DAILY_STOP_DOLLARS", "50"))   # stop trading after -$X realized
KILL_SWITCH = os.getenv("KILL_SWITCH", "0").strip().lower() in ("1", "true", "yes", "y")  # immediate stop

# Only allow longs (you said: stop shorting; no partial patches)
ALLOW_SHORTS = os.getenv("ALLOW_SHORTS", "0").strip().lower() in ("1", "true", "yes", "y")


# -----------------------------
# Clients
# -----------------------------
if not ALPACA_KEY_ID or not ALPACA_SECRET_KEY:
    # still start, but webhook will reject trading
    print(f"[{APP_NAME}] WARNING: Alpaca keys not set.")

trading_client = TradingClient(ALPACA_KEY_ID, ALPACA_SECRET_KEY, paper=ALPACA_PAPER)
data_client = StockHistoricalDataClient(ALPACA_KEY_ID, ALPACA_SECRET_KEY)

app = FastAPI(title=APP_NAME)

# -----------------------------
# In-memory state
# (NOTE: Render restarts will clear these — still OK as safety rails, not as accounting system.)
# -----------------------------
TRADE_PLAN: Dict[str, Dict[str, Any]] = {}           # per-symbol plan: entry/stop/take/qty/order_id/ts
SYMBOL_LOCKS: Dict[str, float] = {}                  # symbol -> lock_expiry_epoch
ALERT_DEDUPE: Dict[str, float] = {}                  # dedupe_key -> last_seen_epoch
REALIZED_PNL_BY_DAY: Dict[str, float] = {}           # "YYYY-MM-DD" -> realized pnl


# -----------------------------
# Helpers
# -----------------------------
def now_ts() -> float:
    return time.time()

def today_key() -> str:
    return date.today().isoformat()

def realized_today() -> float:
    return REALIZED_PNL_BY_DAY.get(today_key(), 0.0)

def daily_stop_tripped() -> bool:
    # Daily stop means: if realized PnL <= -DAILY_STOP_DOLLARS, stop new entries AND flatten
    return realized_today() <= -abs(DAILY_STOP_DOLLARS)

def is_symbol_locked(symbol: str) -> bool:
    exp = SYMBOL_LOCKS.get(symbol)
    if not exp:
        return False
    if exp < now_ts():
        SYMBOL_LOCKS.pop(symbol, None)
        return False
    return True

def lock_symbol(symbol: str) -> None:
    SYMBOL_LOCKS[symbol] = now_ts() + SYMBOL_LOCK_SECONDS

def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        if x != x:  # NaN
            return None
        return x
    except Exception:
        return None

def clamp_qty(qty: float) -> float:
    qty = max(qty, MIN_QTY)
    qty = min(qty, MAX_QTY)
    # Alpaca supports fractional shares; keep up to 6 decimals
    return float(f"{qty:.6f}")

def compute_qty(entry_price: float) -> float:
    if entry_price <= 0:
        raise ValueError("entry_price must be > 0")

    stop_distance = entry_price * STOP_PCT
    if stop_distance <= 0:
        raise ValueError("stop_distance must be > 0")

    qty = RISK_DOLLARS / stop_distance
    return clamp_qty(qty)

def build_trade_plan(symbol: str, entry_price: float, qty: float, side: str, signal: str, order_id: str) -> Dict[str, Any]:
    if side.lower() not in ("buy", "sell"):
        raise ValueError("side must be 'buy' or 'sell'")

    if side.lower() == "buy":
        stop = entry_price * (1 - STOP_PCT)
        take = entry_price * (1 + TAKE_PCT)
    else:
        stop = entry_price * (1 + STOP_PCT)
        take = entry_price * (1 - TAKE_PCT)

    return {
        "symbol": symbol,
        "side": side.lower(),
        "signal": signal,
        "entry_price": float(entry_price),
        "qty": float(qty),
        "stop": float(stop),
        "take": float(take),
        "order_id": order_id,
        "created_ts": now_ts(),
        "last_update_ts": now_ts(),
    }

def get_latest_price(symbol: str) -> float:
    req = StockLatestTradeRequest(symbol_or_symbols=symbol)
    trade = data_client.get_stock_latest_trade(req)
    if symbol not in trade or not trade[symbol] or not getattr(trade[symbol], "price", None):
        raise RuntimeError(f"Could not fetch latest trade price for {symbol}")
    return float(trade[symbol].price)

def dedupe_key(payload: Dict[str, Any]) -> str:
    # Prefer explicit alert_id if you include it in TV JSON
    alert_id = payload.get("alert_id") or payload.get("id") or payload.get("uuid")
    symbol = (payload.get("symbol") or "").upper()
    side = (payload.get("side") or "").lower()
    signal = (payload.get("signal") or "")
    # Bucket by time window to absorb TV retries
    bucket = int(now_ts() // max(1, IDEMPOTENCY_WINDOW_SECONDS))

    raw = f"{alert_id}|{symbol}|{side}|{signal}|{bucket}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def seen_recently(key: str) -> bool:
    t = ALERT_DEDUPE.get(key)
    if not t:
        return False
    if (now_ts() - t) <= IDEMPOTENCY_WINDOW_SECONDS:
        return True
    return False

def mark_seen(key: str) -> None:
    ALERT_DEDUPE[key] = now_ts()
    # keep map from growing unbounded
    if len(ALERT_DEDUPE) > 5000:
        # drop oldest ~20%
        items = sorted(ALERT_DEDUPE.items(), key=lambda kv: kv[1])
        for k, _ in items[:1000]:
            ALERT_DEDUPE.pop(k, None)

def log_event(msg: str, **kv: Any) -> None:
    extra = " ".join([f"{k}={v}" for k, v in kv.items()])
    print(f"[{APP_NAME}] {msg} {extra}".strip())

def require_trading_ready() -> None:
    if not ALPACA_KEY_ID or not ALPACA_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Alpaca keys not configured")
    if not TV_SECRET:
        raise HTTPException(status_code=500, detail="TV_SECRET not configured")


# -----------------------------
# Models
# -----------------------------
class WebhookPayload(BaseModel):
    secret: str
    symbol: str
    side: str
    signal: str
    price: Optional[str] = None
    alert_id: Optional[str] = None


# -----------------------------
# Routes
# -----------------------------
@app.get("/healthz")
def healthz():
    return {
        "ok": True,
        "paper": ALPACA_PAPER,
        "kill_switch": KILL_SWITCH,
        "daily_stop_tripped": daily_stop_tripped(),
        "realized_today": realized_today(),
        "symbols_allowed": ALLOWLIST,
        "active_plans": list(TRADE_PLAN.keys()),
    }


@app.post("/webhook")
async def webhook(request: Request):
    """
    TradingView -> Render webhook

    Safety:
    - Secret check
    - Allowlist
    - Long-only (unless ALLOW_SHORTS=1)
    - Idempotency / dedupe
    - Symbol lock (one open plan/position per symbol)
    - Daily stop / kill switch
    """
    require_trading_ready()

    payload = await request.json()
    try:
        # Basic validation
        if payload.get("secret") != TV_SECRET:
            raise HTTPException(status_code=401, detail="Invalid secret")

        symbol = (payload.get("symbol") or "").upper().strip()
        side = (payload.get("side") or "").lower().strip()
        signal = (payload.get("signal") or "").strip()

        if not symbol or symbol not in ALLOWLIST:
            raise HTTPException(status_code=400, detail=f"Symbol not allowed: {symbol}")

        if side not in ("buy", "sell"):
            raise HTTPException(status_code=400, detail=f"Invalid side: {side}")

        if (side == "sell") and not ALLOW_SHORTS:
            raise HTTPException(status_code=400, detail="Shorts are disabled (sell entries blocked)")

        if KILL_SWITCH:
            log_event("webhook_rejected_kill_switch", symbol=symbol, side=side, signal=signal)
            return {"status": "rejected", "reason": "kill_switch"}

        if daily_stop_tripped():
            log_event("webhook_rejected_daily_stop", symbol=symbol, side=side, signal=signal, realized=realized_today())
            return {"status": "rejected", "reason": "daily_stop_tripped", "realized_today": realized_today()}

        # Idempotency
        key = dedupe_key(payload)
        if seen_recently(key):
            log_event("webhook_duplicate_ignored", symbol=symbol, side=side, signal=signal)
            return {"status": "ignored", "reason": "duplicate"}

        # Symbol lock (in-memory) + plan check
        if symbol in TRADE_PLAN or is_symbol_locked(symbol):
            log_event("webhook_symbol_locked", symbol=symbol, side=side, signal=signal)
            mark_seen(key)
            return {"status": "ignored", "reason": "symbol_locked"}

        # Extra: check Alpaca open position (truth source)
        try:
            _ = trading_client.get_open_position(symbol)
            log_event("webhook_symbol_has_open_position", symbol=symbol, side=side, signal=signal)
            lock_symbol(symbol)
            mark_seen(key)
            return {"status": "ignored", "reason": "alpaca_open_position"}
        except Exception:
            pass  # no open position

        # Price: prefer payload; fallback to Alpaca latest
        p = safe_float(payload.get("price"))
        if not p or p <= 0:
            p = get_latest_price(symbol)
            log_event("webhook_price_fallback", symbol=symbol, price=p)

        qty = compute_qty(p)
        order_req = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        order = trading_client.submit_order(order_req)
        order_id = getattr(order, "id", "") or getattr(order, "client_order_id", "") or "unknown"

        plan = build_trade_plan(symbol, p, qty, side, signal, order_id)
        TRADE_PLAN[symbol] = plan
        lock_symbol(symbol)
        mark_seen(key)

        log_event("webhook_order_submitted", symbol=symbol, side=side, signal=signal, qty=qty, price=p, order_id=order_id,
                  stop=plan["stop"], take=plan["take"])
        return {"status": "ok", "symbol": symbol, "qty": qty, "order_id": order_id, "stop": plan["stop"], "take": plan["take"]}

    except HTTPException:
        raise
    except Exception as e:
        # IMPORTANT: print full traceback so Render shows it (otherwise you only see 500 without context)
        tb = traceback.format_exc()
        log_event("webhook_exception", error=str(e))
        print(tb)
        raise HTTPException(status_code=500, detail="Internal error (see logs for traceback)")


@app.post("/worker/exit")
def worker_exit():
    """
    Background loop calls this endpoint (every ~30s) to manage exits.
    - If kill switch or daily stop: flatten all and stop taking new trades
    - Otherwise: monitor stop/take per active plan
    """
    require_trading_ready()

    # Safety: if daily stop or kill switch => flatten everything
    if KILL_SWITCH or daily_stop_tripped():
        reason = "kill_switch" if KILL_SWITCH else "daily_stop_tripped"
        log_event("flatten_all", reason=reason, realized_today=realized_today())
        try:
            positions = trading_client.get_all_positions()
            for pos in positions:
                sym = getattr(pos, "symbol", None)
                if sym:
                    close_position(sym, reason=reason)
        except Exception as e:
            log_event("flatten_all_error", error=str(e))
            print(traceback.format_exc())

        TRADE_PLAN.clear()
        return {"status": "ok", "action": "flatten_all", "reason": reason}

    # Normal monitoring
    closed = []
    for symbol, plan in list(TRADE_PLAN.items()):
        try:
            last = get_latest_price(symbol)
            plan["last_update_ts"] = now_ts()

            stop = plan["stop"]
            take = plan["take"]
            side = plan["side"]

            hit = None
            if side == "buy":
                if last <= stop:
                    hit = "stop"
                elif last >= take:
                    hit = "take"
            else:
                if last >= stop:
                    hit = "stop"
                elif last <= take:
                    hit = "take"

            if hit:
                close_position(symbol, reason=hit, last_price=last)
                closed.append({"symbol": symbol, "reason": hit, "last": last})
                TRADE_PLAN.pop(symbol, None)

        except Exception as e:
            log_event("exit_loop_error", symbol=symbol, error=str(e))
            print(traceback.format_exc())

    return {"status": "ok", "closed": closed, "active": list(TRADE_PLAN.keys())}


def close_position(symbol: str, reason: str, last_price: Optional[float] = None):
    """
    Closes symbol position and records realized PnL (best-effort).
    """
    symbol = symbol.upper().strip()
    plan = TRADE_PLAN.get(symbol)

    try:
        resp = trading_client.close_position(symbol, ClosePositionRequest(qty=None))  # close full position
        log_event("position_close_submitted", symbol=symbol, reason=reason, response=str(resp)[:200])
    except Exception as e:
        log_event("position_close_error", symbol=symbol, reason=reason, error=str(e))
        print(traceback.format_exc())
        return

    # Best-effort realized PnL estimate:
    # - Use plan entry_price and last_price (or latest price)
    try:
        if plan:
            entry = float(plan["entry_price"])
            qty = float(plan["qty"])
            side = plan["side"]
            exit_px = float(last_price) if last_price else get_latest_price(symbol)

            if side == "buy":
                pnl = (exit_px - entry) * qty
            else:
                pnl = (entry - exit_px) * qty

            k = today_key()
            REALIZED_PNL_BY_DAY[k] = REALIZED_PNL_BY_DAY.get(k, 0.0) + pnl

            log_event("realized_pnl_recorded", symbol=symbol, reason=reason, pnl=round(pnl, 4), realized_today=round(realized_today(), 4),
                      entry=entry, exit=exit_px, qty=qty)

            # If we just tripped daily stop, lock all symbols to prevent re-entry until restart
            if daily_stop_tripped():
                log_event("daily_stop_tripped_after_close", realized_today=realized_today(), threshold=-abs(DAILY_STOP_DOLLARS))
    except Exception:
        log_event("pnl_calc_error", symbol=symbol, reason=reason)
        print(traceback.format_exc())
