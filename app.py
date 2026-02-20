import os
import logging
import hashlib
import traceback
import time as _time
from datetime import datetime, time, timezone, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class Bar:
    ts_utc: datetime
    ts_ny: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: Optional[float] = None


# =============================
# App
# =============================
app = FastAPI()

# Standard library logger used by the scanner/worker endpoints.
# (Some observability patches rely on `logger.*`; keep this defined even if you
# primarily use the `log()` print helper elsewhere.)
logger = logging.getLogger("trading-webhook")
if not logging.getLogger().handlers:
    # Render captures stdout/stderr; basicConfig ensures level + formatting.
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

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


def getenv_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except ValueError:
        return default

def getenv_int(name: str, default: int) -> int:
    """Read an int env var with a safe fallback."""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)
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
ENABLE_IDEMPOTENCY = getenv_any("ENABLE_IDEMPOTENCY", default="true").strip().lower() in ("1","true","yes","y","on")  # allow disabling dedup if needed
# Idempotency
DEDUP_WINDOW_SEC = int(getenv_any("DEDUP_WINDOW_SEC", "IDEMPOTENCY_WINDOW_SECONDS", default="90"))  # absorb retries

# Symbol lock
SYMBOL_LOCK_SEC = int(getenv_any("SYMBOL_LOCK_SEC", "SYMBOL_LOCK_SECONDS", default="180"))  # lock during entry/plan



# =============================
# Scanner (Phase 1C - shadow mode default)
# =============================
SCANNER_ENABLED = env_bool("SCANNER_ENABLED", "false")
SCANNER_DRY_RUN = env_bool("SCANNER_DRY_RUN", "true")
SCANNER_ALLOW_LIVE = env_bool("SCANNER_ALLOW_LIVE", "false")  # hard gate: must be true to ever place scanner orders
LAST_SCAN: dict = {}

SCANNER_UNIVERSE_PROVIDER = getenv_any("SCANNER_UNIVERSE_PROVIDER", default="static").lower()
SCANNER_MAX_SYMBOLS_PER_CYCLE = int(getenv_any("SCANNER_MAX_SYMBOLS_PER_CYCLE", default="200"))


# Rotation scanning (for large universes)
SCANNER_ROTATION_ENABLED = os.getenv("SCANNER_ROTATION_ENABLED", "true").lower() == "true"
# Optional: separate universe list for scanner (comma-separated). If set, scanner uses this instead of ALLOWED_SYMBOLS.
SCANNER_UNIVERSE_SYMBOLS = os.getenv("SCANNER_UNIVERSE_SYMBOLS", "").strip()

# 5m resampling / strategy tuning
RESAMPLE_5M_MIN_BARS = int(os.getenv("RESAMPLE_5M_MIN_BARS", "40"))  # ~ last ~3h20m on 5m

# Midbox loosening knobs
MIDBOX_BREAKOUT_BUFFER_PCT = float(os.getenv("MIDBOX_BREAKOUT_BUFFER_PCT", "0.0005"))  # 0.05%
MIDBOX_BREAKOUT_USE_HIGHLOW = os.getenv("MIDBOX_BREAKOUT_USE_HIGHLOW", "true").lower() == "true"

# Power hour loosening knobs
PWR_BREAKOUT_BUFFER_PCT = float(os.getenv("PWR_BREAKOUT_BUFFER_PCT", "0.0005"))  # 0.05%
PWR_BREAKOUT_USE_HIGHLOW = os.getenv("PWR_BREAKOUT_USE_HIGHLOW", "true").lower() == "true"

# Strategy C: VWAP pullback on 5m
ENABLE_STRATEGY_VWAP_PULLBACK = os.getenv("ENABLE_STRATEGY_VWAP_PULLBACK", "true").lower() == "true"
VWAP_PB_EMA_FAST = int(os.getenv("VWAP_PB_EMA_FAST", "20"))
VWAP_PB_EMA_SLOW = int(os.getenv("VWAP_PB_EMA_SLOW", "50"))
VWAP_PB_BAND_PCT = float(os.getenv("VWAP_PB_BAND_PCT", "0.0015"))  # 0.15% band around VWAP counts as a touch
VWAP_PB_MAX_EXTENSION_PCT = float(os.getenv("VWAP_PB_MAX_EXTENSION_PCT", "0.006"))  # don't chase if >0.6% away from VWAP
VWAP_PB_MIN_EMA_SLOPE = float(os.getenv("VWAP_PB_MIN_EMA_SLOPE", "0.0"))  # per-bar slope
SCANNER_LOOKBACK_DAYS = int(getenv_any("SCANNER_LOOKBACK_DAYS", default="3"))
SCANNER_REQUIRE_MARKET_HOURS = env_bool("SCANNER_REQUIRE_MARKET_HOURS", "true")

# Session windows (configurable for parity; defaults match your Pine inputs)
MIDBOX_BUILD_SESSION = getenv_any("MIDBOX_BUILD_SESSION", default="10:00-11:30")
MIDBOX_TRADE_SESSION = getenv_any("MIDBOX_TRADE_SESSION", default="11:30-15:00")
PWR_SESSION = getenv_any("PWR_SESSION", default="15:00-16:00")

def _parse_session_hhmm_range(rng: str) -> tuple[time, time]:
    # Accept "HH:MM-HH:MM" (24h). If invalid, fall back to full market session.
    try:
        a, b = rng.strip().split("-")
        return parse_hhmm(a), parse_hhmm(b)
    except Exception:
        return MARKET_OPEN, MARKET_CLOSE

# Strategy toggles (approximate parity v1; will refine against Pine)
SCANNER_ENABLE_MIDBOX = env_bool("SCANNER_ENABLE_MIDBOX", "true")
SCANNER_ENABLE_PWR = env_bool("SCANNER_ENABLE_PWR", "true")
SCANNER_PWR_LOOKBACK_BARS = int(getenv_any("SCANNER_PWR_LOOKBACK_BARS", default="30"))

# Optional liquidity filters for 'alpaca' universe provider
SCANNER_MIN_PRICE = float(getenv_any("SCANNER_MIN_PRICE", default="5"))
SCANNER_MAX_PRICE = float(getenv_any("SCANNER_MAX_PRICE", default="1000"))
SCANNER_MIN_AVG_VOLUME = float(getenv_any("SCANNER_MIN_AVG_VOLUME", default="500000"))
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

# Decision traces (in-memory ring buffer)
DECISION_BUFFER_SIZE = int(getenv_any("DECISION_BUFFER_SIZE", default="1000"))
DECISIONS: list[dict] = []  # append-only, trimmed to DECISION_BUFFER_SIZE

# Guards in-memory shared state when scan evaluation runs concurrently
STATE_LOCK = threading.RLock()


# Scan rotation state (in-memory). Keeps a moving window through the universe so we can
# scan hundreds/thousands of symbols without hammering the provider each tick.
_scan_rotation = {"ny_date": None, "idx": 0}


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


def record_decision(event: str, source: str, symbol: str = "", side: str = "", signal: str = "",
                    action: str = "", reason: str = "", **details):
    """Record a structured decision trace for observability and debugging."""
    try:
        item = {
            "ts_utc": datetime.now(tz=timezone.utc).isoformat(),
            "ts_ny": now_ny().isoformat(),
            "event": event,
            "source": source,
            "symbol": (symbol or "").upper(),
            "side": side,
            "signal": signal,
            "action": action,
            "reason": reason,
        }
        if details:
            item["details"] = details
        with STATE_LOCK:
            DECISIONS.append(item)
            if len(DECISIONS) > max(DECISION_BUFFER_SIZE, 50):
                overflow = len(DECISIONS) - max(DECISION_BUFFER_SIZE, 50)
                if overflow > 0:
                    del DECISIONS[:overflow]
    except Exception:
        # Never let tracing break trading.
        pass



def config_effective_snapshot() -> dict:
    return {
        "paper": APCA_PAPER,
        "allowed_symbols_count": len(ALLOWED_SYMBOLS),
        "allow_short": ALLOW_SHORT,
        "allow_reversal": ALLOW_REVERSAL,
        "only_market_hours": ONLY_MARKET_HOURS,
        "eod_flatten_time_ny": EOD_FLATTEN_TIME,
        "enable_idempotency": ENABLE_IDEMPOTENCY,
        "enable_idempotency": ENABLE_IDEMPOTENCY,
        "dedup_window_sec": DEDUP_WINDOW_SEC,
        "symbol_lock_sec": SYMBOL_LOCK_SEC,
        "decision_buffer_size": DECISION_BUFFER_SIZE,
        "daily_stop_dollars": DAILY_STOP_DOLLARS,
        "dry_run": DRY_RUN,
        "decision_buffer_size": DECISION_BUFFER_SIZE,
    }


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # This is the missing piece: you will now see the real traceback in Render logs.
    log("UNHANDLED_EXCEPTION", path=str(request.url.path), err=str(exc))
    traceback.print_exc()
    return JSONResponse(status_code=500, content={"ok": False, "error": "Internal Server Error"})



# Emit effective config once at startup (helps debug Render env mismatches)
log("CONFIG_EFFECTIVE", **config_effective_snapshot())

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


def list_open_positions_details_allowed() -> list[dict]:
    """Alpaca positions (allowed symbols) including avg entry price."""
    out: list[dict] = []
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
            avg_entry = None
            try:
                avg_entry = float(getattr(p, "avg_entry_price", None) or 0) or None
            except Exception:
                avg_entry = None
            out.append({"symbol": sym, "qty": qty, "avg_entry_price": avg_entry})
        except Exception:
            continue
    return out


def reconcile_trade_plans_from_alpaca() -> list[dict]:
    """
    Ensure internal TRADE_PLAN has an active plan for each open Alpaca position.
    This protects live positions across restarts/redeploys where TRADE_PLAN is empty.
    Returns a list of reconcile actions.
    """
    actions: list[dict] = []
    for p in list_open_positions_details_allowed():
        sym = p["symbol"]
        qty_signed = float(p["qty"])
        avg_entry = p.get("avg_entry_price")
        if qty_signed == 0:
            continue

        # Determine current position side.
        side = "buy" if qty_signed > 0 else "sell"
        qty = abs(qty_signed)

        plan = TRADE_PLAN.get(sym, {})
        if plan.get("active"):
            # Already tracking it.
            continue

        if not avg_entry:
            # Fallback: best-effort latest price if avg entry missing.
            try:
                avg_entry = get_latest_price(sym)
            except Exception:
                avg_entry = None

        if not avg_entry:
            actions.append({"symbol": sym, "action": "reconcile_skipped", "reason": "no_entry_price"})
            record_decision("RECONCILE", "worker_exit", sym, side=side, signal="",
                            action="skipped", reason="no_entry_price")
            continue

        recovered_plan = build_trade_plan(sym, side, qty, float(avg_entry), signal="RECOVERED")
        recovered_plan["recovered"] = True
        recovered_plan["recovered_at"] = now_ny().isoformat()
        TRADE_PLAN[sym] = recovered_plan
        actions.append({"symbol": sym, "action": "recovered_plan", "qty": qty_signed, "entry": recovered_plan["entry_price"]})
        record_decision("RECONCILE", "worker_exit", sym, side=side, signal="RECOVERED",
                        action="recovered_plan", reason="missing_internal_plan",
                        qty=qty_signed, entry_price=recovered_plan["entry_price"])
    return actions


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
    """Housekeeping for in-memory caches (dedup + symbol locks)."""
    now = utc_ts()
    with STATE_LOCK:
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
    with STATE_LOCK:
        exp = SYMBOL_LOCKS.get(symbol, 0)
    return exp > now


def lock_symbol(symbol: str):
    with STATE_LOCK:
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
# Scanner helpers (server-side signal evaluation)
# =============================
def _session_key(dt_ny: datetime) -> str:
    return dt_ny.strftime("%Y-%m-%d")


def universe_symbols() -> list[str]:
    """Return the symbol universe for scanning."""
    if SCANNER_UNIVERSE_PROVIDER == "static":
        return sorted(ALLOWED_SYMBOLS)[:SCANNER_MAX_SYMBOLS_PER_CYCLE]

    if SCANNER_UNIVERSE_PROVIDER == "env":
        raw = getenv_any("SCANNER_SYMBOLS", default="")
        if not raw:
            return sorted(ALLOWED_SYMBOLS)[:SCANNER_MAX_SYMBOLS_PER_CYCLE]
        syms = [s.strip().upper() for s in raw.split(",") if s.strip()]
        return syms[:SCANNER_MAX_SYMBOLS_PER_CYCLE]

    # NOTE: 'alpaca' provider can be added later (requires assets + liquidity filter).
    # For safety in Phase 1C, we fall back to ALLOWED_SYMBOLS unless explicitly enabled.
    return sorted(ALLOWED_SYMBOLS)[:SCANNER_MAX_SYMBOLS_PER_CYCLE]


def fetch_1m_bars(symbol: str, lookback_days: int) -> list[dict]:
    """Fetch recent 1-minute bars for a symbol. Returns list of dicts with UTC ts + OHLCV + vwap."""
    # Conservative lookback to stay within API limits and keep scan fast.
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=max(1, lookback_days))
    req = StockBarsRequest(symbol_or_symbols=symbol, timeframe=TimeFrame.Minute, start=start, end=end)
    bars = data_client.get_stock_bars(req)
    rows = []
    try:
        seq = bars.data.get(symbol, [])
    except Exception:
        seq = []
    for b in seq:
        try:
            ts = b.timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            rows.append({
                "ts_utc": ts,
                "ts_ny": ts.astimezone(NY_TZ),
                "open": float(b.open),
                "high": float(b.high),
                "low": float(b.low),
                "close": float(b.close),
                "volume": float(getattr(b, "volume", 0) or 0),
                "vwap": float(getattr(b, "vwap", 0) or 0),
            })
        except Exception:
            continue
    return rows


def ema_series(closes: list[float], length: int) -> list[float]:
    """Compute EMA series."""
    if not closes:
        return []
    alpha = 2.0 / (length + 1.0)
    ema = closes[0]
    out = [ema]
    for c in closes[1:]:
        ema = (alpha * c) + ((1 - alpha) * ema)
        out.append(ema)
    return out


def resample_to_5m(bars_1m: list[Bar]) -> list[Bar]:
    """Resample 1m bars into 5m bars (NY time), producing OHLCV + VWAP.
    Bar.ts_ny is kept as the *end* of each 5m bucket (last minute in the bucket).
    """
    if not bars_1m:
        return []
    buckets: dict[datetime, list[Bar]] = {}
    for b in bars_1m:
        bucket_start = b.ts_ny.replace(second=0, microsecond=0, minute=(b.ts_ny.minute // 5) * 5)
        buckets.setdefault(bucket_start, []).append(b)

    out: list[Bar] = []
    for bucket_start in sorted(buckets.keys()):
        group = buckets[bucket_start]
        group.sort(key=lambda x: x.ts_ny)
        o = group[0].open
        h = max(x.high for x in group)
        l = min(x.low for x in group)
        c = group[-1].close
        v = sum(x.volume for x in group)
        pv = sum((x.close * x.volume) for x in group)
        vwap = (pv / v) if v > 0 else c
        out.append(Bar(ts_utc=group[-1].ts_utc, ts_ny=group[-1].ts_ny, open=o, high=h, low=l, close=c, volume=v, vwap=vwap))
    return out


def _bars_for_today_session(bars: list[dict]) -> list[dict]:
    if not bars:
        return []
    today = now_ny().date()
    out = []
    for r in bars:
        ts = r["ts_ny"]
        if ts.date() != today:
            continue
        # Regular session only
        if ts.time() < MARKET_OPEN or ts.time() > MARKET_CLOSE:
            continue
        out.append(r)
    return out


def eval_midbox_signal(bars_today: list[dict]) -> tuple[str, str] | None:
    """Approx MIDBOX signal eval: break of midbox high/low with VWAP + EMA200 slope filter."""
    if not bars_today:
        return None

    # Build EMA200 over today's closes (approx; Pine uses ema200 on chart timeframe).
    closes = [r["close"] for r in bars_today]
    ema200 = ema_series(closes, 200)
    if len(ema200) < 3:
        return None
    ema_slope_up = ema200[-1] > ema200[-2]
    ema_slope_down = ema200[-1] < ema200[-2]

    # Midbox window 10:00–11:30 NY
    mid_start, mid_end = _parse_session_hhmm_range(MIDBOX_BUILD_SESSION)
    mid = [r for r in bars_today if mid_start <= r["ts_ny"].time() <= mid_end]
    if len(mid) < 10:
        return None
    mid_high = max(r["high"] for r in mid)
    mid_low = min(r["low"] for r in mid)

    # Breakout window 11:30–15:00
    bo_start, bo_end = _parse_session_hhmm_range(MIDBOX_TRADE_SESSION)
    bo = [r for r in bars_today if bo_start <= r["ts_ny"].time() <= bo_end]
    if len(bo) < 2:
        return None
    last = bo[-1]
    prev = bo[-2]
    vwap = last["vwap"] or 0.0

    # Long
    if SCANNER_ENABLE_MIDBOX and (last["close"] > mid_high) and (prev["close"] <= mid_high):
        if (vwap == 0.0 or last["close"] > vwap) and ema_slope_up:
            return ("MIDBOX_LONG_GREEN", "buy")

    # Short
    if SCANNER_ENABLE_MIDBOX and ALLOW_SHORT and (last["close"] < mid_low) and (prev["close"] >= mid_low):
        if (vwap == 0.0 or last["close"] < vwap) and ema_slope_down:
            return ("MIDBOX_SHORT_RED", "sell")

    return None


def eval_power_hour_signal(bars_today: list[dict]) -> tuple[str, str] | None:
    """Approx PWR signal eval: rolling breakout in 15:00–16:00 window."""
    if not bars_today or not SCANNER_ENABLE_PWR:
        return None


def eval_vwap_pullback_signal(bars_5m: list[Bar]) -> str | None:
    """Strategy C: VWAP Pullback (5m)
    Long bias only (unless ALLOW_SHORT=True): requires uptrend (EMA fast above EMA slow),
    pullback to VWAP (touch within band), then reclaim VWAP.
    """
    if not ENABLE_STRATEGY_VWAP_PULLBACK:
        return None
    if len(bars_5m) < max(VWAP_PB_EMA_SLOW + 5, RESAMPLE_5M_MIN_BARS):
        return None

    closes = [b.close for b in bars_5m]
    vwaps = [b.vwap for b in bars_5m]
    ema_fast = ema_series(closes, VWAP_PB_EMA_FAST)
    ema_slow = ema_series(closes, VWAP_PB_EMA_SLOW)

    last = bars_5m[-1]
    prev = bars_5m[-2]
    vwap_last = vwaps[-1]
    vwap_prev = vwaps[-2]

    # Trend filter (long)
    if not (ema_fast[-1] > ema_slow[-1]):
        return None
    if (ema_fast[-1] - ema_fast[-2]) < VWAP_PB_MIN_EMA_SLOPE:
        return None

    # Don't chase
    if abs(last.close - vwap_last) / vwap_last > VWAP_PB_MAX_EXTENSION_PCT:
        return None

    band = vwap_last * VWAP_PB_BAND_PCT
    touched = (last.low <= vwap_last + band) and (last.low >= vwap_last - band)
    reclaimed = (last.close >= vwap_last) and (prev.close <= vwap_prev + band)

    if touched and reclaimed:
        return "BUY"

    # Optional short symmetry
    if ALLOW_SHORT:
        if not (ema_fast[-1] < ema_slow[-1]):
            return None
        if (ema_fast[-2] - ema_fast[-1]) < VWAP_PB_MIN_EMA_SLOPE:
            return None
        if abs(last.close - vwap_last) / vwap_last > VWAP_PB_MAX_EXTENSION_PCT:
            return None
        touched_s = (last.high >= vwap_last - band) and (last.high <= vwap_last + band)
        reclaimed_s = (last.close <= vwap_last) and (prev.close >= vwap_prev - band)
        if touched_s and reclaimed_s:
            return "SELL"

    return None

    ph_start, ph_end = _parse_session_hhmm_range(PWR_SESSION)
    ph = [r for r in bars_today if ph_start <= r["ts_ny"].time() <= ph_end]
    if len(ph) < max(SCANNER_PWR_LOOKBACK_BARS + 2, 5):
        return None

    closes = [r["close"] for r in bars_today]
    ema200 = ema_series(closes, 200)
    if len(ema200) < 3:
        return None
    ema_slope_up = ema200[-1] > ema200[-2]
    ema_slope_down = ema200[-1] < ema200[-2]

    last = ph[-1]
    prev = ph[-2]
    lookback = ph[-(SCANNER_PWR_LOOKBACK_BARS + 1):-1]  # exclude last
    if len(lookback) < 3:
        return None
    roll_high = max(r["high"] for r in lookback)
    roll_low = min(r["low"] for r in lookback)
    vwap = last["vwap"] or 0.0

    if (last["close"] > roll_high) and (prev["close"] <= roll_high) and (vwap == 0.0 or last["close"] > vwap) and ema_slope_up:
        return ("PWR_LONG_GREEN", "buy")

    if ALLOW_SHORT and (last["close"] < roll_low) and (prev["close"] >= roll_low) and (vwap == 0.0 or last["close"] < vwap) and ema_slope_down:
        return ("PWR_SHORT_RED", "sell")

    return None


def scanner_idempotency_key(symbol: str, signal: str, bar_ts_ny: datetime) -> str:
    # One-per-symbol-per-signal-per-minute bucket (Phase 1C)
    bucket = bar_ts_ny.replace(second=0, microsecond=0).isoformat()
    raw = f"scan-{symbol}-{signal}-{bucket}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
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
        "enable_idempotency": ENABLE_IDEMPOTENCY,
        "dedup_window_sec": DEDUP_WINDOW_SEC,
        "symbol_lock_sec": SYMBOL_LOCK_SEC,
        "decision_buffer_size": DECISION_BUFFER_SIZE,
        "daily_stop_dollars": DAILY_STOP_DOLLARS,
        "kill_switch": KILL_SWITCH,
        "active_plans": {k: v.get("active") for k, v in TRADE_PLAN.items()},
    }
@app.get("/scanner/status")
def scanner_status():
    """
    Quick visibility into scanner configuration and the last scan summary.
    Safe for production; does not expose secrets.
    """
    effective_dry_run = bool(SCANNER_DRY_RUN or DRY_RUN or (not SCANNER_ALLOW_LIVE))
    return {
        "ok": True,
        "scanner": {
            "enabled": SCANNER_ENABLED,
            "dry_run": SCANNER_DRY_RUN,
            "allow_live": SCANNER_ALLOW_LIVE,
            "effective_dry_run": effective_dry_run,
            "require_market_hours": SCANNER_REQUIRE_MARKET_HOURS,
            "universe_provider": SCANNER_UNIVERSE_PROVIDER,
            "lookback_days": SCANNER_LOOKBACK_DAYS,
            "max_symbols_per_cycle": SCANNER_MAX_SYMBOLS_PER_CYCLE,
        },
        "last_scan": LAST_SCAN,
    }


@app.get("/state")
def state():
    return {"ok": True, "trade_plan": TRADE_PLAN, "symbol_locks": SYMBOL_LOCKS}

@app.get("/diagnostics/decisions")
def diagnostics_decisions(symbol: str = "", limit: int = 200):
    """Return recent decision traces for debugging (in-memory)."""
    sym = (symbol or "").upper().strip()
    lim = max(1, min(int(limit or 200), 2000))
    items = DECISIONS
    if sym:
        items = [d for d in items if d.get("symbol") == sym]
    return {"ok": True, "count": len(items), "items": items[-lim:]}



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
    record_decision("ENTRY", "webhook", symbol, side=side, signal=signal, action="received", reason="")

    if not symbol:
        return JSONResponse(status_code=200, content={"ok": False, "rejected": True, "reason": "symbol_required"})

    if symbol not in ALLOWED_SYMBOLS:
        record_decision("ENTRY","webhook",symbol,side=side,signal=signal,action="rejected",reason="symbol_not_allowed")
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
        record_decision("ENTRY","webhook",symbol,side=side,signal=signal,action="ignored",reason="outside_market_hours")
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
    if ENABLE_IDEMPOTENCY:
        dk = dedup_key(data)
        last = DEDUP_CACHE.get(dk, 0)
        now = utc_ts()
        if last and (now - last) < DEDUP_WINDOW_SEC:
            record_decision("ENTRY", "webhook", symbol, side=side, signal=signal, action="ignored", reason="dedup")
            return {"ok": True, "ignored": True, "reason": "dedup", "symbol": symbol, "signal": signal}
        DEDUP_CACHE[dk] = now

    # Symbol lock: prevent multiple overlapping entries
    # Also prevent entries if a plan is active or Alpaca shows an open position.
    if is_symbol_locked(symbol):
        record_decision("ENTRY","webhook",symbol,side=side,signal=signal,action="ignored",reason="symbol_locked")
        return {"ok": True, "ignored": True, "reason": "symbol_locked", "symbol": symbol, "signal": signal}

    if TRADE_PLAN.get(symbol, {}).get("active"):
        record_decision("ENTRY","webhook",symbol,side=side,signal=signal,action="ignored",reason="plan_active")
        return {"ok": True, "ignored": True, "reason": "plan_active", "symbol": symbol, "signal": signal}

    qty_signed, pos_side = get_position(symbol)
    if qty_signed != 0:
        desired_side = "long" if side == "buy" else "short"
        if desired_side == pos_side:
            record_decision("ENTRY","webhook",symbol,side=side,signal=signal,action="ignored",reason=f"position_already_open:{pos_side}")
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
        record_decision("ENTRY","webhook",symbol,side=side,signal=signal,action="dry_run_plan_created",reason="")
        return {"ok": True, "submitted": False, "dry_run": True, "order": payload, "plan": TRADE_PLAN[symbol]}

    # Submit order (do NOT throw 500 on broker rejections)
    try:
        order = submit_market_order(symbol, side, qty)
        TRADE_PLAN[symbol] = build_trade_plan(symbol, side, qty, base_price, signal)
        log("ORDER_SUBMITTED", symbol=symbol, side=side, qty=qty, order_id=str(order.id), signal=signal)
        record_decision("ENTRY","webhook",symbol,side=side,signal=signal,action="order_submitted",reason="",order_id=str(order.id),qty=qty)
        return {"ok": True, "submitted": True, "order_id": str(order.id), "order": payload, "plan": TRADE_PLAN[symbol]}
    except Exception as e:
        log("ORDER_REJECTED", symbol=symbol, side=side, qty=qty, err=str(e), signal=signal)
        record_decision("ENTRY","webhook",symbol,side=side,signal=signal,action="rejected",reason="alpaca_submit_failed",err=str(e))
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

    # Reconcile internal plans from Alpaca positions (protects positions across restarts)
    reconcile_actions = reconcile_trade_plans_from_alpaca()

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
        return {"ok": True, "ts_ny": now.isoformat(), "reconcile": reconcile_actions, "results": results, "mode": "eod_flatten"}

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

    return {"ok": True, "ts_ny": now.isoformat(), "reconcile": reconcile_actions, "results": results}


@app.post("/worker/scan_entries")
async def worker_scan_entries(req: Request):
    """Server-side scanner entry evaluation (Phase 1C). Default is shadow-mode (no orders)."""
    cleanup_caches()
    scan_start_utc = utc_ts()

    body = {}
    try:
        body = await req.json()
    except Exception:
        body = {}

    # Optional worker auth
    if WORKER_SECRET:
        if (body.get("worker_secret") or "").strip() != WORKER_SECRET:
            raise HTTPException(status_code=401, detail="Invalid worker secret")

    effective_dry_run = bool(SCANNER_DRY_RUN or DRY_RUN or (not SCANNER_ALLOW_LIVE))

    def _set_last_scan(**kwargs):
        LAST_SCAN.clear()
        LAST_SCAN.update({
            "ts_ny": now_ny().isoformat(),
            "ts_utc": datetime.now(tz=timezone.utc).isoformat(),
            "enabled": SCANNER_ENABLED,
            "dry_run": SCANNER_DRY_RUN,
            "allow_live": SCANNER_ALLOW_LIVE,
            "effective_dry_run": effective_dry_run,
            "universe_provider": SCANNER_UNIVERSE_PROVIDER,
            **kwargs
        })

    try:
            if not SCANNER_ENABLED:
                _set_last_scan(skipped=True, reason="scanner_disabled", scanned=0, signals=0, would_trade=0, blocked=0, duration_ms=int((utc_ts()-scan_start_utc)*1000))
                record_decision("SCAN", "worker_scan", action="skipped", reason="scanner_disabled")
                return {"ok": True, "skipped": True, "reason": "scanner_disabled", **LAST_SCAN}

            if SCANNER_REQUIRE_MARKET_HOURS and ONLY_MARKET_HOURS and not in_market_hours():
                _set_last_scan(skipped=True, reason="outside_market_hours", scanned=0, signals=0, would_trade=0, blocked=0, duration_ms=int((utc_ts()-scan_start_utc)*1000))
                record_decision("SCAN", "worker_scan", action="skipped", reason="outside_market_hours")
                return {"ok": True, "skipped": True, "reason": "outside_market_hours", **LAST_SCAN}

            # Reconcile first: never place entries against stale internal state.
            reconcile_actions = reconcile_trade_plans_from_alpaca()

            syms = universe_symbols()
            blocked = 0
            results = []
            signals = []

            logger.info(
                "SCAN_START enabled=%s dry_run=%s allow_live=%s effective_dry_run=%s provider=%s symbols=%s",
                SCANNER_ENABLED, SCANNER_DRY_RUN, SCANNER_ALLOW_LIVE, effective_dry_run, SCANNER_UNIVERSE_PROVIDER, len(syms)
            )

            max_workers = getenv_int("SCAN_EVAL_CONCURRENCY", 8)
            max_workers = max(1, min(max_workers, len(syms) or 1))

            def _eval_one(sym: str) -> dict:
                local_results: list[dict] = []
                local_signals: list[dict] = []
                local_blocked = 0
                try:
                    if is_symbol_locked(sym):
                        local_blocked += 1
                        return {"results": local_results, "signals": local_signals, "blocked": local_blocked}


                    # DEDUP (thread-safe)
                    # Idempotency / dedup key for scan evaluation.
                    # This used to include a config object (cfg.strategy) but cfg is not
                    # in scope here. Since this service currently runs a single scan
                    # evaluation pipeline, the symbol is sufficient.
                    key = f"scan|{sym}"
                    nowu = utc_ts()
                    with STATE_LOCK:
                        last = DEDUP_CACHE.get(key, 0)
                        if nowu - last < DEDUP_WINDOW_SEC:
                            return {"results": local_results, "signals": local_signals, "blocked": local_blocked}
                        DEDUP_CACHE[key] = nowu

                    # (optional) 1m bars fetch removed; it was unused and caused signature mismatch
                    price = get_latest_price(sym)
                    if price is None:
                        local_blocked += 1
                        return {"results": local_results, "signals": local_signals, "blocked": local_blocked}

                    action = "hold"
                    reason = ""
                    plan = TRADE_PLAN.get(sym)

                    if plan:
                        stop = float(plan.get("stop", 0) or 0)
                        take = float(plan.get("take", 0) or 0)
                        if stop and price <= stop:
                            action = "exit"
                            reason = "stop_hit"
                        elif take and price >= take:
                            action = "exit"
                            reason = "take_hit"

                    stop_out = plan.get("stop") if plan else None
                    take_out = plan.get("take") if plan else None

                    if action != "hold":
                        local_signals.append({"symbol": sym, "action": action, "price": price})

                    local_results.append({
                        "symbol": sym,
                        "action": action,
                        "price": price,
                        "stop": stop_out,
                        "take": take_out,
                    })

                    if reason:
                        record_decision("scan_decision", source="scan", symbol=sym, action=action, reason=reason, price=price)

                except Exception as e:
                    logger.exception("SCAN_EVAL_ERROR symbol=%s err=%s", sym, str(e))
                    local_blocked += 1
                return {"results": local_results, "signals": local_signals, "blocked": local_blocked}

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(_eval_one, sym) for sym in syms]
                for fut in as_completed(futures):
                    out = fut.result()
                    results.extend(out.get("results", []))
                    signals.extend(out.get("signals", []))
                    blocked += int(out.get("blocked", 0))

            scan_end_utc = utc_ts()
            duration_ms = int((scan_end_utc - scan_start_utc) * 1000)

            _set_last_scan(
                skipped=False,
                reason=None,
                scanned=len(syms),
                signals=len(signals),
                would_trade=len(signals),
                blocked=blocked,
                duration_ms=duration_ms,
            )

            logger.info("SCAN_DONE scanned=%s signals=%s would_trade=%s blocked=%s duration_ms=%s",
                        len(syms), len(signals), len(signals), blocked, duration_ms)

            return {
                "ok": True,
                "scanner": {
                    "enabled": SCANNER_ENABLED,
                    "dry_run": SCANNER_DRY_RUN,
                    "allow_live": SCANNER_ALLOW_LIVE,
                    "effective_dry_run": effective_dry_run,
                    "universe_provider": SCANNER_UNIVERSE_PROVIDER,
                    "symbols_scanned": len(syms),
                    "signals": len(signals),
                    "would_trade": len(signals),
                    "blocked": blocked,
                    "duration_ms": duration_ms,
                },
                "reconcile": reconcile_actions,
                "would_submit": signals,
                "results": results,
            }
    except Exception as e:
        scan_end_utc = utc_ts()
        duration_ms = int((scan_end_utc - scan_start_utc) * 1000)
        try:
            _set_last_scan(skipped=False, reason='scan_exception', error=str(e), scanned=0, signals=0, would_trade=0, blocked=0, duration_ms=duration_ms)
        except Exception:
            pass
        try:
            record_decision('SCAN', 'worker_scan', action='error', reason='scan_exception', err=str(e))
        except Exception:
            pass
        logger.exception('SCAN_FATAL error=%s', str(e))
        return {'ok': False, 'error': 'scan_exception', 'detail': str(e), **LAST_SCAN}
