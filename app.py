import os
import logging
import hashlib
import traceback
import time as _time
from datetime import datetime, time, timezone, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import Counter

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# --- Data feed for Alpaca market data ---
# Valid values typically include 'iex' (free) and 'sip' (paid). Default to 'iex'
# to avoid runtime errors on accounts without SIP access.
_DATA_FEED_RAW = os.getenv("DATA_FEED", "iex").strip().lower() or "iex"
try:
    from alpaca.data.enums import DataFeed as _AlpacaDataFeed  # type: ignore

    DATA_FEED = getattr(_AlpacaDataFeed, _DATA_FEED_RAW.upper(), _AlpacaDataFeed.IEX)
except Exception:
    # Fallback: alpaca-py version may accept strings directly.
    DATA_FEED = _DATA_FEED_RAW

# --- Data adjustment for Alpaca bars ---
# Some Alpaca endpoints accept either an enum (alpaca.data.enums.Adjustment)
# or a string. Default to 'raw' for consistent intraday signals.
DATA_ADJUSTMENT_RAW = os.getenv("DATA_ADJUSTMENT", "raw").strip().lower() or "raw"
try:
    from alpaca.data.enums import Adjustment as _AlpacaAdjustment  # type: ignore

    _ADJ_MAP = {
        "raw": _AlpacaAdjustment.RAW,
        "split": _AlpacaAdjustment.SPLIT,
        "dividend": _AlpacaAdjustment.DIVIDEND,
        "all": _AlpacaAdjustment.ALL,
    }
    ADJUSTMENT = _ADJ_MAP.get(DATA_ADJUSTMENT_RAW, _AlpacaAdjustment.RAW)
except Exception:
    ADJUSTMENT = DATA_ADJUSTMENT_RAW

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


def parse_session_window(raw: str) -> tuple[time, time] | None:
    """Parse 'HH:MM-HH:MM' in NY (market) time."""
    if not raw:
        return None
    s = raw.strip()
    if "-" not in s:
        return None
    a, b = [x.strip() for x in s.split("-", 1)]
    try:
        return parse_hhmm(a), parse_hhmm(b)
    except Exception:
        return None

def in_session(raw: str, t: time | None = None) -> bool:
    win = parse_session_window(raw)
    if not win:
        return True
    start, end = win
    tt = t or now_ny().time()
    return (tt >= start) and (tt <= end)

def parse_session_ranges(raw: str) -> list[tuple[time, time]]:
    """Parse comma/semicolon separated session windows like '09:35-11:30,13:00-15:50'."""
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = [p.strip() for p in re.split(r"[;,]", raw) if p.strip()]
    ranges: list[tuple[time, time]] = []
    for p in parts:
        try:
            start_t, end_t = parse_session_window(p)
            ranges.append((start_t, end_t))
        except Exception:
            # Ignore malformed fragments; keep scanner running rather than crashing.
            log("SCANNER_SESSION_PARSE_ERROR", raw=raw, fragment=p)
    return ranges

_SCANNER_SESSION_RANGES_CACHE = None  # parsed (start,end) times

def in_scanner_session(now_ny: datetime | None = None) -> bool:
    """True if within configured scanner session windows. If no windows configured, True."""
    global _SCANNER_SESSION_RANGES_CACHE
    if not SCANNER_SESSIONS_NY:
        return True
    if now_ny is None:
        now_ny = ny_now()
    t = now_ny.time()
    if _SCANNER_SESSION_RANGES_CACHE is None:
        _SCANNER_SESSION_RANGES_CACHE = parse_session_ranges(SCANNER_SESSIONS_NY)
    if not _SCANNER_SESSION_RANGES_CACHE:
        return True
    for start_t, end_t in _SCANNER_SESSION_RANGES_CACHE:
        if start_t <= t <= end_t:
            return True
    return False

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
# Scanner session gating: optional intraday windows in NY time (comma/semicolon separated).
# Example: "09:35-11:30,13:00-15:50". If empty, scanner runs any time market-hours gating allows.
SCANNER_SESSIONS_NY = os.getenv("SCANNER_SESSIONS_NY", "").strip()

# Latest prices caching (seconds). Helps reduce Alpaca rate-limit pressure.
try:
    LATEST_PRICES_CACHE_TTL_SEC = max(0, int(os.getenv("LATEST_PRICES_CACHE_TTL_SEC", "2")))
except Exception:
    LATEST_PRICES_CACHE_TTL_SEC = 2



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
VWAP_PB_PULLBACK_LOOKBACK_BARS = int(os.getenv("VWAP_PB_PULLBACK_LOOKBACK_BARS", "12"))  # bars to look back for VWAP touch
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
SCANNER_ENABLE_VWAP_PB = env_bool("SCANNER_ENABLE_VWAP_PB", str(ENABLE_STRATEGY_VWAP_PULLBACK).lower())
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

# In-memory scan history to help debug the equities scanner.
# This is intentionally small and ephemeral (in-memory only).
SCAN_HISTORY_SIZE = int(os.getenv("SCAN_HISTORY_SIZE", "50"))
SCAN_HISTORY: list[dict] = []  # append-only, trimmed to SCAN_HISTORY_SIZE

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


def get_latest_price(symbol: str) -> Optional[float]:
    req = StockLatestTradeRequest(symbol_or_symbols=[symbol])
    latest = data_client.get_stock_latest_trade(req)
    # Alpaca may omit symbols with no recent trade / unsupported tickers / feed limits.
    # Return None so callers can treat it as "no data" instead of raising KeyError.
    trade = None
    try:
        trade = latest.get(symbol) if hasattr(latest, "get") else latest[symbol]
    except KeyError:
        trade = None
    if trade is None:
        logging.warning("LATEST_PRICE_MISSING symbol=%s", symbol)
        return None
    px = float(trade.price)
    if px <= 0:
        raise ValueError("Latest trade price invalid")
    return px



# -------- Latest price batching + small cache (reduces Alpaca 429) --------
_LATEST_PRICES_CACHE: dict[str, float] | None = None
_LATEST_PRICES_CACHE_TS: float = 0.0
LATEST_PRICES_CACHE_TTL_SEC = getenv_int("LATEST_PRICES_CACHE_TTL_SEC", 5)

# In-process cache for latest prices (reduces rate-limit pressure).
_LATEST_PRICES_CACHE: dict[str, float] = {}
_LATEST_PRICES_CACHE_TS: float = 0.0

def get_latest_prices(symbols: list[str]) -> dict[str, float]:
    """Batch latest-trade prices for many symbols.
    Uses a tiny in-process cache to avoid hammering Alpaca across rapid scans.
    Returns {symbol: price}. Missing symbols are omitted.
    """
    global _LATEST_PRICES_CACHE, _LATEST_PRICES_CACHE_TS
    if not symbols:
        return {}
    now_ts = _time.time()
    ttl = float(LATEST_PRICES_CACHE_TTL_SEC)
    if ttl > 0 and _LATEST_PRICES_CACHE and (now_ts - _LATEST_PRICES_CACHE_TS) <= ttl:
        return {s: _LATEST_PRICES_CACHE[s] for s in symbols if s in _LATEST_PRICES_CACHE}

    # Alpaca can rate-limit (429). Do a small bounded retry with backoff to avoid nuking the whole scan.
    backoffs = [0.15, 0.35, 0.75]  # total <= ~1.25s
    last_err: str | None = None

    for attempt, sleep_s in enumerate([0.0] + backoffs):
        if sleep_s:
            _time.sleep(sleep_s)
        try:
            req = StockLatestTradeRequest(symbol_or_symbols=symbols)
            latest = data_client.get_stock_latest_trade(req)
            out: dict[str, float] = {}
            for sym, trade in (latest or {}).items():
                px = None
                try:
                    px = getattr(trade, "price", None)
                    if px is None and isinstance(trade, dict):
                        px = trade.get("price")
                except Exception:
                    px = None
                if px is not None:
                    out[str(sym)] = float(px)

            # Update cache even if partial; it still reduces load next pass.
            _LATEST_PRICES_CACHE = out
            _LATEST_PRICES_CACHE_TS = _time.time()
            return out
        except Exception as e:
            s = str(e)
            last_err = s
            # Detect rate limit in a few common forms.
            if ("too many requests" in s.lower()) or ("429" in s):
                logger.warning("LATEST_PRICES_BATCH_RATE_LIMIT attempt=%s err=%s", attempt, s)
                continue
            logger.warning("LATEST_PRICES_BATCH_ERROR attempt=%s err=%s", attempt, s)
            break

    logger.warning("LATEST_PRICES_BATCH_GIVEUP err=%s", last_err)
    return {}
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
        raw = getenv_any("SCANNER_UNIVERSE_SYMBOLS", "SCANNER_SYMBOLS", default="")
        if not raw:
            return sorted(ALLOWED_SYMBOLS)[:SCANNER_MAX_SYMBOLS_PER_CYCLE]
        syms = [s.strip().upper() for s in raw.split(",") if s.strip()]
        return syms[:SCANNER_MAX_SYMBOLS_PER_CYCLE]

    # NOTE: 'alpaca' provider can be added later (requires assets + liquidity filter).
    # For safety in Phase 1C, we fall back to ALLOWED_SYMBOLS unless explicitly enabled.
    return sorted(ALLOWED_SYMBOLS)[:SCANNER_MAX_SYMBOLS_PER_CYCLE]


def fetch_1m_bars(symbol: str, lookback_days: int = 1, limit: int | None = None) -> list[dict]:
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


def fetch_1m_bars_multi(
    symbols: list[str], lookback_days: int = 1, limit_per_symbol: int | None = None
) -> dict[str, list[dict]]:
    """Fetch 1-minute bars for multiple symbols in a single Alpaca call.

    Returns mapping symbol -> list[dict] with the same schema as fetch_1m_bars().
    Symbols with no data will have an empty list.
    """
    symbols = [s.strip().upper() for s in (symbols or []) if s and s.strip()]
    if not symbols:
        return {}

    end = now_ny()
    start = end - timedelta(days=max(1, int(lookback_days)))
    req = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Minute,
        start=start,
        end=end,
        adjustment=ADJUSTMENT,
        feed=DATA_FEED,
    )

    out: dict[str, list[dict]] = {s: [] for s in symbols}
    try:
        df = data_client().get_stock_bars(req).df
    except Exception:
        return out
    if df is None or len(df) == 0:
        return out

    # Expected Alpaca shape: multiindex (symbol, timestamp)
    try:
        for (sym, ts), row in df.iterrows():
            ts_utc = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
            ts_ny = ts_utc.astimezone(NY)
            out.setdefault(sym, []).append(
                {
                    "ts_utc": ts_utc,
                    "ts_ny": ts_ny,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0.0)),
                    "vwap": float(row.get("vwap", 0.0) or 0.0),
                }
            )
    except Exception:
        # Fallback if df isn't multiindex
        for sym in symbols:
            try:
                sdf = df.xs(sym, level=0)
            except Exception:
                continue
            for ts, row in sdf.iterrows():
                ts_utc = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                ts_ny = ts_utc.astimezone(NY)
                out.setdefault(sym, []).append(
                    {
                        "ts_utc": ts_utc,
                        "ts_ny": ts_ny,
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": float(row.get("volume", 0.0)),
                        "vwap": float(row.get("vwap", 0.0) or 0.0),
                    }
                )

    for sym in list(out.keys()):
        out[sym] = sorted(out[sym], key=lambda r: r["ts_utc"])
        if limit_per_symbol and len(out[sym]) > limit_per_symbol:
            out[sym] = out[sym][-limit_per_symbol:]
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
    if not bars_today:
        return None
    ts_ny = bars_today[-1].get("ts_ny")
    if ts_ny is None:
        return None

    # Must be inside trade session
    if not _bars_for_today_session(bars_today, ts_ny, MIDBOX_TRADE_SESSION):
        return None

    build_bars = _bars_for_today_session(bars_today, ts_ny, MIDBOX_BUILD_SESSION)
    if len(build_bars) < 10:
        return None

    use_hl = bool(MIDBOX_BREAKOUT_USE_HIGHLOW)
    highs = [float(b.get("high", b.get("close"))) for b in build_bars]
    lows = [float(b.get("low", b.get("close"))) for b in build_bars]
    closes = [float(b.get("close")) for b in build_bars]
    box_high = max(highs) if use_hl else max(closes)
    box_low = min(lows) if use_hl else min(closes)

    price = float(bars_today[-1].get("close"))
    buf = float(MIDBOX_BREAKOUT_BUFFER_PCT)

    # Light trend filter (keeps frequency reasonable, avoids chop)
    ema_fast = ema_series(closes + [price], 20)
    ema_slow = ema_series(closes + [price], 50)
    if ema_fast is None or ema_slow is None:
        return None
    if price < ema_fast or ema_fast < ema_slow:
        return None

    if price >= box_high * (1.0 + buf):
        return ("midbox_breakout_up", "BUY")

    # Shorts are disabled in this system; return None for breakdowns.
    return None

def _scan_diag_midbox(bars_today: list[dict]) -> dict:
    # Minimal diagnostics to explain why MIDBOX is not firing yet.
    out: dict = {"eligible": True}
    nowt = now_ny().time()
    # Trade window gating
    if not in_session(MIDBOX_TRADE_SESSION, nowt):
        out["eligible"] = False
        out["reason"] = "session_closed"
        return out
    # Bars requirements (EMA200 needs ~200 bars)
    out["bars_1m"] = len(bars_today)
    if len(bars_today) < 205:
        out["eligible"] = False
        out["reason"] = "insufficient_1m_bars"
        return out
    # EMA200 slope
    closes = [r["close"] for r in bars_today]
    ema200 = ema_series(closes, 200)
    if len(ema200) < 3:
        out["eligible"] = False
        out["reason"] = "ema200_not_ready"
        return out
    out["ema200_slope_up"] = ema200[-1] > ema200[-2]
    out["ema200_slope_down"] = ema200[-1] < ema200[-2]
    # Breakout detection not implemented in this repo yet
    out["crossed_up"] = False
    out["crossed_down"] = False
    out["long_signal"] = False
    out["short_signal"] = False
    out["reason"] = "logic_not_implemented"
    return out

def _scan_diag_pwr(bars_today: list[dict]) -> dict:
    out: dict = {"eligible": True}
    nowt = now_ny().time()
    if not in_session(PWR_SESSION, nowt):
        out["eligible"] = False
        out["reason"] = "session_closed"
        return out
    out["bars_1m"] = len(bars_today)
    lookback = max(5, int(SCANNER_PWR_LOOKBACK_BARS))
    if len(bars_today) < lookback + 5:
        out["eligible"] = False
        out["reason"] = "insufficient_1m_bars"
        return out
    # Placeholder logic (not implemented fully)
    out["ema200_slope_up"] = True
    out["breakout"] = False
    out["volume_ok"] = False
    out["reason"] = "logic_not_implemented"
    return out

def _scan_diag_vwap_pb(bars_today: list[dict]) -> dict:
    out: dict = {"eligible": True}
    nowt = now_ny().time()
    # VWAP PB typically regular-session only; gate by market hours if configured
    if ONLY_MARKET_HOURS and not in_market_hours():
        out["eligible"] = False
        out["reason"] = "outside_market_hours"
        return out
    bars_5m = resample_5m(bars_today) if bars_today else []
    out["bars_5m"] = len(bars_5m)
    min_bars = max(int(RESAMPLE_5M_MIN_BARS), int(VWAP_PB_EMA_SLOW) + 5)
    out["min_bars_5m"] = int(min_bars)
    if len(bars_5m) < min_bars:
        out["eligible"] = False
        out["reason"] = "insufficient_5m_bars"
        return out
    # We can compute some basics to aid explainability
    closes = [b.close for b in bars_5m]
    ema_fast = ema_series(closes, int(VWAP_PB_EMA_FAST))
    ema_slow = ema_series(closes, int(VWAP_PB_EMA_SLOW))
    out["ema_fast_gt_slow"] = bool(ema_fast and ema_slow and ema_fast[-1] > ema_slow[-1])
    out["reason"] = "filters_not_met_or_logic_not_implemented"
    # Remaining fields expected by explainability helpers
    out.setdefault("uptrend", False)
    out.setdefault("slope_ok", False)
    out.setdefault("extension_ok", False)
    out.setdefault("touched", False)
    out.setdefault("reclaimed", False)
    return out

def eval_power_hour_signal(bars_today: list[dict]) -> tuple[str, str] | None:
    if not bars_today:
        return None
    ts_ny = bars_today[-1].get("ts_ny")
    if ts_ny is None:
        return None

    pwr_bars = _bars_for_today_session(bars_today, ts_ny, PWR_SESSION)
    if not pwr_bars:
        return None

    first_pwr_ts = pwr_bars[0]["ts_ny"]
    pre_bars = [b for b in bars_today if b.get("ts_ny") and b["ts_ny"] < first_pwr_ts]
    if len(pre_bars) < 20:
        return None

    use_hl = bool(PWR_BREAKOUT_USE_HIGHLOW)
    pre_high = max(float(b.get("high", b.get("close"))) for b in pre_bars) if use_hl else max(float(b.get("close")) for b in pre_bars)
    pre_low = min(float(b.get("low", b.get("close"))) for b in pre_bars) if use_hl else min(float(b.get("close")) for b in pre_bars)

    price = float(bars_today[-1].get("close"))
    buf = float(PWR_BREAKOUT_BUFFER_PCT)

    closes = [float(b.get("close")) for b in pre_bars[-200:]] + [price]
    ema_fast = ema_series(closes, 20)
    ema_slow = ema_series(closes, 50)
    if ema_fast is None or ema_slow is None:
        return None
    if price < ema_fast or ema_fast < ema_slow:
        return None

    if price >= pre_high * (1.0 + buf):
        return ("power_hour_breakout_up", "BUY")

    return None

def _strategy_reason_disabled() -> str:
    return "strategy_disabled"

def eval_vwap_pullback_signal(bars_today: list[dict]) -> str | None:
    if not bars_today:
        return None
    ts_ny = bars_today[-1].get("ts_ny")
    if ts_ny is None:
        return None

    # Only attempt within the scanner session window (keeps it controlled)
    if not in_scanner_session(ts_ny):
        return None

    closes = [float(b.get("close")) for b in bars_today]
    highs = [float(b.get("high", c)) for b, c in zip(bars_today, closes)]
    lows = [float(b.get("low", c)) for b, c in zip(bars_today, closes)]
    vols = [float(b.get("volume", 0) or 0) for b in bars_today]

    price = closes[-1]
    prev = closes[-2] if len(closes) >= 2 else price

    ema_fast = ema_series(closes, VWAP_PB_EMA_FAST)
    ema_slow = ema_series(closes, VWAP_PB_EMA_SLOW)
    if ema_fast is None or ema_slow is None:
        return None
    if price < ema_fast or ema_fast < ema_slow:
        return None

    # VWAP
    v_num = 0.0
    v_den = 0.0
    for i in range(len(closes)):
        v = vols[i]
        tp = (highs[i] + lows[i] + closes[i]) / 3.0
        if v > 0:
            v_num += tp * v
            v_den += v
    vwap = (v_num / v_den) if v_den > 0 else sum(closes) / len(closes)

    # Don't chase: must be near VWAP
    if abs(price - vwap) / vwap > float(VWAP_PB_MAX_EXTENSION_PCT):
        return None

    lookback = int(VWAP_PB_PULLBACK_LOOKBACK_BARS)
    lookback = max(3, min(lookback, len(closes)))
    band = float(VWAP_PB_BAND_PCT)

    touched = any(l <= vwap * (1.0 + band) for l in lows[-lookback:])
    if not touched:
        return None

    # Trigger: reclaim VWAP (cross up)
    if prev <= vwap and price > vwap:
        return "BUY"

    # Alternate trigger: reclaim EMA fast after a dip
    dip = any(c < ema_fast for c in closes[-lookback:])
    if dip and prev <= ema_fast and price > ema_fast:
        return "BUY"

    return None

def _no_signal_from_midbox(diag: dict) -> str:
    if not diag.get("enabled"):
        return _strategy_reason_disabled()
    if diag.get("eligible") is False:
        return diag.get("reason") or "not_eligible"
    if not diag.get("ema200_slope_up", True) and not diag.get("ema200_slope_down", True):
        return "ema200_not_ready_or_flat"
    if not diag.get("crossed_up") and not diag.get("crossed_down"):
        return "no_breakout_cross"
    if diag.get("crossed_up") and not diag.get("ema200_slope_up", True):
        return "ema200_slope_not_up"
    if diag.get("crossed_down") and not diag.get("ema200_slope_down", True):
        return "ema200_slope_not_down"
    if diag.get("crossed_up") and not diag.get("long_signal", False):
        return "long_filters_failed"
    if diag.get("crossed_down") and not diag.get("short_signal", False):
        return "short_filters_failed"
    return "no_signal"

def _no_signal_from_pwr(diag: dict) -> str:
    if not diag.get("enabled"):
        return _strategy_reason_disabled()
    if diag.get("eligible") is False:
        return diag.get("reason") or "not_eligible"
    if not diag.get("ema200_slope_up", True):
        return "ema200_slope_not_up"
    if not diag.get("breakout", False):
        return "no_breakout"
    if not diag.get("volume_ok", False):
        return "volume_not_ok"
    if not (diag.get("last_close", 0) > (diag.get("vwap_5m", float("inf")))):
        return "below_vwap"
    if not (diag.get("ema20_5m") is None or (diag.get("last_close", 0) > diag.get("ema20_5m", float("inf")))):
        return "below_ema20_5m"
    return "no_signal"

def _no_signal_from_vwap_pb(diag: dict) -> str:
    if not diag.get("enabled"):
        return _strategy_reason_disabled()
    if diag.get("eligible") is False:
        return diag.get("reason") or "not_eligible"
    if not diag.get("uptrend", False):
        return "trend_fail"
    if not diag.get("slope_ok", False):
        return "ema_slope_fail"
    if not diag.get("extension_ok", False):
        return "too_extended_from_vwap"
    if not diag.get("touched", False):
        return "did_not_touch_vwap_band"
    if not diag.get("reclaimed", False):
        return "did_not_reclaim_vwap"
    return "no_signal"

def _derive_no_signal_details(diag: dict) -> tuple[str, dict]:
    """Return (primary_reason, details_by_strategy) for hold/no_signal cases.

    Primary selection rules:
    1) If any enabled strategy is blocked by market-hours gating, report `outside_market_hours`.
    2) Else if all enabled strategies are blocked by session gating, report `session_closed`.
    3) Else fall back to the first enabled strategy's specific reason.
    """
    details: dict = {}
    mb = diag.get("midbox") or {}
    pw = diag.get("pwr") or {}
    vp = diag.get("vwap_pullback") or {}

    mb_reason = _no_signal_from_midbox(mb)
    pw_reason = _no_signal_from_pwr(pw)
    vp_reason = _no_signal_from_vwap_pb(vp)

    details["midbox"] = {"enabled": bool(mb.get("enabled")), "eligible": mb.get("eligible"), "reason": mb_reason}
    details["pwr"] = {"enabled": bool(pw.get("enabled")), "eligible": pw.get("eligible"), "reason": pw_reason}
    details["vwap_pullback"] = {"enabled": bool(vp.get("enabled")), "eligible": vp.get("eligible"), "reason": vp_reason}

    enabled = [s for s in ("midbox","pwr","vwap_pullback") if details[s]["enabled"]]
    if not enabled:
        return "no_strategy_enabled", details

    # Hard market-hours gating takes precedence
    for s in enabled:
        if details[s]["reason"] in ("outside_market_hours", "outside_regular_session"):
            return "outside_market_hours", details

    # If every enabled strategy is session-gated, expose that clearly
    if all(details[s]["reason"] in ("session_closed", "outside_market_hours", "outside_regular_session") for s in enabled):
        if any(details[s]["reason"] == "session_closed" for s in enabled):
            return "session_closed", details
        return "outside_market_hours", details

    # Otherwise: first enabled strategy's reason
    return details[enabled[0]]["reason"], details

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


@app.get("/diagnostics/scans")
def diagnostics_scans(limit: int = 50, symbol: str = ""):
    """Return recent scan cycles (in-memory) with per-symbol evaluation results.

    This is useful when the scanner is running but generating 0 signals and you want
    to see *why* each symbol passed/failed.

    Notes:
    - Not persisted; a restart clears history.
    - `symbol` (optional) filters each scan down to that symbol.
    """
    lim = max(1, min(int(limit or 50), 500))
    scans = SCAN_HISTORY[-lim:]
    sym = (symbol or "").upper().strip()
    if sym:
        filtered: list[dict] = []
        for s in scans:
            items = [it for it in (s.get("results") or []) if it.get("symbol") == sym]
            if items:
                copy_s = dict(s)
                copy_s["results"] = items
                filtered.append(copy_s)
        scans = filtered
    return {"ok": True, "count": len(scans), "items": scans}


@app.get("/diagnostics/last_scan")
def diagnostics_last_scan(symbol: str = ""):
    """Convenience: return the most recent scan cycle (optionally filtered to a symbol)."""
    if not SCAN_HISTORY:
        return {"ok": True, "item": None}
    item = SCAN_HISTORY[-1]
    sym = (symbol or "").upper().strip()
    if sym:
        items = [it for it in (item.get("results") or []) if it.get("symbol") == sym]
        copy_item = dict(item)
        copy_item["results"] = items
        item = copy_item
    return {"ok": True, "item": item}


@app.get("/diagnostics/scans/latest")
def diagnostics_scans_latest(symbol: str = ""):
    """Compatibility alias for clients expecting /diagnostics/scans/latest."""
    return diagnostics_last_scan(symbol=symbol)



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
            # Store skipped scan diagnostics so /diagnostics/scans/latest is never null
            try:
                scan_summary = {
                    "skipped": True,
                    "skip_reason": "scanner_disabled",
                    "actions": {"skipped": 1},
                    "no_signal_total": 0,
                    "top_no_signal_reasons": [("scanner_disabled", 1)],
                    "strategy_breakdown": {},
                }
                SCAN_HISTORY.append({
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "universe_provider": SCANNER_UNIVERSE_PROVIDER,
                    "symbols": [],
                    "scanned": 0,
                    "signals": 0,
                    "would_trade": 0,
                    "blocked": 0,
                    "duration_ms": int((utc_ts()-scan_start_utc)*1000),
                    "summary": scan_summary,
                    "results": [],
                })
                if len(SCAN_HISTORY) > SCAN_HISTORY_SIZE:
                    del SCAN_HISTORY[: max(0, len(SCAN_HISTORY) - SCAN_HISTORY_SIZE)]
            except Exception:
                pass
            return {"ok": True, "skipped": True, "reason": "scanner_disabled", **LAST_SCAN}

        if SCANNER_REQUIRE_MARKET_HOURS and ONLY_MARKET_HOURS and not in_market_hours():
            _set_last_scan(skipped=True, reason="outside_market_hours", scanned=0, signals=0, would_trade=0, blocked=0, duration_ms=int((utc_ts()-scan_start_utc)*1000))
            record_decision("SCAN", "worker_scan", action="skipped", reason="outside_market_hours")
            # Store skipped scan diagnostics so /diagnostics/scans/latest is never null
            try:
                scan_summary = {
                    "skipped": True,
                    "skip_reason": "outside_market_hours",
                    "actions": {"skipped": 1},
                    "no_signal_total": 0,
                    "top_no_signal_reasons": [("outside_market_hours", 1)],
                    "strategy_breakdown": {},
                }
                SCAN_HISTORY.append({
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "universe_provider": SCANNER_UNIVERSE_PROVIDER,
                    "symbols": [],
                    "scanned": 0,
                    "signals": 0,
                    "would_trade": 0,
                    "blocked": 0,
                    "duration_ms": int((utc_ts()-scan_start_utc)*1000),
                    "summary": scan_summary,
                    "results": [],
                })
                if len(SCAN_HISTORY) > SCAN_HISTORY_SIZE:
                    del SCAN_HISTORY[: max(0, len(SCAN_HISTORY) - SCAN_HISTORY_SIZE)]
            except Exception:
                pass
            return {"ok": True, "skipped": True, "reason": "outside_market_hours", **LAST_SCAN}

                # Optional intraday scanner session gating (NY time).
        if SCANNER_SESSIONS_NY and not in_scanner_session():
            _set_last_scan(skipped=True, reason="outside_scanner_session", scanned=0, signals=0, would_trade=0, blocked=0, duration_ms=int((utc_ts()-scan_start_utc)*1000))
            record_decision("SCAN", "worker_scan", action="skipped", reason="outside_scanner_session")
            try:
                scan_summary = {
                    "skipped": True,
                    "skip_reason": "outside_scanner_session",
                    "actions": {"skipped": 1},
                    "no_signal_total": 0,
                    "top_no_signal_reasons": [("outside_scanner_session", 1)],
                    "strategy_breakdown": {},
                }
                SCAN_HISTORY.append({
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "universe_provider": SCANNER_UNIVERSE_PROVIDER,
                    "symbols": [],
                    "scanned": 0,
                    "signals": 0,
                    "would_trade": 0,
                    "blocked": 0,
                    "duration_ms": int((utc_ts()-scan_start_utc)*1000),
                    "summary": scan_summary,
                    "results": [],
                })
                if len(SCAN_HISTORY) > SCAN_HISTORY_SIZE:
                    del SCAN_HISTORY[: max(0, len(SCAN_HISTORY) - SCAN_HISTORY_SIZE)]
            except Exception:
                pass
            return {"ok": True, "skipped": True, "reason": "outside_scanner_session", **LAST_SCAN}

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

        # Batch-fetch bars once per scan so we can compute entry signals + diagnostics.
        bars_map = fetch_1m_bars_multi(syms, lookback_days=SCANNER_LOOKBACK_DAYS)
        # Batch latest prices once per scan (fallback when bars are missing)
        latest_prices_map = get_latest_prices(syms)

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
                    bars_all = bars_map.get(sym) or []
                    bars_today = _bars_for_today_session(bars_all)

                    diag = {
                        'midbox': {'enabled': bool(SCANNER_ENABLE_MIDBOX)},
                        'pwr': {'enabled': bool(SCANNER_ENABLE_PWR), 'session': PWR_SESSION},
                        'vwap_pullback': {'enabled': bool(SCANNER_ENABLE_VWAP_PB)},
                    }

                    # Market-hours context (NY)
                    _now_ny_dt = now_ny()
                    _in_mkt = in_market_hours()
                    diag['market'] = {
                        'now_ny': _now_ny_dt.isoformat(),
                        'weekday': int(_now_ny_dt.weekday()),
                        'only_market_hours': bool(ONLY_MARKET_HOURS),
                        'in_market_hours': bool(_in_mkt),
                        'market_open_ny': MARKET_OPEN,
                        'market_close_ny': MARKET_CLOSE,
                    }
                    _hard_market_closed = bool(ONLY_MARKET_HOURS) and (not _in_mkt)
                    if _hard_market_closed:
                        # Hard gate: do not allow signals outside market hours.
                        for _k in ('midbox', 'pwr', 'vwap_pullback'):
                            if diag.get(_k, {}).get('enabled'):
                                diag[_k].update({'eligible': False, 'reason': 'outside_market_hours'})
                    price = float(bars_today[-1]["close"]) if bars_today else (latest_prices_map.get(sym) or get_latest_price(sym))
                    if price is None:
                        local_blocked += 1
                        local_results.append({
                            "symbol": sym,
                            "action": "blocked",
                            "reason": "latest_price_missing",
                            "price": None,
                            "stop": None,
                            "take": None,
                        })
                        return {"results": local_results, "signals": local_signals, "blocked": local_blocked}

                    if bars_today:
                        if SCANNER_ENABLE_MIDBOX and (not _hard_market_closed) and diag.get('midbox', {}).get('eligible', True):
                            diag["midbox"].update(_scan_diag_midbox(bars_today))
                        if SCANNER_ENABLE_PWR and (not _hard_market_closed) and diag.get('pwr', {}).get('eligible', True):
                            diag["pwr"].update(_scan_diag_pwr(bars_today))
                        if SCANNER_ENABLE_VWAP_PB and (not _hard_market_closed) and diag.get('vwap_pullback', {}).get('eligible', True):
                            diag["vwap_pullback"].update(_scan_diag_vwap_pb(bars_today))

                    action = "hold"
                    reason = "no_signal"
                    signal_name = None
                    side = None
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

                    # Entry evaluation (only if we are not already managing a plan for this symbol)
                    if not plan and action == "hold":
                        mb = eval_midbox_signal(bars_today)
                        if mb:
                            signal_name, side = mb
                        else:
                            pwr = None
                            if SCANNER_ENABLE_PWR and (not _hard_market_closed) and diag.get('pwr', {}).get('eligible', True):
                                pwr = eval_power_hour_signal(bars_today)
                            if pwr:
                                signal_name, side = pwr
                            else:
                                bars_5m = resample_5m(bars_today) if bars_today else []
                                vp = None
                                if SCANNER_ENABLE_VWAP_PB and (not _hard_market_closed) and diag.get('vwap_pullback', {}).get('eligible', True):
                                    vp = eval_vwap_pullback_signal(bars_5m)
                                if vp == "BUY":
                                    signal_name, side = ("VWAP_PULLBACK", "buy")
                                elif vp == "SELL":
                                    signal_name, side = ("VWAP_PULLBACK", "sell")

                        if signal_name and side in ("buy", "sell"):
                            action = side
                            reason = signal_name
                            try:
                                plan = build_trade_plan(sym, price)
                                TRADE_PLAN[sym] = plan
                            except Exception as e:
                                diag["trade_plan_error"] = str(e)
                            local_signals.append({"symbol": sym, "action": action, "price": price, "signal": signal_name})

                    stop_out = plan.get("stop") if plan else None
                    take_out = plan.get("take") if plan else None

                    if action == "exit":
                        local_signals.append({"symbol": sym, "action": action, "price": price})

                    no_signal_primary = None
                    no_signal_details = None
                    if action == "hold" and (reason == "no_signal" or reason == ""):
                        no_signal_primary, no_signal_details = _derive_no_signal_details(diag)
                        reason = f"no_signal:{no_signal_primary}"
                    local_results.append({
                        "symbol": sym,
                        "action": action,
                        "reason": reason or "",
                        "price": price,
                        "stop": stop_out,
                        "take": take_out,
                        "diagnostics": diag,
                        "no_signal": {"primary": no_signal_primary, "details": no_signal_details},
                    })

                    record_decision(
                        "scan_decision",
                        source="scan",
                        symbol=sym,
                        action=action,
                        reason=reason,
                        signal=signal_name,
                        price=price,
                        stop=stop_out,
                        take=take_out,
                        meta=diag,
                    )

                except Exception as e:
                    logger.exception("SCAN_EVAL_ERROR symbol=%s err=%s", sym, str(e))
                    local_blocked += 1
                    local_results.append({
                        "symbol": sym,
                        "action": "blocked",
                        "reason": "exception",
                        "err": str(e),
                        "price": None,
                        "stop": None,
                        "take": None,
                    })
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

        # ---- Scan-level summary (top no-signal reasons, action counts) ----
        action_counts = Counter()
        no_signal_counts = Counter()
        for r in results:
            try:
                action_counts[str(r.get("action", "") or "")] += 1
                reason = str(r.get("reason", "") or "")
                if reason.startswith("no_signal:"):
                    primary = (r.get("no_signal") or {}).get("primary") or reason.split(":", 1)[1]
                    if primary:
                        no_signal_counts[str(primary)] += 1
            except Exception:
                pass

        top_no_signal = [
            {"reason": k, "count": int(v)}
            for k, v in no_signal_counts.most_common(10)
        ]
        
        # Strategy-level breakdown from per-row no_signal.details
        strat_counts: dict[str, Counter] = {
            "midbox": Counter(),
            "pwr": Counter(),
            "vwap_pullback": Counter(),
        }
        for r in results:
            ns = (r.get("no_signal") or {}).get("details") or {}
            for strat, payload in ns.items():
                try:
                    rr = str((payload or {}).get("reason") or "")
                    if rr:
                        strat_counts[strat][rr] += 1
                except Exception:
                    pass

        strategy_breakdown = {
            strat: [{"reason": k, "count": int(v)} for k, v in c.most_common(8)]
            for strat, c in strat_counts.items()
        }

        scan_summary = {
            "actions": dict(action_counts),
            "no_signal_total": int(sum(no_signal_counts.values())),
            "top_no_signal_reasons": top_no_signal,
            "strategy_breakdown": strategy_breakdown,
        }

        _set_last_scan(
                skipped=False,
                reason=None,
                scanned=len(syms),
                signals=len(signals),
                would_trade=len(signals),
                blocked=blocked,
                duration_ms=duration_ms,
                summary=scan_summary,
        )

        logger.info("SCAN_DONE scanned=%s signals=%s would_trade=%s blocked=%s duration_ms=%s",
                        len(syms), len(signals), len(signals), blocked, duration_ms)

        # Store diagnostics for Postman/curl inspection.
        try:
                SCAN_HISTORY.append({
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "universe_provider": SCANNER_UNIVERSE_PROVIDER,
                    "symbols": syms,
                    "scanned": len(syms),
                    "signals": len(signals),
                    "would_trade": len(signals),
                    "blocked": blocked,
                    "duration_ms": duration_ms,
                    "summary": scan_summary,
                    "results": results,
                    "would_submit": signals,
                })
                if len(SCAN_HISTORY) > SCAN_HISTORY_SIZE:
                    del SCAN_HISTORY[: len(SCAN_HISTORY) - SCAN_HISTORY_SIZE]
        except Exception:
                pass

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
                    "summary": scan_summary,
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
        return JSONResponse(
            status_code=500,
            content={'ok': False, 'error': 'scan_exception', 'detail': str(e), **LAST_SCAN},
        )
