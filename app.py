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
import re
import json
from urllib.request import Request as UrlRequest, urlopen
from urllib.parse import urlencode

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


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_bar_ts(ts_raw):
    if not ts_raw:
        return None
    try:
        if isinstance(ts_raw, datetime):
            ts = ts_raw
        else:
            s = str(ts_raw)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            ts = datetime.fromisoformat(s)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_bar_row(ts_utc: datetime, row) -> Optional[dict]:
    try:
        return {
            "ts_utc": ts_utc,
            "ts_ny": ts_utc.astimezone(NY_TZ),
            "open": float((row.get("open") if isinstance(row, dict) else getattr(row, "open", 0)) or 0),
            "high": float((row.get("high") if isinstance(row, dict) else getattr(row, "high", 0)) or 0),
            "low": float((row.get("low") if isinstance(row, dict) else getattr(row, "low", 0)) or 0),
            "close": float((row.get("close") if isinstance(row, dict) else getattr(row, "close", 0)) or 0),
            "volume": float((row.get("volume") if isinstance(row, dict) else getattr(row, "volume", 0)) or 0),
            "vwap": float(((row.get("vwap") if isinstance(row, dict) else getattr(row, "vwap", 0)) or 0)),
        }
    except Exception:
        return None


def _alpaca_data_base_url() -> str:
    return (os.getenv("APCA_DATA_BASE_URL", "https://data.alpaca.markets").strip() or "https://data.alpaca.markets").rstrip("/")


def _fetch_bars_via_rest(symbols: list[str], start: datetime, end: datetime, feed_override=None, limit: int = 10000) -> tuple[dict[str, list[dict]], dict]:
    symbols = [s.strip().upper() for s in (symbols or []) if s and s.strip()]
    out: dict[str, list[dict]] = {s: [] for s in symbols}
    debug = {
        "method": "rest",
        "feed": str(feed_override or DATA_FEED),
        "start": _iso_utc(start),
        "end": _iso_utc(end),
        "count": 0,
        "url": None,
    }
    if not symbols:
        return out, debug
    params = {
        "symbols": ",".join(symbols),
        "timeframe": "1Min",
        "start": _iso_utc(start),
        "end": _iso_utc(end),
        "limit": str(int(limit)),
        "adjustment": str(DATA_ADJUSTMENT_RAW),
        "feed": str(feed_override or _DATA_FEED_RAW),
        "sort": "asc",
    }
    url = f"{_alpaca_data_base_url()}/v2/stocks/bars?{urlencode(params)}"
    debug["url"] = url
    req = UrlRequest(url, headers={
        "APCA-API-KEY-ID": APCA_KEY,
        "APCA-API-SECRET-KEY": APCA_SECRET,
        "accept": "application/json",
        "user-agent": "trading-webhook/patch-006",
    })
    try:
        with urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        bars_payload = payload.get("bars", {}) if isinstance(payload, dict) else {}
        for sym, seq in (bars_payload or {}).items():
            rows = []
            for b in seq or []:
                ts_utc = _parse_bar_ts((b or {}).get("t"))
                if not ts_utc:
                    continue
                row = _normalize_bar_row(ts_utc, {
                    "open": (b or {}).get("o"),
                    "high": (b or {}).get("h"),
                    "low": (b or {}).get("l"),
                    "close": (b or {}).get("c"),
                    "volume": (b or {}).get("v"),
                    "vwap": (b or {}).get("vw"),
                })
                if row:
                    rows.append(row)
            out[str(sym)] = rows
        debug["count"] = sum(len(v) for v in out.values())
    except Exception as e:
        debug["error"] = str(e)
    return out, debug


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
NY = NY_TZ  # backward-compat alias


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
    """Read an int env var with a safe fallback."""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return int(default)
    try:
                return int(raw)
    except Exception:
        return int(default)
def env_bool(name: str, default: str | bool = "false") -> bool:
    raw = os.getenv(name)
    if raw is None:
        raw = str(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


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

def in_scanner_session(now_dt_ny: datetime | None = None) -> bool:
    """True if within configured scanner session windows. If no windows configured, True."""
    global _SCANNER_SESSION_RANGES_CACHE
    if not SCANNER_SESSIONS_NY:
        return True
    if now_dt_ny is None:
        now_dt_ny = now_ny()
    t = now_dt_ny.time()
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
MAX_OPEN_POSITIONS = int(getenv_any("MAX_OPEN_POSITIONS", default="2"))



# =============================
# Scanner (Phase 1C - shadow mode default)
# =============================
SCANNER_ENABLED = env_bool("SCANNER_ENABLED", "false")
SCANNER_DRY_RUN = env_bool("SCANNER_DRY_RUN", "true")
SCANNER_ALLOW_LIVE = env_bool("SCANNER_ALLOW_LIVE", "false")  # hard gate: must be true to ever place scanner orders

# --- Trades-Today forcing (emergency mode) ---
TRADES_TODAY_ENABLE = env_bool("TRADES_TODAY_ENABLE", False)
TRADES_TODAY_TARGET_TRADES = int(getenv_any("TRADES_TODAY_TARGET_TRADES", default="1"))
TRADES_TODAY_SIGNAL = getenv_any("TRADES_TODAY_SIGNAL", default="trades_today_force")
TRADES_TODAY_PREFERRED_SYMBOLS = [s.strip().upper() for s in getenv_any("TRADES_TODAY_PREFERRED_SYMBOLS", default="SPY,QQQ,IWM,TQQQ").split(",") if s.strip()]
LAST_SCAN: dict = {}

SCANNER_UNIVERSE_PROVIDER = getenv_any("SCANNER_UNIVERSE_PROVIDER", default="static").lower()
SCANNER_MAX_SYMBOLS_PER_CYCLE = int(getenv_any("SCANNER_MAX_SYMBOLS_PER_CYCLE", default="200"))
# Volatility ranking (Option A)
SCANNER_VOL_RANK_ENABLE = env_bool("SCANNER_VOL_RANK_ENABLE", False)
# Canonical name: SCANNER_VOL_RANK_TOP_N
# Backward-compat: SCANNER_VOL_RANK_N (older)
_tmp_top_n = os.getenv("SCANNER_VOL_RANK_TOP_N")
if _tmp_top_n is not None and _tmp_top_n.strip() != "":
    SCANNER_VOL_RANK_TOP_N = int(_tmp_top_n)
else:
    SCANNER_VOL_RANK_TOP_N = int(getenv_any("SCANNER_VOL_RANK_N", default="50"))

# --- Higher-frequency scanner controls (always defined) ---
# Default ON so the system can trade out of the box; can be disabled via env.
SCANNER_ENABLE_HF = env_bool("SCANNER_ENABLE_HF", "true")
SCANNER_HF_ORB_BARS = int(getenv_any("SCANNER_HF_ORB_BARS", default="5"))  # 5-min ORB window
SCANNER_HF_EMA_FAST = int(getenv_any("SCANNER_HF_EMA_FAST", default="9"))
SCANNER_HF_EMA_SLOW = int(getenv_any("SCANNER_HF_EMA_SLOW", default="20"))
SCANNER_HF_NEAR_PCT = float(getenv_any("SCANNER_HF_NEAR_PCT", default="0.0015"))  # 0.15%
SCANNER_HF_DEBUG = env_bool("SCANNER_HF_DEBUG", "false")
SCANNER_MAX_ENTRIES_PER_SCAN = int(getenv_any("SCANNER_MAX_ENTRIES_PER_SCAN", default="1"))

SCANNER_VOL_RANK_BARS = int(getenv_any("SCANNER_VOL_RANK_BARS", default="390"))  # ~1 session of 1m bars
SCANNER_VOL_RANK_METRIC = getenv_any("SCANNER_VOL_RANK_METRIC", "range_pct")  # range_pct | stdev_ret

# Near-miss diagnostics
SCANNER_NEAR_MISS_PCT = float(getenv_any("SCANNER_NEAR_MISS_PCT", default="0.005"))  # 0.5%
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
MIDBOX_TOUCH_EPS_PCT = float(os.getenv("MIDBOX_TOUCH_EPS_PCT", "0.004"))  # 0.4% default (looser)
MIDBOX_BREAKOUT_USE_HIGHLOW = os.getenv("MIDBOX_BREAKOUT_USE_HIGHLOW", "true").lower() == "true"

# Power hour loosening knobs
PWR_BREAKOUT_BUFFER_PCT = float(os.getenv("PWR_BREAKOUT_BUFFER_PCT", "0.0005"))  # 0.05%
PWR_BREAKOUT_USE_HIGHLOW = os.getenv("PWR_BREAKOUT_USE_HIGHLOW", "true").lower() == "true"

# Strategy C: VWAP pullback on 5m
ENABLE_STRATEGY_VWAP_PULLBACK = os.getenv("ENABLE_STRATEGY_VWAP_PULLBACK", "true").lower() == "true"
VWAP_PB_EMA_FAST = int(os.getenv("VWAP_PB_EMA_FAST", "9"))
VWAP_PB_EMA_SLOW = int(os.getenv("VWAP_PB_EMA_SLOW", "20"))
VWAP_PB_BAND_PCT = float(os.getenv("VWAP_PB_BAND_PCT", "0.0035"))  # 0.35% band around VWAP counts as a touch
VWAP_PB_PULLBACK_LOOKBACK_BARS = int(os.getenv("VWAP_PB_PULLBACK_LOOKBACK_BARS", "6"))
VWAP_PB_MAX_EXTENSION_PCT = float(os.getenv("VWAP_PB_MAX_EXTENSION_PCT", "0.008"))  # don't chase if >0.8% away from VWAP
VWAP_PB_MIN_EMA_SLOPE = float(os.getenv("VWAP_PB_MIN_EMA_SLOPE", "0.0"))
VWAP_PB_TOUCH_BAND_PCT = float(os.getenv("VWAP_PB_TOUCH_BAND_PCT", os.getenv("VWAP_PB_BAND_PCT", "0.0035")))
VWAP_PB_TOUCH_LOOKBACK_BARS = int(os.getenv("VWAP_PB_TOUCH_LOOKBACK_BARS", os.getenv("VWAP_PB_PULLBACK_LOOKBACK_BARS", "6")))
VWAP_PB_VWAP_WINDOW_BARS = int(os.getenv("VWAP_PB_VWAP_WINDOW_BARS", "12"))
VWAP_PB_SLOPE_LOOKBACK_BARS = int(os.getenv("VWAP_PB_SLOPE_LOOKBACK_BARS", "3"))
VWAP_PB_SLOPE_EPS_PCT = float(os.getenv("VWAP_PB_SLOPE_EPS_PCT", "0.0"))
VWAP_PB_MIN_RELVOL = float(os.getenv("VWAP_PB_MIN_RELVOL", "0.90"))
VWAP_PB_ALLOW_BELOW_VWAP_PCT = float(os.getenv("VWAP_PB_ALLOW_BELOW_VWAP_PCT", "0.0015"))
VWAP_PB_EMA_STACK_SLACK_PCT = float(os.getenv("VWAP_PB_EMA_STACK_SLACK_PCT", "0.0015"))
VWAP_PB_ALLOW_PRICE_BELOW_EMA_FAST_PCT = float(os.getenv("VWAP_PB_ALLOW_PRICE_BELOW_EMA_FAST_PCT", "0.0020"))
VWAP_PB_ALLOW_NEG_VWAP_SLOPE_PCT = float(os.getenv("VWAP_PB_ALLOW_NEG_VWAP_SLOPE_PCT", "-0.0008"))
VWAP_PB_NEAR_MISS_SCORE_MIN = float(os.getenv("VWAP_PB_NEAR_MISS_SCORE_MIN", "48"))
VWAP_PB_FALLBACK_SIGNAL_SCORE_MIN = float(os.getenv("VWAP_PB_FALLBACK_SIGNAL_SCORE_MIN", "58"))
VWAP_PB_ALLOW_NEAR_MISS_FALLBACK = os.getenv("VWAP_PB_ALLOW_NEAR_MISS_FALLBACK", "true").lower() == "true"
SCANNER_LOOKBACK_DAYS = int(getenv_any("SCANNER_LOOKBACK_DAYS", default="3"))
SCANNER_REQUIRE_MARKET_HOURS = env_bool("SCANNER_REQUIRE_MARKET_HOURS", "true")
SCANNER_PRIMARY_STRATEGY = getenv_any("SCANNER_PRIMARY_STRATEGY", default="vwap_pullback").strip().lower()
SCANNER_CANDIDATE_LIMIT = int(getenv_any("SCANNER_CANDIDATE_LIMIT", default="25"))
SCANNER_ACTIVITY_LOOKBACK_BARS = int(getenv_any("SCANNER_ACTIVITY_LOOKBACK_BARS", default="30"))
SCANNER_ACTIVITY_RECENT_BARS = int(getenv_any("SCANNER_ACTIVITY_RECENT_BARS", default="6"))

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
VWAP_PB_ENABLE = SCANNER_ENABLE_VWAP_PB
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

def _count_forced_trades_today_ny() -> int:
    """Count today's forced trades (NY date) for the Trades-Today mode."""
    try:
        today = now_ny().date()
    except Exception:
        return 0
    n = 0
    for d in DECISIONS:
        try:
            if d.get("event") != "SCAN":
                continue
            if d.get("source") != "worker_scan":
                continue
            if d.get("action") != "submit":
                continue
            if d.get("signal") != TRADES_TODAY_SIGNAL:
                continue
            ts_ny = d.get("ts_ny")
            if not ts_ny:
                continue
            # ts_ny is an ISO string with offset
            dt = datetime.fromisoformat(ts_ny)
            if dt.date() == today:
                n += 1
        except Exception:
            continue
    return n


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
        "max_open_positions": MAX_OPEN_POSITIONS,
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




def eval_hf_signal_with_debug(bars_today: list[dict], bars_5m: list[dict]) -> tuple[tuple[str, str] | None, dict]:
    """
    Higher-frequency entry logic (5m): 5m ORB + VWAP reclaim + EMA pullback.

    Returns: (signal_or_None, debug_dict)
    """
    debug: dict = {
        "enabled": True,
        "reason": None,
        "components": {},
        "near_miss": {},
    }

    if not bars_today or not bars_5m:
        debug["reason"] = "no_bars"
        return None, debug

    # Require market hours gating is handled elsewhere; this is pure signal logic.

    # --- Config (all optional envs; safe defaults) ---
    ORB_LOOKBACK_BARS = int(os.getenv("HF5_ORB_LOOKBACK_BARS", "6"))  # ~30 minutes
    RECLAIM_LOOKBACK_BARS = int(os.getenv("HF5_RECLAIM_LOOKBACK_BARS", "6"))
    ORB_TOUCH_EPS = float(os.getenv("HF5_ORB_TOUCH_EPS", "0.0015"))   # 0.15%
    VWAP_RECLAIM_EPS = float(os.getenv("HF5_VWAP_RECLAIM_EPS", "0.0005"))  # 0.05%
    EMA_RECLAIM_EPS = float(os.getenv("HF5_EMA_RECLAIM_EPS", "0.0005"))    # 0.05%

    # If you want *more* trades, set these to 0 (False)
    REQUIRE_EMA_CONFIRM = env_bool("HF5_REQUIRE_EMA_CONFIRM", False)
    REQUIRE_VWAP_CONFIRM = env_bool("HF5_REQUIRE_VWAP_CONFIRM", True)

    # --- Helpers ---
    def _bar_num(b: dict, primary: str, fallback: str, default: float = 0.0) -> float:
        v = b.get(primary)
        if v is None:
            v = b.get(fallback)
        return float(v or default)

    def _recent_cross_above(level: float, n: int) -> bool:
        if level is None:
            return False
        start_i = max(1, len(bars_5m) - n)
        for i in range(start_i, len(bars_5m)):
            prev = bars_5m[i - 1]
            cur = bars_5m[i]
            prev_close = _bar_num(prev, "close", "c")
            cur_close = _bar_num(cur, "close", "c")
            if prev_close < level and cur_close >= level:
                return True
        return False

    def _recent_reclaim_field(field: str, n: int, eps: float) -> bool:
        start_i = max(1, len(bars_5m) - n)
        for i in range(start_i, len(bars_5m)):
            prev = bars_5m[i - 1]
            cur = bars_5m[i]
            prev_close = _bar_num(prev, "close", "c")
            cur_close = _bar_num(cur, "close", "c")
            cur_low = _bar_num(cur, "low", "l", cur_close)

            prev_level = prev.get(field)
            cur_level = cur.get(field)
            if prev_level is None or cur_level is None:
                continue

            prev_level = float(prev_level)
            cur_level = float(cur_level)

            # Reclaim: was below, then closes above (or wicks through and closes above)
            if prev_close < prev_level and (cur_low <= cur_level) and cur_close >= cur_level * (1.0 + eps):
                return True
        return False

    # --- ORB levels (first 5m bar of the day) ---
    orb = bars_5m[0]
    orb_high = _bar_num(orb, "high", "h")
    orb_low = _bar_num(orb, "low", "l")

    last5 = bars_5m[-1]
    price = _bar_num(last5, "close", "c")

    # Use the most recent indicators (per-bar values exist in bars_5m)
    vwap_now = last5.get("vwap")
    ema_now = last5.get("ema_fast") or last5.get("ema")

    vwap_now_f = float(vwap_now) if vwap_now is not None else None
    ema_now_f = float(ema_now) if ema_now is not None else None

    # --- Core conditions ---
    orb_is_above = (price > orb_high) if orb_high else False
    orb_recent = _recent_cross_above(orb_high, ORB_LOOKBACK_BARS) if orb_high else False
    orb_touch = (price >= orb_high * (1.0 - ORB_TOUCH_EPS)) if orb_high else False
    orb_breakout_like = bool(orb_is_above or orb_recent or orb_touch)

    vwap_above = (vwap_now_f is not None and price >= vwap_now_f)
    vwap_reclaim = _recent_reclaim_field("vwap", RECLAIM_LOOKBACK_BARS, VWAP_RECLAIM_EPS) if vwap_now_f is not None else False

    ema_touch = (ema_now_f is not None and abs(price - ema_now_f) / max(ema_now_f, 1e-9) <= 0.0025)  # 0.25% proximity
    ema_reclaim = _recent_reclaim_field("ema_fast", RECLAIM_LOOKBACK_BARS, EMA_RECLAIM_EPS) if ema_now_f is not None else False

    debug["components"] = {
        "price": price,
        "orb_high": orb_high,
        "orb_low": orb_low,
        "orb_is_above": orb_is_above,
        "orb_recent": orb_recent,
        "orb_touch": orb_touch,
        "vwap_now": vwap_now_f,
        "vwap_above": vwap_above,
        "vwap_reclaim": vwap_reclaim,
        "ema_now": ema_now_f,
        "ema_touch": ema_touch,
        "ema_reclaim": ema_reclaim,
        "require_vwap_confirm": REQUIRE_VWAP_CONFIRM,
        "require_ema_confirm": REQUIRE_EMA_CONFIRM,
    }

    # Near-miss metrics (so we can see what's close)
    if orb_high:
        debug["near_miss"]["orb_dist_pct"] = max(0.0, (orb_high - price) / max(orb_high, 1e-9))
        debug["near_miss"]["orb_touch_eps"] = ORB_TOUCH_EPS
    if vwap_now_f:
        debug["near_miss"]["vwap_dist_pct"] = max(0.0, (vwap_now_f - price) / max(vwap_now_f, 1e-9))
        debug["near_miss"]["vwap_reclaim_eps"] = VWAP_RECLAIM_EPS
    if ema_now_f:
        debug["near_miss"]["ema_dist_pct"] = abs(price - ema_now_f) / max(ema_now_f, 1e-9)
        debug["near_miss"]["ema_reclaim_eps"] = EMA_RECLAIM_EPS

    # --- Decision logic ---
    if not orb_breakout_like:
        debug["reason"] = "no_orb_breakout"
        return None, debug

    if REQUIRE_VWAP_CONFIRM and not (vwap_above or vwap_reclaim):
        debug["reason"] = "no_vwap_confirm"
        return None, debug

    if REQUIRE_EMA_CONFIRM and not (ema_touch or ema_reclaim):
        debug["reason"] = "no_ema_confirm"
        return None, debug

    # If VWAP confirm isn't required, at least ensure we aren't breaking down
    if not REQUIRE_VWAP_CONFIRM and vwap_now_f is not None and price < vwap_now_f * 0.995:
        debug["reason"] = "below_vwap_hard"
        return None, debug

    debug["reason"] = None
    return ("hf5", "buy"), debug


def eval_hf_signal(bars_today: list[dict], bars_5m: list[dict]) -> tuple[str, str] | None:
    sig, _dbg = eval_hf_signal_with_debug(bars_today, bars_5m)
    return sig
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


def take_symbol_lock(symbol: str, lock_sec: int | None = None) -> bool:
    """Atomically take/update a symbol lock if it is not currently locked."""
    now = utc_ts()
    ttl = int(lock_sec if lock_sec is not None else SYMBOL_LOCK_SEC)
    with STATE_LOCK:
        exp = SYMBOL_LOCKS.get(symbol, 0)
        if exp > now:
            return False
        SYMBOL_LOCKS[symbol] = now + max(ttl, 1)
        return True


def release_symbol_lock(symbol: str):
    with STATE_LOCK:
        SYMBOL_LOCKS.pop(symbol, None)


def soften_symbol_lock(symbol: str, lock_sec: int = 5):
    with STATE_LOCK:
        SYMBOL_LOCKS[symbol] = utc_ts() + max(int(lock_sec), 1)


def count_open_positions_allowed() -> int:
    return len(list_open_positions_allowed())


def max_open_positions_reached(extra_buffer: int = 0) -> bool:
    if MAX_OPEN_POSITIONS <= 0:
        return False
    return count_open_positions_allowed() + max(int(extra_buffer), 0) >= MAX_OPEN_POSITIONS


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
      

def require_admin_if_configured(request: Request):
    if ADMIN_SECRET:
        require_admin(request)


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


def _bars_for_today_regular_session(bars: list[dict]) -> list[dict]:
    if not bars:
        return []
    today = now_ny().date()
    out: list[dict] = []
    for b in bars:
        try:
            ts = b.get("ts_ny")
            if not ts or ts.date() != today:
                continue
            if MARKET_OPEN <= ts.time() <= MARKET_CLOSE:
                out.append(b)
        except Exception:
            continue
    return out


def _mean(nums: list[float]) -> float:
    return (sum(nums) / len(nums)) if nums else 0.0


def _activity_profile(symbol: str, bars: list[dict]) -> dict:
    today = _bars_for_today_regular_session(bars)
    if len(today) < 10:
        return {"symbol": symbol, "eligible": False, "reason": "insufficient_intraday_bars", "score": 0.0}

    price = float(today[-1].get("close") or 0.0)
    if price <= 0:
        return {"symbol": symbol, "eligible": False, "reason": "bad_price", "score": 0.0}
    if price < SCANNER_MIN_PRICE or price > SCANNER_MAX_PRICE:
        return {"symbol": symbol, "eligible": False, "reason": "price_filter", "score": 0.0, "price": price}

    lookback = max(10, min(len(today), int(SCANNER_ACTIVITY_LOOKBACK_BARS)))
    recent_n = max(3, min(lookback // 2, int(SCANNER_ACTIVITY_RECENT_BARS)))
    win = today[-lookback:]
    recent = win[-recent_n:]
    earlier = win[:-recent_n] or win

    closes = [float(b.get("close") or 0.0) for b in win]
    highs = [float(b.get("high", b.get("close") or 0.0) or 0.0) for b in win]
    lows = [float(b.get("low", b.get("close") or 0.0) or 0.0) for b in win]
    vols = [float(b.get("volume") or 0.0) for b in win]
    recent_vol = sum(float(b.get("volume") or 0.0) for b in recent)
    earlier_vol_avg = _mean([float(b.get("volume") or 0.0) for b in earlier]) or 0.0
    relvol = recent_vol / max(earlier_vol_avg * len(recent), 1.0) if earlier_vol_avg > 0 else 0.0

    open_price = float(win[0].get("open", closes[0]) or closes[0] or 0.0)
    intraday_change = (price / open_price - 1.0) if open_price > 0 else 0.0
    recent_change = (price / max(float(recent[0].get("open", price) or price), 1e-9) - 1.0) if recent else 0.0
    range_pct = ((max(highs) - min(lows)) / price) if price > 0 else 0.0
    dollar_vol = price * sum(vols)

    score = 0.0
    score += abs(intraday_change) * 800.0
    score += abs(recent_change) * 600.0
    score += range_pct * 400.0
    score += max(0.0, min(relvol, 3.0)) * 20.0
    if intraday_change > 0:
        score += 8.0
    if recent_change > 0:
        score += 4.0
    if dollar_vol >= 1_000_000:
        score += min(20.0, dollar_vol / 50_000_000.0)

    return {
        "symbol": symbol,
        "eligible": True,
        "score": round(score, 4),
        "price": round(price, 4),
        "intraday_change_pct": round(intraday_change * 100.0, 3),
        "recent_change_pct": round(recent_change * 100.0, 3),
        "range_pct": round(range_pct * 100.0, 3),
        "relvol": round(relvol, 3),
        "dollar_vol": round(dollar_vol, 2),
        "bars": len(today),
    }


def rank_scan_candidates(symbols: list[str], bars_map: dict[str, list[dict]]) -> list[dict]:
    profiles = [_activity_profile(sym, bars_map.get(sym, [])) for sym in symbols]
    profiles = [p for p in profiles if p.get("eligible")]
    profiles.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    return profiles


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

def resample_5m(bars_1m: list[dict], minutes: int = 5) -> list[dict]:
    """Resample 1-minute bars (dicts) into N-minute bars with simple indicators.

    Input bar keys expected: ts_utc (epoch sec), ts_ny (ISO), open, high, low, close, volume, vwap (optional).
    Output bars include: ts_utc, ts_ny, open, high, low, close, volume, vwap (session cumulative),
    ema_fast, ema_slow.
    """
    if not bars_1m:
        return []

    def _parse_ts_ny(x) -> datetime:
        # bars_1m["ts_ny"] is usually a timezone-aware datetime; sometimes an ISO string.
        if isinstance(x, datetime):
            return x
        return datetime.fromisoformat(str(x))

    buckets: dict[datetime, list[dict]] = {}

    for b in bars_1m:
        tsn = b.get("ts_ny")
        if not tsn:
            continue
        dt = _parse_ts_ny(tsn)
        mm = (dt.minute // minutes) * minutes
        key = dt.replace(minute=mm, second=0, microsecond=0)
        buckets.setdefault(key, []).append(b)

    out: list[dict] = []
    for key in sorted(buckets.keys()):
        grp = buckets[key]
        if not grp:
            continue
        grp_sorted = sorted(grp, key=lambda x: x.get("ts_utc") or datetime.min.replace(tzinfo=timezone.utc))
        o = float(grp_sorted[0].get("open", grp_sorted[0].get("close", 0.0)) or 0.0)
        h = max(float(x.get("high", x.get("close", 0.0)) or 0.0) for x in grp_sorted)
        l = min(float(x.get("low", x.get("close", 0.0)) or 0.0) for x in grp_sorted)
        c = float(grp_sorted[-1].get("close", 0.0) or 0.0)
        v = sum(float(x.get("volume", 0.0) or 0.0) for x in grp_sorted)

        pv = 0.0
        for x in grp_sorted:
            vol = float(x.get("volume", 0.0) or 0.0)
            px = x.get("vwap", None)
            if px is None:
                px = x.get("close", 0.0)
            pv += float(px or 0.0) * vol
        bar_vwap = (pv / v) if v > 0 else c
        
        ts_utc = grp_sorted[-1].get("ts_utc")
        out.append({
            "ts_utc": ts_utc,
            "ts_ny": key.isoformat(),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
            "vwap_bar": bar_vwap,
        })

    if not out:
        return []

    cum_pv = 0.0
    cum_v = 0.0
    for b in out:
        vol = float(b.get("volume", 0.0) or 0.0)
        px = float(b.get("vwap_bar", b.get("close", 0.0)) or 0.0)
        cum_pv += px * vol
        cum_v += vol
        b["vwap"] = (cum_pv / cum_v) if cum_v > 0 else float(b.get("close", 0.0) or 0.0)

    closes = [float(b.get("close", 0.0) or 0.0) for b in out]
    ema_fast = ema_series(closes, SCANNER_HF_EMA_FAST)
    ema_slow = ema_series(closes, SCANNER_HF_EMA_SLOW)
    for i, b in enumerate(out):
        b["ema_fast"] = ema_fast[i]
        b["ema_slow"] = ema_slow[i]

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

    def _fallback_from_rest(reason: str) -> dict[str, list[dict]]:
        rest_rows, rest_debug = _fetch_bars_via_rest(
            symbols,
            start=start.astimezone(timezone.utc),
            end=end.astimezone(timezone.utc),
            feed_override=_DATA_FEED_RAW,
            limit=(limit_per_symbol or 10000),
        )
        logger.warning(
            "BARS_MULTI_FALLBACK reason=%s symbols=%s feed=%s count=%s debug=%s",
            reason,
            ",".join(symbols[:10]),
            str(DATA_FEED),
            sum(len(v) for v in rest_rows.values()),
            rest_debug,
        )
        return rest_rows

    try:
        df = data_client.get_stock_bars(req).df
    except Exception as e:
        return _fallback_from_rest(f"sdk_exception:{e}")
    if df is None or len(df) == 0:
        return _fallback_from_rest("sdk_empty")

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
    if not any(out.values()):
        return _fallback_from_rest("sdk_parsed_empty")
    return out


def _volatility_score(bars_1m: list[dict], bars_n: int, metric: str) -> float:
    """Compute a simple volatility score for ranking symbols."""
    if not bars_1m:
        return 0.0
    closes = [float(b.get("close") or 0.0) for b in bars_1m[-max(10, bars_n):] if b.get("close") is not None]
    closes = [c for c in closes if c > 0]
    if len(closes) < 10:
        return 0.0

    metric = (metric or "range_pct").strip()
    if metric == "stdev_ret":
        rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0]
        if len(rets) < 5:
            return 0.0
        mu = sum(rets) / len(rets)
        var = sum((r - mu) ** 2 for r in rets) / max(1, (len(rets) - 1))
        return float(math.sqrt(var))

    cmin = min(closes)
    cmax = max(closes)
    cmean = sum(closes) / len(closes)
    return float((cmax - cmin) / cmean) if cmean > 0 else 0.0



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
    if price < ema_fast[-1] or ema_fast[-1] < ema_slow[-1]:
        return None

    if price >= box_high * (1.0 + buf):
        return ("midbox_breakout_up", "BUY")

    # Shorts are disabled in this system; return None for breakdowns.
    return None

def _scan_diag_midbox(bars_1m: list[dict]) -> dict:
    out: dict = {"enabled": True, "eligible": True}
    try:
        # calc_midbox expects 1m bars
        box = calc_midbox(bars_1m)
        box_high = float(box["box_high"])
        buf = float(MIDBOX_BREAKOUT_BUFFER_PCT)

        # Buffered confirm level vs raw box high (for touch/near)
        level = box_high * (1.0 + buf)
        raw_level = box_high

        price = float(bars_1m[-1].get("close") or 0.0)
        prev_close = float(bars_1m[-2].get("close") or 0.0)
        high = float(bars_1m[-1].get("high", price))

        crossed_up = (price >= level) and (prev_close < level)
        crossed_down = (price <= level) and (prev_close > level)

        touch_eps = float(MIDBOX_TOUCH_EPS_PCT)
        # Touch uses RAW level (not buffered)
        touched = bool(high >= raw_level and price >= raw_level * (1.0 - touch_eps) and prev_close < raw_level)

        out.update({
            "box_high": box_high,
            "level": level,
            "raw_level": raw_level,
            "prev_close": prev_close,
            "close": price,
            "high": high,
            "buf": buf,
            "crossed_up": bool(crossed_up),
            "crossed_down": bool(crossed_down),
            "touched": bool(touched),
        })

        # Trend filter
        trend_ok = True
        if MIDBOX_TREND_FILTER_ENABLE:
            ema_fast = ema_series(bars_1m, MIDBOX_TREND_FAST_LEN)
            ema_slow = ema_series(bars_1m, MIDBOX_TREND_SLOW_LEN)
            if ema_fast and ema_slow:
                trend_ok = (price >= ema_fast[-1]) and (ema_fast[-1] >= ema_slow[-1])
        out["trend_ok"] = bool(trend_ok)

        if not trend_ok:
            out["reason"] = "trend_fail"
            return out

        if crossed_up or crossed_down or touched:
            out["reason"] = "breakout_cross"
            return out

        out["reason"] = "no_breakout_cross"
        near_pct = float(MIDBOX_NEAR_PCT)
        out["near_miss"] = {
            "near": bool(price >= raw_level * (1.0 - near_pct) and prev_close < raw_level),
            "near_pct": near_pct,
            "dist_to_level_pct": (raw_level - price) / raw_level if raw_level else None,
            "raw_level": raw_level,
            "touch_eps": touch_eps,
        }
        return out
    except Exception as e:
        out["eligible"] = None
        out["reason"] = f"error:{type(e).__name__}"
        return out

def _scan_diag_pwr(bars_today: list[dict]) -> dict:
    out: dict = {"enabled": True, "eligible": True}
    try:
        session_ok, pre_bars, _ = split_power_hour(bars_today)
        if not session_ok:
            out["eligible"] = False
            out["reason"] = "not_in_session"
            return out

        use_hl = any(("high" in b) for b in pre_bars)
        pre_high = (
            max(float(b.get("high", b.get("close"))) for b in pre_bars)
            if use_hl
            else max(float(b.get("close")) for b in pre_bars)
        )

        buf = float(PWR_BREAKOUT_BUFFER_PCT)
        level = pre_high * (1.0 + buf)
        raw_level = pre_high

        price = float(bars_today[-1].get("close") or 0.0)
        prev_close = float(bars_today[-2].get("close") or 0.0)
        high = float(bars_today[-1].get("high", price))

        breakout = (price >= level) and (prev_close < level)

        touch_eps = float(PWR_TOUCH_EPS_PCT)
        touched = bool(high >= raw_level and price >= raw_level * (1.0 - touch_eps) and prev_close < raw_level)

        out.update({
            "pre_high": pre_high,
            "level": level,
            "raw_level": raw_level,
            "prev_close": prev_close,
            "close": price,
            "high": high,
            "buf": buf,
            "breakout": bool(breakout),
            "touched": bool(touched),
        })

        # Volume confirmation (loose)
        volume_ok = True
        try:
            last_vol = float(bars_today[-1].get("volume") or 0.0)
            vols20 = sorted([float(b.get("volume") or 0.0) for b in bars_today[-20:]])
            med = vols20[len(vols20) // 2] if vols20 else 0.0
            if med > 0:
                volume_ok = last_vol >= (0.8 * med)
        except Exception:
            volume_ok = True
        out["volume_ok"] = bool(volume_ok)

        out["vwap_5m"] = None
        out["ema20_5m"] = None
        out["ema200_slope_up"] = True

        if (breakout or touched) and not volume_ok:
            out["reason"] = "volume_fail"
            return out

        if not breakout and not touched:
            out["reason"] = "no_breakout"
            near_pct = float(PWR_NEAR_PCT)
            out["near_miss"] = {
                "near": bool(price >= raw_level * (1.0 - near_pct) and prev_close < raw_level),
                "near_pct": near_pct,
                "dist_to_level_pct": (raw_level - price) / raw_level if raw_level else None,
                "raw_level": raw_level,
                "touch_eps": touch_eps,
            }
            return out

        out["reason"] = "ok"
        return out
    except Exception as e:
        out["eligible"] = None
        out["reason"] = f"error:{type(e).__name__}"
        return out

def _vwap_pullback_setup(bars_today: list[dict]) -> dict:
    out: dict = {"enabled": bool(VWAP_PB_ENABLE), "eligible": True, "strategy": "vwap_pullback_5m"}
    if not VWAP_PB_ENABLE:
        out["eligible"] = False
        out["reason"] = "disabled"
        return out

    bars_today = _bars_for_today_regular_session(bars_today)
    bars_5m = resample_5m(bars_today) if bars_today else []
    out["bars_5m"] = len(bars_5m)
    min_bars = max(12, int(VWAP_PB_EMA_SLOW) + 2)
    out["min_bars_5m"] = min_bars
    if len(bars_5m) < min_bars:
        out["eligible"] = False
        out["reason"] = "insufficient_5m_bars"
        return out

    closes = [float(b.get("close") or 0.0) for b in bars_5m]
    highs = [float(b.get("high", b.get("close") or 0.0) or 0.0) for b in bars_5m]
    lows = [float(b.get("low", b.get("close") or 0.0) or 0.0) for b in bars_5m]
    opens = [float(b.get("open", b.get("close") or 0.0) or 0.0) for b in bars_5m]
    volumes = [float(b.get("volume") or 0.0) for b in bars_5m]
    price = closes[-1]
    prev_close = closes[-2]

    ema_fast = ema_series(closes, int(VWAP_PB_EMA_FAST))
    ema_slow = ema_series(closes, int(VWAP_PB_EMA_SLOW))
    if not ema_fast or not ema_slow:
        out["eligible"] = False
        out["reason"] = "ema_not_ready"
        return out

    vwaps: list[float] = []
    pv = 0.0
    vv = 0.0
    for h, l, c, v in zip(highs, lows, closes, volumes):
        tp = (h + l + c) / 3.0
        pv += tp * v
        vv += v
        vwaps.append((pv / vv) if vv > 0 else c)
    vwap = vwaps[-1]

    lookback = max(3, min(int(VWAP_PB_TOUCH_LOOKBACK_BARS), len(closes)))
    touch_band = float(VWAP_PB_TOUCH_BAND_PCT)
    touch_level = vwap * (1.0 + touch_band)
    touched = min(lows[-lookback:]) <= touch_level

    extension_pct = ((price - vwap) / vwap) if vwap else 0.0
    extension_ok = extension_pct <= float(VWAP_PB_MAX_EXTENSION_PCT)

    slope_back = max(1, min(int(VWAP_PB_SLOPE_LOOKBACK_BARS), len(ema_fast) - 1, len(vwaps) - 1))
    ema_slope = (ema_fast[-1] / max(ema_fast[-1 - slope_back], 1e-9) - 1.0) if len(ema_fast) > slope_back else 0.0
    vwap_slope = (vwaps[-1] / max(vwaps[-1 - slope_back], 1e-9) - 1.0) if len(vwaps) > slope_back else 0.0

    allow_below_vwap_pct = float(VWAP_PB_ALLOW_BELOW_VWAP_PCT)
    ema_stack_slack_pct = float(VWAP_PB_EMA_STACK_SLACK_PCT)
    allow_below_fast_pct = float(VWAP_PB_ALLOW_PRICE_BELOW_EMA_FAST_PCT)

    price_above_vwap = price >= vwap * (1.0 - allow_below_vwap_pct)
    ema_stack_ok = ema_fast[-1] >= ema_slow[-1] * (1.0 - ema_stack_slack_pct)
    price_above_fast = price >= ema_fast[-1] * (1.0 - allow_below_fast_pct)
    strict_trend_ok = (ema_fast[-1] > ema_slow[-1]) and (price >= ema_slow[-1]) and (price >= vwap)
    permissive_trend_ok = price_above_vwap and ema_stack_ok and price_above_fast

    slope_ok = ema_slope >= float(VWAP_PB_MIN_EMA_SLOPE) and vwap_slope >= float(VWAP_PB_ALLOW_NEG_VWAP_SLOPE_PCT)
    regained_vwap = price >= vwap * 0.9985
    momentum_ok = price >= prev_close * 0.999 and price >= opens[-1] * 0.998

    recent_vol = sum(volumes[-3:])
    baseline_vol = _mean(volumes[-9:-3]) * 3 if len(volumes) >= 9 else _mean(volumes[:-3]) * 3
    relvol = recent_vol / max(baseline_vol, 1.0) if baseline_vol > 0 else 1.0
    relvol_ok = relvol >= float(VWAP_PB_MIN_RELVOL)

    dist_to_vwap_pct = ((price - vwap) / vwap) if vwap else 0.0
    trend_strength = 0.0
    if price_above_vwap:
        trend_strength += 12.0
    if ema_stack_ok:
        trend_strength += 12.0
    if price_above_fast:
        trend_strength += 8.0
    if strict_trend_ok:
        trend_strength += 8.0

    score = 0.0
    score += trend_strength
    if permissive_trend_ok:
        score += 12.0
    if touched:
        score += 16.0
    if regained_vwap:
        score += 14.0
    if momentum_ok:
        score += 8.0
    if extension_ok:
        score += 8.0
    if slope_ok:
        score += 6.0
    score += max(0.0, min(relvol, 2.5)) * 6.0
    score += max(0.0, min(ema_slope * 10000.0, 8.0))
    score += max(0.0, 6.0 - abs(dist_to_vwap_pct) * 1000.0)

    component_reasons = []
    if not price_above_vwap:
        component_reasons.append("price_below_vwap")
    if not ema_stack_ok:
        component_reasons.append("ema_stack_fail")
    if not price_above_fast:
        component_reasons.append("price_below_ema_fast")
    if not slope_ok:
        component_reasons.append("slope_fail")
    if not touched:
        component_reasons.append("touch_fail")
    if not regained_vwap:
        component_reasons.append("not_back_above_vwap")
    if not extension_ok:
        component_reasons.append("too_extended_from_vwap")
    if not relvol_ok:
        component_reasons.append("relvol_fail")
    if not momentum_ok:
        component_reasons.append("bounce_not_confirmed")

    near = bool(
        (permissive_trend_ok and touched and (not regained_vwap))
        or (touched and regained_vwap and score >= float(VWAP_PB_NEAR_MISS_SCORE_MIN))
        or (price_above_vwap and ema_stack_ok and abs(dist_to_vwap_pct) <= touch_band * 1.35)
    )
    fallback_ready = bool(
        VWAP_PB_ALLOW_NEAR_MISS_FALLBACK
        and permissive_trend_ok
        and touched
        and regained_vwap
        and extension_ok
        and score >= float(VWAP_PB_FALLBACK_SIGNAL_SCORE_MIN)
    )

    out.update({
        "price": round(price, 4),
        "prev_close": round(prev_close, 4),
        "vwap": round(vwap, 4),
        "ema_fast": round(ema_fast[-1], 4),
        "ema_slow": round(ema_slow[-1], 4),
        "ema_slope": round(ema_slope, 6),
        "vwap_slope": round(vwap_slope, 6),
        "strict_trend_ok": bool(strict_trend_ok),
        "trend_ok": bool(permissive_trend_ok),
        "price_above_vwap": bool(price_above_vwap),
        "ema_stack_ok": bool(ema_stack_ok),
        "price_above_fast": bool(price_above_fast),
        "touched": bool(touched),
        "regained_vwap": bool(regained_vwap),
        "momentum_ok": bool(momentum_ok),
        "extension_ok": bool(extension_ok),
        "relvol": round(relvol, 3),
        "relvol_ok": bool(relvol_ok),
        "dist_to_vwap_pct": round(dist_to_vwap_pct * 100.0, 3),
        "score": round(score, 3),
        "component_reasons": component_reasons,
        "trend_components": {
            "price_above_vwap": bool(price_above_vwap),
            "ema_stack_ok": bool(ema_stack_ok),
            "price_above_fast": bool(price_above_fast),
            "strict_trend_ok": bool(strict_trend_ok),
            "allow_below_vwap_pct": round(allow_below_vwap_pct * 100.0, 3),
            "ema_stack_slack_pct": round(ema_stack_slack_pct * 100.0, 3),
            "allow_price_below_ema_fast_pct": round(allow_below_fast_pct * 100.0, 3),
            "fallback_ready": bool(fallback_ready),
        },
        "near_miss": {
            "near": near,
            "near_pct": round(touch_band * 100.0, 3),
            "dist_to_level_pct": round(((vwap - price) / vwap) * 100.0, 3) if vwap else None,
            "fallback_ready": bool(fallback_ready),
            "score": round(score, 3),
        },
    })

    if strict_trend_ok and slope_ok and touched and regained_vwap and extension_ok and relvol_ok and momentum_ok:
        out["reason"] = "ok"
        out["triggered"] = True
    elif fallback_ready and slope_ok:
        out["reason"] = "fallback_ready"
        out["triggered"] = True
        out["fallback_trigger"] = True
    else:
        if not permissive_trend_ok:
            out["reason"] = component_reasons[0] if component_reasons else "trend_fail"
        elif not slope_ok:
            out["reason"] = "slope_fail"
        elif not touched:
            out["reason"] = "touch_fail"
        elif not regained_vwap:
            out["reason"] = "not_back_above_vwap"
        elif not extension_ok:
            out["reason"] = "too_extended_from_vwap"
        elif not relvol_ok:
            out["reason"] = "relvol_fail"
        elif not momentum_ok:
            out["reason"] = "bounce_not_confirmed"
        else:
            out["reason"] = component_reasons[0] if component_reasons else "trend_fail"

    return out

def _scan_diag_vwap_pb(bars_today: list[dict]) -> dict:
    return _vwap_pullback_setup(bars_today)


def eval_vwap_pullback_signal(bars_5m_or_today: list[dict]) -> str | None:
    diag = _vwap_pullback_setup(bars_5m_or_today)
    return "BUY" if diag.get("triggered") else None


def eval_vwap_pullback_signal_with_diag(bars_5m_or_today: list[dict]) -> tuple[str | None, dict]:
    diag = _vwap_pullback_setup(bars_5m_or_today)
    return ("BUY" if diag.get("triggered") else None, diag)


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
    if price < ema_fast[-1] or ema_fast[-1] < ema_slow[-1]:
        return None

    if price >= pre_high * (1.0 + buf):
        return ("power_hour_breakout_up", "BUY")

    return None

def _strategy_reason_disabled() -> str:
    return "strategy_disabled"


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


def _no_signal_from_hf5(diag: dict) -> str:
    # Higher-frequency ORB/VWAP/EMA strategy
    if not diag.get("enabled"):
        return "disabled"
    if diag.get("eligible") is False:
        return diag.get("reason") or "ineligible"
    # When there's no signal, we expect a structured reason
    return diag.get("reason") or "no_signal"


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
    hf = diag.get("hf5") or {}
    vp = diag.get("vwap_pullback") or {}

    mb_reason = _no_signal_from_midbox(mb)
    pw_reason = _no_signal_from_pwr(pw)
    vp_reason = _no_signal_from_vwap_pb(vp)
    hf_reason = "no_signal"

    details["midbox"] = {"enabled": bool(mb.get("enabled")), "eligible": mb.get("eligible"), "reason": mb_reason}
    details["pwr"] = {"enabled": bool(pw.get("enabled")), "eligible": pw.get("eligible"), "reason": pw_reason}
    details["vwap_pullback"] = {"enabled": bool(vp.get("enabled")), "eligible": vp.get("eligible"), "reason": vp_reason}
    details["hf5"] = {"enabled": bool(hf.get("enabled")), "eligible": hf.get("eligible"), "reason": hf_reason}

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
        "max_open_positions": MAX_OPEN_POSITIONS,
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
def state(request: Request):
    require_admin_if_configured(request)
    return {"ok": True, "trade_plan": TRADE_PLAN, "symbol_locks": SYMBOL_LOCKS}

@app.get("/diagnostics/positions")
def diagnostics_positions(request: Request):
    require_admin_if_configured(request)
    positions = list_open_positions_details_allowed()
    active_plans = {sym: plan for sym, plan in TRADE_PLAN.items() if plan.get("active")}
    return {
        "ok": True,
        "max_open_positions": MAX_OPEN_POSITIONS,
        "open_positions_count": len(positions),
        "positions": positions,
        "active_plans": active_plans,
    }


@app.get("/diagnostics/orders")
def diagnostics_orders(request: Request, limit: int = 50):
    require_admin_if_configured(request)
    rows = [d for d in DECISIONS if d.get("event") == "ENTRY"]
    rows = rows[-max(1, min(limit, 500)):]
    return {"ok": True, "count": len(rows), "orders": rows}


@app.get("/diagnostics/decisions")
def diagnostics_decisions(request: Request, symbol: str = "", limit: int = 200):
    require_admin_if_configured(request)
    """Return recent decision traces for debugging (in-memory)."""
    sym = (symbol or "").upper().strip()
    lim = max(1, min(int(limit or 200), 2000))
    items = DECISIONS
    if sym:
        items = [d for d in items if d.get("symbol") == sym]
    return {"ok": True, "count": len(items), "items": items[-lim:]}


@app.get("/diagnostics/scans")
def diagnostics_scans(request: Request, limit: int = 50, symbol: str = ""):
    require_admin_if_configured(request)
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
def diagnostics_last_scan(request: Request, symbol: str = ""):
    require_admin_if_configured(request)
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
def diagnostics_scans_latest(request: Request, symbol: str = ""):
    """Compatibility alias for clients expecting /diagnostics/scans/latest."""
    require_admin_if_configured(request)
    return diagnostics_last_scan(request=request, symbol=symbol)



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



def execute_entry_signal(symbol: str, side: str, signal: str, source: str, meta: dict | None = None, auth_payload: dict | None = None) -> dict:
    """Shared entry execution path for scanner + webhook."""
    meta = meta or {}
    auth_payload = auth_payload or {}
    symbol = (symbol or "").upper().strip()
    side = (side or "").lower().strip()
    signal = (signal or "").strip()

    if not symbol:
        return {"ok": False, "rejected": True, "reason": "symbol_required"}
    if side not in ("buy", "sell"):
        return {"ok": False, "rejected": True, "reason": "invalid_side"}
    if ALLOWED_SYMBOLS and symbol not in ALLOWED_SYMBOLS:
        return {"ok": True, "ignored": True, "reason": "symbol_not_allowed", "symbol": symbol, "signal": signal}

    if KILL_SWITCH:
        return {"ok": False, "rejected": True, "reason": "kill_switch_enabled"}
    if daily_stop_hit():
        return {"ok": False, "rejected": True, "reason": "daily_stop_hit"}
    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "ignored": True, "reason": "outside_market_hours", "symbol": symbol, "signal": signal}

    if side == "sell" and not ALLOW_SHORT:
        qty_signed, _pos_side = get_position(symbol)
        if qty_signed > 0:
            out = close_position(symbol)
            if symbol in TRADE_PLAN:
                TRADE_PLAN[symbol]["active"] = False
            return {"ok": True, "closed": True, "reason": "shorts_disabled_closed_long", "symbol": symbol, "signal": signal, **out}
        return {"ok": True, "ignored": True, "reason": "shorts_disabled", "symbol": symbol, "signal": signal}

    if source == "webhook" and ENABLE_IDEMPOTENCY:
        dk = dedup_key(auth_payload)
        last = DEDUP_CACHE.get(dk, 0)
        now = utc_ts()
        if last and (now - last) < DEDUP_WINDOW_SEC:
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="ignored", reason="dedup", meta=meta)
            return {"ok": True, "ignored": True, "reason": "dedup", "symbol": symbol, "signal": signal}
        DEDUP_CACHE[dk] = now

    if TRADE_PLAN.get(symbol, {}).get("active"):
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="ignored", reason="plan_active", meta=meta)
        return {"ok": True, "ignored": True, "reason": "plan_active", "symbol": symbol, "signal": signal}

    qty_signed, pos_side = get_position(symbol)
    if qty_signed != 0:
        desired_side = "long" if side == "buy" else "short"
        if desired_side == pos_side:
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="ignored", reason=f"position_already_open:{pos_side}", meta=meta)
            return {"ok": True, "ignored": True, "reason": f"position_already_open:{pos_side}", "symbol": symbol, "signal": signal}

        close_out = close_position(symbol)
        if symbol in TRADE_PLAN:
            TRADE_PLAN[symbol]["active"] = False

        if not close_out.get("closed"):
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="reverse_close_failed", meta=meta)
            return {"ok": False, "rejected": True, "reason": "reverse_close_failed", "symbol": symbol, "signal": signal, **close_out}

        if not ALLOW_REVERSAL:
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="closed_opposite_position", reason="allow_reversal_false", meta=meta)
            return {"ok": True, "action": "closed_opposite_position", "symbol": symbol, "signal": signal, **close_out}

    if max_open_positions_reached():
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="ignored", reason="max_open_positions_reached", meta=meta)
        return {"ok": True, "ignored": True, "reason": "max_open_positions_reached", "symbol": symbol, "signal": signal, "max_open_positions": MAX_OPEN_POSITIONS}

    if not take_symbol_lock(symbol, SYMBOL_LOCK_SEC):
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="ignored", reason="symbol_locked", meta=meta)
        return {"ok": True, "ignored": True, "reason": "symbol_locked", "symbol": symbol, "signal": signal}

    effective_dry_run = bool((SCANNER_DRY_RUN if source == "worker_scan" else False) or DRY_RUN or (source == "worker_scan" and (not SCANNER_ALLOW_LIVE)))

    try:
        base_price = get_latest_price(symbol)
        if base_price is None or base_price <= 0:
            raise ValueError("latest_price_missing")
        qty = compute_qty(float(base_price)) if side == "buy" else round(abs(qty_signed), 2)
        if qty <= 0:
            raise ValueError("qty_zero")

        payload = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "base_price": round(float(base_price), 2),
            "signal": signal,
            "paper": APCA_PAPER,
            "dry_run": effective_dry_run,
            "source": source,
        }

        if effective_dry_run:
            plan = build_trade_plan(symbol, side, qty, float(base_price), signal)
            plan["source"] = source
            TRADE_PLAN[symbol] = plan
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="dry_run_plan_created", reason="", qty=qty, meta=meta)
            return {"ok": True, "submitted": False, "dry_run": True, "order": payload, "plan": plan}

        order = submit_market_order(symbol, side, qty)
        plan = build_trade_plan(symbol, side, qty, float(base_price), signal)
        plan["source"] = source
        plan["order_id"] = str(getattr(order, "id", ""))
        TRADE_PLAN[symbol] = plan
        log("ORDER_SUBMITTED", symbol=symbol, side=side, qty=qty, order_id=str(getattr(order, "id", "")), signal=signal, source=source)
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="order_submitted", reason="", order_id=str(getattr(order, "id", "")), qty=qty, meta=meta)
        return {"ok": True, "submitted": True, "order_id": str(getattr(order, "id", "")), "order": payload, "plan": plan}
    except Exception as e:
        log("ORDER_REJECTED", symbol=symbol, side=side, err=str(e), signal=signal, source=source)
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="alpaca_submit_failed", err=str(e), meta=meta)
        soften_symbol_lock(symbol, 5)
        return {"ok": False, "rejected": True, "reason": f"alpaca_submit_failed:{e}", "symbol": symbol, "signal": signal}


def submit_scan_trade(symbol: str, side: str, signal: str, meta: dict | None = None) -> dict:
    """Submit a market order originating from the scanner (shared execution path)."""
    return execute_entry_signal(symbol=symbol, side=side, signal=signal, source="worker_scan", meta=meta)

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

    out = execute_entry_signal(symbol=symbol, side=side, signal=signal, source="webhook", auth_payload=data)
    return out


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


    # --- Trades-Today forcing (optional, emergency) ---
    # Keep this path conservative and self-contained so it cannot crash the exit worker.
    try:
        effective_dry_run = bool(SCANNER_DRY_RUN or DRY_RUN or (not SCANNER_ALLOW_LIVE))
        if TRADES_TODAY_ENABLE and SCANNER_ALLOW_LIVE and (not effective_dry_run) and in_market_hours():
            forced_today = _count_forced_trades_today_ny()
            already_actionable = any(str(r.get("action", "")).startswith("exit_") for r in results)
            allowed_pool = [s for s in TRADES_TODAY_PREFERRED_SYMBOLS if (not ALLOWED_SYMBOLS or s in ALLOWED_SYMBOLS)]
            pick = allowed_pool[0] if allowed_pool else (sorted(ALLOWED_SYMBOLS)[0] if ALLOWED_SYMBOLS else None)
            if (not already_actionable) and pick and forced_today < max(TRADES_TODAY_TARGET_TRADES, 0):
                side = "buy"
                signal = TRADES_TODAY_SIGNAL
                submit = submit_scan_trade(pick, side=side, signal=signal, meta={"forced": True, "mode": "trades_today"})
                results.insert(0, {
                    "symbol": pick,
                    "action": submit.get("action", "submit"),
                    "reason": f"forced:{signal}",
                    "price": submit.get("price"),
                    "stop": submit.get("stop"),
                    "take": submit.get("take"),
                    "order_id": submit.get("order_id"),
                    "diagnostics": {"forced": True},
                })
                record_decision("SCAN", "worker_exit", symbol=pick, side=side, signal=signal,
                                action=submit.get("action", "submit"), reason="forced_trade",
                                price=submit.get("price"), stop=submit.get("stop"), take=submit.get("take"),
                                meta={"forced": True})
    except Exception as e:
        logger.exception("TRADES_TODAY_ERROR err=%s", e)
    return {"ok": True, "ts_ny": now.isoformat(), "reconcile": reconcile_actions, "results": results}




def _bar_path_probe(symbol: str, lookback_days: int = 1) -> dict:
    sym = (symbol or "AAPL").strip().upper()
    now_local = now_ny()
    start_local = (now_local - timedelta(days=max(1, int(lookback_days)))).replace(hour=9, minute=30, second=0, microsecond=0)
    end_local = now_local
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    rows = fetch_1m_bars_multi([sym], lookback_days=lookback_days, limit_per_symbol=500).get(sym, [])
    today_rows = _bars_for_today_regular_session(rows)
    bars_5m = resample_5m(today_rows) if today_rows else []
    rest_rows, rest_debug = _fetch_bars_via_rest([sym], start_utc, end_utc, feed_override=_DATA_FEED_RAW, limit=500)
    rest_today = _bars_for_today_regular_session(rest_rows.get(sym, []))
    rest_5m = resample_5m(rest_today) if rest_today else []
    return {
        "symbol": sym,
        "feed": str(DATA_FEED),
        "adjustment": str(ADJUSTMENT),
        "request_start_utc": _iso_utc(start_utc),
        "request_end_utc": _iso_utc(end_utc),
        "bars_1m": len(rows),
        "bars_1m_today": len(today_rows),
        "bars_5m_today": len(bars_5m),
        "latest_1m_ts": today_rows[-1].get("ts_ny").isoformat() if today_rows else None,
        "latest_5m_ts": bars_5m[-1].get("ts_ny") if bars_5m else None,
        "first_1m_ts": today_rows[0].get("ts_ny").isoformat() if today_rows else None,
        "first_5m_ts": bars_5m[0].get("ts_ny") if bars_5m else None,
        "rest_probe": {
            "bars_1m": len(rest_rows.get(sym, [])),
            "bars_1m_today": len(rest_today),
            "bars_5m_today": len(rest_5m),
            "latest_1m_ts": rest_today[-1].get("ts_ny").isoformat() if rest_today else None,
            "latest_5m_ts": rest_5m[-1].get("ts_ny") if rest_5m else None,
            "first_1m_ts": rest_today[0].get("ts_ny").isoformat() if rest_today else None,
            "first_5m_ts": rest_5m[0].get("ts_ny") if rest_5m else None,
            "debug": rest_debug,
        },
        "ok": len(bars_5m) > 0 or len(rest_5m) > 0,
    }


@app.get("/diagnostics/bars_5m")
def diagnostics_bars_5m(request: Request, symbol: str = "AAPL", lookback_days: int = 1):
    require_admin_if_configured(request)
    sym = (symbol or "AAPL").strip().upper()
    try:
        probe = _bar_path_probe(sym, lookback_days=max(1, int(lookback_days)))
        return {"ok": True, "probe": probe}
    except Exception as e:
        return {"ok": False, "symbol": sym, "error": str(e)}


@app.get("/diagnostics/bars_debug")
def diagnostics_bars_debug(request: Request, symbol: str = "AAPL", lookback_days: int = 1):
    require_admin_if_configured(request)
    sym = (symbol or "AAPL").strip().upper()
    days = max(1, int(lookback_days))
    now_local = now_ny()
    start_local = (now_local - timedelta(days=days)).replace(hour=9, minute=30, second=0, microsecond=0)
    end_local = now_local
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    rows, debug = _fetch_bars_via_rest([sym], start_utc, end_utc, feed_override=_DATA_FEED_RAW, limit=500)
    seq = rows.get(sym, [])
    return {
        "ok": True,
        "symbol": sym,
        "request_start_utc": _iso_utc(start_utc),
        "request_end_utc": _iso_utc(end_utc),
        "feed": str(DATA_FEED),
        "raw_count": len(seq),
        "first_ts": seq[0].get("ts_ny").isoformat() if seq else None,
        "last_ts": seq[-1].get("ts_ny").isoformat() if seq else None,
        "sample": [{
            "ts_ny": r.get("ts_ny").isoformat(),
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
            "volume": r.get("volume"),
        } for r in seq[:5]],
        "debug": debug,
    }


@app.get("/diagnostics/runtime")
def diagnostics_runtime(request: Request):
    """Lightweight runtime sanity checks for live-readiness."""
    require_admin_if_configured(request)
    checks = {}
    try:
        checks["alpaca_clock"] = {"ok": bool(trading_client.get_clock())}
    except Exception as e:
        checks["alpaca_clock"] = {"ok": False, "error": str(e)}

    sample_symbol = None
    try:
        universe = universe_symbols()
        sample_symbol = universe[0] if universe else (sorted(ALLOWED_SYMBOLS)[0] if ALLOWED_SYMBOLS else None)
    except Exception:
        sample_symbol = None

    if sample_symbol:
        try:
            px = get_latest_price(sample_symbol)
            qty = compute_qty(float(px)) if px else 0
            plan = build_trade_plan(sample_symbol, "buy", qty, float(px), "runtime_probe") if px else None
            checks["sample_symbol"] = {
                "ok": bool(px and qty and plan),
                "symbol": sample_symbol,
                "price": px,
                "qty": qty,
                "stop": plan.get("stop_price") if plan else None,
                "take": plan.get("take_price") if plan else None,
            }
        except Exception as e:
            checks["sample_symbol"] = {"ok": False, "symbol": sample_symbol, "error": str(e)}
        try:
            checks["sample_symbol_5m_bars"] = _bar_path_probe(sample_symbol, lookback_days=1)
        except Exception as e:
            checks["sample_symbol_5m_bars"] = {"ok": False, "symbol": sample_symbol, "error": str(e)}
    else:
        checks["sample_symbol"] = {"ok": False, "error": "no_symbols_available"}
        checks["sample_symbol_5m_bars"] = {"ok": False, "error": "no_symbols_available"}

    live_ready = all(v.get("ok") for v in checks.values()) and (not DRY_RUN) and SCANNER_ALLOW_LIVE
    return {
        "ok": True,
        "live_ready": live_ready,
        "paper": APCA_PAPER,
        "dry_run": DRY_RUN,
        "scanner_dry_run": SCANNER_DRY_RUN,
        "scanner_allow_live": SCANNER_ALLOW_LIVE,
        "checks": checks,
    }

@app.post("/worker/scan_entries")
async def worker_scan_entries(req: Request):
    """Server-side scanner entry evaluation (Phase 1C). Default is shadow-mode (no orders)."""
    cleanup_caches()
    scan_started = _time.perf_counter()

    def _elapsed_ms() -> int:
        return int(max(0.0, (_time.perf_counter() - scan_started) * 1000.0))

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
            _set_last_scan(skipped=True, reason="scanner_disabled", scanned=0, signals=0, would_trade=0, blocked=0, duration_ms=_elapsed_ms())
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
                    "duration_ms": _elapsed_ms(),
                    "summary": scan_summary,
                    "results": [],
                })
                if len(SCAN_HISTORY) > SCAN_HISTORY_SIZE:
                    del SCAN_HISTORY[: max(0, len(SCAN_HISTORY) - SCAN_HISTORY_SIZE)]
            except Exception:
                pass
            return {"ok": True, "skipped": True, "reason": "scanner_disabled", **LAST_SCAN}

        if SCANNER_REQUIRE_MARKET_HOURS and ONLY_MARKET_HOURS and not in_market_hours():
            _set_last_scan(skipped=True, reason="outside_market_hours", scanned=0, signals=0, would_trade=0, blocked=0, duration_ms=_elapsed_ms())
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
                    "duration_ms": _elapsed_ms(),
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
            _set_last_scan(skipped=True, reason="outside_scanner_session", scanned=0, signals=0, would_trade=0, blocked=0, duration_ms=_elapsed_ms())
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
                    "duration_ms": _elapsed_ms(),
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

        candidate_info = rank_scan_candidates(syms, bars_map)
        if candidate_info:
            selected_n = max(1, int(SCANNER_CANDIDATE_LIMIT))
            syms = [row["symbol"] for row in candidate_info[:selected_n]]

        vol_rank_info = {
            "enabled": True,
            "mode": "activity_rank",
            "primary_strategy": SCANNER_PRIMARY_STRATEGY,
            "selected_n": len(syms),
            "universe_n": len(candidate_info),
            "top": candidate_info[: min(10, len(candidate_info))],
        }

        # Optional legacy volatility ranking can further refine the already-active universe.
        if SCANNER_VOL_RANK_ENABLE and syms:
            metric = str(SCANNER_VOL_RANK_METRIC or "range_pct").strip()
            bars_n = max(10, int(SCANNER_VOL_RANK_BARS))
            scores = [(s, _volatility_score(bars_map.get(s, []), bars_n=bars_n, metric=metric)) for s in syms]
            scores.sort(key=lambda t: t[1], reverse=True)

            n = max(1, int(SCANNER_VOL_RANK_TOP_N))
            ranked = [s for s, _ in scores[:n]]
            ranked = [s for s in ranked if bars_map.get(s)]
            if ranked:
                syms = ranked

            vol_rank_info["volatility_refine"] = {
                "metric": metric,
                "bars": bars_n,
                "selected_n": len(syms),
                "top": [{"symbol": s, "score": sc} for s, sc in scores[: min(10, len(scores))]],
            }
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

                    use_vwap_only = SCANNER_PRIMARY_STRATEGY in ("vwap", "vwap_pullback", "vwap_pullback_only")
                    diag = {
                        'hf5': {'enabled': bool(SCANNER_ENABLE_HF and (not use_vwap_only))},
                        'midbox': {'enabled': bool(SCANNER_ENABLE_MIDBOX and (not use_vwap_only))},
                        'pwr': {'enabled': bool(SCANNER_ENABLE_PWR and (not use_vwap_only)), 'session': PWR_SESSION},
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
                        bars_5m = resample_5m(bars_today) if bars_today else []

                        # Primary production path: 5m VWAP pullback. Legacy strategies remain available only
                        # when SCANNER_PRIMARY_STRATEGY is changed away from vwap_pullback.
                        if use_vwap_only:
                            vp = None
                            vp_diag = None
                            if SCANNER_ENABLE_VWAP_PB and (not _hard_market_closed) and diag.get('vwap_pullback', {}).get('eligible', True):
                                vp, vp_diag = eval_vwap_pullback_signal_with_diag(bars_today)
                                if isinstance(vp_diag, dict):
                                    diag["vwap_pullback"].update(vp_diag)
                            if vp == "BUY":
                                signal_name, side = ("VWAP_PULLBACK", "buy")
                                if isinstance(vp_diag, dict) and vp_diag.get("fallback_trigger"):
                                    signal_name = "VWAP_PULLBACK_FALLBACK"
                            elif vp == "SELL":
                                signal_name, side = ("VWAP_PULLBACK", "sell")
                        else:
                            hf_sig = None
                            if SCANNER_ENABLE_HF:
                                hf_sig, hf_dbg = eval_hf_signal_with_debug(bars_today, bars_5m)
                                if hf_dbg:
                                    diag.setdefault("hf5", {}).update({
                                        "reason": hf_dbg.get("reason"),
                                        "components": hf_dbg.get("components", {}),
                                        "near_miss": hf_dbg.get("near_miss", {}),
                                    })
                            if hf_sig:
                                signal_name, side = hf_sig
                            else:
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
                                        vp = None
                                        if SCANNER_ENABLE_VWAP_PB and (not _hard_market_closed) and diag.get('vwap_pullback', {}).get('eligible', True):
                                            vp = eval_vwap_pullback_signal(bars_today)
                                        if vp == "BUY":
                                            signal_name, side = ("VWAP_PULLBACK", "buy")
                                        elif vp == "SELL":
                                            signal_name, side = ("VWAP_PULLBACK", "sell")

                        if signal_name and side in ("buy", "sell"):
                            action = side
                            reason = signal_name
                            preview_plan = None
                            try:
                                qty_for_plan = compute_qty(float(price))
                                preview_plan = build_trade_plan(sym, side, qty_for_plan, float(price), signal_name)
                                preview_plan["active"] = False
                                preview_plan["preview_only"] = True
                            except Exception as e:
                                diag["trade_plan_error"] = str(e)
                            local_signals.append({"symbol": sym, "action": action, "side": side, "price": price, "signal": signal_name, "score": float(diag.get("vwap_pullback", {}).get("score", 0.0)), "plan_preview": preview_plan})

                    stop_out = plan.get("stop_price") if plan else None
                    take_out = plan.get("take_price") if plan else None

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

        duration_ms = int((_time.perf_counter() - scan_started) * 1000)

        # ---- Scan-level summary (top no-signal reasons, action counts) ----
        action_counts = Counter()
        no_signal_counts = Counter()
        near_miss_counts = Counter()
        for r in results:
            try:
                action_counts[str(r.get("action", "") or "")] += 1
                reason = str(r.get("reason", "") or "")
                if reason.startswith("no_signal:"):
                    primary = (r.get("no_signal") or {}).get("primary") or reason.split(":", 1)[1]
                    if primary:
                        no_signal_counts[str(primary)] += 1

                # Near-miss (per-strategy) - best-effort
                ns_details = ((r.get("no_signal") or {}).get("details") or {})
                for strat, payload in ns_details.items():
                    try:
                        nm = (payload or {}).get("near_miss") or {}
                        if isinstance(nm, dict) and bool(nm.get("near")):
                            near_miss_counts[str(strat)] += 1
                    except Exception:
                        pass
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

        signals.sort(key=lambda r: float(r.get("score", 0.0)), reverse=True)

        scan_summary = {
            "actions": dict(action_counts),
            "no_signal_total": int(sum(no_signal_counts.values())),
            "top_no_signal_reasons": top_no_signal,
            "near_miss_total": int(sum(near_miss_counts.values())),
            "near_miss_by_strategy": {k: int(v) for k, v in near_miss_counts.items()},
            "strategy_breakdown": strategy_breakdown,
            "top_candidates": vol_rank_info.get("top", []) if isinstance(vol_rank_info, dict) else [],
            "top_signals": [
                {"symbol": s.get("symbol"), "signal": s.get("signal"), "score": s.get("score"), "price": s.get("price")}
                for s in signals[: min(5, len(signals))]
            ],
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

        # Execute (optional): submit up to N live orders per scan.
        would_submit = []
        if signals:
            for plan in signals[: max(1, SCANNER_MAX_ENTRIES_PER_SCAN)]:
                sym = plan.get("symbol")
                side = plan.get("side") or "buy"
                sig = plan.get("signal") or "scan"
                if SCANNER_ALLOW_LIVE and (not SCANNER_DRY_RUN) and (not effective_dry_run):
                    resp = submit_scan_trade(sym, side, sig)
                    would_submit.append({"symbol": sym, "side": side, "signal": sig, **resp})
                else:
                    would_submit.append({"symbol": sym, "side": side, "signal": sig, "ok": True, "action": "dry_run", "reason": "scanner_not_live_or_dry_run"})

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
                    "would_submit": would_submit,
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
                "would_submit": would_submit,
                "results": results,
        }
    except Exception as e:
        duration_ms = int((_time.perf_counter() - scan_started) * 1000)
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