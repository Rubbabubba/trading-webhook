import os
import logging
import hashlib
import traceback
import html
import time as _time
from datetime import datetime, time, timezone, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import Counter
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse

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
from urllib.error import HTTPError, URLError

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


def _fetch_latest_quotes_via_rest(symbols: list[str]) -> tuple[dict[str, dict], dict]:
    symbols = [s.strip().upper() for s in (symbols or []) if s and s.strip()]
    out: dict[str, dict] = {s: {} for s in symbols}
    debug = {"method": "rest_quote", "feed": str(_DATA_FEED_RAW), "count": 0, "url": None}
    if not symbols:
        return out, debug
    params = {"symbols": ",".join(symbols), "feed": str(_DATA_FEED_RAW)}
    url = f"{_alpaca_data_base_url()}/v2/stocks/quotes/latest?{urlencode(params)}"
    debug["url"] = url
    req = UrlRequest(url, headers={
        "APCA-API-KEY-ID": APCA_KEY,
        "APCA-API-SECRET-KEY": APCA_SECRET,
        "accept": "application/json",
        "user-agent": "trading-webhook/patch-003",
    })
    try:
        with urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        quotes_payload = payload.get("quotes", {}) if isinstance(payload, dict) else {}
        for sym, q in (quotes_payload or {}).items():
            q = q or {}
            out[str(sym)] = {
                "bid": float(q.get("bp") or 0) or None,
                "ask": float(q.get("ap") or 0) or None,
                "bid_size": float(q.get("bs") or 0) or None,
                "ask_size": float(q.get("as") or 0) or None,
                "ts_utc": _parse_bar_ts(q.get("t")),
            }
        debug["count"] = sum(1 for v in out.values() if v)
    except Exception as e:
        debug["error"] = str(e)
    return out, debug


def get_latest_quote_snapshot(symbol: str) -> dict:
    symbol = str(symbol or "").upper()
    trade_px = None
    trade_ts = None
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=[symbol])
        latest = data_client.get_stock_latest_trade(req)
        trade = None
        try:
            trade = latest.get(symbol) if hasattr(latest, "get") else latest[symbol]
        except Exception:
            trade = None
        if trade is not None:
            px = getattr(trade, "price", None)
            if px is not None:
                trade_px = float(px)
            ts_val = getattr(trade, "timestamp", None)
            if ts_val is not None:
                trade_ts = _parse_bar_ts(ts_val)
    except Exception:
        trade_px = None
        trade_ts = None

    quotes, quote_debug = _fetch_latest_quotes_via_rest([symbol])
    quote = quotes.get(symbol) or {}
    bid = quote.get("bid")
    ask = quote.get("ask")
    quote_ts = quote.get("ts_utc")
    mid = None
    spread = None
    spread_pct = None
    if bid and ask and bid > 0 and ask > 0 and ask >= bid:
        mid = round((float(bid) + float(ask)) / 2.0, 6)
        spread = round(float(ask) - float(bid), 6)
        if mid > 0:
            spread_pct = float(spread) / float(mid)

    ref_ts = quote_ts or trade_ts
    age_sec = None
    if ref_ts is not None:
        age_sec = max(0.0, (datetime.now(timezone.utc) - ref_ts).total_seconds())

    return {
        "symbol": symbol,
        "price": trade_px or mid,
        "trade_price": trade_px,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
        "quote_ts_utc": quote_ts.isoformat() if quote_ts else None,
        "trade_ts_utc": trade_ts.isoformat() if trade_ts else None,
        "price_age_sec": age_sec,
        "quote_ok": bool(bid and ask and ask >= bid),
        "fresh": bool(age_sec is not None and age_sec <= ENTRY_PRICE_MAX_AGE_SEC),
        "quote_debug": quote_debug,
    }


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


def env_bool_any(*names: str, default: str | bool = "false") -> bool:
    raw = getenv_any(*names, default=str(default))
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def getenv_float_any(*names: str, default: float = 0.0) -> float:
    raw = getenv_any(*names, default=str(default))
    try:
        return float(raw)
    except Exception:
        return float(default)


def getenv_int_any(*names: str, default: int = 0) -> int:
    raw = getenv_any(*names, default=str(default))
    try:
        return int(float(raw))
    except Exception:
        return int(default)


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
RISK_DOLLARS = getenv_float_any("SWING_RISK_PER_TRADE_DOLLARS", "RISK_DOLLARS", default=3.0)
STOP_PCT = getenv_float_any("SWING_STOP_PCT", "STOP_PCT", default=0.003)  # 0.30%
TAKE_PCT = getenv_float_any("SWING_TAKE_PCT", "TAKE_PCT", default=0.006)  # 0.60%
MIN_QTY = float(os.getenv("MIN_QTY", "0.01"))
MAX_QTY = float(os.getenv("MAX_QTY", "1.50"))
ORDER_BP_HAIRCUT_PCT = float(os.getenv("ORDER_BP_HAIRCUT_PCT", "0.95"))
MIN_AFFORDABLE_QTY = float(os.getenv("MIN_AFFORDABLE_QTY", str(MIN_QTY)))

# Safety / behavior
ALLOW_SHORT = env_bool_any("ALLOW_SHORT", default="false")
ALLOW_REVERSAL = env_bool_any("ALLOW_REVERSAL", default="true")
DRY_RUN = env_bool_any("DRY_RUN", default="false")

# Market hours
ONLY_MARKET_HOURS = env_bool_any("SWING_ONLY_MARKET_HOURS", "ONLY_MARKET_HOURS", default="true")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Exit worker
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()
EOD_FLATTEN_TIME = getenv_any("EOD_FLATTEN_TIME", default=("" if getenv_any("STRATEGY_MODE", default="intraday").strip().lower() == "swing" else "15:55"))  # NY time
EXIT_COOLDOWN_SEC = int(os.getenv("EXIT_COOLDOWN_SEC", "20"))

# Idempotency
ENABLE_IDEMPOTENCY = getenv_any("ENABLE_IDEMPOTENCY", default="true").strip().lower() in ("1","true","yes","y","on")  # allow disabling dedup if needed
# Idempotency
DEDUP_WINDOW_SEC = int(getenv_any("DEDUP_WINDOW_SEC", "IDEMPOTENCY_WINDOW_SECONDS", default="90"))  # absorb retries

# Symbol lock
SYMBOL_LOCK_SEC = int(getenv_any("SYMBOL_LOCK_SEC", "SYMBOL_LOCK_SECONDS", default="180"))  # lock during entry/plan
MAX_OPEN_POSITIONS = getenv_int_any("SWING_MAX_OPEN_POSITIONS", "MAX_OPEN_POSITIONS", default=2)

# Durable execution journal / restart diagnostics
JOURNAL_ENABLED = env_bool("JOURNAL_ENABLED", "true")
JOURNAL_PERSIST_SCANS = env_bool("JOURNAL_PERSIST_SCANS", "false")
JOURNAL_PATH = getenv_any("JOURNAL_PATH", default="/var/data/execution_journal.jsonl")
POSITION_SNAPSHOT_PATH = getenv_any("POSITION_SNAPSHOT_PATH", default="/var/data/positions_snapshot.json")
SCAN_STATE_PATH = getenv_any("SCAN_STATE_PATH", default="/var/data/scan_state.json")
REGIME_STATE_PATH = getenv_any("REGIME_STATE_PATH", default="/var/data/regime_state.json")
PAPER_LIFECYCLE_STATE_PATH = getenv_any("PAPER_LIFECYCLE_STATE_PATH", default="/var/data/paper_lifecycle_state.json")
EXECUTION_LIFECYCLE_HISTORY_LIMIT = int(getenv_any("EXECUTION_LIFECYCLE_HISTORY_LIMIT", default="50"))
SCANNER_TELEMETRY_STATE_PATH = getenv_any("SCANNER_TELEMETRY_STATE_PATH", default="/var/data/scanner_telemetry_state.json")
PAPER_LIFECYCLE_HISTORY_LIMIT = int(getenv_any("PAPER_LIFECYCLE_HISTORY_LIMIT", default="500"))
SCANNER_TELEMETRY_HISTORY_LIMIT = int(getenv_any("SCANNER_TELEMETRY_HISTORY_LIMIT", default="500"))
REGIME_BREADTH_RETURN_LOOKBACK_DAYS = int(getenv_any("REGIME_BREADTH_RETURN_LOOKBACK_DAYS", default="20"))
REGIME_MIN_SYMBOLS_FOR_BREADTH = int(getenv_any("REGIME_MIN_SYMBOLS_FOR_BREADTH", default="5"))
REGIME_REQUIRE_COMPLETE_DATA = env_bool("REGIME_REQUIRE_COMPLETE_DATA", True)
SYSTEM_NAME = getenv_any("SYSTEM_NAME", default="trading-webhook")
ENV_NAME = getenv_any("ENV_NAME", default="prod")
STRATEGY_MODE = getenv_any("STRATEGY_MODE", default="intraday").strip().lower() or "intraday"
LIVE_TRADING_ENABLED = env_bool_any("LIVE_TRADING_ENABLED", default="false")
SYSTEM_RELEASE_STAGE = str(getenv_any("SYSTEM_RELEASE_STAGE", default="paper") or "paper").strip().lower()
RELEASE_GATE_ENFORCED = env_bool("RELEASE_GATE_ENFORCED", True)
RELEASE_ALLOWED_LIVE_STAGES = {s.strip().lower() for s in str(getenv_any("RELEASE_ALLOWED_LIVE_STAGES", default="live_guarded") or "live_guarded").split(",") if s.strip()}
RELEASE_STATE_PATH = getenv_any("RELEASE_STATE_PATH", default="/var/data/release_state.json")
RELEASE_WORKFLOW_ENFORCED = env_bool("RELEASE_WORKFLOW_ENFORCED", True)
RELEASE_PROMOTION_REQUIRE_READINESS = env_bool("RELEASE_PROMOTION_REQUIRE_READINESS", True)
RELEASE_STATE_HISTORY_LIMIT = int(getenv_any("RELEASE_STATE_HISTORY_LIMIT", default="50"))
RELEASE_VALID_STAGES = {"paper", "guarded_live_eligible", "live_guarded", "emergency_disabled"}
RELEASE_REQUIRE_REGIME_COMPLETE = env_bool("RELEASE_REQUIRE_REGIME_COMPLETE", True)
RELEASE_REQUIRE_REGIME_FAVORABLE = env_bool("RELEASE_REQUIRE_REGIME_FAVORABLE", True)
RELEASE_REQUIRE_RECENT_MARKET_SCAN = env_bool("RELEASE_REQUIRE_RECENT_MARKET_SCAN", True)
RELEASE_MAX_SCAN_AGE_SEC = int(getenv_any("RELEASE_MAX_SCAN_AGE_SEC", default="14400"))
RELEASE_MIN_COMPLETED_SCANS = int(getenv_any("RELEASE_MIN_COMPLETED_SCANS", default="1"))
RELEASE_MIN_SELECTED_CANDIDATES = int(getenv_any("RELEASE_MIN_SELECTED_CANDIDATES", default="0"))
RELEASE_MIN_ENTRY_EVENTS = int(getenv_any("RELEASE_MIN_ENTRY_EVENTS", default="0"))
RELEASE_MIN_EXIT_EVENTS = int(getenv_any("RELEASE_MIN_EXIT_EVENTS", default="0"))
PERSISTENCE_REQUIRED = env_bool_any("PERSISTENCE_REQUIRED", default="true")
SWING_ALLOW_SAME_DAY_EXIT = env_bool_any("SWING_ALLOW_SAME_DAY_EXIT", default="false")
SWING_MAX_HOLD_DAYS = getenv_int_any("SWING_MAX_HOLD_DAYS", default=5)
SWING_MAX_PORTFOLIO_EXPOSURE_PCT = getenv_float_any("SWING_MAX_PORTFOLIO_EXPOSURE_PCT", default=0.90)
SWING_MAX_SYMBOL_EXPOSURE_PCT = getenv_float_any("SWING_MAX_SYMBOL_EXPOSURE_PCT", default=0.35)
SWING_PORTFOLIO_CAP_BLOCK_MODE = str(getenv_any("SWING_PORTFOLIO_CAP_BLOCK_MODE", default="total") or "total").strip().lower()
if SWING_PORTFOLIO_CAP_BLOCK_MODE not in {"total", "strategy", "both", "off"}:
    SWING_PORTFOLIO_CAP_BLOCK_MODE = "total"
SWING_STOP_ATR_DAILY_MULT = getenv_float_any("SWING_STOP_ATR_DAILY_MULT", default=1.20)
SWING_TARGET_ATR_DAILY_MULT = getenv_float_any("SWING_TARGET_ATR_DAILY_MULT", default=2.00)
SWING_STRATEGY_NAME = getenv_any("SWING_STRATEGY_NAME", default="daily_breakout").strip().lower()
SWING_BREAKOUT_LOOKBACK_DAYS = getenv_int_any("SWING_BREAKOUT_LOOKBACK_DAYS", default=20)
SWING_BREAKOUT_MIN_CLOSE_TO_HIGH_PCT = getenv_float_any("SWING_BREAKOUT_MIN_CLOSE_TO_HIGH_PCT", default=0.985)
SWING_FAST_MA_DAYS = getenv_int_any("SWING_FAST_MA_DAYS", default=10)
SWING_SLOW_MA_DAYS = getenv_int_any("SWING_SLOW_MA_DAYS", default=20)
SWING_MIN_PRICE = getenv_float_any("SWING_MIN_PRICE", default=15.0)
SWING_MIN_AVG_DOLLAR_VOLUME = getenv_float_any("SWING_MIN_AVG_DOLLAR_VOLUME", default=20000000.0)
SWING_MIN_20D_RETURN_PCT = getenv_float_any("SWING_MIN_20D_RETURN_PCT", default=0.03)
SWING_MAX_CANDIDATES = getenv_int_any("SWING_MAX_CANDIDATES", default=10)
SWING_MAX_NEW_ENTRIES_PER_DAY = getenv_int_any("SWING_MAX_NEW_ENTRIES_PER_DAY", default=1)
SWING_REQUIRE_INDEX_ALIGNMENT = env_bool_any("SWING_REQUIRE_INDEX_ALIGNMENT", default="true")
SWING_INDEX_SYMBOL = getenv_any("SWING_INDEX_SYMBOL", default="SPY").strip().upper()
SWING_ENTRY_MODE = getenv_any("SWING_ENTRY_MODE", default="next_session_market").strip().lower()
SWING_STOP_MODE = getenv_any("SWING_STOP_MODE", default="breakout_buffer").strip().lower()
SWING_BREAKOUT_BUFFER_PCT = getenv_float_any("SWING_BREAKOUT_BUFFER_PCT", default=0.015)
SWING_TARGET_R_MULT = getenv_float_any("SWING_TARGET_R_MULT", default=2.0)
SWING_ENABLE_BREAK_EVEN_STOP = env_bool_any("SWING_ENABLE_BREAK_EVEN_STOP", default=True)
SWING_BREAK_EVEN_R = getenv_float_any("SWING_BREAK_EVEN_R", default=1.0)
SWING_ENABLE_TRAILING_STOP = env_bool_any("SWING_ENABLE_TRAILING_STOP", default=True)
SWING_TRAIL_AFTER_R = getenv_float_any("SWING_TRAIL_AFTER_R", default=1.25)
SWING_TRAIL_LOOKBACK_DAYS = getenv_int_any("SWING_TRAIL_LOOKBACK_DAYS", default=3)
SWING_STALL_EXIT_DAYS = getenv_int_any("SWING_STALL_EXIT_DAYS", default=3)
SWING_STALL_MIN_R = getenv_float_any("SWING_STALL_MIN_R", default=0.25)
SWING_CANDIDATE_TTL_HOURS = getenv_int_any("SWING_CANDIDATE_TTL_HOURS", default=24)
SWING_REGIME_FILTER_ENABLED = env_bool_any("SWING_REGIME_FILTER_ENABLED", default=True)
SWING_REGIME_FAST_MA_DAYS = getenv_int_any("SWING_REGIME_FAST_MA_DAYS", default=20)
SWING_REGIME_SLOW_MA_DAYS = getenv_int_any("SWING_REGIME_SLOW_MA_DAYS", default=50)
SWING_REGIME_MIN_BREADTH = getenv_float_any("SWING_REGIME_MIN_BREADTH", default=0.50)
SWING_ALLOW_NEW_ENTRIES_IN_WEAK_TAPE = env_bool_any("SWING_ALLOW_NEW_ENTRIES_IN_WEAK_TAPE", default=False)
SWING_WEAK_TAPE_MAX_NEW_ENTRIES = getenv_int_any("SWING_WEAK_TAPE_MAX_NEW_ENTRIES", default=0)
SWING_MAX_GROUP_POSITIONS = getenv_int_any("SWING_MAX_GROUP_POSITIONS", default=1)
SWING_CORRELATION_GROUPS = getenv_any("SWING_CORRELATION_GROUPS", default="SPY,QQQ,IWM|AAPL,MSFT,NVDA,AMD,AVGO|AMZN,META,GOOGL,CRM,ORCL,SNOW")
SWING_REGIME_HISTORY_SIZE = getenv_int_any("SWING_REGIME_HISTORY_SIZE", default=100)
JOURNAL_BOOTSTRAP_LIMIT = int(getenv_any("JOURNAL_BOOTSTRAP_LIMIT", default="500"))
ORDER_DIAGNOSTIC_LOOKBACK = int(getenv_any("ORDER_DIAGNOSTIC_LOOKBACK", default="50"))

# Patch 003: quote / staleness / broker-sync gates
ENTRY_REQUIRE_QUOTE = env_bool("ENTRY_REQUIRE_QUOTE", True)
ENTRY_REQUIRE_FRESH_QUOTE = env_bool("ENTRY_REQUIRE_FRESH_QUOTE", True)
ENTRY_PRICE_MAX_AGE_SEC = float(getenv_any("ENTRY_PRICE_MAX_AGE_SEC", default="20"))
ENTRY_MAX_SPREAD_PCT = float(getenv_any("ENTRY_MAX_SPREAD_PCT", default="0.0025"))
PLAN_STALE_SUBMITTED_SEC = int(getenv_any("PLAN_STALE_SUBMITTED_SEC", default="180"))
PLAN_STALE_NO_POSITION_SEC = int(getenv_any("PLAN_STALE_NO_POSITION_SEC", default="90"))
RECONCILE_ORDER_LOOKBACK_LIMIT = int(getenv_any("RECONCILE_ORDER_LOOKBACK_LIMIT", default="100"))
RECONCILE_ORPHAN_ORDER_MAX_AGE_SEC = int(getenv_any("RECONCILE_ORPHAN_ORDER_MAX_AGE_SEC", default="900"))
RECONCILE_DEACTIVATE_ORPHAN_PLANS = env_bool_any("RECONCILE_DEACTIVATE_ORPHAN_PLANS", default=True)
RECONCILE_PARTIAL_FILL_MAX_AGE_SEC = int(getenv_any("RECONCILE_PARTIAL_FILL_MAX_AGE_SEC", default="1800"))
PLAN_RECONCILE_ORDER_STATUS = env_bool("PLAN_RECONCILE_ORDER_STATUS", True)
PLAN_SYNC_ON_WORKER_EXIT = env_bool("PLAN_SYNC_ON_WORKER_EXIT", True)
ENABLE_RISK_RECHECK_AFTER_FILL = env_bool("ENABLE_RISK_RECHECK_AFTER_FILL", True)
ENABLE_PARTIAL_FILL_TRACKING = env_bool("ENABLE_PARTIAL_FILL_TRACKING", True)
READINESS_REQUIRE_WORKERS = env_bool("READINESS_REQUIRE_WORKERS", True)
REJECTION_HISTORY_LIMIT = int(getenv_any("REJECTION_HISTORY_LIMIT", default="200"))
AUTO_FLATTEN_ON_DAILY_STOP = env_bool("AUTO_FLATTEN_ON_DAILY_STOP", True)
READINESS_SYMBOL = getenv_any("READINESS_SYMBOL", default="SPY").strip().upper() or "SPY"
READINESS_SCANNER_MAX_AGE_SEC = int(getenv_any("READINESS_SCANNER_MAX_AGE_SEC", default="240"))
SCANNER_INTERVAL_SEC = getenv_int("SCAN_INTERVAL_SEC", getenv_int("SWING_SCAN_INTERVAL_SEC", 3600))
SCANNER_TIMEOUT_SEC = getenv_int("SCAN_TIMEOUT_SEC", 60)
SCANNER_JITTER_SEC = max(0, getenv_int("SCAN_JITTER_SEC", 0))
READINESS_EXIT_MAX_AGE_SEC = int(getenv_any("READINESS_EXIT_MAX_AGE_SEC", default="90"))
RISK_RECHECK_TOLERANCE_PCT = float(getenv_any("RISK_RECHECK_TOLERANCE_PCT", default="0.10"))

# Patch 007: monitoring + alerts
ALERTS_ENABLED = env_bool("ALERTS_ENABLED", False)
ALERT_WEBHOOK_URL = getenv_any("ALERT_WEBHOOK_URL", default="").strip()
ALERT_WEBHOOK_TIMEOUT_SEC = float(getenv_any("ALERT_WEBHOOK_TIMEOUT_SEC", default="8"))
ALERT_DEDUP_SEC = int(getenv_any("ALERT_DEDUP_SEC", default="300"))
ALERT_HISTORY_LIMIT = int(getenv_any("ALERT_HISTORY_LIMIT", default="200"))
ALERT_ON_ENTRY = env_bool("ALERT_ON_ENTRY", True)
ALERT_ON_EXIT = env_bool("ALERT_ON_EXIT", True)
ALERT_ON_REJECTION = env_bool("ALERT_ON_REJECTION", False)
ALERT_ON_DAILY_HALT = env_bool("ALERT_ON_DAILY_HALT", True)
ALERT_ON_READINESS_FAIL = env_bool("ALERT_ON_READINESS_FAIL", True)
ALERT_INCLUDE_DETAILS = env_bool("ALERT_INCLUDE_DETAILS", True)


# =============================
# Scanner (Phase 1C - shadow mode default)
# =============================
SCANNER_ENABLED = env_bool_any("SWING_SCANNER_ENABLED", "SCANNER_ENABLED", default="false")
SCANNER_DRY_RUN = env_bool_any("SCANNER_DRY_RUN", default="true")
SCANNER_ALLOW_LIVE = env_bool("SCANNER_ALLOW_LIVE", "false")  # hard gate: must be true to ever place scanner orders

# --- Trades-Today forcing (emergency mode) ---
TRADES_TODAY_ENABLE = env_bool("TRADES_TODAY_ENABLE", False)
TRADES_TODAY_TARGET_TRADES = int(getenv_any("TRADES_TODAY_TARGET_TRADES", default="1"))
TRADES_TODAY_SIGNAL = getenv_any("TRADES_TODAY_SIGNAL", default="trades_today_force")
TRADES_TODAY_PREFERRED_SYMBOLS = [s.strip().upper() for s in getenv_any("TRADES_TODAY_PREFERRED_SYMBOLS", default="SPY,QQQ,IWM,TQQQ").split(",") if s.strip()]
LAST_SCAN: dict = {}
LAST_SWING_CANDIDATES: list[dict] = []
LAST_REGIME_SNAPSHOT: dict = {}
SCAN_STATE_RESTORE: dict = {}
REGIME_STATE_RESTORE: dict = {}
REGIME_HISTORY: list[dict] = []
PAPER_LIFECYCLE_STATE_RESTORE: dict = {}
LAST_PAPER_LIFECYCLE: dict = {}
PAPER_LIFECYCLE_HISTORY: list[dict] = []
CANDIDATE_HISTORY_SIZE = int(os.getenv("CANDIDATE_HISTORY_SIZE", "100"))
CANDIDATE_HISTORY: list[dict] = []

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
SIGNAL_RANKING_ENABLED = env_bool("SIGNAL_RANKING_ENABLED", True)
SCANNER_RANK_MIN_SCORE = float(getenv_any("SCANNER_RANK_MIN_SCORE", default="115"))
SCANNER_FALLBACK_MIN_RANK_SCORE = float(getenv_any("SCANNER_FALLBACK_MIN_RANK_SCORE", default="125"))
SCANNER_FALLBACK_MIN_RAW_SCORE = float(getenv_any("SCANNER_FALLBACK_MIN_RAW_SCORE", default="120"))
SIGNAL_RANK_PRIMARY_BONUS = float(getenv_any("SIGNAL_RANK_PRIMARY_BONUS", default="8"))
SIGNAL_RANK_FALLBACK_PENALTY = float(getenv_any("SIGNAL_RANK_FALLBACK_PENALTY", default="12"))
SIGNAL_RANK_TOUCH_BONUS = float(getenv_any("SIGNAL_RANK_TOUCH_BONUS", default="4"))
SIGNAL_RANK_RELVOL_BONUS = float(getenv_any("SIGNAL_RANK_RELVOL_BONUS", default="3"))
SIGNAL_RANK_ATR_BONUS = float(getenv_any("SIGNAL_RANK_ATR_BONUS", default="3"))
SIGNAL_RANK_MICRO_BONUS = float(getenv_any("SIGNAL_RANK_MICRO_BONUS", default="2"))
SIGNAL_RANK_CONFIRM_BONUS = float(getenv_any("SIGNAL_RANK_CONFIRM_BONUS", default="2"))
SIGNAL_RANK_GREEN_BONUS = float(getenv_any("SIGNAL_RANK_GREEN_BONUS", default="2"))
SIGNAL_RANK_HIGHER_LOW_BONUS = float(getenv_any("SIGNAL_RANK_HIGHER_LOW_BONUS", default="2"))
SIGNAL_RANK_MAX_DIST_VWAP_PCT = float(getenv_any("SIGNAL_RANK_MAX_DIST_VWAP_PCT", default="0.90"))
SIGNAL_RANK_DISTANCE_PENALTY = float(getenv_any("SIGNAL_RANK_DISTANCE_PENALTY", default="10"))

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

# Patch 009: dynamic liquid universe selection
SCANNER_DYNAMIC_TOP_N = int(getenv_any("SCANNER_DYNAMIC_TOP_N", default=str(SCANNER_MAX_SYMBOLS_PER_CYCLE or 20)))
SCANNER_DYNAMIC_MIN_PRICE = float(getenv_any("SCANNER_DYNAMIC_MIN_PRICE", default="10"))
SCANNER_DYNAMIC_MIN_DOLLAR_VOL = float(getenv_any("SCANNER_DYNAMIC_MIN_DOLLAR_VOL", default="5000000"))
SCANNER_DYNAMIC_MIN_RELVOL = float(getenv_any("SCANNER_DYNAMIC_MIN_RELVOL", default="0.75"))
SCANNER_DYNAMIC_MIN_RANGE_PCT = float(getenv_any("SCANNER_DYNAMIC_MIN_RANGE_PCT", default="0.0035"))
SCANNER_DYNAMIC_KEEP_ANCHORS = env_bool("SCANNER_DYNAMIC_KEEP_ANCHORS", True)
SCANNER_ANCHOR_SYMBOLS = [s.strip().upper() for s in getenv_any("SCANNER_ANCHOR_SYMBOLS", default="SPY,QQQ,IWM,AAPL,MSFT,NVDA,AMD,AMZN,META").split(",") if s.strip()]
SCANNER_POOL_SYMBOLS = os.getenv("SCANNER_POOL_SYMBOLS", "").strip()
DEFAULT_DYNAMIC_POOL = [
    "SPY","QQQ","IWM","DIA","AAPL","MSFT","NVDA","AMD","AMZN","META","GOOGL","TSLA","AVGO",
    "NFLX","CRM","ORCL","ADBE","INTC","MU","QCOM","TSM","SHOP","PLTR","UBER","COIN",
    "BAC","JPM","GS","XLF","SMH","XLK","XLE","XLI","JNJ","UNH","PFE","WMT","COST","HD","LOW"
]

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
VWAP_PB_MIN_BARS_5M = int(os.getenv("VWAP_PB_MIN_BARS_5M", "15"))
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
VWAP_PB_FALLBACK_MIN_EMA_SLOPE = float(os.getenv("VWAP_PB_FALLBACK_MIN_EMA_SLOPE", "-0.0003"))
VWAP_PB_FALLBACK_MIN_VWAP_SLOPE = float(os.getenv("VWAP_PB_FALLBACK_MIN_VWAP_SLOPE", "-0.0008"))
VWAP_PB_FALLBACK_ALLOW_BOUNCE_SLACK = env_bool("VWAP_PB_FALLBACK_ALLOW_BOUNCE_SLACK", "true")
VWAP_PB_FALLBACK_BOUNCE_SLACK_PCT = float(os.getenv("VWAP_PB_FALLBACK_BOUNCE_SLACK_PCT", "0.0015"))
VWAP_PB_FALLBACK_MAX_DIST_VWAP_PCT = float(os.getenv("VWAP_PB_FALLBACK_MAX_DIST_VWAP_PCT", "0.0090"))
VWAP_PB_FALLBACK_ALLOW_TOUCHLESS = env_bool("VWAP_PB_FALLBACK_ALLOW_TOUCHLESS", "false")
VWAP_PB_SCORE_MIN = float(os.getenv("VWAP_PB_SCORE_MIN", "72"))
VWAP_PB_MIN_5M_ATR_PCT = float(os.getenv("VWAP_PB_MIN_5M_ATR_PCT", "0.0025"))
VWAP_PB_MAX_5M_ATR_PCT = float(os.getenv("VWAP_PB_MAX_5M_ATR_PCT", "0.0300"))
VWAP_PB_MIN_DAY_RANGE_PCT = float(os.getenv("VWAP_PB_MIN_DAY_RANGE_PCT", "0.0060"))
VWAP_PB_MIN_RECENT_1M_VOL_RATIO = float(os.getenv("VWAP_PB_MIN_RECENT_1M_VOL_RATIO", "1.05"))
VWAP_PB_REQUIRE_GREEN_LAST_1M = env_bool("VWAP_PB_REQUIRE_GREEN_LAST_1M", "true")
VWAP_PB_REQUIRE_HIGHER_LOW = env_bool("VWAP_PB_REQUIRE_HIGHER_LOW", "true")
VWAP_PB_ENTRY_CONFIRM_ABOVE_PRIOR_1M_HIGH = env_bool("VWAP_PB_ENTRY_CONFIRM_ABOVE_PRIOR_1M_HIGH", "true")
VWAP_PB_ENTRY_CONFIRM_BUFFER_PCT = float(os.getenv("VWAP_PB_ENTRY_CONFIRM_BUFFER_PCT", "0.0002"))
VWAP_PB_MICRO_CONFIRM_MODE = str(os.getenv("VWAP_PB_MICRO_CONFIRM_MODE", "soft2") or "soft2").strip().lower()
VWAP_PB_SOFT_CONFIRM_MIN_PASSES = int(os.getenv("VWAP_PB_SOFT_CONFIRM_MIN_PASSES", "2"))
SCANNER_LOOKBACK_DAYS = int(getenv_any("SCANNER_LOOKBACK_DAYS", default="3"))
SCANNER_REQUIRE_MARKET_HOURS = env_bool("SCANNER_REQUIRE_MARKET_HOURS", "true")
SCANNER_PRIMARY_STRATEGY = getenv_any("SCANNER_PRIMARY_STRATEGY", default=("daily_breakout" if getenv_any("STRATEGY_MODE", default="intraday").strip().lower() == "swing" else "vwap_pullback")).strip().lower()
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
ALERT_HISTORY: list[dict] = []
ALERT_DEDUP: dict[str, float] = {}
REJECTION_HISTORY: list[dict] = []
DAILY_HALT_STATE: dict = {"session": None, "active": False, "triggered_at": None, "reason": ""}
LAST_EXIT_HEARTBEAT: dict = {}
LAST_ALERT_HEARTBEAT: dict = {}
LAST_SCANNER_TELEMETRY: dict = {}
SCANNER_TELEMETRY_HISTORY: list[dict] = []
RELEASE_STATE: dict = {}



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

STARTUP_STATE: dict[str, object] = {
    "ran": False,
    "ts_utc": None,
    "ts_ny": None,
    "snapshot_found": False,
    "snapshot_path": None,
    "journal_path": None,
    "recovered_from_snapshot_count": 0,
    "recovered_from_broker_only_count": 0,
    "stale_snapshot_count": 0,
    "stale_snapshot_symbols": [],
    "reconcile_actions": [],
    "error": "",
}


# Scan rotation state (in-memory). Keeps a moving window through the universe so we can
# scan hundreds/thousands of symbols without hammering the provider each tick.
_scan_rotation = {"ny_date": None, "idx": 0}



# =============================
# Durable journal helpers
# =============================
def _ensure_parent_dir(path_str: str):
    try:
        Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _journal_should_persist(event: str, action: str = "") -> bool:
    if not JOURNAL_ENABLED:
        return False
    event_u = str(event or "").upper()
    action_s = str(action or "").lower()
    if event_u in {"ENTRY", "EXIT", "RECONCILE", "SYSTEM"}:
        return True
    if JOURNAL_PERSIST_SCANS and event_u == "SCAN":
        return True
    return action_s in {"error", "rejected"}


def _journal_append(record: dict):
    if not JOURNAL_ENABLED:
        return
    try:
        _ensure_parent_dir(JOURNAL_PATH)
        with open(JOURNAL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":"), default=str) + "\n")
    except Exception as e:
        logger.warning("JOURNAL_APPEND_FAILED err=%s", e)


def _read_journal(limit: int = 100, event: str = "", symbol: str = "") -> list[dict]:
    path = Path(JOURNAL_PATH)
    if (not JOURNAL_ENABLED) or (not path.exists()):
        return []
    lim = max(1, min(int(limit or 100), 5000))
    event = str(event or "").upper().strip()
    symbol = str(symbol or "").upper().strip()
    rows: list[dict] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if event and str(row.get("event", "")).upper() != event:
                    continue
                if symbol and str(row.get("symbol", "")).upper() != symbol:
                    continue
                rows.append(row)
        return rows[-lim:]
    except Exception as e:
        logger.warning("JOURNAL_READ_FAILED err=%s", e)
        return []



def _safe_json_write(path_str: str, payload: dict):
    try:
        _ensure_parent_dir(path_str)
        path = Path(path_str).expanduser().resolve()
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(path)
        return True
    except Exception as e:
        logger.warning("SAFE_JSON_WRITE_FAILED path=%s err=%s", path_str, e)
        return False


def _safe_json_read(path_str: str) -> dict:
    try:
        path = Path(path_str).expanduser().resolve()
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("SAFE_JSON_READ_FAILED path=%s err=%s", path_str, e)
        return {}


def persist_scan_runtime_state(reason: str = ""):
    payload = {
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "last_scan": dict(LAST_SCAN or {}),
        "scan_history": list(SCAN_HISTORY or []),
    }
    return _safe_json_write(SCAN_STATE_PATH, payload)


def restore_scan_runtime_state() -> dict:
    payload = _safe_json_read(SCAN_STATE_PATH)
    restored = {"path": SCAN_STATE_PATH, "loaded": False, "last_scan_restored": False, "scan_history_restored": 0}
    if not payload:
        return restored
    try:
        last_scan = payload.get("last_scan") or {}
        scan_history = payload.get("scan_history") or []
        if isinstance(last_scan, dict) and last_scan:
            LAST_SCAN.clear()
            LAST_SCAN.update(last_scan)
            restored["last_scan_restored"] = True
        if isinstance(scan_history, list) and scan_history:
            SCAN_HISTORY.clear()
            SCAN_HISTORY.extend(scan_history[-SCAN_HISTORY_SIZE:])
            restored["scan_history_restored"] = len(SCAN_HISTORY)
        restored["loaded"] = restored["last_scan_restored"] or bool(restored["scan_history_restored"])
    except Exception as e:
        restored["error"] = str(e)
    globals()["SCAN_STATE_RESTORE"] = restored
    return restored


def persist_regime_runtime_state(reason: str = ""):
    payload = {
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "current": dict(LAST_REGIME_SNAPSHOT or {}),
        "history": list(REGIME_HISTORY or []),
    }
    return _safe_json_write(REGIME_STATE_PATH, payload)


def restore_regime_runtime_state() -> dict:
    payload = _safe_json_read(REGIME_STATE_PATH)
    restored = {"path": REGIME_STATE_PATH, "loaded": False, "current_restored": False, "history_restored": 0}
    if not payload:
        return restored
    try:
        current = payload.get("current") or {}
        history = payload.get("history") or []
        if isinstance(current, dict) and current:
            LAST_REGIME_SNAPSHOT.clear()
            LAST_REGIME_SNAPSHOT.update(current)
            restored["current_restored"] = True
        if isinstance(history, list) and history:
            REGIME_HISTORY.clear()
            REGIME_HISTORY.extend(history[-SWING_REGIME_HISTORY_SIZE:])
            restored["history_restored"] = len(REGIME_HISTORY)
        restored["loaded"] = restored["current_restored"] or bool(restored["history_restored"])
    except Exception as e:
        restored["error"] = str(e)
    globals()["REGIME_STATE_RESTORE"] = restored
    return restored




def persist_scanner_telemetry_state(reason: str = ""):
    payload = {
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "last": dict(LAST_SCANNER_TELEMETRY or {}),
        "history": list(SCANNER_TELEMETRY_HISTORY or []),
    }
    return _safe_json_write(SCANNER_TELEMETRY_STATE_PATH, payload)


def restore_scanner_telemetry_state() -> dict:
    payload = _safe_json_read(SCANNER_TELEMETRY_STATE_PATH)
    restored = {"path": SCANNER_TELEMETRY_STATE_PATH, "loaded": False, "last_restored": False, "history_restored": 0}
    if not payload:
        return restored
    try:
        last = payload.get("last") or {}
        history = payload.get("history") or []
        if isinstance(last, dict) and last:
            restored_last = dict(last)
            restored_last["restored_from_state"] = True
            LAST_SCANNER_TELEMETRY.clear()
            LAST_SCANNER_TELEMETRY.update(restored_last)
            restored["last_restored"] = True
        if isinstance(history, list) and history:
            SCANNER_TELEMETRY_HISTORY.clear()
            SCANNER_TELEMETRY_HISTORY.extend(history[-SCANNER_TELEMETRY_HISTORY_LIMIT:])
            restored["history_restored"] = len(SCANNER_TELEMETRY_HISTORY)
        restored["loaded"] = restored["last_restored"] or bool(restored["history_restored"])
    except Exception as e:
        restored["error"] = str(e)
    globals()["SCANNER_TELEMETRY_STATE_RESTORE"] = restored
    return restored




def _safe_parse_iso_utc(ts):
    try:
        if not ts:
            return None
        dt = datetime.fromisoformat(str(ts))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _classify_scanner_warning_codes(state: dict) -> dict:
    state = dict(state or {})
    active = []
    recovered = []
    historical = []

    def _add_unique(seq, code):
        if code and code not in seq:
            seq.append(code)

    in_flight_run = bool(state.get("in_flight_run"))
    incomplete_runs_total = int(state.get("incomplete_runs_total") or 0)
    historical_incomplete_runs_total = int(state.get("historical_incomplete_runs_total") or 0)

    worker_status = str(state.get("worker_status") or "unknown").strip().lower()

    last_dispatch_failure_dt = _safe_parse_iso_utc(state.get("last_dispatch_failure_utc"))
    last_success_dt = _safe_parse_iso_utc(state.get("last_success_utc"))
    last_closed_dt = _safe_parse_iso_utc(state.get("last_closed_utc"))
    last_closed_status = str(state.get("last_closed_status") or "").strip().lower()

    manual_request_today = int(state.get("manual_request_today") or 0)
    external_request_today = int(state.get("external_request_today") or 0)

    if in_flight_run or incomplete_runs_total > 0:
        _add_unique(active, "partial_run_open")
    elif historical_incomplete_runs_total > 0:
        _add_unique(historical, "restored_partial_run_history")

    if manual_request_today > 0:
        _add_unique(active, "manual_scan_request_observed")
    if external_request_today > 0:
        _add_unique(active, "external_scan_request_observed")

    if worker_status != "up":
        if worker_status == "late":
            _add_unique(active, "worker_heartbeat_late")
        elif worker_status in {"stale", "down"}:
            _add_unique(active, "worker_heartbeat_stale")
        else:
            _add_unique(active, "worker_status_unknown")

    if last_dispatch_failure_dt is not None:
        dispatch_recovered = bool(
            last_success_dt
            and last_closed_dt
            and last_closed_status in {"success", "skipped"}
            and last_success_dt >= last_dispatch_failure_dt
            and last_closed_dt >= last_dispatch_failure_dt
        )
        if dispatch_recovered:
            _add_unique(recovered, "dispatch_failure_recovered")
        else:
            _add_unique(active, "dispatch_failure")

    return {
        "active_warning_codes": active,
        "recovered_warning_codes": recovered,
        "historical_warning_codes": historical,
        "warning_codes": list(active),
        "has_warnings": bool(active),
        "has_active_warnings": bool(active),
        "has_recovered_warnings": bool(recovered),
    }

def _scanner_telemetry_summary(history: list | None = None, today_prefix: str | None = None) -> dict:
    rows = list(history if history is not None else (SCANNER_TELEMETRY_HISTORY or []))
    tel = dict(LAST_SCANNER_TELEMETRY or {})
    if today_prefix is None:
        today_prefix = str(now_ny().date())

    def _is_today(row: dict) -> bool:
        return str((row or {}).get("ts_ny") or "").startswith(today_prefix)

    def _event_count(names: set[str], only_today: bool = False) -> int:
        total = 0
        for ev in rows:
            row = ev or {}
            if str(row.get("event") or "").strip().lower() not in names:
                continue
            if only_today and not _is_today(row):
                continue
            total += 1
        return total

    def _source_kind(row: dict) -> str:
        details = (row or {}).get("details") or {}
        kind = str(details.get("source_kind") or "").strip().lower()
        return kind or "unknown"

    def _dispatch_implies_skip(row: dict) -> bool:
        if str((row or {}).get("event") or "").strip().lower() != "scan_dispatch_ok":
            return False
        details = (row or {}).get("details") or {}
        body = str(details.get("body_prefix") or "").lower()
        return '"skipped":true' in body

    request_indexes = [idx for idx, row in enumerate(rows) if str((row or {}).get("event") or "").strip().lower() == "scan_request"]
    success_total = success_today = 0
    failure_total = failure_today = 0
    skipped_total = skipped_today = 0
    closed_runs_total = closed_runs_today = 0
    active_incomplete_total = active_incomplete_today = 0
    historical_incomplete_total = historical_incomplete_today = 0
    manual_request_total = manual_request_today = 0
    external_request_total = external_request_today = 0
    last_closed_event = None
    last_closed_status = None
    last_closed_utc = None
    last_request_source_kind = str(tel.get("last_request_source_kind") or "").strip().lower() or None
    explicit_close_events = {"scan_ok": "success", "scan_fail": "failure", "scan_error": "failure", "scan_skip": "skipped"}

    for pos, start_idx in enumerate(request_indexes):
        end_idx = request_indexes[pos + 1] if (pos + 1) < len(request_indexes) else len(rows)
        segment = rows[start_idx:end_idx]
        request_row = rows[start_idx] or {}
        segment_today = _is_today(request_row)
        req_kind = _source_kind(request_row)
        if req_kind == "manual":
            manual_request_total += 1
            if segment_today:
                manual_request_today += 1
        elif req_kind == "external":
            external_request_total += 1
            if segment_today:
                external_request_today += 1
        last_request_source_kind = req_kind or last_request_source_kind
        closed = None
        for row in segment[1:]:
            ev = str((row or {}).get("event") or "").strip().lower()
            if ev in explicit_close_events:
                closed = {
                    "event": ev,
                    "status": explicit_close_events[ev],
                    "ts_utc": (row or {}).get("ts_utc"),
                    "today": _is_today(row or {}),
                }
                break
        if closed is None:
            for row in segment[1:]:
                if _dispatch_implies_skip(row or {}):
                    closed = {
                        "event": "scan_skip",
                        "status": "skipped",
                        "ts_utc": (row or {}).get("ts_utc"),
                        "today": _is_today(row or {}),
                    }
                    break
        if closed is None:
            if pos == len(request_indexes) - 1:
                active_incomplete_total += 1
                if segment_today:
                    active_incomplete_today += 1
            else:
                historical_incomplete_total += 1
                if segment_today:
                    historical_incomplete_today += 1
            continue

        if closed["status"] == "success":
            success_total += 1
            if closed["today"]:
                success_today += 1
        elif closed["status"] == "failure":
            failure_total += 1
            if closed["today"]:
                failure_today += 1
        else:
            skipped_total += 1
            if closed["today"]:
                skipped_today += 1
        closed_runs_total += 1
        if closed["today"]:
            closed_runs_today += 1
        last_closed_event = closed["event"]
        last_closed_status = closed["status"]
        last_closed_utc = closed["ts_utc"]

    attempts_total = len(request_indexes)
    attempts_today = sum(1 for idx in request_indexes if _is_today(rows[idx] or {}))

    worker = _worker_status_snapshot()
    worker_status = str(worker.get("scanner_status") or ("up" if worker.get("scanner_running") else "unknown"))

    warning_state = _classify_scanner_warning_codes({
        "in_flight_run": active_incomplete_total > 0,
        "incomplete_runs_total": active_incomplete_total,
        "historical_incomplete_runs_total": historical_incomplete_total,
        "last_dispatch_failure_utc": tel.get("last_dispatch_failure_utc"),
        "last_success_utc": tel.get("last_success_utc"),
        "last_closed_utc": last_closed_utc,
        "last_closed_status": last_closed_status,
        "manual_request_today": manual_request_today,
        "external_request_today": external_request_today,
        "worker_status": worker_status,
    })

    return {
        "attempts_total": attempts_total,
        "success_total": success_total,
        "failure_total": failure_total,
        "skipped_total": skipped_total,
        "attempts_today": attempts_today,
        "success_today": success_today,
        "failure_today": failure_today,
        "skipped_today": skipped_today,
        "dispatch_attempts_total": _event_count({"scan_attempt"}),
        "dispatch_attempts_today": _event_count({"scan_attempt"}, only_today=True),
        "dispatch_failures_total": _event_count({"scan_dispatch_http_error", "scan_dispatch_error"}),
        "dispatch_failures_today": _event_count({"scan_dispatch_http_error", "scan_dispatch_error"}, only_today=True),
        "closed_runs_total": closed_runs_total,
        "closed_runs_today": closed_runs_today,
        "incomplete_runs_total": active_incomplete_total,
        "incomplete_runs_today": active_incomplete_today,
        "historical_incomplete_runs_total": historical_incomplete_total,
        "historical_incomplete_runs_today": historical_incomplete_today,
        "consecutive_failures": int(tel.get("consecutive_failures") or 0),
        "last_event": tel.get("event") or ((rows[-1] or {}).get("event") if rows else None),
        "last_event_status": tel.get("status") or ((rows[-1] or {}).get("status") if rows else None),
        "last_closed_event": last_closed_event,
        "last_closed_status": last_closed_status,
        "last_closed_utc": last_closed_utc,
        "last_request_source_kind": last_request_source_kind,
        "manual_request_total": manual_request_total,
        "manual_request_today": manual_request_today,
        "external_request_total": external_request_total,
        "external_request_today": external_request_today,
        "worker_status": worker.get("scanner_status"),
        "worker_age_sec": worker.get("scanner_age_sec"),
        "worker_last_event_utc": worker.get("scanner_last_event_utc"),
        "worker_last_event": worker.get("scanner_last_event"),
        "in_flight_run": active_incomplete_total > 0,
        "has_warnings": warning_state.get("has_warnings"),
        "warning_codes": list(warning_state.get("warning_codes") or []),
        "active_warning_codes": list(warning_state.get("active_warning_codes") or []),
        "recovered_warning_codes": list(warning_state.get("recovered_warning_codes") or []),
        "historical_warning_codes": list(warning_state.get("historical_warning_codes") or []),
        "has_active_warnings": warning_state.get("has_active_warnings"),
        "has_recovered_warnings": warning_state.get("has_recovered_warnings"),
        "history_count": len(rows),
    }


def _record_scanner_telemetry(event: str, status: str, details: dict | None = None):
    prev = dict(LAST_SCANNER_TELEMETRY or {})
    details = dict(details or {})
    now_utc = datetime.now(timezone.utc)
    now_utc_iso = now_utc.isoformat()
    now_ny_ts = now_ny().isoformat()
    today_ny = now_ny().date().isoformat()
    last_day = str(prev.get("today_ny") or "")

    def _day_value(key: str) -> int:
        value = int(prev.get(key) or 0)
        return value if last_day == today_ny else 0

    event_l = str(event or "").strip().lower()
    status_l = str(status or "").strip().lower()
    open_events = {"scan_request"}
    close_success_events = {"scan_ok"}
    close_failure_events = {"scan_fail", "scan_error"}
    close_skip_events = {"scan_skip"}
    dispatch_attempt_events = {"scan_attempt"}
    dispatch_failure_events = {"scan_dispatch_http_error", "scan_dispatch_error"}
    worker_keepalive_events = {"boot", "preflight_ok", "preflight_error", "sleep", "heartbeat", *dispatch_attempt_events, *dispatch_failure_events}
    source_kind = str(details.get("source_kind") or prev.get("last_request_source_kind") or "unknown").strip().lower() or "unknown"
    worker_source = bool(details.get("worker_source"))
    manual_source = source_kind == "manual"
    external_source = source_kind == "external"

    is_dispatch_attempt = event_l in dispatch_attempt_events
    is_dispatch_failure = event_l in dispatch_failure_events
    is_run_open = event_l in open_events
    is_run_success = event_l in close_success_events
    is_run_failure = event_l in close_failure_events
    is_run_skip = event_l in close_skip_events
    is_run_close = is_run_success or is_run_failure or is_run_skip
    is_worker_event = event_l in worker_keepalive_events or (worker_source and (is_run_open or is_run_close))

    dispatch_attempts_total = int(prev.get("dispatch_attempts_total") or 0) + (1 if is_dispatch_attempt else 0)
    dispatch_attempts_today = _day_value("dispatch_attempts_today") + (1 if is_dispatch_attempt else 0)
    dispatch_failures_total = int(prev.get("dispatch_failures_total") or 0) + (1 if is_dispatch_failure else 0)
    dispatch_failures_today = _day_value("dispatch_failures_today") + (1 if is_dispatch_failure else 0)

    attempts_total = int(prev.get("attempts_total") or 0) + (1 if is_run_open else 0)
    attempts_today = _day_value("attempts_today") + (1 if is_run_open else 0)
    success_total = int(prev.get("success_total") or 0) + (1 if is_run_success else 0)
    success_today = _day_value("success_today") + (1 if is_run_success else 0)
    failure_total = int(prev.get("failure_total") or 0) + (1 if is_run_failure else 0)
    failure_today = _day_value("failure_today") + (1 if is_run_failure else 0)
    skipped_total = int(prev.get("skipped_total") or 0) + (1 if is_run_skip else 0)
    skipped_today = _day_value("skipped_today") + (1 if is_run_skip else 0)
    closed_runs_total = success_total + failure_total + skipped_total
    closed_runs_today = success_today + failure_today + skipped_today
    incomplete_runs_total = max(0, attempts_total - closed_runs_total)
    incomplete_runs_today = max(0, attempts_today - closed_runs_today)

    last_error = prev.get("last_error")
    if is_dispatch_failure or is_run_failure:
        last_error = details.get("error") or prev.get("last_error")
    elif details.get("error") not in (None, ""):
        last_error = details.get("error")

    snapshot = {
        "ts_utc": now_utc_iso,
        "ts_ny": now_ny_ts,
        "today_ny": today_ny,
        "event": event,
        "status": status,
        "details": details,
        "boot_ts_utc": prev.get("boot_ts_utc") or (now_utc_iso if event_l == "boot" else None),
        "boot_ts_ny": prev.get("boot_ts_ny") or (now_ny_ts if event_l == "boot" else None),
        "restored_from_state": bool(prev.get("restored_from_state")),
        "last_event_utc": now_utc_iso,
        "last_event_ny": now_ny_ts,
        "last_worker_event_utc": now_utc_iso if is_worker_event else prev.get("last_worker_event_utc"),
        "last_worker_event_ny": now_ny_ts if is_worker_event else prev.get("last_worker_event_ny"),
        "last_worker_event": event if is_worker_event else prev.get("last_worker_event"),
        "last_dispatch_attempt_utc": now_utc_iso if is_dispatch_attempt else prev.get("last_dispatch_attempt_utc"),
        "last_dispatch_failure_utc": now_utc_iso if is_dispatch_failure else prev.get("last_dispatch_failure_utc"),
        "last_attempt_utc": now_utc_iso if is_run_open else prev.get("last_attempt_utc"),
        "last_success_utc": now_utc_iso if is_run_success else prev.get("last_success_utc"),
        "last_failure_utc": now_utc_iso if is_run_failure else prev.get("last_failure_utc"),
        "last_closed_utc": now_utc_iso if is_run_close else prev.get("last_closed_utc"),
        "last_closed_ny": now_ny_ts if is_run_close else prev.get("last_closed_ny"),
        "last_closed_event": event if is_run_close else prev.get("last_closed_event"),
        "last_closed_status": status if is_run_close else prev.get("last_closed_status"),
        "last_open_utc": now_utc_iso if is_run_open else prev.get("last_open_utc"),
        "last_open_ny": now_ny_ts if is_run_open else prev.get("last_open_ny"),
        "last_open_event": event if is_run_open else prev.get("last_open_event"),
        "last_open_status": status if is_run_open else prev.get("last_open_status"),
        "last_request_source_kind": source_kind if is_run_open else prev.get("last_request_source_kind"),
        "last_worker_source_kind": source_kind if (is_run_open and worker_source) else prev.get("last_worker_source_kind"),
        "manual_request_total": int(prev.get("manual_request_total") or 0) + (1 if (is_run_open and manual_source) else 0),
        "manual_request_today": _day_value("manual_request_today") + (1 if (is_run_open and manual_source) else 0),
        "external_request_total": int(prev.get("external_request_total") or 0) + (1 if (is_run_open and external_source) else 0),
        "external_request_today": _day_value("external_request_today") + (1 if (is_run_open and external_source) else 0),
        "dispatch_attempts_total": dispatch_attempts_total,
        "dispatch_attempts_today": dispatch_attempts_today,
        "dispatch_failures_total": dispatch_failures_total,
        "dispatch_failures_today": dispatch_failures_today,
        "attempts_total": attempts_total,
        "success_total": success_total,
        "failure_total": failure_total,
        "skipped_total": skipped_total,
        "attempts_today": attempts_today,
        "success_today": success_today,
        "failure_today": failure_today,
        "skipped_today": skipped_today,
        "closed_runs_total": closed_runs_total,
        "closed_runs_today": closed_runs_today,
        "incomplete_runs_total": incomplete_runs_total,
        "incomplete_runs_today": incomplete_runs_today,
        "in_flight_run": incomplete_runs_total > 0,
        "consecutive_failures": 0 if is_run_success else (int(prev.get("consecutive_failures") or 0) + (1 if is_run_failure else 0)),
        "current_sleep_sec": details.get("sleep_sec", prev.get("current_sleep_sec")),
        "next_run_estimate_utc": details.get("next_run_estimate_utc", prev.get("next_run_estimate_utc")),
        "last_http_status": details.get("status", prev.get("last_http_status")),
        "last_error": last_error,
        "worker_pid": details.get("pid", prev.get("worker_pid")),
        "interval_sec": details.get("interval_sec", prev.get("interval_sec")),
        "timeout_sec": details.get("timeout_sec", prev.get("timeout_sec")),
        "run_on_start": details.get("run_on_start", prev.get("run_on_start")),
        "jitter_sec": details.get("jitter_sec", prev.get("jitter_sec")),
    }
    warning_state = _classify_scanner_warning_codes(snapshot)
    snapshot["warning_codes"] = list(warning_state.get("warning_codes") or [])
    snapshot["active_warning_codes"] = list(warning_state.get("active_warning_codes") or [])
    snapshot["recovered_warning_codes"] = list(warning_state.get("recovered_warning_codes") or [])
    snapshot["historical_warning_codes"] = list(warning_state.get("historical_warning_codes") or [])
    snapshot["has_warnings"] = warning_state.get("has_warnings")
    snapshot["has_active_warnings"] = warning_state.get("has_active_warnings")
    snapshot["has_recovered_warnings"] = warning_state.get("has_recovered_warnings")
    LAST_SCANNER_TELEMETRY.clear()
    LAST_SCANNER_TELEMETRY.update(snapshot)
    history_event = {
        "ts_utc": now_utc_iso,
        "ts_ny": now_ny_ts,
        "event": event,
        "status": status,
        "details": details,
    }
    SCANNER_TELEMETRY_HISTORY.append(history_event)
    if len(SCANNER_TELEMETRY_HISTORY) > SCANNER_TELEMETRY_HISTORY_LIMIT:
        del SCANNER_TELEMETRY_HISTORY[: len(SCANNER_TELEMETRY_HISTORY) - SCANNER_TELEMETRY_HISTORY_LIMIT]
    try:
        persist_scanner_telemetry_state(reason=f"{event}:{status}")
    except Exception:
        pass
    return snapshot




EXECUTION_LIFECYCLE_ACTIVE_STATES = {"planned", "submitted", "acknowledged", "partially_filled", "filled", "close_submitted"}
EXECUTION_LIFECYCLE_TERMINAL_STATES = {"canceled", "rejected", "closed", "error"}
EXECUTION_LIFECYCLE_ALLOWED_TRANSITIONS = {
    "": {"planned", "submitted", "filled", "closed", "rejected", "error"},
    "idle": {"planned", "submitted", "filled", "closed", "rejected", "error"},
    "planned": {"submitted", "canceled", "rejected", "error", "filled"},
    "submitted": {"acknowledged", "partially_filled", "filled", "canceled", "rejected", "error", "close_submitted"},
    "acknowledged": {"partially_filled", "filled", "canceled", "rejected", "error", "close_submitted"},
    "partially_filled": {"filled", "canceled", "rejected", "error", "close_submitted"},
    "filled": {"close_submitted", "closed", "error"},
    "close_submitted": {"closed", "partially_filled", "filled", "canceled", "rejected", "error"},
    "canceled": set(),
    "rejected": set(),
    "closed": set(),
    "error": set(),
}

def _execution_lifecycle_history(plan: dict) -> list:
    hist = plan.get("execution_lifecycle_history")
    if not isinstance(hist, list):
        hist = []
        plan["execution_lifecycle_history"] = hist
    return hist

def _append_execution_lifecycle_issue(plan: dict, code: str, severity: str = "error", details: dict | None = None):
    issues = plan.get("execution_lifecycle_issues")
    if not isinstance(issues, list):
        issues = []
        plan["execution_lifecycle_issues"] = issues
    issue = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "code": str(code or ""),
        "severity": str(severity or "error"),
        "details": dict(details or {}),
    }
    issues.append(issue)
    if len(issues) > EXECUTION_LIFECYCLE_HISTORY_LIMIT:
        del issues[: len(issues) - EXECUTION_LIFECYCLE_HISTORY_LIMIT]
    return issue

def _transition_execution_lifecycle(plan: dict, symbol: str, new_state: str, reason: str = "", details: dict | None = None, allow_illegal: bool = False) -> dict:
    symbol = str(symbol or plan.get("symbol") or "").upper()
    new_state = str(new_state or "").strip().lower()
    prior_state = str(plan.get("execution_state") or plan.get("lifecycle_state") or "").strip().lower()
    allowed = set(EXECUTION_LIFECYCLE_ALLOWED_TRANSITIONS.get(prior_state, set()))
    illegal = bool(prior_state != new_state and prior_state and new_state and new_state not in allowed and not allow_illegal)
    if illegal:
        _append_execution_lifecycle_issue(plan, "illegal_execution_transition", details={"symbol": symbol, "from_state": prior_state, "to_state": new_state, "reason": reason, **dict(details or {})})
        return {"ok": False, "symbol": symbol, "from_state": prior_state, "to_state": new_state, "illegal": True}
    ts_utc = datetime.now(timezone.utc).isoformat()
    ts_ny = now_ny().isoformat()
    event = {
        "ts_utc": ts_utc,
        "ts_ny": ts_ny,
        "symbol": symbol or None,
        "from_state": prior_state or None,
        "to_state": new_state,
        "reason": str(reason or ""),
        "details": dict(details or {}),
    }
    plan["execution_state"] = new_state
    plan["lifecycle_state"] = new_state
    plan["execution_state_reason"] = str(reason or "")
    plan["execution_updated_utc"] = ts_utc
    plan["execution_updated_ny"] = ts_ny
    hist = _execution_lifecycle_history(plan)
    hist.append(event)
    if len(hist) > EXECUTION_LIFECYCLE_HISTORY_LIMIT:
        del hist[: len(hist) - EXECUTION_LIFECYCLE_HISTORY_LIMIT]
    return {"ok": True, **event}

def _ensure_execution_lifecycle_plan(symbol: str, plan: dict) -> dict:
    if not isinstance(plan, dict):
        return {}
    symbol = str(symbol or plan.get("symbol") or "").upper()
    plan.setdefault("symbol", symbol or None)
    if plan.get("execution_state"):
        plan.setdefault("lifecycle_state", plan.get("execution_state"))
        _execution_lifecycle_history(plan)
        return plan
    initial_state = "planned" if bool(plan.get("filled_qty") or 0) and bool(plan.get("avg_fill_price") or 0) and not plan.get("order_id") else "submitted" if plan.get("order_id") else "idle"
    _transition_execution_lifecycle(plan, symbol, initial_state, reason="initialize_plan", details={"backfilled": True}, allow_illegal=True)
    return plan

def _canonical_order_state(status: str) -> str | None:
    s = str(status or "").strip().lower()
    if not s:
        return None
    if s in {"new", "accepted", "pending_new", "accepted_for_bidding", "held", "pending_replace"}:
        return "acknowledged"
    if s == "partially_filled":
        return "partially_filled"
    if s == "filled":
        return "filled"
    if s in {"canceled", "cancelled", "expired"}:
        return "canceled"
    if s in {"rejected", "suspended"}:
        return "rejected"
    return None

def _derive_execution_lifecycle_state(symbol: str, plan: dict | None, broker_order: dict | None = None, broker_position_qty: float | None = None) -> dict:
    symbol = str(symbol or (plan or {}).get("symbol") or "").upper()
    plan = dict(plan or {})
    current = str(plan.get("execution_state") or plan.get("lifecycle_state") or "").strip().lower()
    issues: list[dict] = []
    order_state = _canonical_order_state((broker_order or {}).get("status"))
    order_status_lc = str((broker_order or {}).get("status") or "").strip().lower()
    position_qty = None
    try:
        position_qty = abs(float(broker_position_qty)) if broker_position_qty is not None else None
    except Exception:
        position_qty = None
    active = bool(plan.get("active"))
    if position_qty and position_qty > 0:
        derived = "filled"
    elif order_state in {"acknowledged", "partially_filled", "filled", "canceled", "rejected"}:
        derived = order_state
    elif current:
        derived = current
    elif active:
        derived = "submitted" if plan.get("order_id") else "planned"
    else:
        derived = "closed"
    if current and derived != current:
        allowed = set(EXECUTION_LIFECYCLE_ALLOWED_TRANSITIONS.get(current, set()))
        if derived not in allowed and current != derived:
            issues.append({"code": "illegal_execution_transition", "severity": "error", "symbol": symbol or None, "details": {"from_state": current, "to_state": derived, "order_status": order_status_lc}})
    if current in EXECUTION_LIFECYCLE_ACTIVE_STATES and not active and not (position_qty and position_qty > 0) and order_state not in {"canceled", "rejected"}:
        issues.append({"code": "inactive_plan_with_active_execution_state", "severity": "warn", "symbol": symbol or None, "details": {"current_state": current, "order_status": order_status_lc}})
    if current in {"submitted", "acknowledged", "partially_filled"} and not plan.get("order_id"):
        issues.append({"code": "execution_state_missing_order_id", "severity": "error", "symbol": symbol or None, "details": {"current_state": current}})
    if current == "filled" and not (position_qty and position_qty > 0) and active:
        issues.append({"code": "filled_state_without_broker_position", "severity": "warn", "symbol": symbol or None, "details": {"order_status": order_status_lc}})
    return {"symbol": symbol or None, "current_state": current or None, "derived_state": derived, "order_state": order_state, "order_status": order_status_lc or None, "position_qty": position_qty, "issues": issues}

def _apply_execution_lifecycle_reconcile(symbol: str, plan: dict, broker_order: dict | None = None, broker_position_qty: float | None = None) -> dict:
    if not isinstance(plan, dict):
        return {"symbol": str(symbol or "").upper(), "changed": False, "issues": []}
    _ensure_execution_lifecycle_plan(symbol, plan)
    derived = _derive_execution_lifecycle_state(symbol, plan, broker_order=broker_order, broker_position_qty=broker_position_qty)
    for issue in list(derived.get("issues") or []):
        _append_execution_lifecycle_issue(plan, str(issue.get("code") or "execution_issue"), severity=str(issue.get("severity") or "error"), details=dict(issue.get("details") or {}))
    current = str(plan.get("execution_state") or "").strip().lower()
    target = str(derived.get("derived_state") or current).strip().lower()
    changed = False
    if target and target != current:
        if not derived.get("issues") or not any(str((i or {}).get("code") or "") == "illegal_execution_transition" for i in derived.get("issues") or []):
            _transition_execution_lifecycle(plan, str(symbol or "").upper(), target, reason="reconcile", details={"order_status": derived.get("order_status"), "position_qty": derived.get("position_qty")})
            changed = True
    return {"symbol": str(symbol or "").upper(), "changed": changed, "current_state": plan.get("execution_state"), "derived_state": target, "issues": list(derived.get("issues") or [])}

def execution_lifecycle_snapshot(limit: int = 100) -> dict:
    _ensure_runtime_state_loaded()
    lim = max(1, min(int(limit or 100), 500))
    symbols = sorted({str(sym or "").upper() for sym in (TRADE_PLAN or {}).keys() if str(sym or "").upper()})
    rows = []
    issue_counts = {"error": 0, "warn": 0}
    for sym in symbols[:lim]:
        plan = TRADE_PLAN.get(sym) or {}
        _ensure_execution_lifecycle_plan(sym, plan)
        qty_signed, _pos_side = get_position(sym)
        broker_order = get_order_status(str(plan.get("order_id") or "")) if str(plan.get("order_id") or "").strip() else {}
        derived = _derive_execution_lifecycle_state(sym, plan, broker_order=broker_order, broker_position_qty=qty_signed)
        issues = list(derived.get("issues") or [])
        persisted_issues = list(plan.get("execution_lifecycle_issues") or [])
        for issue in issues:
            sev = str(issue.get("severity") or "warn").lower()
            issue_counts["error" if sev == "error" else "warn"] = issue_counts.get("error" if sev == "error" else "warn", 0) + 1
        row = {
            "symbol": sym,
            "active": bool(plan.get("active")),
            "execution_state": plan.get("execution_state") or None,
            "derived_state": derived.get("derived_state"),
            "order_status": derived.get("order_status"),
            "order_id": str(plan.get("order_id") or "") or None,
            "position_qty": derived.get("position_qty"),
            "submitted_at": plan.get("submitted_at") or plan.get("opened_at") or None,
            "execution_updated_utc": plan.get("execution_updated_utc") or None,
            "history_tail": list((_execution_lifecycle_history(plan) or [])[-5:]),
            "issues": issues,
            "persisted_issue_count": len(persisted_issues),
        }
        rows.append(row)
    return {
        "ok": True,
        "execution_lifecycle_history_limit": EXECUTION_LIFECYCLE_HISTORY_LIMIT,
        "plan_count": len(rows),
        "issue_counts": issue_counts,
        "items": rows,
    }

def persist_paper_lifecycle_state(reason: str = ""):
    payload = {
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "last_event": dict(LAST_PAPER_LIFECYCLE or {}),
        "history": list(PAPER_LIFECYCLE_HISTORY or []),
    }
    return _safe_json_write(PAPER_LIFECYCLE_STATE_PATH, payload)


def restore_paper_lifecycle_state() -> dict:
    payload = _safe_json_read(PAPER_LIFECYCLE_STATE_PATH)
    restored = {"path": PAPER_LIFECYCLE_STATE_PATH, "loaded": False, "last_event_restored": False, "history_restored": 0}
    if not payload:
        return restored
    try:
        last_event = payload.get("last_event") or {}
        history = payload.get("history") or []
        if isinstance(last_event, dict) and last_event:
            LAST_PAPER_LIFECYCLE.clear()
            LAST_PAPER_LIFECYCLE.update(last_event)
            restored["last_event_restored"] = True
        if isinstance(history, list) and history:
            PAPER_LIFECYCLE_HISTORY.clear()
            PAPER_LIFECYCLE_HISTORY.extend(history[-PAPER_LIFECYCLE_HISTORY_LIMIT:])
            restored["history_restored"] = len(PAPER_LIFECYCLE_HISTORY)
        restored["loaded"] = restored["last_event_restored"] or bool(restored["history_restored"])
    except Exception as e:
        restored["error"] = str(e)
    globals()["PAPER_LIFECYCLE_STATE_RESTORE"] = restored
    return restored


def _record_paper_lifecycle(stage: str, status: str, symbol: str | None = None, details: dict | None = None):
    event = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "ts_ny": now_ny().isoformat(),
        "stage": str(stage or ""),
        "status": str(status or ""),
        "symbol": (symbol or None),
        "details": dict(details or {}),
    }
    LAST_PAPER_LIFECYCLE.clear()
    LAST_PAPER_LIFECYCLE.update(event)
    PAPER_LIFECYCLE_HISTORY.append(dict(event))
    if len(PAPER_LIFECYCLE_HISTORY) > PAPER_LIFECYCLE_HISTORY_LIMIT:
        del PAPER_LIFECYCLE_HISTORY[: len(PAPER_LIFECYCLE_HISTORY) - PAPER_LIFECYCLE_HISTORY_LIMIT]
    try:
        persist_paper_lifecycle_state(reason=f"{stage}:{status}")
    except Exception:
        pass
    return event


def _authoritative_runtime_state_snapshot() -> dict:
    snap = build_reconcile_snapshot()
    active_plan_symbols = sorted([str(sym or '').upper() for sym, plan in (TRADE_PLAN or {}).items() if isinstance(plan, dict) and bool(plan.get('active'))])
    pending_entry_plan_symbols = sorted([str(sym or '').upper() for sym in (snap.get('pending_entry_plan_symbols') or [])])
    broker_symbols = sorted([str(sym or '').upper() for sym in (snap.get('broker_symbols') or [])])
    open_order_symbols = sorted([str(sym or '').upper() for sym in (snap.get('open_order_symbols') or [])])
    position_like_symbols = sorted(set(broker_symbols) | set(active_plan_symbols))
    entry_pending_symbols = sorted(set(pending_entry_plan_symbols) | set(open_order_symbols))
    return {
        'active_plan_symbols': active_plan_symbols,
        'pending_entry_plan_symbols': pending_entry_plan_symbols,
        'broker_symbols': broker_symbols,
        'open_order_symbols': open_order_symbols,
        'position_like_symbols': position_like_symbols,
        'entry_pending_symbols': entry_pending_symbols,
        'reconcile': snap,
        'idle': not bool(position_like_symbols or entry_pending_symbols),
    }


def _paper_lifecycle_integrity_issues(last_event: dict | None = None, state: dict | None = None) -> list[dict]:
    event = dict(last_event or LAST_PAPER_LIFECYCLE or {})
    runtime = dict(state or _authoritative_runtime_state_snapshot())
    issues: list[dict] = []
    if not event:
        return issues

    stage = str(event.get('stage') or '').lower()
    status = str(event.get('status') or '').lower()
    symbol = str(event.get('symbol') or '').upper()
    pending_symbols = set(runtime.get('entry_pending_symbols') or [])
    position_symbols = set(runtime.get('position_like_symbols') or [])
    active_plan_symbols = set(runtime.get('active_plan_symbols') or [])
    broker_symbols = set(runtime.get('broker_symbols') or [])
    open_order_symbols = set(runtime.get('open_order_symbols') or [])

    if stage == 'entry' and status in {'planned', 'submitted', 'dry_run'}:
        symbol_ok = (symbol in pending_symbols) or (symbol in position_symbols) if symbol else bool(pending_symbols or position_symbols)
        if not symbol_ok:
            issues.append({
                'code': 'stale_entry_lifecycle_without_authoritative_state',
                'severity': 'error',
                'symbol': symbol or None,
                'details': {
                    'stage': stage,
                    'status': status,
                    'pending_entry_plan_symbols': sorted(pending_symbols),
                    'position_like_symbols': sorted(position_symbols),
                },
            })

    if stage == 'exit' and status in {'submitted', 'dry_run'}:
        symbol_open = (symbol in broker_symbols) or (symbol in active_plan_symbols) or (symbol in open_order_symbols) if symbol else bool(broker_symbols or active_plan_symbols or open_order_symbols)
        if not symbol_open:
            issues.append({
                'code': 'stale_exit_lifecycle_without_authoritative_state',
                'severity': 'warn',
                'symbol': symbol or None,
                'details': {
                    'stage': stage,
                    'status': status,
                    'broker_symbols': sorted(broker_symbols),
                    'active_plan_symbols': sorted(active_plan_symbols),
                    'open_order_symbols': sorted(open_order_symbols),
                },
            })
    if symbol:
        plan = (TRADE_PLAN or {}).get(symbol) or {}
        qty_signed, _pos_side = get_position(symbol)
        broker_order = get_order_status(str(plan.get('order_id') or '')) if str(plan.get('order_id') or '').strip() else {}
        exec_state = _derive_execution_lifecycle_state(symbol, plan, broker_order=broker_order, broker_position_qty=qty_signed)
        for item in list(exec_state.get('issues') or []):
            issues.append(item)
    return issues


def normalize_paper_lifecycle_current_state(reason: str = 'runtime_check') -> dict:
    runtime = _authoritative_runtime_state_snapshot()
    issues = _paper_lifecycle_integrity_issues(LAST_PAPER_LIFECYCLE, runtime)
    normalized = {'checked': True, 'mutated': False, 'issues': issues}
    if not issues:
        return normalized

    codes = {str(item.get('code') or '') for item in issues}
    if 'stale_entry_lifecycle_without_authoritative_state' in codes or 'stale_exit_lifecycle_without_authoritative_state' in codes:
        last_event = dict(LAST_PAPER_LIFECYCLE or {})
        event = _record_paper_lifecycle(
            'state',
            'idle',
            symbol=None,
            details={
                'normalized': True,
                'normalization_reason': reason,
                'previous_event': {
                    'ts_utc': last_event.get('ts_utc'),
                    'stage': last_event.get('stage'),
                    'status': last_event.get('status'),
                    'symbol': last_event.get('symbol'),
                },
                'issues': issues,
                'active_plan_count': int((runtime.get('reconcile') or {}).get('active_plan_count') or 0),
                'open_order_count': int((runtime.get('reconcile') or {}).get('open_order_count') or 0),
                'broker_positions_count': int((runtime.get('reconcile') or {}).get('broker_positions_count') or 0),
            },
        )
        normalized['mutated'] = True
        normalized['event'] = event
    return normalized


def _continuity_display_lifecycle_event(event: dict | None = None, issues: list | None = None) -> dict:
    row = dict(event or {})
    details = dict(row.get('details') or {})
    historical_issue_codes = [str((item or {}).get('code') or '') for item in (details.get('issues') or []) if str((item or {}).get('code') or '')]
    if 'issues' in details:
        details.pop('issues', None)
    if historical_issue_codes:
        details['historical_issue_codes'] = historical_issue_codes
    row['details'] = details
    row['authoritative_issue_count'] = len(list(issues or []))
    row['authoritative_ok'] = len(list(issues or [])) == 0
    return row


def continuity_snapshot(normalize_current: bool = False) -> dict:
    _ensure_runtime_state_loaded()
    normalization = normalize_paper_lifecycle_current_state(reason='continuity_snapshot') if normalize_current else {'checked': False, 'mutated': False, 'issues': []}
    runtime = _authoritative_runtime_state_snapshot()
    last_event = dict(LAST_PAPER_LIFECYCLE or {})
    issues = list(_paper_lifecycle_integrity_issues(last_event, runtime))

    reconcile = runtime.get('reconcile') or {}
    for sym in sorted(set(reconcile.get('plans_missing_open_order') or [])):
        issues.append({'code': 'pending_plan_missing_open_order', 'severity': 'error', 'symbol': sym})
    for sym in sorted(set(reconcile.get('stale_active_plans') or [])):
        issues.append({'code': 'stale_active_plan', 'severity': 'error', 'symbol': sym})
    for sym in sorted(set(reconcile.get('partial_fill_plan_symbols') or [])):
        issues.append({'code': 'partial_fill_plan_aging', 'severity': 'warn', 'symbol': sym})
    for sym in sorted(set(reconcile.get('orphan_open_order_symbols') or [])):
        issues.append({'code': 'orphan_open_order', 'severity': 'error', 'symbol': sym})

    issue_codes = [str(item.get('code') or '') for item in issues]
    display_event = _continuity_display_lifecycle_event(last_event, issues)
    return {
        'session': _session_boundary_snapshot(),
        'startup_state': dict(globals().get('STARTUP_STATE') or {}),
        'current_lifecycle_event': display_event,
        'raw_current_lifecycle_event': last_event,
        'normalization': normalization,
        'authoritative_state': {
            'idle': bool(runtime.get('idle')),
            'active_plan_symbols': list(runtime.get('active_plan_symbols') or []),
            'pending_entry_plan_symbols': list(runtime.get('pending_entry_plan_symbols') or []),
            'broker_symbols': list(runtime.get('broker_symbols') or []),
            'open_order_symbols': list(runtime.get('open_order_symbols') or []),
            'position_like_symbols': list(runtime.get('position_like_symbols') or []),
            'entry_pending_symbols': list(runtime.get('entry_pending_symbols') or []),
        },
        'reconcile_snapshot': {
            'open_order_count': int(reconcile.get('open_order_count') or 0),
            'active_plan_count': int(reconcile.get('active_plan_count') or 0),
            'broker_positions_count': int(reconcile.get('broker_positions_count') or 0),
            'plans_missing_open_order': list(reconcile.get('plans_missing_open_order') or []),
            'stale_active_plans': list(reconcile.get('stale_active_plans') or []),
            'partial_fill_plan_symbols': list(reconcile.get('partial_fill_plan_symbols') or []),
            'orphan_open_order_symbols': list(reconcile.get('orphan_open_order_symbols') or []),
            'pending_entry_plan_symbols': list(reconcile.get('pending_entry_plan_symbols') or []),
        },
        'issues': issues,
        'issue_codes': issue_codes,
        'ok': len(issues) == 0,
    }


def _ensure_runtime_state_loaded():
    try:
        if (not LAST_SCAN) and (not SCAN_HISTORY):
            restore_scan_runtime_state()
    except Exception:
        pass
    try:
        if (not LAST_REGIME_SNAPSHOT) and (not REGIME_HISTORY):
            restore_regime_runtime_state()
    except Exception:
        pass
    try:
        if (not LAST_PAPER_LIFECYCLE) and (not PAPER_LIFECYCLE_HISTORY):
            restore_paper_lifecycle_state()
    except Exception:
        pass
    try:
        if (not LAST_SCANNER_TELEMETRY) and (not SCANNER_TELEMETRY_HISTORY):
            restore_scanner_telemetry_state()
    except Exception:
        pass
    try:
        _backfill_runtime_state_views()
    except Exception:
        pass
    try:
        normalize_paper_lifecycle_current_state(reason="ensure_runtime_state_loaded")
    except Exception:
        pass


def _backfill_runtime_state_views():
    changed_scan = False
    if (not SCAN_HISTORY) and isinstance(LAST_SCAN, dict) and LAST_SCAN.get("ts_utc"):
        SCAN_HISTORY.append({
            "ts_utc": LAST_SCAN.get("ts_utc"),
            "universe_provider": LAST_SCAN.get("universe_provider") or SCANNER_UNIVERSE_PROVIDER,
            "symbols": list(((LAST_SCAN.get("summary") or {}).get("symbols") or [])),
            "scanned": int(LAST_SCAN.get("scanned") or 0),
            "signals": int(LAST_SCAN.get("signals") or 0),
            "would_trade": int(LAST_SCAN.get("would_trade") or 0),
            "blocked": int(LAST_SCAN.get("blocked") or 0),
            "duration_ms": int(LAST_SCAN.get("duration_ms") or 0),
            "summary": dict(LAST_SCAN.get("summary") or {}),
            "results": list(((LAST_SCAN.get("summary") or {}).get("top_candidates") or [])),
            "candidate_slots": int(candidate_slots_available()) if callable(candidate_slots_available) else 0,
            "ignored_ranked_out": [],
            "would_submit": [],
            "backfilled": True,
        })
        changed_scan = True
    if changed_scan:
        try:
            persist_scan_runtime_state(reason="backfill_runtime_state_views")
        except Exception:
            pass

    changed_paper = False
    if (not LAST_PAPER_LIFECYCLE) and (not PAPER_LIFECYCLE_HISTORY) and isinstance(LAST_SCAN, dict) and LAST_SCAN.get("ts_utc"):
        summary = dict(LAST_SCAN.get("summary") or {})
        status = "completed"
        if LAST_SCAN.get("skipped"):
            status = "skipped"
        elif str(LAST_SCAN.get("reason") or "").endswith("exception"):
            status = "failed"
        event = {
            "ts_utc": LAST_SCAN.get("ts_utc"),
            "ts_ny": LAST_SCAN.get("ts_ny") or now_ny().isoformat(),
            "stage": "scan",
            "status": status,
            "symbol": None,
            "details": {
                "scan_reason": summary.get("scan_reason") or LAST_SCAN.get("reason"),
                "candidates_total": int(summary.get("candidates_total") or 0),
                "eligible_total": int(summary.get("eligible_total") or 0),
                "selected_total": int(summary.get("selected_total") or 0),
                "global_block_reasons": list(summary.get("global_block_reasons") or []),
                "regime_favorable": summary.get("regime", {}).get("favorable") if isinstance(summary.get("regime"), dict) else None,
                "regime_data_complete": summary.get("regime", {}).get("data_complete") if isinstance(summary.get("regime"), dict) else None,
                "backfilled": True,
            },
        }
        LAST_PAPER_LIFECYCLE.clear()
        LAST_PAPER_LIFECYCLE.update(event)
        PAPER_LIFECYCLE_HISTORY.append(dict(event))
        changed_paper = True
    if changed_paper:
        try:
            persist_paper_lifecycle_state(reason="backfill_runtime_state_views")
        except Exception:
            pass


def _refresh_regime_snapshot_if_needed(force: bool = False) -> dict:
    current = dict(LAST_REGIME_SNAPSHOT or {})
    should_refresh = bool(force or (not current) or (current.get("data_complete") is False))
    if not should_refresh:
        return current
    try:
        syms = universe_symbols()
        syms_for_fetch = list(syms)
        if SWING_INDEX_SYMBOL and SWING_INDEX_SYMBOL not in syms_for_fetch:
            syms_for_fetch.append(SWING_INDEX_SYMBOL)
        lookback_days = max(
            int(SCANNER_LOOKBACK_DAYS or 20) + 40,
            SWING_REGIME_SLOW_MA_DAYS + REGIME_BREADTH_RETURN_LOOKBACK_DAYS + 30,
            SWING_SLOW_MA_DAYS + SWING_BREAKOUT_LOOKBACK_DAYS + 20,
        )
        daily_map = fetch_daily_bars_multi(syms_for_fetch, lookback_days=lookback_days)
        refreshed = _build_swing_regime(daily_map.get(SWING_INDEX_SYMBOL, []), daily_map, syms)
        if isinstance(refreshed, dict) and refreshed:
            LAST_REGIME_SNAPSHOT.clear()
            LAST_REGIME_SNAPSHOT.update(refreshed)
            REGIME_HISTORY.append(dict(refreshed))
            if len(REGIME_HISTORY) > SWING_REGIME_HISTORY_SIZE:
                del REGIME_HISTORY[: len(REGIME_HISTORY) - SWING_REGIME_HISTORY_SIZE]
            persist_regime_runtime_state(reason="refresh_regime_snapshot_if_needed")
            return dict(refreshed)
    except Exception as e:
        logger.warning("REGIME_REFRESH_FAILED err=%s", e)
    return dict(LAST_REGIME_SNAPSHOT or {})


def persist_positions_snapshot(reason: str = "", extra: dict | None = None) -> dict:
    snapshot = {
        "ts_utc": datetime.now(tz=timezone.utc).isoformat(),
        "ts_ny": now_ny().isoformat(),
        "reason": reason,
        "positions": list_open_positions_details_allowed(),
        "active_plans": {sym: plan for sym, plan in TRADE_PLAN.items() if plan.get("active")},
    }
    if extra:
        snapshot["extra"] = extra
    if JOURNAL_ENABLED:
        try:
            _ensure_parent_dir(POSITION_SNAPSHOT_PATH)
            Path(POSITION_SNAPSHOT_PATH).write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")
        except Exception as e:
            logger.warning("POSITION_SNAPSHOT_WRITE_FAILED err=%s", e)
    return snapshot


def read_positions_snapshot() -> dict:
    path = Path(POSITION_SNAPSHOT_PATH)
    if (not JOURNAL_ENABLED) or (not path.exists()):
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _bootstrap_journal_decisions():
    if not JOURNAL_ENABLED:
        return
    rows = _read_journal(limit=JOURNAL_BOOTSTRAP_LIMIT)
    if not rows:
        return
    try:
        with STATE_LOCK:
            DECISIONS.clear()
            DECISIONS.extend(rows[-max(DECISION_BUFFER_SIZE, 50):])
        logger.info("JOURNAL_BOOTSTRAP_LOADED count=%s", len(rows[-max(DECISION_BUFFER_SIZE, 50):]))
    except Exception as e:
        logger.warning("JOURNAL_BOOTSTRAP_FAILED err=%s", e)


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
        if _journal_should_persist(event, action):
            _journal_append(item)
        if str(action).lower() in {"rejected", "ignored"}:
            _append_rejection_history({
                "ts_utc": item.get("ts_utc"),
                "ts_ny": item.get("ts_ny"),
                "event": event,
                "source": source,
                "symbol": (symbol or "").upper(),
                "side": side,
                "signal": signal,
                "action": action,
                "reason": reason,
                "reason_bucket": _normalize_reject_reason(reason),
                "details": details or {},
            })
        maybe_emit_alert_for_decision(item)
    except Exception:
        # Never let tracing break trading.
        pass



def _normalize_reject_reason(reason: str) -> str:
    r = str(reason or "").strip().lower()
    if not r:
        return "UNKNOWN"
    mapping = [
        ("daily_stop", "DAILY_LOSS_LIMIT"),
        ("daily_halt", "DAILY_LOSS_LIMIT"),
        ("max_open_positions", "MAX_POSITIONS"),
        ("spread_too_wide", "SPREAD_TOO_WIDE"),
        ("price_stale", "STALE_PRICE"),
        ("symbol_locked", "SYMBOL_LOCK"),
        ("position_already_open", "POSITION_ALREADY_EXISTS"),
        ("position_open_after_lock", "POSITION_ALREADY_EXISTS"),
        ("quote_missing", "QUOTE_UNAVAILABLE"),
        ("alpaca_submit_failed", "ORDER_REJECTED"),
        ("risk_exceeded", "RISK_EXCEEDED"),
        ("kill_switch", "KILL_SWITCH"),
        ("plan_active", "PLAN_ACTIVE"),
        ("outside_market_hours", "OUTSIDE_MARKET_HOURS"),
    ]
    for needle, label in mapping:
        if needle in r:
            return label
    return re.sub(r"[^A-Z0-9]+", "_", r.upper()).strip("_") or "UNKNOWN"


def _append_rejection_history(item: dict):
    try:
        with STATE_LOCK:
            REJECTION_HISTORY.append(item)
            if len(REJECTION_HISTORY) > max(REJECTION_HISTORY_LIMIT, 50):
                overflow = len(REJECTION_HISTORY) - max(REJECTION_HISTORY_LIMIT, 50)
                if overflow > 0:
                    del REJECTION_HISTORY[:overflow]
    except Exception:
        pass


def _append_alert_history(item: dict):
    try:
        with STATE_LOCK:
            ALERT_HISTORY.append(item)
            if len(ALERT_HISTORY) > max(ALERT_HISTORY_LIMIT, 50):
                overflow = len(ALERT_HISTORY) - max(ALERT_HISTORY_LIMIT, 50)
                if overflow > 0:
                    del ALERT_HISTORY[:overflow]
    except Exception:
        pass


def _update_alert_history_status(alert_id: str, **updates):
    try:
        with STATE_LOCK:
            for row in reversed(ALERT_HISTORY):
                if str(row.get("id")) == str(alert_id):
                    row.update(updates)
                    row["updated_at_utc"] = datetime.now(tz=timezone.utc).isoformat()
                    break
    except Exception:
        pass


def _alert_payload_for_destination(item: dict) -> tuple[bytes, dict]:
    title = str(item.get("title") or "Trading Bot Alert")
    text = str(item.get("text") or title)
    level = str(item.get("level") or "info")
    url = ALERT_WEBHOOK_URL
    if "discord.com/api/webhooks" in url or "discordapp.com/api/webhooks" in url:
        body = {"content": text}
    elif "hooks.slack.com/" in url:
        body = {"text": text}
    else:
        body = {
            "title": title,
            "text": text,
            "level": level,
            "event": item.get("event"),
            "symbol": item.get("symbol"),
            "action": item.get("action"),
            "reason": item.get("reason"),
            "details": item.get("details") or {},
            "ts_utc": item.get("ts_utc"),
        }
    return json.dumps(body, default=str).encode("utf-8"), {
        "content-type": "application/json",
        "user-agent": "trading-webhook/patch-007",
    }


def _dispatch_alert_http(item: dict):
    alert_id = str(item.get("id") or "")
    if (not ALERTS_ENABLED) or (not ALERT_WEBHOOK_URL):
        _update_alert_history_status(alert_id, status="disabled")
        return
    try:
        payload, headers = _alert_payload_for_destination(item)
        req = UrlRequest(ALERT_WEBHOOK_URL, data=payload, headers=headers, method="POST")
        with urlopen(req, timeout=max(ALERT_WEBHOOK_TIMEOUT_SEC, 1.0)) as resp:
            code = int(getattr(resp, "status", 200) or 200)
            response_text = ""
            try:
                response_text = (resp.read() or b"").decode("utf-8", errors="ignore")[:500]
            except Exception:
                response_text = ""
        _update_alert_history_status(alert_id, status="sent", http_status=code, response=response_text)
    except HTTPError as e:
        _update_alert_history_status(alert_id, status="error", http_status=int(getattr(e, "code", 0) or 0), error=str(e))
    except URLError as e:
        _update_alert_history_status(alert_id, status="error", error=str(e))
    except Exception as e:
        _update_alert_history_status(alert_id, status="error", error=str(e))


def send_alert(kind: str, title: str, text: str, level: str = "info", dedup_key: str = "", **payload):
    now_ts = _time.time()
    dedup = str(dedup_key or f"{kind}|{title}|{payload.get('symbol') or ''}|{payload.get('action') or ''}|{payload.get('reason') or ''}")
    if ALERT_DEDUP_SEC > 0:
        with STATE_LOCK:
            last = float(ALERT_DEDUP.get(dedup, 0) or 0)
            if (now_ts - last) < ALERT_DEDUP_SEC:
                return {"ok": True, "queued": False, "suppressed": True, "dedup_key": dedup}
            ALERT_DEDUP[dedup] = now_ts
    alert_id = hashlib.sha1(f"{now_ts}|{dedup}|{title}".encode("utf-8")).hexdigest()[:16]
    item = {
        "id": alert_id,
        "ts_utc": datetime.now(tz=timezone.utc).isoformat(),
        "ts_ny": now_ny().isoformat(),
        "kind": kind,
        "title": title,
        "text": text,
        "level": level,
        "status": "queued",
        "dedup_key": dedup,
        **payload,
    }
    _append_alert_history(item)
    LAST_ALERT_HEARTBEAT.clear()
    LAST_ALERT_HEARTBEAT.update({
        "ts_utc": item["ts_utc"],
        "ts_ny": item["ts_ny"],
        "kind": kind,
        "title": title,
        "status": "queued",
    })
    t = threading.Thread(target=_dispatch_alert_http, args=(item,), daemon=True)
    t.start()
    return {"ok": True, "queued": True, "id": alert_id}


def _decision_alert_text(item: dict) -> str:
    event = str(item.get("event") or "")
    action = str(item.get("action") or "")
    symbol = str(item.get("symbol") or "")
    reason = str(item.get("reason") or "")
    details = item.get("details") or {}
    side = str(item.get("side") or "")
    qty = None
    order_id = None
    try:
        qty = ((details or {}).get("qty") if isinstance(details, dict) else None)
        order_id = ((details or {}).get("order_id") if isinstance(details, dict) else None)
    except Exception:
        qty = None
        order_id = None
    parts = [f"{event} {action}".strip(), symbol, side]
    if qty:
        parts.append(f"qty={qty}")
    if reason:
        parts.append(f"reason={reason}")
    if order_id:
        parts.append(f"order_id={order_id}")
    return " | ".join([p for p in parts if p])


def maybe_emit_alert_for_decision(item: dict):
    if not ALERTS_ENABLED or not ALERT_WEBHOOK_URL:
        return
    event = str(item.get("event") or "").upper()
    action = str(item.get("action") or "").lower()
    symbol = str(item.get("symbol") or "").upper()
    reason = str(item.get("reason") or "")
    details = item.get("details") or {}
    if event == "ENTRY" and action == "order_submitted" and ALERT_ON_ENTRY:
        send_alert(
            kind="entry",
            title=f"ENTRY {symbol}",
            text=_decision_alert_text(item),
            level="info",
            dedup_key=f"entry:{symbol}:{details.get('order_id') or action}",
            event=event,
            symbol=symbol,
            action=action,
            reason=reason,
            details=(details if ALERT_INCLUDE_DETAILS else {}),
        )
    elif event == "EXIT" and action == "order_submitted" and ALERT_ON_EXIT:
        send_alert(
            kind="exit",
            title=f"EXIT {symbol}",
            text=_decision_alert_text(item),
            level="warning",
            dedup_key=f"exit:{symbol}:{details.get('order_id') or action}",
            event=event,
            symbol=symbol,
            action=action,
            reason=reason,
            details=(details if ALERT_INCLUDE_DETAILS else {}),
        )
    elif event == "RISK" and action == "halted" and ALERT_ON_DAILY_HALT:
        send_alert(
            kind="daily_halt",
            title="DAILY HALT ACTIVATED",
            text=_decision_alert_text(item),
            level="critical",
            dedup_key=f"daily_halt:{_current_session_key()}:{reason}",
            event=event,
            symbol=symbol,
            action=action,
            reason=reason,
            details=(details if ALERT_INCLUDE_DETAILS else {}),
        )
    elif action in {"rejected", "ignored"} and ALERT_ON_REJECTION:
        bucket = _normalize_reject_reason(reason)
        send_alert(
            kind="rejection",
            title=f"REJECT {symbol or event}",
            text=f"{_decision_alert_text(item)} | bucket={bucket}",
            level="warning",
            dedup_key=f"reject:{bucket}:{symbol}:{action}",
            event=event,
            symbol=symbol,
            action=action,
            reason=reason,
            details=(details if ALERT_INCLUDE_DETAILS else {}),
        )


def _current_session_key() -> str:
    return now_ny().strftime("%Y-%m-%d")


def _ensure_daily_halt_rollover():
    session = _current_session_key()
    if DAILY_HALT_STATE.get("session") != session:
        DAILY_HALT_STATE.clear()
        DAILY_HALT_STATE.update({"session": session, "active": False, "triggered_at": None, "reason": ""})


def activate_daily_halt(reason: str = "daily_stop_hit"):
    _ensure_daily_halt_rollover()
    DAILY_HALT_STATE.update({
        "session": _current_session_key(),
        "active": True,
        "triggered_at": now_ny().isoformat(),
        "reason": reason,
    })
    record_decision("RISK", "risk_guard", action="halted", reason=reason)


def daily_halt_active() -> bool:
    _ensure_daily_halt_rollover()
    return bool(DAILY_HALT_STATE.get("active"))


def update_exit_heartbeat(status: str = "ok", **extra):
    LAST_EXIT_HEARTBEAT.clear()
    LAST_EXIT_HEARTBEAT.update({
        "ts_ny": now_ny().isoformat(),
        "ts_utc": datetime.now(tz=timezone.utc).isoformat(),
        "status": status,
        **extra,
    })

_bootstrap_journal_decisions()


def config_effective_snapshot() -> dict:
    return {
        "paper": APCA_PAPER,
        "system_name": SYSTEM_NAME,
        "env_name": ENV_NAME,
        "strategy_mode": STRATEGY_MODE,
        "live_trading_enabled": LIVE_TRADING_ENABLED,
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
        "journal_enabled": JOURNAL_ENABLED,
        "journal_path": JOURNAL_PATH,
        "position_snapshot_path": POSITION_SNAPSHOT_PATH,
        "persistence_required": PERSISTENCE_REQUIRED,
        "swing_max_hold_days": SWING_MAX_HOLD_DAYS,
        "swing_allow_same_day_exit": SWING_ALLOW_SAME_DAY_EXIT,
        "decision_buffer_size": DECISION_BUFFER_SIZE,
        "daily_stop_dollars": DAILY_STOP_DOLLARS,
        "enable_risk_recheck_after_fill": ENABLE_RISK_RECHECK_AFTER_FILL,
        "enable_partial_fill_tracking": ENABLE_PARTIAL_FILL_TRACKING,
        "readiness_require_workers": READINESS_REQUIRE_WORKERS,
        "rejection_history_limit": REJECTION_HISTORY_LIMIT,
        "auto_flatten_on_daily_stop": AUTO_FLATTEN_ON_DAILY_STOP,
        "alerts_enabled": ALERTS_ENABLED,
        "alert_webhook_configured": bool(ALERT_WEBHOOK_URL),
        "alert_dedup_sec": ALERT_DEDUP_SEC,
        "alert_history_limit": ALERT_HISTORY_LIMIT,
        "entry_require_quote": ENTRY_REQUIRE_QUOTE,
        "entry_require_fresh_quote": ENTRY_REQUIRE_FRESH_QUOTE,
        "entry_price_max_age_sec": ENTRY_PRICE_MAX_AGE_SEC,
        "entry_max_spread_pct": ENTRY_MAX_SPREAD_PCT,
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
def _is_regular_market_day(dt_local=None) -> bool:
    dt_local = dt_local or now_ny()
    try:
        return int(dt_local.weekday()) < 5
    except Exception:
        return False


def in_market_hours() -> bool:
    dt_local = now_ny()
    if not _is_regular_market_day(dt_local):
        return False
    t = dt_local.time()
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


def get_buying_power_snapshot() -> dict:
    try:
        acct = trading_client.get_account()
        bp = float(getattr(acct, "buying_power", 0) or 0)
        equity = float(getattr(acct, "equity", 0) or 0)
        cash = float(getattr(acct, "cash", 0) or 0)
        return {"ok": True, "buying_power": bp, "equity": equity, "cash": cash}
    except Exception as e:
        return {"ok": False, "buying_power": 0.0, "equity": None, "cash": None, "error": str(e)}


def clip_qty_for_affordability(price: float, requested_qty: float) -> dict:
    price = float(price or 0)
    requested_qty = float(requested_qty or 0)
    bp = get_buying_power_snapshot()
    if price <= 0 or requested_qty <= 0:
        return {"requested_qty": requested_qty, "affordable_qty": 0.0, "submitted_qty": 0.0, "affordability_clipped": False, "buying_power_snapshot": bp, "reason": "invalid_price_or_qty"}
    effective_bp = max(0.0, float(bp.get("buying_power") or 0.0) * max(0.0, min(1.0, ORDER_BP_HAIRCUT_PCT)))
    affordable_qty = 0.0 if effective_bp <= 0 else round(effective_bp / price, 2)
    submitted_qty = min(requested_qty, affordable_qty)
    submitted_qty = round(max(0.0, submitted_qty), 2)
    clipped = submitted_qty < round(requested_qty, 2)
    reason = ""
    if submitted_qty <= 0:
        reason = "insufficient_buying_power_internal"
    elif submitted_qty < max(MIN_AFFORDABLE_QTY, MIN_QTY):
        reason = "qty_below_min_after_affordability_clip"
    return {
        "requested_qty": round(requested_qty, 2),
        "affordable_qty": round(affordable_qty, 2),
        "submitted_qty": round(submitted_qty, 2),
        "affordability_clipped": clipped,
        "buying_power_snapshot": bp,
        "effective_buying_power": round(effective_bp, 2),
        "reason": reason,
    }


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


def get_order_status(order_id: str) -> dict:
    oid = str(order_id or "").strip()
    if not oid:
        return {}
    try:
        order = trading_client.get_order_by_id(oid)
        return {
            "id": str(getattr(order, "id", oid)),
            "symbol": str(getattr(order, "symbol", "") or "").upper(),
            "side": str(getattr(getattr(order, "side", None), "value", getattr(order, "side", ""))),
            "status": str(getattr(getattr(order, "status", None), "value", getattr(order, "status", ""))),
            "type": str(getattr(getattr(order, "type", None), "value", getattr(order, "type", ""))),
            "filled_qty": str(getattr(order, "filled_qty", "") or ""),
            "filled_avg_price": str(getattr(order, "filled_avg_price", "") or ""),
            "submitted_at": str(getattr(order, "submitted_at", "") or ""),
        }
    except Exception as e:
        return {"id": oid, "status_error": str(e)}




def list_open_orders_safe(limit: int | None = None) -> list[dict]:
    lim = max(1, min(int(limit or RECONCILE_ORDER_LOOKBACK_LIMIT), 500))
    try:
        orders = trading_client.get_orders()
    except Exception:
        return []
    out: list[dict] = []
    active = {"new", "accepted", "pending_new", "partially_filled", "pending_replace", "accepted_for_bidding", "held"}
    for order in list(orders or []):
        try:
            status = str(getattr(getattr(order, "status", None), "value", getattr(order, "status", "")) or "").lower()
            if status and status not in active:
                continue
            rec = {
                "id": str(getattr(order, "id", "") or ""),
                "symbol": str(getattr(order, "symbol", "") or "").upper(),
                "side": str(getattr(getattr(order, "side", None), "value", getattr(order, "side", "")) or ""),
                "status": status,
                "type": str(getattr(getattr(order, "type", None), "value", getattr(order, "type", "")) or ""),
                "qty": str(getattr(order, "qty", "") or ""),
                "filled_qty": str(getattr(order, "filled_qty", "") or ""),
                "submitted_at": str(getattr(order, "submitted_at", "") or ""),
            }
            out.append(rec)
        except Exception:
            continue
    out.sort(key=lambda x: str(x.get("submitted_at") or ""), reverse=True)
    return out[:lim]


def build_reconcile_snapshot() -> dict:
    active_plans = {sym: plan for sym, plan in TRADE_PLAN.items() if plan.get("active")}
    broker_positions = list_open_positions_details_allowed()
    broker_syms = sorted({str(p.get("symbol") or "").upper() for p in broker_positions if str(p.get("symbol") or "").upper()})
    open_orders = list_open_orders_safe()
    active_order_statuses = {"submitted", "new", "accepted", "pending_new", "partially_filled"}
    plan_symbols = sorted(active_plans.keys())
    missing_from_plans = sorted([sym for sym in broker_syms if sym not in active_plans])
    stale_active_plans = sorted([sym for sym in active_plans if sym not in broker_syms])
    pending_entry_plan_symbols = sorted([sym for sym, plan in active_plans.items() if str(plan.get("order_status") or "").lower() in active_order_statuses])
    open_order_symbols = sorted({str(o.get("symbol") or "").upper() for o in open_orders if str(o.get("symbol") or "").upper()})
    orphan_open_order_symbols = sorted([sym for sym in open_order_symbols if sym not in active_plans and sym not in broker_syms])
    plans_missing_open_order = sorted([sym for sym in pending_entry_plan_symbols if sym not in open_order_symbols and sym not in broker_syms])
    partial_fill_plan_symbols = sorted([sym for sym, plan in active_plans.items() if str(plan.get("order_status") or "").lower() == "partially_filled"])
    snap = {
        "broker_positions_count": len(broker_positions),
        "broker_symbols": broker_syms,
        "active_plan_count": len(active_plans),
        "active_plan_symbols": plan_symbols,
        "open_order_count": len(open_orders),
        "open_order_symbols": open_order_symbols,
        "missing_from_plans": missing_from_plans,
        "stale_active_plans": stale_active_plans,
        "pending_entry_plan_symbols": pending_entry_plan_symbols,
        "orphan_open_order_symbols": orphan_open_order_symbols,
        "plans_missing_open_order": plans_missing_open_order,
        "partial_fill_plan_symbols": partial_fill_plan_symbols,
        "open_orders": open_orders[:25],
    }
    snap.update(_build_reconcile_assessment(snap))
    return snap


_RECONCILE_SEVERITY_RANK = {"info": 0, "warn": 1, "error": 2, "critical": 3}


def _reconcile_issue(code: str, severity: str, summary: str, details: dict | None = None,
                     recommended_action: str = "", blocking: bool | None = None,
                     symbols: list[str] | None = None) -> dict:
    sev = str(severity or "warn").lower()
    syms = [str(s or "").upper() for s in (symbols or []) if str(s or "").upper()]
    return {
        "code": code,
        "severity": sev,
        "summary": summary,
        "symbols": syms,
        "blocking": bool(blocking if blocking is not None else sev in {"error", "critical"}),
        "recommended_action": recommended_action,
        "details": details or {},
    }


def _reconcile_health_grade(issues: list[dict]) -> str:
    max_rank = max((_RECONCILE_SEVERITY_RANK.get(str((i or {}).get("severity") or "info").lower(), 0) for i in (issues or [])), default=0)
    if max_rank >= _RECONCILE_SEVERITY_RANK["critical"]:
        return "critical"
    if max_rank >= _RECONCILE_SEVERITY_RANK["error"]:
        return "blocking"
    if max_rank >= _RECONCILE_SEVERITY_RANK["warn"]:
        return "degraded"
    return "healthy"


def _build_reconcile_assessment(snap: dict) -> dict:
    issues: list[dict] = []
    missing_from_plans = list(snap.get("missing_from_plans") or [])
    stale_active_plans = list(snap.get("stale_active_plans") or [])
    orphan_open_order_symbols = list(snap.get("orphan_open_order_symbols") or [])
    plans_missing_open_order = list(snap.get("plans_missing_open_order") or [])
    partial_fill_plan_symbols = list(snap.get("partial_fill_plan_symbols") or [])

    if missing_from_plans:
        issues.append(_reconcile_issue(
            "broker_positions_missing_internal_plan",
            "critical",
            "Broker position exists without an active internal plan.",
            details={"count": len(missing_from_plans)},
            symbols=missing_from_plans,
            recommended_action="Inspect the unmanaged broker position immediately. Freeze new entries for affected symbols until the position is reconciled or manually closed.",
            blocking=True,
        ))
    if orphan_open_order_symbols:
        issues.append(_reconcile_issue(
            "orphan_open_orders",
            "critical",
            "Open broker orders exist without matching plan or position state.",
            details={"count": len(orphan_open_order_symbols), "max_age_sec": RECONCILE_ORPHAN_ORDER_MAX_AGE_SEC},
            symbols=orphan_open_order_symbols,
            recommended_action="Review orphan orders immediately and cancel or adopt them into authoritative state before allowing new trading.",
            blocking=True,
        ))
    if plans_missing_open_order:
        issues.append(_reconcile_issue(
            "plans_missing_open_order",
            "error",
            "Pending entry plans exist without matching open orders or positions.",
            details={"count": len(plans_missing_open_order)},
            symbols=plans_missing_open_order,
            recommended_action="Inspect affected plans and either restore the missing order linkage or deactivate the stale pending plans.",
            blocking=True,
        ))
    if partial_fill_plan_symbols:
        issues.append(_reconcile_issue(
            "partial_fills_open",
            "warn",
            "Plans are stuck in partially filled state and require monitoring.",
            details={"count": len(partial_fill_plan_symbols), "max_age_sec": RECONCILE_PARTIAL_FILL_MAX_AGE_SEC},
            symbols=partial_fill_plan_symbols,
            recommended_action="Monitor partial fills closely. Reconcile remaining quantity, open orders, and broker position before the next trading session.",
            blocking=False,
        ))
    if stale_active_plans:
        issues.append(_reconcile_issue(
            "stale_active_plans",
            "warn",
            "Active plans exist without broker position backing.",
            details={"count": len(stale_active_plans)},
            symbols=stale_active_plans,
            recommended_action="Review stale active plans and deactivate any plan that no longer has broker backing or an active entry order.",
            blocking=False,
        ))

    severity_counts = {"info": 0, "warn": 0, "error": 0, "critical": 0}
    recommended_actions: list[str] = []
    for issue in issues:
        sev = str(issue.get("severity") or "info").lower()
        severity_counts[sev] = severity_counts.get(sev, 0) + 1
        action = str(issue.get("recommended_action") or "").strip()
        if action and action not in recommended_actions:
            recommended_actions.append(action)
    health_grade = _reconcile_health_grade(issues)
    max_severity = "info"
    if issues:
        max_severity = max((str(i.get("severity") or "info").lower() for i in issues), key=lambda s: _RECONCILE_SEVERITY_RANK.get(s, 0))
    return {
        "health_grade": health_grade,
        "max_severity": max_severity,
        "trading_blocked": any(bool(i.get("blocking")) for i in issues),
        "issue_counts": severity_counts,
        "issue_total": len(issues),
        "issues": issues,
        "recommended_actions": recommended_actions,
    }


def close_position(symbol: str, reason: str = "", source: str = "system") -> dict:
    qty_signed, _side = get_position(symbol)
    if qty_signed == 0:
        record_decision("EXIT", source, symbol, action="ignored", reason="no_open_position", exit_reason=reason)
        return {"closed": False, "reason": "No open position"}

    qty = abs(qty_signed)
    close_side = "sell" if qty_signed > 0 else "buy"

    if not is_live_trading_permitted(source):
        payload = {"closed": False, "dry_run": True, "symbol": symbol, "qty": qty, "close_side": close_side, "live_trading_enabled": LIVE_TRADING_ENABLED}
        record_decision("EXIT", source, symbol, side=close_side, action="dry_run", reason=reason or "dry_run", qty=qty)
        try:
            _record_paper_lifecycle("exit", "dry_run", symbol=symbol, details={"reason": reason or "dry_run", "qty": qty, "source": source})
        except Exception:
            pass
        try:
            if isinstance(TRADE_PLAN.get(symbol), dict):
                _transition_execution_lifecycle(TRADE_PLAN[symbol], symbol, "close_submitted", reason="exit_dry_run", details={"qty": qty, "source": source})
        except Exception:
            pass
        return payload

    order = submit_market_order(symbol, close_side, qty)
    out = {"closed": True, "symbol": symbol, "qty": qty, "close_side": close_side, "order_id": str(order.id)}
    record_decision("EXIT", source, symbol, side=close_side, action="order_submitted", reason=reason or "exit", qty=qty, order_id=str(order.id))
    persist_positions_snapshot(reason="close_position_submitted", extra={"symbol": symbol, "order_id": str(order.id), "exit_reason": reason, "source": source})
    try:
        _record_paper_lifecycle("exit", "submitted", symbol=symbol, details={"reason": reason or "exit", "qty": qty, "source": source, "order_id": str(order.id)})
    except Exception:
        pass
    try:
        if isinstance(TRADE_PLAN.get(symbol), dict):
            plan_ref = TRADE_PLAN[symbol]
            plan_ref["last_exit_order_id"] = str(order.id)
            _transition_execution_lifecycle(plan_ref, symbol, "close_submitted", reason="exit_submitted", details={"qty": qty, "source": source, "order_id": str(order.id)})
    except Exception:
        pass
    return out


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


def sync_trade_plan_with_broker(symbol: str, plan: dict) -> dict:
    """Best-effort sync between internal plan, broker order state, and live position."""
    out = {"symbol": symbol, "active": bool((plan or {}).get("active")), "changes": []}
    if not plan or not plan.get("active"):
        return out

    now = now_ny()
    submitted_raw = plan.get("submitted_at") or plan.get("created_at")
    submitted_at = None
    if submitted_raw:
        try:
            submitted_at = datetime.fromisoformat(str(submitted_raw))
        except Exception:
            submitted_at = None
    age_sec = max(0.0, (now - submitted_at).total_seconds()) if submitted_at else None

    qty_signed, pos_side = get_position(symbol)
    order_status = {}
    order_id = str(plan.get("order_id") or "").strip()
    if PLAN_RECONCILE_ORDER_STATUS and order_id:
        order_status = get_order_status(order_id)
        if order_status:
            plan["order_status"] = order_status.get("status")
            out["order_status"] = order_status
    _ensure_execution_lifecycle_plan(symbol, plan)

    if qty_signed == 0:
        terminal = {"canceled", "cancelled", "rejected", "expired"}
        order_status_lc = str(order_status.get("status") or "").lower()
        if order_status_lc in terminal:
            plan["active"] = False
            out["changes"].append("deactivated_terminal_order_without_position")
            _apply_execution_lifecycle_reconcile(symbol, plan, broker_order=order_status, broker_position_qty=qty_signed)
            record_decision("RECONCILE", "worker_exit", symbol, action="deactivated", reason="terminal_order_without_position", meta={"order_status": order_status})
            return out
        orphan_status = bool(order_id and order_status.get("status_error")) or (order_id and not order_status)
        if RECONCILE_DEACTIVATE_ORPHAN_PLANS and orphan_status and age_sec is not None and age_sec >= RECONCILE_ORPHAN_ORDER_MAX_AGE_SEC:
            plan["active"] = False
            out["changes"].append("deactivated_orphan_plan_without_position")
            _apply_execution_lifecycle_reconcile(symbol, plan, broker_order=order_status, broker_position_qty=qty_signed)
            record_decision("RECONCILE", "worker_exit", symbol, action="deactivated", reason="orphan_plan_without_position", meta={"age_sec": age_sec, "order_id": order_id, "order_status": order_status})
            return out
        if age_sec is not None and age_sec >= PLAN_STALE_NO_POSITION_SEC:
            plan["active"] = False
            out["changes"].append("deactivated_stale_without_position")
            _apply_execution_lifecycle_reconcile(symbol, plan, broker_order=order_status, broker_position_qty=qty_signed)
            record_decision("RECONCILE", "worker_exit", symbol, action="deactivated", reason="stale_without_position", meta={"age_sec": age_sec, "order_status": order_status})
            return out
        _apply_execution_lifecycle_reconcile(symbol, plan, broker_order=order_status, broker_position_qty=qty_signed)
        return out

    desired_side = "buy" if qty_signed > 0 else "sell"
    if plan.get("side") != desired_side:
        plan["side"] = desired_side
        out["changes"].append("side_updated_from_position")

    try:
        live_qty = round(abs(float(qty_signed)), 2)
        current_qty = round(float(plan.get("qty") or 0), 2)
        if current_qty != live_qty:
            plan["qty"] = live_qty
            out["changes"].append("qty_updated_from_position")
        if ENABLE_PARTIAL_FILL_TRACKING:
            plan["filled_qty"] = live_qty
            plan["requested_qty"] = float(plan.get("requested_qty") or current_qty or live_qty)
    except Exception:
        live_qty = abs(float(qty_signed))

    fill_avg = None
    try:
        fill_avg = float(order_status.get("filled_avg_price") or 0) or None
    except Exception:
        fill_avg = None
    filled_qty = None
    try:
        filled_qty = float(order_status.get("filled_qty") or 0) or None
    except Exception:
        filled_qty = None
    if ENABLE_PARTIAL_FILL_TRACKING and filled_qty:
        plan["filled_qty"] = round(float(filled_qty), 4)
        plan["requested_qty"] = float(plan.get("requested_qty") or plan.get("qty") or live_qty)
        if age_sec is not None and age_sec >= RECONCILE_PARTIAL_FILL_MAX_AGE_SEC and str(order_status.get("status") or "").lower() == "partially_filled":
            out["changes"].append("partial_fill_age_exceeded")
            record_decision("RECONCILE", "worker_exit", symbol, action="partial_fill_age_exceeded", reason="partial_fill_age_exceeded", meta={"age_sec": age_sec, "order_status": order_status})

    if fill_avg and fill_avg > 0:
        old_entry = float(plan.get("entry_price") or 0)
        if old_entry <= 0 or abs(fill_avg - old_entry) >= 0.01 or (not plan.get("fill_reconciled")):
            rebuilt = build_trade_plan(symbol, plan.get("side") or desired_side, float(plan.get("qty") or live_qty), float(fill_avg), plan.get("signal") or "FILLED")
            for k, v in rebuilt.items():
                plan[k] = v
            plan["order_id"] = order_id
            plan["submitted_at"] = plan.get("submitted_at") or now.isoformat()
            plan["fill_reconciled"] = True
            plan["filled_avg_price"] = float(fill_avg)
            plan["avg_fill_price"] = float(fill_avg)
            plan["filled_qty"] = round(float(plan.get("filled_qty") or live_qty), 4)
            plan["requested_qty"] = float(plan.get("requested_qty") or plan.get("qty") or live_qty)
            actual_risk = round(abs(float(plan.get("entry_price") or fill_avg) - float(plan.get("stop_price") or 0)) * abs(float(plan.get("filled_qty") or live_qty)), 4)
            plan["actual_risk_dollars"] = actual_risk
            out["changes"].append("entry_price_reconciled_to_fill")
            if ENABLE_RISK_RECHECK_AFTER_FILL and RISK_DOLLARS > 0 and actual_risk > (float(RISK_DOLLARS) * (1.0 + max(float(RISK_RECHECK_TOLERANCE_PCT), 0.0))):
                close_out = close_position(symbol, reason="risk_exceeded_after_fill", source="risk_guard")
                out["changes"].append("risk_recheck_after_fill")
                out["risk_close"] = close_out
                record_decision("RECONCILE", "worker_exit", symbol, action="risk_recheck", reason="risk_exceeded_after_fill", meta={"actual_risk_dollars": actual_risk, "risk_dollars": RISK_DOLLARS})
                if close_out.get("closed"):
                    plan["active"] = False
                _apply_execution_lifecycle_reconcile(symbol, plan, broker_order=order_status, broker_position_qty=qty_signed)
                return out
    elif age_sec is not None and age_sec >= PLAN_STALE_SUBMITTED_SEC and str(order_status.get("status") or "").lower() not in {"filled", "partially_filled"}:
        plan["active"] = False
        out["changes"].append("deactivated_stale_submitted_plan")
        _apply_execution_lifecycle_reconcile(symbol, plan, broker_order=order_status, broker_position_qty=qty_signed)
        record_decision("RECONCILE", "worker_exit", symbol, action="deactivated", reason="stale_submitted_plan", meta={"age_sec": age_sec, "order_status": order_status})
        return out

    _apply_execution_lifecycle_reconcile(symbol, plan, broker_order=order_status, broker_position_qty=qty_signed)
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
    if actions:
        persist_positions_snapshot(reason="reconcile_trade_plans", extra={"actions": actions})
    return actions


def startup_restore_state() -> dict:
    """
    Restore active trade plans from persistent snapshot and broker positions.
    Runs at startup so redeploys do not silently lose internal exit plans.
    """
    global STARTUP_STATE
    state = {
        "ran": True,
        "ts_utc": datetime.now(tz=timezone.utc).isoformat(),
        "ts_ny": now_ny().isoformat(),
        "snapshot_found": False,
        "snapshot_path": POSITION_SNAPSHOT_PATH,
        "journal_path": JOURNAL_PATH,
        "recovered_from_snapshot_count": 0,
        "recovered_from_broker_only_count": 0,
        "stale_snapshot_count": 0,
        "stale_snapshot_symbols": [],
        "reconcile_actions": [],
        "error": "",
    }
    try:
        broker_positions = list_open_positions_details_allowed()
        broker_syms = {str(p.get("symbol") or "").upper() for p in broker_positions if str(p.get("symbol") or "").upper()}

        snapshot = read_positions_snapshot()
        active_snapshot_plans = (snapshot or {}).get("active_plans") or {}
        if active_snapshot_plans:
            state["snapshot_found"] = True

        with STATE_LOCK:
            for sym, plan in active_snapshot_plans.items():
                sym = str(sym or "").upper()
                if not sym:
                    continue
                if sym not in broker_syms:
                    state["stale_snapshot_count"] += 1
                    state["stale_snapshot_symbols"].append(sym)
                    continue
                if TRADE_PLAN.get(sym, {}).get("active"):
                    continue
                restored = json.loads(json.dumps(plan))
                restored["active"] = True
                restored["recovered"] = True
                restored["recovered_at"] = now_ny().isoformat()
                restored["startup_restored"] = True
                TRADE_PLAN[sym] = restored
                state["recovered_from_snapshot_count"] += 1

        rec_actions = reconcile_trade_plans_from_alpaca()
        state["reconcile_actions"] = rec_actions
        state["recovered_from_broker_only_count"] = len([a for a in rec_actions if str(a.get("action")) == "recovered_plan"])
        try:
            state["paper_lifecycle_normalization"] = normalize_paper_lifecycle_current_state(reason="startup_restore_state")
        except Exception as _norm_err:
            state["paper_lifecycle_normalization"] = {"checked": False, "error": str(_norm_err)}

        if (state["recovered_from_snapshot_count"] or state["recovered_from_broker_only_count"] or state["stale_snapshot_count"]):
            persist_positions_snapshot(reason="startup_state_restore", extra=state)
            logger.warning(
                "STARTUP_STATE_RESTORE snapshot=%s recovered_from_snapshot=%s recovered_from_broker_only=%s stale_snapshot=%s",
                state["snapshot_found"], state["recovered_from_snapshot_count"], state["recovered_from_broker_only_count"], state["stale_snapshot_count"]
            )
        else:
            logger.info("STARTUP_STATE_RESTORE snapshot=%s recovered_from_snapshot=0 recovered_from_broker_only=0 stale_snapshot=0", state["snapshot_found"])
    except Exception as e:
        state["error"] = str(e)
        logger.warning("STARTUP_STATE_RESTORE_FAILED err=%s", e)
    scan_restore = restore_scan_runtime_state()
    regime_restore = restore_regime_runtime_state()
    paper_restore = restore_paper_lifecycle_state()
    state["scan_state_restore"] = scan_restore
    state["regime_state_restore"] = regime_restore
    state["paper_lifecycle_state_restore"] = paper_restore
    state["release_state_restore"] = restore_release_state()
    STARTUP_STATE = state
    return state


# Run startup restore only after all helper functions it depends on are defined.


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
def build_trade_plan(symbol: str, side: str, qty: float, entry_price: float, signal: str, meta: dict | None = None) -> dict:
    entry_price = float(entry_price)
    meta = meta or {}
    stop_override = meta.get("stop_price")
    take_override = meta.get("take_price")
    if stop_override is not None:
        stop_price = round(float(stop_override), 4)
    elif side == "buy":
        stop_price = round(entry_price * (1 - STOP_PCT), 2)
    else:
        stop_price = round(entry_price * (1 + STOP_PCT), 2)
    if take_override is not None:
        take_price = round(float(take_override), 4)
    elif side == "buy":
        take_price = round(entry_price * (1 + TAKE_PCT), 2)
    else:
        take_price = round(entry_price * (1 - TAKE_PCT), 2)

    requested_qty = float(qty)
    risk_per_share = abs(float(entry_price) - float(stop_price))
    actual_risk_dollars = round(abs(requested_qty) * risk_per_share, 4)
    plan = {
        "active": True,
        "side": side,
        "qty": requested_qty,
        "requested_qty": requested_qty,
        "filled_qty": requested_qty,
        "avg_fill_price": round(entry_price, 4),
        "entry_price": round(entry_price, 4),
        "stop_price": stop_price,
        "take_price": take_price,
        "initial_stop_price": stop_price,
        "initial_take_price": take_price,
        "signal": signal,
        "opened_at": now_ny().isoformat(),
        "last_exit_attempt_ts": 0,
        "actual_risk_dollars": actual_risk_dollars,
        "risk_per_share": round(risk_per_share, 4),
        "max_hold_days": int(meta.get("max_hold_days") or SWING_MAX_HOLD_DAYS),
        "strategy_name": str(meta.get("strategy_name") or meta.get("strategy") or signal or "").strip(),
    }
    plan["thesis"] = {
        "candidate_rank_score": meta.get("rank_score"),
        "breakout_level": meta.get("breakout_level"),
        "breakout_ref": meta.get("breakout_ref"),
        "breakout_lookback_days": meta.get("breakout_lookback_days") or SWING_BREAKOUT_LOOKBACK_DAYS,
        "stop_basis": meta.get("stop_basis") or SWING_STOP_MODE,
        "target_r_mult": meta.get("target_r_mult") or SWING_TARGET_R_MULT,
        "candidate_ts": meta.get("scan_ts") or now_ny().isoformat(),
    }
    return plan

def _paper_lifecycle_counts() -> dict:
    events = list(PAPER_LIFECYCLE_HISTORY or [])
    if LAST_PAPER_LIFECYCLE and (not events or events[-1] != LAST_PAPER_LIFECYCLE):
        events.append(dict(LAST_PAPER_LIFECYCLE))
    counts = {
        "scan_completed": 0,
        "candidate_selected": 0,
        "entry_events": 0,
        "exit_events": 0,
    }
    for ev in events:
        stage = str((ev or {}).get("stage") or "").strip().lower()
        status = str((ev or {}).get("status") or "").strip().lower()
        if stage == "scan" and status == "completed":
            counts["scan_completed"] += 1
        if stage == "candidate" and status == "selected":
            counts["candidate_selected"] += 1
        if stage == "entry" and status in {"planned", "submitted", "filled", "opened"}:
            counts["entry_events"] += 1
        if stage == "exit" and status in {"submitted", "closed", "filled", "completed", "dry_run"}:
            counts["exit_events"] += 1
    counts["history_count"] = len(events)
    return counts


def _worker_status_snapshot() -> dict:
    now_utc = datetime.now(tz=timezone.utc)
    scanner_running = False
    scanner_age_sec = None
    scanner_ref_ts = str((LAST_SCANNER_TELEMETRY.get("last_worker_event_utc") or "")).strip()
    scanner_status = "unknown"
    scanner_last_event = (LAST_SCANNER_TELEMETRY.get("last_worker_event") or None)
    scanner_expected_cycle_sec = max(30, SCANNER_INTERVAL_SEC + SCANNER_TIMEOUT_SEC + max(10, SCANNER_JITTER_SEC) + 30)
    scanner_late_after_sec = scanner_expected_cycle_sec
    scanner_stale_after_sec = max(READINESS_SCANNER_MAX_AGE_SEC, scanner_expected_cycle_sec + max(60, SCANNER_TIMEOUT_SEC))
    scanner_down_after_sec = max(scanner_stale_after_sec + max(120, SCANNER_TIMEOUT_SEC), scanner_expected_cycle_sec * 2)
    if scanner_ref_ts:
        try:
            scanner_ts = datetime.fromisoformat(scanner_ref_ts)
            if scanner_ts.tzinfo is None:
                scanner_ts = scanner_ts.replace(tzinfo=timezone.utc)
            scanner_age_sec = max(0.0, (now_utc - scanner_ts.astimezone(timezone.utc)).total_seconds())
            if scanner_age_sec <= scanner_late_after_sec:
                scanner_status = "up"
                scanner_running = True
            elif scanner_age_sec <= scanner_stale_after_sec:
                scanner_status = "late"
                scanner_running = True
            elif scanner_age_sec <= scanner_down_after_sec:
                scanner_status = "stale"
                scanner_running = False
            else:
                scanner_status = "down"
                scanner_running = False
        except Exception:
            scanner_running = False
            scanner_status = "unknown"
    exit_worker_running = False
    exit_age_sec = None
    exit_status = "unknown"
    if LAST_EXIT_HEARTBEAT.get("ts_utc"):
        try:
            exit_ts = datetime.fromisoformat(str(LAST_EXIT_HEARTBEAT.get("ts_utc")))
            if exit_ts.tzinfo is None:
                exit_ts = exit_ts.replace(tzinfo=timezone.utc)
            exit_age_sec = max(0.0, (now_utc - exit_ts.astimezone(timezone.utc)).total_seconds())
            if exit_age_sec <= max(READINESS_EXIT_MAX_AGE_SEC, 15):
                exit_worker_running = True
                exit_status = "up"
            elif exit_age_sec <= max(READINESS_EXIT_MAX_AGE_SEC * 2, 60):
                exit_worker_running = False
                exit_status = "stale"
            else:
                exit_worker_running = False
                exit_status = "down"
        except Exception:
            exit_worker_running = False
            exit_status = "unknown"
    return {
        "scanner_running": scanner_running,
        "scanner_status": scanner_status,
        "scanner_age_sec": scanner_age_sec,
        "scanner_last_event": scanner_last_event,
        "scanner_last_event_utc": LAST_SCANNER_TELEMETRY.get("last_worker_event_utc"),
        "scanner_expected_cycle_sec": scanner_expected_cycle_sec,
        "scanner_late_after_sec": scanner_late_after_sec,
        "scanner_stale_after_sec": scanner_stale_after_sec,
        "scanner_down_after_sec": scanner_down_after_sec,
        "exit_worker_running": exit_worker_running,
        "exit_worker_status": exit_status,
        "exit_worker_age_sec": exit_age_sec,
    }


def _safe_iso_to_dt(value) -> datetime | None:
    s = str(value or '').strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _session_boundary_snapshot() -> dict:
    now_local = now_ny()
    today_ny = now_local.date().isoformat()
    open_local = datetime.combine(now_local.date(), MARKET_OPEN, tzinfo=NY_TZ)
    close_local = datetime.combine(now_local.date(), MARKET_CLOSE, tzinfo=NY_TZ)
    market_day = _is_regular_market_day(now_local)
    market_closed_reason = "weekend" if not market_day else ""
    return {
        "today_ny": today_ny,
        "now_ny": now_local.isoformat(),
        "market_open_ny": open_local.isoformat(),
        "market_close_ny": close_local.isoformat(),
        "market_open_utc": open_local.astimezone(timezone.utc).isoformat(),
        "market_close_utc": close_local.astimezone(timezone.utc).isoformat(),
        "market_day": bool(market_day),
        "market_closed_reason": market_closed_reason,
        "market_open_now": bool(in_market_hours()),
    }


def _freshness_entry(name: str, ts_value, *, source: str = "", max_age_sec: float | None = None, require_same_session: bool = False, extra: dict | None = None) -> dict:
    now_utc = datetime.now(tz=timezone.utc)
    session = _session_boundary_snapshot()
    dt = _safe_iso_to_dt(ts_value)
    row = {
        "name": name,
        "ts_utc": dt.isoformat() if dt else None,
        "age_sec": None,
        "source": source or "",
        "same_session": None,
        "fresh": False,
        "status": "missing",
    }
    if dt:
        age_sec = max(0.0, (now_utc - dt).total_seconds())
        dt_ny = dt.astimezone(NY_TZ)
        same_session = (dt_ny.date().isoformat() == session["today_ny"])
        fresh = True
        if max_age_sec is not None:
            fresh = fresh and (age_sec <= max_age_sec)
        if require_same_session:
            fresh = fresh and same_session
        row.update({
            "ts_ny": dt_ny.isoformat(),
            "age_sec": age_sec,
            "same_session": same_session,
            "fresh": bool(fresh),
            "status": "fresh" if fresh else "stale",
        })
    if extra:
        row.update(extra)
    return row



def _normalize_release_stage(stage: object, default: str = "paper") -> str:
    stage_s = str(stage or "").strip().lower()
    aliases = {
        "eligible": "guarded_live_eligible",
        "guarded": "live_guarded",
        "disabled": "emergency_disabled",
        "emergency_disable": "emergency_disabled",
        "emergency": "emergency_disabled",
    }
    stage_s = aliases.get(stage_s, stage_s)
    return stage_s if stage_s in RELEASE_VALID_STAGES else default


def _default_release_state() -> dict:
    configured_stage = _normalize_release_stage(SYSTEM_RELEASE_STAGE, default="paper")
    now_utc = datetime.now(timezone.utc).isoformat()
    now_ny_iso = now_ny().isoformat()
    return {
        "configured_stage": configured_stage,
        "current_stage": configured_stage,
        "last_transition_utc": now_utc,
        "last_transition_ny": now_ny_iso,
        "last_transition_reason": "startup_default",
        "last_transition_actor": "system",
        "approval_status": "approved" if configured_stage == "live_guarded" else "not_required",
        "approval_armed": bool(configured_stage == "live_guarded"),
        "history": [
            {
                "ts_utc": now_utc,
                "ts_ny": now_ny_iso,
                "from_stage": None,
                "to_stage": configured_stage,
                "reason": "startup_default",
                "actor": "system",
            }
        ],
    }


def _sanitize_release_state(payload: dict) -> dict:
    state = _default_release_state()
    if not isinstance(payload, dict):
        return state
    configured_stage = _normalize_release_stage(payload.get("configured_stage") or SYSTEM_RELEASE_STAGE, default=state["configured_stage"])
    current_stage = _normalize_release_stage(payload.get("current_stage") or configured_stage, default=configured_stage)
    state.update({
        "configured_stage": configured_stage,
        "current_stage": current_stage,
        "last_transition_utc": payload.get("last_transition_utc") or state.get("last_transition_utc"),
        "last_transition_ny": payload.get("last_transition_ny") or state.get("last_transition_ny"),
        "last_transition_reason": str(payload.get("last_transition_reason") or state.get("last_transition_reason") or ""),
        "last_transition_actor": str(payload.get("last_transition_actor") or state.get("last_transition_actor") or "system"),
        "approval_status": str(payload.get("approval_status") or ("approved" if current_stage == "live_guarded" else "not_required")),
        "approval_armed": bool(payload.get("approval_armed") or (current_stage == "live_guarded")),
    })
    history = []
    for item in list(payload.get("history") or [])[-max(1, RELEASE_STATE_HISTORY_LIMIT):]:
        if not isinstance(item, dict):
            continue
        history.append({
            "ts_utc": item.get("ts_utc"),
            "ts_ny": item.get("ts_ny"),
            "from_stage": _normalize_release_stage(item.get("from_stage"), default="paper") if item.get("from_stage") is not None else None,
            "to_stage": _normalize_release_stage(item.get("to_stage"), default=current_stage),
            "reason": str(item.get("reason") or ""),
            "actor": str(item.get("actor") or "system"),
        })
    if history:
        state["history"] = history
    return state


def restore_release_state() -> dict:
    global RELEASE_STATE
    payload = _safe_json_read(RELEASE_STATE_PATH)
    if payload:
        state = _sanitize_release_state(payload)
        state["restored_from_state"] = True
        RELEASE_STATE = state
        return {
            "path": RELEASE_STATE_PATH,
            "loaded": True,
            "current_stage": state.get("current_stage"),
            "history_restored": len(state.get("history") or []),
        }
    state = _default_release_state()
    state["restored_from_state"] = False
    RELEASE_STATE = state
    _safe_json_write(RELEASE_STATE_PATH, state)
    return {
        "path": RELEASE_STATE_PATH,
        "loaded": False,
        "current_stage": state.get("current_stage"),
        "history_restored": len(state.get("history") or []),
    }


def persist_release_state(reason: str = "") -> bool:
    payload = dict(RELEASE_STATE or _default_release_state())
    payload["saved_at_utc"] = datetime.now(timezone.utc).isoformat()
    if reason:
        payload["save_reason"] = str(reason)
    return _safe_json_write(RELEASE_STATE_PATH, payload)


def _release_transition_allowed(current_stage: str, target_stage: str) -> bool:
    allowed = {
        "paper": {"paper", "guarded_live_eligible", "emergency_disabled"},
        "guarded_live_eligible": {"paper", "guarded_live_eligible", "live_guarded", "emergency_disabled"},
        "live_guarded": {"paper", "live_guarded", "emergency_disabled"},
        "emergency_disabled": {"paper", "emergency_disabled"},
    }
    return target_stage in allowed.get(current_stage, {"paper"})


def _release_workflow_snapshot(include_gate: bool = True) -> dict:
    configured_stage = _normalize_release_stage(SYSTEM_RELEASE_STAGE, default="paper")
    state = _sanitize_release_state(RELEASE_STATE or {})
    persisted_stage = _normalize_release_stage(state.get("current_stage") or configured_stage, default=configured_stage)
    effective_stage = persisted_stage if RELEASE_WORKFLOW_ENFORCED else configured_stage
    history = list(state.get("history") or [])[-max(1, RELEASE_STATE_HISTORY_LIMIT):]
    out = {
        "workflow_enforced": bool(RELEASE_WORKFLOW_ENFORCED),
        "promotion_require_readiness": bool(RELEASE_PROMOTION_REQUIRE_READINESS),
        "configured_stage": configured_stage,
        "persisted_stage": persisted_stage,
        "effective_stage": effective_stage,
        "configured_stage_drift": bool(configured_stage != persisted_stage),
        "approval_status": str(state.get("approval_status") or "not_required"),
        "approval_armed": bool(state.get("approval_armed")),
        "last_transition_utc": state.get("last_transition_utc"),
        "last_transition_ny": state.get("last_transition_ny"),
        "last_transition_reason": state.get("last_transition_reason"),
        "last_transition_actor": state.get("last_transition_actor"),
        "history_count": len(history),
        "history": history,
        "allowed_transitions": sorted(_release_transition_allowed(effective_stage, s) and s or None for s in RELEASE_VALID_STAGES if _release_transition_allowed(effective_stage, s)),
    }
    out["allowed_transitions"] = [x for x in out["allowed_transitions"] if x]
    if include_gate:
        target_live = _build_release_gate_snapshot("live_guarded", include_stage_check=False)
        target_eligible = _build_release_gate_snapshot("guarded_live_eligible", include_stage_check=False)
        out["promotion_targets"] = {
            "guarded_live_eligible": {
                "ready": len(target_eligible.get("unmet_conditions") or []) == 0,
                "unmet_conditions": list(target_eligible.get("unmet_conditions") or []),
            },
            "live_guarded": {
                "ready": len(target_live.get("unmet_conditions") or []) == 0 and LIVE_TRADING_ENABLED and (not DRY_RUN),
                "unmet_conditions": list(target_live.get("unmet_conditions") or []) + (["live_env_not_armed"] if (DRY_RUN or (not LIVE_TRADING_ENABLED)) else []),
            },
        }
        out["live_activation_armed"] = bool(effective_stage == "live_guarded" and out.get("approval_armed") and LIVE_TRADING_ENABLED and (not DRY_RUN))
    return out


def _build_release_gate_snapshot(stage: str, include_stage_check: bool = True) -> dict:
    lifecycle = _paper_lifecycle_counts()
    worker_status = _worker_status_snapshot()
    reconcile = build_reconcile_snapshot()
    regime = dict(LAST_REGIME_SNAPSHOT or {})
    session = _session_boundary_snapshot()
    market_open_now = bool(session.get("market_open_now"))
    now_utc = datetime.now(tz=timezone.utc)
    last_scan_age_sec = None
    recent_market_scan_ok = False
    if LAST_SCAN.get("ts_utc"):
        try:
            scan_ts = datetime.fromisoformat(str(LAST_SCAN.get("ts_utc")))
            if scan_ts.tzinfo is None:
                scan_ts = scan_ts.replace(tzinfo=timezone.utc)
            last_scan_age_sec = max(0.0, (now_utc - scan_ts.astimezone(timezone.utc)).total_seconds())
            last_scan_reason = str(LAST_SCAN.get("reason") or "")
            recent_market_scan_ok = (
                last_scan_reason == "scan_completed"
                and last_scan_age_sec <= max(60, RELEASE_MAX_SCAN_AGE_SEC)
            )
            if (not market_open_now) and (not recent_market_scan_ok):
                recent_market_scan_ok = (
                    last_scan_reason == "outside_market_hours"
                    and last_scan_age_sec <= max(60, RELEASE_MAX_SCAN_AGE_SEC)
                )
        except Exception:
            recent_market_scan_ok = False
    unmet = []
    if include_stage_check and stage not in RELEASE_ALLOWED_LIVE_STAGES:
        unmet.append("release_stage_not_allowed")
    if stage == "emergency_disabled":
        unmet.append("release_stage_emergency_disabled")
    if KILL_SWITCH:
        unmet.append("kill_switch_on")
    if daily_halt_active():
        unmet.append("daily_halt_active")
    if READINESS_REQUIRE_WORKERS and not worker_status["scanner_running"]:
        unmet.append("scanner_worker_not_ready")
    if READINESS_REQUIRE_WORKERS and not worker_status["exit_worker_running"]:
        unmet.append("exit_worker_not_ready")
    if RELEASE_REQUIRE_REGIME_COMPLETE and not bool(regime.get("data_complete")):
        unmet.append("regime_incomplete")
    if RELEASE_REQUIRE_REGIME_FAVORABLE and not bool(regime.get("favorable")):
        unmet.append("regime_not_favorable")
    if RELEASE_REQUIRE_RECENT_MARKET_SCAN and not recent_market_scan_ok:
        unmet.append("recent_market_scan_missing")
    if lifecycle["scan_completed"] < RELEASE_MIN_COMPLETED_SCANS:
        unmet.append("insufficient_completed_scans")
    if lifecycle["candidate_selected"] < RELEASE_MIN_SELECTED_CANDIDATES:
        unmet.append("insufficient_selected_candidates")
    if lifecycle["entry_events"] < RELEASE_MIN_ENTRY_EVENTS:
        unmet.append("insufficient_entry_events")
    if lifecycle["exit_events"] < RELEASE_MIN_EXIT_EVENTS:
        unmet.append("insufficient_exit_events")
    if reconcile.get("orphan_open_order_symbols"):
        unmet.append("orphan_open_orders_present")
    if reconcile.get("plans_missing_open_order"):
        unmet.append("plans_missing_open_order")
    if reconcile.get("stale_active_plans"):
        unmet.append("stale_active_plans_present")
    if reconcile.get("partial_fill_plan_symbols"):
        unmet.append("partial_fill_aging_present")

    freshness = freshness_snapshot()
    continuity = {}
    continuity_issues = []
    continuity_error = ""
    try:
        continuity = continuity_snapshot(normalize_current=False)
        continuity_issues = list(continuity.get("issues") or [])
    except Exception as e:
        continuity_error = str(e)
        continuity = {
            "ok": False,
            "error": continuity_error,
            "issues": [
                {
                    "code": "continuity_snapshot_failed",
                    "severity": "error",
                    "details": {"error": continuity_error},
                }
            ],
            "issue_codes": ["continuity_snapshot_failed"],
        }
        continuity_issues = list(continuity.get("issues") or [])

    if continuity_issues:
        unmet.append("continuity_issues_present")
    if continuity.get("issue_codes"):
        unmet.extend(sorted(set(str(code or "") for code in continuity.get("issue_codes") or [] if code)))

    unmet = list(dict.fromkeys(str(x) for x in unmet if x))
    go_live_eligible = len(unmet) == 0
    return {
        "system_release_stage": stage,
        "session": freshness.get("session"),
        "freshness": freshness,
        "release_gate_enforced": RELEASE_GATE_ENFORCED,
        "release_allowed_live_stages": sorted(RELEASE_ALLOWED_LIVE_STAGES),
        "go_live_eligible": go_live_eligible,
        "unmet_conditions": unmet,
        "last_scan_age_sec": last_scan_age_sec,
        "recent_market_scan_ok": recent_market_scan_ok,
        "regime": {
            "known": bool(regime),
            "data_complete": bool(regime.get("data_complete")),
            "favorable": regime.get("favorable"),
            "reasons": list(regime.get("reasons") or []),
        },
        "lifecycle_counts": lifecycle,
        "worker_status": worker_status,
        "reconcile_snapshot": {
            "orphan_open_order_symbols": list(reconcile.get("orphan_open_order_symbols") or []),
            "plans_missing_open_order": list(reconcile.get("plans_missing_open_order") or []),
            "stale_active_plans": list(reconcile.get("stale_active_plans") or []),
            "partial_fill_plan_symbols": list(reconcile.get("partial_fill_plan_symbols") or []),
            "open_order_count": int(reconcile.get("open_order_count") or 0),
            "active_plan_count": int(reconcile.get("active_plan_count") or 0),
            "broker_positions_count": int(reconcile.get("broker_positions_count") or 0),
        },
        "continuity": continuity,
    }


def release_stage_transition(target_stage: str, actor: str = "system", reason: str = "") -> dict:
    global RELEASE_STATE
    target_stage = _normalize_release_stage(target_stage, default="paper")
    current = _sanitize_release_state(RELEASE_STATE or {})
    current_stage = _normalize_release_stage(current.get("current_stage") or SYSTEM_RELEASE_STAGE, default="paper")
    if not _release_transition_allowed(current_stage, target_stage):
        raise HTTPException(status_code=400, detail=f"Transition not allowed: {current_stage} -> {target_stage}")
    if RELEASE_PROMOTION_REQUIRE_READINESS and target_stage in {"guarded_live_eligible", "live_guarded"}:
        preflight = _release_workflow_snapshot(include_gate=True).get("promotion_targets", {}).get(target_stage, {})
        unmet = list(preflight.get("unmet_conditions") or [])
        if unmet:
            raise HTTPException(status_code=409, detail={"target_stage": target_stage, "unmet_conditions": unmet})
    now_utc = datetime.now(timezone.utc).isoformat()
    now_ny_iso = now_ny().isoformat()
    entry = {
        "ts_utc": now_utc,
        "ts_ny": now_ny_iso,
        "from_stage": current_stage,
        "to_stage": target_stage,
        "reason": str(reason or "manual_transition"),
        "actor": str(actor or "system"),
    }
    history = list(current.get("history") or [])
    history.append(entry)
    current.update({
        "configured_stage": _normalize_release_stage(SYSTEM_RELEASE_STAGE, default="paper"),
        "current_stage": target_stage,
        "last_transition_utc": now_utc,
        "last_transition_ny": now_ny_iso,
        "last_transition_reason": entry["reason"],
        "last_transition_actor": entry["actor"],
        "approval_status": "approved" if target_stage == "live_guarded" else ("not_required" if target_stage == "paper" else str(current.get("approval_status") or "not_required")),
        "approval_armed": bool(target_stage == "live_guarded"),
        "history": history[-max(1, RELEASE_STATE_HISTORY_LIMIT):],
        "restored_from_state": False,
    })
    RELEASE_STATE = current
    persist_release_state(reason=entry["reason"])
    return _release_workflow_snapshot(include_gate=True)


# Run startup restore only after all helper functions it depends on are defined.
startup_restore_state()


def freshness_snapshot() -> dict:
    session = _session_boundary_snapshot()
    scanner_ref = (LAST_SCANNER_TELEMETRY or {}).get("last_worker_event_utc") or (LAST_SCANNER_TELEMETRY or {}).get("last_success_utc") or (LAST_SCANNER_TELEMETRY or {}).get("last_event_utc")
    scan_source = "memory" if LAST_SCAN else ("restored" if (globals().get("SCAN_STATE_RESTORE") or {}).get("last_scan_restored") else "empty")
    regime_source = "memory" if LAST_REGIME_SNAPSHOT else ("restored" if (globals().get("REGIME_STATE_RESTORE") or {}).get("current_restored") else "empty")
    lifecycle_source = "memory" if LAST_PAPER_LIFECYCLE else ("restored" if (globals().get("PAPER_LIFECYCLE_STATE_RESTORE") or {}).get("last_event_restored") else "empty")
    scanner_source = "memory" if LAST_SCANNER_TELEMETRY else ("restored" if (globals().get("SCANNER_TELEMETRY_STATE_RESTORE") or {}).get("last_event_restored") else "empty")
    entries = {
        "last_scan": _freshness_entry("last_scan", LAST_SCAN.get("ts_utc"), source=scan_source, max_age_sec=max(60, RELEASE_MAX_SCAN_AGE_SEC), require_same_session=True, extra={"reason": LAST_SCAN.get("reason")}),
        "regime": _freshness_entry("regime", (LAST_REGIME_SNAPSHOT or {}).get("ts_utc"), source=regime_source, max_age_sec=max(60, RELEASE_MAX_SCAN_AGE_SEC), require_same_session=True, extra={"favorable": (LAST_REGIME_SNAPSHOT or {}).get("favorable"), "data_complete": (LAST_REGIME_SNAPSHOT or {}).get("data_complete")}),
        "paper_lifecycle": _freshness_entry("paper_lifecycle", (LAST_PAPER_LIFECYCLE or {}).get("ts_utc"), source=lifecycle_source, require_same_session=True, extra={"stage": (LAST_PAPER_LIFECYCLE or {}).get("stage"), "status_value": (LAST_PAPER_LIFECYCLE or {}).get("status")}),
        "scanner_telemetry": _freshness_entry("scanner_telemetry", scanner_ref, source=scanner_source, max_age_sec=max(READINESS_SCANNER_MAX_AGE_SEC, SCANNER_INTERVAL_SEC + SCANNER_TIMEOUT_SEC + max(10, SCANNER_JITTER_SEC) + 60), extra={"event": (LAST_SCANNER_TELEMETRY or {}).get("event"), "attempts_today": (LAST_SCANNER_TELEMETRY or {}).get("attempts_today")}),
        "exit_heartbeat": _freshness_entry("exit_heartbeat", LAST_EXIT_HEARTBEAT.get("ts_utc"), source="memory" if LAST_EXIT_HEARTBEAT else "empty", max_age_sec=max(READINESS_EXIT_MAX_AGE_SEC, 30)),
    }
    regime_row = entries.get("regime") or {}
    if (not bool(session.get("market_day"))) and regime_row.get("status") == "stale":
        regime_row["fresh"] = True
        regime_row["status"] = "deferred"
        regime_row["freshness_deferred"] = True
        regime_row["freshness_deferred_reason"] = session.get("market_closed_reason") or "market_closed"
        regime_row["freshness_note"] = "Regime freshness is deferred while the market is closed."
    stale = [name for name, row in entries.items() if row.get("status") == "stale"]
    missing = [name for name, row in entries.items() if row.get("status") == "missing"]
    return {
        "session": session,
        "entries": entries,
        "stale_entries": stale,
        "missing_entries": missing,
        "all_fresh": (not stale and not missing),
    }


def release_gate_status() -> dict:
    workflow = _release_workflow_snapshot(include_gate=True)
    stage = _normalize_release_stage(workflow.get("effective_stage") or SYSTEM_RELEASE_STAGE, default="paper")
    status = _build_release_gate_snapshot(stage, include_stage_check=True)
    status.update({
        "system_release_stage": stage,
        "configured_release_stage": workflow.get("configured_stage"),
        "effective_release_stage": workflow.get("effective_stage"),
        "release_workflow": workflow,
    })
    status["live_orders_permitted"] = bool(status.get("go_live_eligible") and workflow.get("live_activation_armed"))
    if workflow.get("configured_stage_drift"):
        unmet = list(status.get("unmet_conditions") or [])
        if "release_stage_config_drift" not in unmet:
            unmet.append("release_stage_config_drift")
        status["unmet_conditions"] = unmet
        status["go_live_eligible"] = False
        status["live_orders_permitted"] = False
    return status


def is_live_trading_permitted(source: str = "") -> bool:
    if DRY_RUN or (not LIVE_TRADING_ENABLED):
        return False
    if source == "worker_scan" and (not SCANNER_ALLOW_LIVE):
        return False
    if RELEASE_GATE_ENFORCED and (not release_gate_status().get("live_orders_permitted")):
        return False
    return True


def _parse_plan_opened_dt(plan: dict) -> Optional[datetime]:
    raw = str((plan or {}).get("opened_at") or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=NY_TZ)
        return dt.astimezone(NY_TZ)
    except Exception:
        return None


def plan_days_held(plan: dict) -> int:
    opened = _parse_plan_opened_dt(plan)
    if opened is None:
        return 0
    return max(0, (now_ny().date() - opened.date()).days)


def same_day_exit_blocked(plan: dict, reason: str = "") -> bool:
    if STRATEGY_MODE != "swing":
        return False
    if SWING_ALLOW_SAME_DAY_EXIT:
        return False
    if (reason or "").strip().lower() in {"kill_switch", "daily_stop_hit", "broker_reconcile_failure", "manual_emergency", "catastrophic_invalid"}:
        return False
    opened = _parse_plan_opened_dt(plan)
    if opened is None:
        return False
    return opened.date() == now_ny().date()

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


def compute_signal_rank(signal_name: str, vp_diag: dict | None = None) -> tuple[float, dict]:
    vp_diag = vp_diag or {}
    raw_score = float(vp_diag.get("score", 0.0) or 0.0)
    rank = raw_score
    family = "fallback" if "FALLBACK" in str(signal_name or "").upper() else "primary"
    components: dict[str, float] = {"raw_score": raw_score}
    if family == "primary":
        rank += SIGNAL_RANK_PRIMARY_BONUS
        components["primary_bonus"] = SIGNAL_RANK_PRIMARY_BONUS
    else:
        rank -= SIGNAL_RANK_FALLBACK_PENALTY
        components["fallback_penalty"] = -SIGNAL_RANK_FALLBACK_PENALTY
    if vp_diag.get("touched"):
        rank += SIGNAL_RANK_TOUCH_BONUS
        components["touch_bonus"] = SIGNAL_RANK_TOUCH_BONUS
    if vp_diag.get("relvol_ok"):
        rank += SIGNAL_RANK_RELVOL_BONUS
        components["relvol_bonus"] = SIGNAL_RANK_RELVOL_BONUS
    if vp_diag.get("atr_ok"):
        rank += SIGNAL_RANK_ATR_BONUS
        components["atr_bonus"] = SIGNAL_RANK_ATR_BONUS
    if vp_diag.get("micro_vol_ok"):
        rank += SIGNAL_RANK_MICRO_BONUS
        components["micro_bonus"] = SIGNAL_RANK_MICRO_BONUS
    if vp_diag.get("entry_confirm_ok"):
        rank += SIGNAL_RANK_CONFIRM_BONUS
        components["confirm_bonus"] = SIGNAL_RANK_CONFIRM_BONUS
    if vp_diag.get("last_1m_green"):
        rank += SIGNAL_RANK_GREEN_BONUS
        components["green_bonus"] = SIGNAL_RANK_GREEN_BONUS
    if vp_diag.get("higher_low_ok"):
        rank += SIGNAL_RANK_HIGHER_LOW_BONUS
        components["higher_low_bonus"] = SIGNAL_RANK_HIGHER_LOW_BONUS
    dist = abs(float(vp_diag.get("dist_to_vwap_pct", 0.0) or 0.0))
    if dist > SIGNAL_RANK_MAX_DIST_VWAP_PCT:
        rank -= SIGNAL_RANK_DISTANCE_PENALTY
        components["distance_penalty"] = -SIGNAL_RANK_DISTANCE_PENALTY
    return float(rank), {"family": family, "raw_score": raw_score, "rank_score": float(rank), "components": components}

def candidate_slots_available(extra_buffer: int = 0) -> int:
    try:
        remaining = int(MAX_OPEN_POSITIONS) - count_open_positions_allowed() - max(int(extra_buffer), 0)
        return max(0, remaining)
    except Exception:
        return max(0, int(MAX_OPEN_POSITIONS))

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


def risk_limits_ok() -> bool:
    return (not KILL_SWITCH) and (not daily_halt_active()) and (not daily_stop_hit())


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
        out = close_position(sym, reason=reason, source="risk_guard")
        if sym in TRADE_PLAN:
            TRADE_PLAN[sym]["active"] = False
        results.append({"symbol": sym, "action": "flatten", "reason": reason, **out})
    return results




# =============================
# Scanner helpers (server-side signal evaluation)
# =============================
def _session_key(dt_ny: datetime) -> str:
    return dt_ny.strftime("%Y-%m-%d")



def _dedupe_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for s in items or []:
        sym = str(s or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def _base_scanner_pool() -> list[str]:
    if SCANNER_POOL_SYMBOLS:
        return _dedupe_keep_order([s for s in SCANNER_POOL_SYMBOLS.split(",") if s.strip()])
    if SCANNER_UNIVERSE_SYMBOLS:
        return _dedupe_keep_order([s for s in SCANNER_UNIVERSE_SYMBOLS.split(",") if s.strip()])
    if ALLOWED_SYMBOLS:
        allowed = sorted(ALLOWED_SYMBOLS)
        if len(allowed) >= 15:
            return _dedupe_keep_order(allowed)
    return _dedupe_keep_order(DEFAULT_DYNAMIC_POOL)


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

    if SCANNER_UNIVERSE_PROVIDER == "dynamic":
        base = _base_scanner_pool()
        top_n = max(1, int(SCANNER_DYNAMIC_TOP_N or SCANNER_MAX_SYMBOLS_PER_CYCLE or 20))
        fetch_n = min(len(base), max(top_n * 2, top_n))
        return base[:fetch_n]

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


def fetch_daily_bars_multi(symbols: list[str], lookback_days: int = 90) -> dict[str, list[dict]]:
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=max(5, int(lookback_days)))
    req = StockBarsRequest(symbol_or_symbols=list(symbols or []), timeframe=TimeFrame.Day, start=start, end=end, adjustment=ADJUSTMENT, feed=DATA_FEED)
    bars = data_client.get_stock_bars(req)
    out: dict[str, list[dict]] = {}
    data = getattr(bars, 'data', {}) or {}
    for symbol in symbols or []:
        rows = []
        for b in data.get(symbol, []) or []:
            try:
                ts = b.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                rows.append({
                    'ts_utc': ts,
                    'ts_ny': ts.astimezone(NY_TZ),
                    'open': float(b.open),
                    'high': float(b.high),
                    'low': float(b.low),
                    'close': float(b.close),
                    'volume': float(getattr(b, 'volume', 0) or 0),
                    'vwap': float(getattr(b, 'vwap', 0) or 0),
                })
            except Exception:
                continue
        out[symbol] = rows
    return out

def _sma(values: list[float], length: int) -> float | None:
    if len(values) < max(1, int(length)):
        return None
    win = values[-int(length):]
    return sum(win) / len(win) if win else None

def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)

def _current_equity_estimate() -> float:
    snap = get_buying_power_snapshot()
    return _safe_float(snap.get('equity') or 0.0)

def _plan_is_recovered(plan: dict | None) -> bool:
    p = plan or {}
    signal = str(p.get("signal") or "").upper()
    return bool(p.get("recovered")) or signal == "RECOVERED"


def _current_portfolio_exposure_breakdown() -> dict:
    total = 0.0
    strategy_managed = 0.0
    recovered = 0.0
    unmanaged = 0.0
    by_symbol: dict[str, float] = {}
    by_symbol_class: dict[str, str] = {}
    recovered_symbols: list[str] = []
    strategy_symbols: list[str] = []
    unmanaged_symbols: list[str] = []
    for p in list_open_positions_details_allowed():
        sym = str(p.get('symbol') or '').upper()
        qty = abs(_safe_float(p.get('qty') or 0.0))
        px = _safe_float(p.get('avg_entry_price') or 0.0)
        notion = qty * px
        total += notion
        if sym:
            by_symbol[sym] = notion
        plan = (TRADE_PLAN or {}).get(sym) or {}
        if _plan_is_recovered(plan):
            recovered += notion
            if sym:
                by_symbol_class[sym] = 'recovered'
                recovered_symbols.append(sym)
        elif bool(plan.get('active')):
            strategy_managed += notion
            if sym:
                by_symbol_class[sym] = 'strategy_managed'
                strategy_symbols.append(sym)
        else:
            unmanaged += notion
            if sym:
                by_symbol_class[sym] = 'unmanaged'
                unmanaged_symbols.append(sym)
    return {
        'total': total,
        'strategy_managed': strategy_managed,
        'recovered': recovered,
        'unmanaged': unmanaged,
        'by_symbol': by_symbol,
        'by_symbol_class': by_symbol_class,
        'recovered_symbols': recovered_symbols,
        'strategy_symbols': strategy_symbols,
        'unmanaged_symbols': unmanaged_symbols,
    }


def _current_portfolio_exposure() -> tuple[float, dict[str, float]]:
    b = _current_portfolio_exposure_breakdown()
    return float(b.get('total') or 0.0), dict(b.get('by_symbol') or {})

def _same_day_entry_stats() -> dict:
    today = now_ny().date()
    counted = 0
    skipped_recovered = 0
    skipped_inactive = 0
    skipped_not_today = 0
    items = []
    for symbol, plan in (TRADE_PLAN or {}).items():
        p = plan or {}
        active = bool(p.get("active"))
        signal = str(p.get("signal") or "").upper()
        recovered = bool(p.get("recovered")) or signal == "RECOVERED"
        dt = _parse_plan_opened_dt(p)
        opened_today = bool(dt and dt.date() == today)
        counted_here = False
        if not active:
            skipped_inactive += 1
        elif recovered:
            skipped_recovered += 1
        elif not opened_today:
            skipped_not_today += 1
        else:
            counted += 1
            counted_here = True
        items.append({
            "symbol": str(symbol or "").upper(),
            "active": active,
            "recovered": recovered,
            "signal": str(p.get("signal") or ""),
            "opened_at": dt.isoformat() if dt else None,
            "opened_today": opened_today,
            "counted": counted_here,
        })
    return {
        "today_ny": today.isoformat(),
        "counted": counted,
        "total_active": sum(1 for plan in (TRADE_PLAN or {}).values() if bool((plan or {}).get("active"))),
        "skipped_recovered": skipped_recovered,
        "skipped_inactive": skipped_inactive,
        "skipped_not_today": skipped_not_today,
        "items": items[:50],
    }


def _same_day_entry_count() -> int:
    return int((_same_day_entry_stats() or {}).get("counted") or 0)


def _has_pending_entry_plan(symbol: str) -> bool:
    p = (TRADE_PLAN or {}).get(str(symbol or "").upper()) or {}
    if not p:
        return False
    if bool(p.get("active")):
        return True
    status = str(p.get("order_status") or "").lower()
    return status in {"submitted", "new", "accepted", "pending_new", "partially_filled"}


def _normalize_correlation_groups_raw(raw: str) -> str:
    raw_s = str(raw or '').replace("\r", "\n")
    raw_s = raw_s.replace("\n", "|").replace(";", "|")
    raw_s = re.sub(r"\s*\|\s*", "|", raw_s)
    raw_s = re.sub(r"\s*,\s*", ",", raw_s)
    raw_s = re.sub(r"\|{2,}", "|", raw_s)
    return raw_s.strip(" |,\t")


def _parse_correlation_groups(raw: str) -> dict[str, int]:
    mapping: dict[str, int] = {}
    normalized = _normalize_correlation_groups_raw(raw)
    groups = [g.strip() for g in normalized.split('|') if g.strip()]
    for idx, group in enumerate(groups, start=1):
        for sym in [s.strip().upper() for s in group.split(',') if s.strip()]:
            mapping[sym] = idx
    return mapping


def _correlation_groups_list(raw: str | None = None) -> list[list[str]]:
    normalized = _normalize_correlation_groups_raw(raw if raw is not None else SWING_CORRELATION_GROUPS)
    out: list[list[str]] = []
    for group in [g.strip() for g in normalized.split('|') if g.strip()]:
        syms = [s.strip().upper() for s in group.split(',') if s.strip()]
        if syms:
            out.append(syms)
    return out


def _symbol_group_id(symbol: str) -> int | None:
    return _parse_correlation_groups(SWING_CORRELATION_GROUPS).get(str(symbol or '').upper())


def _open_group_position_count(symbol: str) -> int:
    gid = _symbol_group_id(symbol)
    if gid is None:
        return 0
    count = 0
    seen = set()
    for p in list_open_positions_details_allowed():
        sym = str((p or {}).get('symbol') or '').upper()
        if not sym or sym in seen:
            continue
        if _symbol_group_id(sym) == gid:
            count += 1
            seen.add(sym)
    for sym, plan in (TRADE_PLAN or {}).items():
        s = str(sym or '').upper()
        if not s or s in seen:
            continue
        if isinstance(plan, dict) and plan.get('active') and _symbol_group_id(s) == gid:
            count += 1
            seen.add(s)
    return count


def _build_swing_regime(index_bars: list[dict], daily_map: dict[str, list[dict]], symbols: list[str]) -> dict:
    closes = [_safe_float(b.get('close')) for b in (index_bars or []) if _safe_float(b.get('close'))]
    idx_close = closes[-1] if closes else None
    idx_fast = _sma(closes, SWING_REGIME_FAST_MA_DAYS) if closes else None
    idx_slow = _sma(closes, SWING_REGIME_SLOW_MA_DAYS) if closes else None
    index_trend_ok = bool(idx_close and idx_fast and idx_slow and idx_close > idx_fast > idx_slow) if idx_close and idx_fast and idx_slow else None
    breadth_total = 0
    breadth_pass = 0
    ret_pass = 0
    ret_lookback = max(2, int(REGIME_BREADTH_RETURN_LOOKBACK_DAYS))
    min_seq_needed = max(SWING_REGIME_SLOW_MA_DAYS + 1, ret_lookback + 1)
    for sym in (symbols or []):
        bars = daily_map.get(sym, []) or []
        seq = [_safe_float(b.get('close')) for b in bars if _safe_float(b.get('close'))]
        if len(seq) < min_seq_needed:
            continue
        breadth_total += 1
        slow = _sma(seq, SWING_REGIME_SLOW_MA_DAYS)
        if slow and seq[-1] > slow:
            breadth_pass += 1
        ref = seq[-(ret_lookback + 1)]
        if ref > 0 and (seq[-1] / ref - 1.0) > 0:
            ret_pass += 1
    breadth = (breadth_pass / breadth_total) if breadth_total else None
    ret_breadth = (ret_pass / breadth_total) if breadth_total else None
    data_complete = bool(index_trend_ok is not None and breadth is not None and breadth_total >= REGIME_MIN_SYMBOLS_FOR_BREADTH)
    favorable = None
    reasons = []
    if SWING_REGIME_FILTER_ENABLED:
        if index_trend_ok is None:
            reasons.append('index_trend_unknown')
        if breadth is None or breadth_total < REGIME_MIN_SYMBOLS_FOR_BREADTH:
            reasons.append('breadth_unknown')
        if REGIME_REQUIRE_COMPLETE_DATA and not data_complete:
            favorable = None
        elif index_trend_ok is False:
            favorable = False
            reasons.append('index_trend_weak')
        elif breadth is not None and breadth < SWING_REGIME_MIN_BREADTH:
            favorable = False
            reasons.append('breadth_below_min')
        elif index_trend_ok is True and breadth is not None:
            favorable = True
        else:
            favorable = None
    else:
        favorable = True
    score = 0.0
    if index_trend_ok is True:
        score += 50.0
    if breadth is not None:
        score += max(0.0, min(30.0, breadth * 30.0))
    if ret_breadth is not None:
        score += max(0.0, min(20.0, ret_breadth * 20.0))
    return {
        'ts_utc': datetime.now(timezone.utc).isoformat(),
        'enabled': SWING_REGIME_FILTER_ENABLED,
        'index_symbol': SWING_INDEX_SYMBOL,
        'index_close': round(idx_close, 4) if idx_close else None,
        'index_fast_ma': round(idx_fast, 4) if idx_fast else None,
        'index_slow_ma': round(idx_slow, 4) if idx_slow else None,
        'index_trend_ok': index_trend_ok,
        'breadth': round(breadth, 4) if breadth is not None else None,
        'ret_breadth': round(ret_breadth, 4) if ret_breadth is not None else None,
        'breadth_total': int(breadth_total),
        'favorable': favorable,
        'score': round(score, 2),
        'reasons': reasons,
        'data_complete': data_complete,
    }

def evaluate_daily_breakout_candidate(symbol: str, bars: list[dict], index_aligned: bool | None = None) -> dict:
    candidate = {
        'symbol': symbol,
        'strategy': SWING_STRATEGY_NAME,
        'scan_ts_utc': datetime.now(timezone.utc).isoformat(),
        'eligible': False,
        'rejection_reasons': [],
    }
    closes = [_safe_float(b.get('close')) for b in bars]
    highs = [_safe_float(b.get('high')) for b in bars]
    lows = [_safe_float(b.get('low')) for b in bars]
    vols = [_safe_float(b.get('volume')) for b in bars]
    need = max(SWING_SLOW_MA_DAYS + 5, SWING_BREAKOUT_LOOKBACK_DAYS + 2, 25)
    if len(closes) < need:
        candidate['rejection_reasons'].append('insufficient_daily_bars')
        return candidate
    close = closes[-1]
    prev_close = closes[-2]
    high = highs[-1]
    low = lows[-1]
    fast_ma = _sma(closes, SWING_FAST_MA_DAYS)
    slow_ma = _sma(closes, SWING_SLOW_MA_DAYS)
    avg_dollar_vol_20 = sum((closes[-20+i] * vols[-20+i]) for i in range(20)) / 20.0
    ret_20 = (close / closes[-21] - 1.0) if len(closes) >= 21 and closes[-21] > 0 else 0.0
    breakout_ref = max(highs[-(SWING_BREAKOUT_LOOKBACK_DAYS+1):-1])
    trailing_low = min(lows[-5:])
    close_to_high = (close / max(high, 1e-9))
    breakout_distance = (close / max(breakout_ref, 1e-9)) - 1.0
    range_pct = (high - low) / max(close, 1e-9)
    stop_price = min(trailing_low, breakout_ref * (1.0 - SWING_BREAKOUT_BUFFER_PCT))
    risk_per_share = max(close - stop_price, close * 0.0025)
    target_price = close + (risk_per_share * SWING_TARGET_R_MULT)
    requested_qty = min(MAX_QTY, max(MIN_QTY, round(RISK_DOLLARS / max(risk_per_share, 1e-9), 2)))
    affordable = clip_qty_for_affordability(close, requested_qty)
    est_qty = float(affordable.get('submitted_qty') or 0.0)
    score = 0.0
    if close >= SWING_MIN_PRICE:
        score += 10
    else:
        candidate['rejection_reasons'].append('price_below_min')
    if avg_dollar_vol_20 >= SWING_MIN_AVG_DOLLAR_VOLUME:
        score += min(20.0, avg_dollar_vol_20 / SWING_MIN_AVG_DOLLAR_VOLUME * 10.0)
    else:
        candidate['rejection_reasons'].append('avg_dollar_volume_below_min')
    if fast_ma and slow_ma and close > fast_ma > slow_ma:
        score += 25
    else:
        candidate['rejection_reasons'].append('trend_filter_failed')
    if ret_20 >= SWING_MIN_20D_RETURN_PCT:
        score += min(20.0, ret_20 * 200.0)
    else:
        candidate['rejection_reasons'].append('return_20d_below_min')
    if close_to_high >= SWING_BREAKOUT_MIN_CLOSE_TO_HIGH_PCT:
        score += 12
    else:
        candidate['rejection_reasons'].append('close_not_near_high')
    if breakout_distance >= -SWING_BREAKOUT_BUFFER_PCT:
        score += 18
    else:
        candidate['rejection_reasons'].append('too_far_below_breakout')
    score += max(0.0, min(10.0, range_pct * 100.0))
    if SWING_REQUIRE_INDEX_ALIGNMENT and index_aligned is False:
        candidate['rejection_reasons'].append('index_alignment_failed')
    candidate.update({
        'close': round(close, 4),
        'prev_close': round(prev_close, 4),
        'high': round(high, 4),
        'low': round(low, 4),
        'fast_ma': round(fast_ma, 4) if fast_ma else None,
        'slow_ma': round(slow_ma, 4) if slow_ma else None,
        'avg_dollar_volume_20d': round(avg_dollar_vol_20, 2),
        'return_20d_pct': round(ret_20 * 100.0, 3),
        'close_to_high_pct': round(close_to_high * 100.0, 3),
        'breakout_level': round(breakout_ref, 4),
        'breakout_distance_pct': round(breakout_distance * 100.0, 3),
        'range_pct': round(range_pct * 100.0, 3),
        'stop_price': round(stop_price, 4),
        'target_price': round(target_price, 4),
        'risk_per_share': round(risk_per_share, 4),
        'requested_qty': round(requested_qty, 2),
        'estimated_qty': round(est_qty, 2),
        'rank_score': round(score, 4),
        'signal': 'daily_breakout',
        'side': 'buy',
    })
    candidate['eligible'] = len(candidate['rejection_reasons']) == 0 and est_qty >= max(MIN_AFFORDABLE_QTY, MIN_QTY)
    if not candidate['eligible'] and est_qty < max(MIN_AFFORDABLE_QTY, MIN_QTY):
        candidate['rejection_reasons'].append('insufficient_buying_power')
    return candidate

def _index_alignment_ok(index_bars: list[dict]) -> bool | None:
    closes = [_safe_float(b.get('close')) for b in index_bars]
    if len(closes) < max(25, SWING_SLOW_MA_DAYS + 2):
        return None
    fast_ma = _sma(closes, SWING_FAST_MA_DAYS)
    slow_ma = _sma(closes, SWING_SLOW_MA_DAYS)
    if not fast_ma or not slow_ma:
        return None
    return bool(closes[-1] > fast_ma > slow_ma)

def run_swing_daily_scan(effective_dry_run: bool, set_last_scan_fn, elapsed_ms_fn, reconcile_actions: list | None = None) -> dict:
    reconcile_actions = reconcile_actions or []
    syms = universe_symbols()
    if SWING_INDEX_SYMBOL and SWING_INDEX_SYMBOL not in syms:
        syms_for_fetch = syms + [SWING_INDEX_SYMBOL]
    else:
        syms_for_fetch = list(syms)
    lookback_days = max(int(SCANNER_LOOKBACK_DAYS or 20) + 40, SWING_REGIME_SLOW_MA_DAYS + REGIME_BREADTH_RETURN_LOOKBACK_DAYS + 30, SWING_SLOW_MA_DAYS + SWING_BREAKOUT_LOOKBACK_DAYS + 20)
    daily_map = fetch_daily_bars_multi(syms_for_fetch, lookback_days=lookback_days)
    index_ok = _index_alignment_ok(daily_map.get(SWING_INDEX_SYMBOL, [])) if SWING_REQUIRE_INDEX_ALIGNMENT else None
    regime = _build_swing_regime(daily_map.get(SWING_INDEX_SYMBOL, []), daily_map, syms)
    LAST_REGIME_SNAPSHOT.clear()
    LAST_REGIME_SNAPSHOT.update(regime)
    REGIME_HISTORY.append(dict(regime))
    if len(REGIME_HISTORY) > SWING_REGIME_HISTORY_SIZE:
        del REGIME_HISTORY[: len(REGIME_HISTORY) - SWING_REGIME_HISTORY_SIZE]
    persist_regime_runtime_state(reason="run_swing_daily_scan")
    exposure = _current_portfolio_exposure_breakdown()
    open_total = float(exposure.get('total') or 0.0)
    open_strategy = float(exposure.get('strategy_managed') or 0.0)
    open_recovered = float(exposure.get('recovered') or 0.0)
    open_unmanaged = float(exposure.get('unmanaged') or 0.0)
    open_by_symbol = dict(exposure.get('by_symbol') or {})
    equity = max(0.0, _current_equity_estimate())
    portfolio_cap = equity * SWING_MAX_PORTFOLIO_EXPOSURE_PCT if equity > 0 else 0.0
    symbol_cap = equity * SWING_MAX_SYMBOL_EXPOSURE_PCT if equity > 0 else 0.0
    block_total_cap = bool(portfolio_cap > 0 and open_total >= portfolio_cap)
    block_strategy_cap = bool(portfolio_cap > 0 and open_strategy >= portfolio_cap)
    portfolio_cap_blocked = False
    if SWING_PORTFOLIO_CAP_BLOCK_MODE == 'total':
        portfolio_cap_blocked = block_total_cap
    elif SWING_PORTFOLIO_CAP_BLOCK_MODE == 'strategy':
        portfolio_cap_blocked = block_strategy_cap
    elif SWING_PORTFOLIO_CAP_BLOCK_MODE == 'both':
        portfolio_cap_blocked = block_total_cap or block_strategy_cap
    else:
        portfolio_cap_blocked = False
    new_entries_globally_blocked = False
    global_block_reasons = []
    if daily_halt_active() or daily_stop_hit():
        new_entries_globally_blocked = True
        global_block_reasons.append('daily_halt_active')
    if regime.get('favorable') is False and not SWING_ALLOW_NEW_ENTRIES_IN_WEAK_TAPE:
        new_entries_globally_blocked = True
        global_block_reasons.append('weak_tape')
    if portfolio_cap_blocked:
        new_entries_globally_blocked = True
        if block_total_cap:
            global_block_reasons.append('portfolio_already_over_cap_total')
        if block_strategy_cap:
            global_block_reasons.append('portfolio_already_over_cap_strategy')
    candidates = []
    rejection_counts = Counter()
    for sym in syms:
        c = evaluate_daily_breakout_candidate(sym, daily_map.get(sym, []), index_ok)
        if _has_pending_entry_plan(sym):
            c['eligible'] = False
            c.setdefault('rejection_reasons', []).append('plan_or_pending_entry_exists')
        qty_signed, _ = get_position(sym)
        if qty_signed != 0:
            c['eligible'] = False
            c.setdefault('rejection_reasons', []).append('position_already_open')
        projected_notional = _safe_float(c.get('estimated_qty')) * _safe_float(c.get('close'))
        if c.get('eligible') and new_entries_globally_blocked:
            c['eligible'] = False
            c.setdefault('rejection_reasons', []).extend(global_block_reasons)
        group_count = _open_group_position_count(sym)
        if c.get('eligible') and SWING_MAX_GROUP_POSITIONS > 0 and group_count >= SWING_MAX_GROUP_POSITIONS:
            c['eligible'] = False
            c.setdefault('rejection_reasons', []).append('correlation_group_limit')
        c['correlation_group_id'] = _symbol_group_id(sym)
        c['correlation_group_open_count'] = int(group_count)
        if c.get('eligible') and symbol_cap > 0 and projected_notional + open_by_symbol.get(sym, 0.0) > symbol_cap:
            c['eligible'] = False
            c.setdefault('rejection_reasons', []).append('symbol_exposure_limit')
        if c.get('eligible') and portfolio_cap > 0 and open_total + projected_notional > portfolio_cap:
            c['eligible'] = False
            c.setdefault('rejection_reasons', []).append('portfolio_exposure_limit')
        for r in c.get('rejection_reasons', []):
            rejection_counts[str(r)] += 1
        candidates.append(c)
    candidates.sort(key=lambda x: float(x.get('rank_score', 0.0) or 0.0), reverse=True)
    approved = [c for c in candidates if c.get('eligible')]
    max_new_entries = max(0, min(int(SWING_MAX_NEW_ENTRIES_PER_DAY), int(candidate_slots_available()), int(SCANNER_MAX_ENTRIES_PER_SCAN)))
    if regime.get('favorable') is False:
        max_new_entries = min(max_new_entries, max(0, int(SWING_WEAK_TAPE_MAX_NEW_ENTRIES)))
    same_day_stats = _same_day_entry_stats()
    remaining_today = max(0, SWING_MAX_NEW_ENTRIES_PER_DAY - int(same_day_stats.get('counted') or 0))
    max_new_entries = min(max_new_entries, remaining_today)
    selected = approved[:max_new_entries]
    would_submit = []
    for c in selected:
        meta = {
            'rank_score': c.get('rank_score'),
            'strategy_name': c.get('strategy'),
            'breakout_level': c.get('breakout_level'),
            'stop_price': c.get('stop_price'),
            'target_price': c.get('target_price'),
            'risk_per_share': c.get('risk_per_share'),
        }
        if SCANNER_ALLOW_LIVE and (not SCANNER_DRY_RUN) and (not effective_dry_run):
            resp = submit_scan_trade(c['symbol'], 'buy', c.get('signal') or 'daily_breakout', meta=meta)
            would_submit.append({'symbol': c['symbol'], 'signal': c.get('signal'), 'rank_score': c.get('rank_score'), **resp})
        else:
            resp = execute_entry_signal(c['symbol'], 'buy', c.get('signal') or 'daily_breakout', 'worker_scan', meta=meta)
            would_submit.append({'symbol': c['symbol'], 'signal': c.get('signal'), 'rank_score': c.get('rank_score'), **resp})
    LAST_SWING_CANDIDATES.clear()
    LAST_SWING_CANDIDATES.extend(candidates[: max(1, min(len(candidates), SWING_MAX_CANDIDATES))])
    CANDIDATE_HISTORY.append({
        'ts_utc': datetime.now(timezone.utc).isoformat(),
        'strategy_name': SWING_STRATEGY_NAME,
        'index_symbol': SWING_INDEX_SYMBOL,
        'index_alignment_ok': index_ok,
        'regime': dict(regime),
        'candidates': LAST_SWING_CANDIDATES.copy(),
        'selected': [c.get('symbol') for c in selected],
        'rejection_counts': dict(rejection_counts),
    })
    if len(CANDIDATE_HISTORY) > CANDIDATE_HISTORY_SIZE:
        del CANDIDATE_HISTORY[: len(CANDIDATE_HISTORY) - CANDIDATE_HISTORY_SIZE]
    summary = {
        'strategy_name': SWING_STRATEGY_NAME,
        'scan_reason': None,
        'index_symbol': SWING_INDEX_SYMBOL,
        'index_alignment_ok': index_ok,
        'regime': dict(regime),
        'candidates_total': len(candidates),
        'eligible_total': len(approved),
        'selected_total': len(selected),
        'top_candidates': LAST_SWING_CANDIDATES[:5],
        'top_rejection_reasons': [{
            'reason': k, 'count': int(v)
        } for k, v in rejection_counts.most_common(10)],
        'portfolio_exposure': round(open_total, 2),
        'strategy_portfolio_exposure': round(open_strategy, 2),
        'recovered_portfolio_exposure': round(open_recovered, 2),
        'unmanaged_portfolio_exposure': round(open_unmanaged, 2),
        'portfolio_exposure_cap': round(portfolio_cap, 2),
        'symbol_exposure_cap': round(symbol_cap, 2),
        'portfolio_cap_block_mode': SWING_PORTFOLIO_CAP_BLOCK_MODE,
        'remaining_new_entries_today': int(remaining_today),
        'new_entries_globally_blocked': bool(new_entries_globally_blocked),
        'global_block_reasons': list(dict.fromkeys(global_block_reasons)),
        'recovered_symbols': list(exposure.get('recovered_symbols') or []),
        'strategy_symbols': list(exposure.get('strategy_symbols') or []),
        'unmanaged_symbols': list(exposure.get('unmanaged_symbols') or []),
    }
    duration_ms = elapsed_ms_fn()
    set_last_scan_fn(skipped=False, reason='scan_completed', scanned=len(syms), signals=len(approved), would_trade=len(selected), blocked=max(0, len(candidates)-len(approved)), duration_ms=duration_ms, summary=summary)
    try:
        _record_paper_lifecycle(
            stage='scan',
            status='completed',
            details={
                'scan_reason': summary.get('scan_reason'),
                'candidates_total': int(summary.get('candidates_total') or 0),
                'eligible_total': int(summary.get('eligible_total') or 0),
                'selected_total': int(summary.get('selected_total') or 0),
                'global_block_reasons': list(summary.get('global_block_reasons') or []),
                'regime_favorable': summary.get('regime', {}).get('favorable') if isinstance(summary.get('regime'), dict) else None,
                'regime_data_complete': summary.get('regime', {}).get('data_complete') if isinstance(summary.get('regime'), dict) else None,
            }
        )
        for sel in (selected or []):
            _record_paper_lifecycle(
                stage='candidate',
                status='selected',
                symbol=str(sel.get('symbol') or ''),
                details={
                    'signal': sel.get('signal'),
                    'rank_score': sel.get('rank_score'),
                    'estimated_qty': sel.get('estimated_qty'),
                    'close': sel.get('close'),
                }
            )
    except Exception:
        pass
    try:
        SCAN_HISTORY.append({
            'ts_utc': datetime.now(timezone.utc).isoformat(),
            'universe_provider': SCANNER_UNIVERSE_PROVIDER,
            'symbols': syms,
            'scanned': len(syms),
            'signals': len(approved),
            'would_trade': len(selected),
            'blocked': max(0, len(candidates)-len(approved)),
            'duration_ms': duration_ms,
            'summary': summary,
            'results': LAST_SWING_CANDIDATES.copy(),
            'candidate_slots': candidate_slots_available(),
            'ignored_ranked_out': [c for c in candidates if not c.get('eligible')][:20],
            'would_submit': would_submit,
        })
        if len(SCAN_HISTORY) > SCAN_HISTORY_SIZE:
            del SCAN_HISTORY[: len(SCAN_HISTORY) - SCAN_HISTORY_SIZE]
    except Exception:
        pass
    return {
        'ok': True,
        'scanner': {
            'enabled': SCANNER_ENABLED,
            'dry_run': SCANNER_DRY_RUN,
            'allow_live': SCANNER_ALLOW_LIVE,
            'effective_dry_run': effective_dry_run,
            'universe_provider': SCANNER_UNIVERSE_PROVIDER,
            'symbols_scanned': len(syms),
            'signals': len(approved),
            'would_trade': len(selected),
            'blocked': max(0, len(candidates)-len(approved)),
            'duration_ms': duration_ms,
            'summary': summary,
        },
        'reconcile': reconcile_actions,
        'would_submit': would_submit,
        'results': LAST_SWING_CANDIDATES[:SWING_MAX_CANDIDATES],
    }

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
    min_bars = max(12, int(VWAP_PB_MIN_BARS_5M))
    out["min_bars_5m"] = min_bars
    out["min_bars_5m_source"] = "env" if os.getenv("VWAP_PB_MIN_BARS_5M") is not None else "default"
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

    trs: list[float] = []
    for i, (h, l, c) in enumerate(zip(highs, lows, closes)):
        prev_c_i = closes[i - 1] if i > 0 else c
        tr = max(h - l, abs(h - prev_c_i), abs(l - prev_c_i))
        trs.append(float(tr))
    atr_window = min(5, len(trs))
    atr_5m = _mean(trs[-atr_window:]) if atr_window else 0.0
    atr_pct = (atr_5m / max(price, 1e-9)) if price else 0.0
    atr_ok = float(VWAP_PB_MIN_5M_ATR_PCT) <= atr_pct <= float(VWAP_PB_MAX_5M_ATR_PCT)

    day_high = max(highs) if highs else price
    day_low = min(lows) if lows else price
    day_range_pct = ((day_high - day_low) / max(price, 1e-9)) if price else 0.0
    day_range_ok = day_range_pct >= float(VWAP_PB_MIN_DAY_RANGE_PCT)

    bars_1m = bars_today[-20:] if len(bars_today) >= 20 else list(bars_today)
    last_1m = bars_1m[-1] if bars_1m else None
    prev_1m = bars_1m[-2] if len(bars_1m) >= 2 else None
    raw_last_1m_green = True
    raw_higher_low_ok = True
    raw_entry_confirm_ok = True
    if last_1m:
        raw_last_1m_green = float(last_1m.get("close") or 0.0) >= float(last_1m.get("open") or 0.0)
    if prev_1m and last_1m:
        raw_higher_low_ok = float(last_1m.get("low") or 0.0) >= float(prev_1m.get("low") or 0.0)
        confirm_level = float(prev_1m.get("high") or 0.0) * (1.0 + float(VWAP_PB_ENTRY_CONFIRM_BUFFER_PCT))
        raw_entry_confirm_ok = float(last_1m.get("close") or 0.0) >= confirm_level

    micro_checks = []
    if VWAP_PB_REQUIRE_GREEN_LAST_1M:
        micro_checks.append(("green", bool(raw_last_1m_green)))
    if VWAP_PB_REQUIRE_HIGHER_LOW:
        micro_checks.append(("higher_low", bool(raw_higher_low_ok)))
    if VWAP_PB_ENTRY_CONFIRM_ABOVE_PRIOR_1M_HIGH:
        micro_checks.append(("entry_confirm", bool(raw_entry_confirm_ok)))

    last_1m_green = bool(raw_last_1m_green) if VWAP_PB_REQUIRE_GREEN_LAST_1M else True
    higher_low_ok = bool(raw_higher_low_ok) if VWAP_PB_REQUIRE_HIGHER_LOW else True
    entry_confirm_ok = bool(raw_entry_confirm_ok) if VWAP_PB_ENTRY_CONFIRM_ABOVE_PRIOR_1M_HIGH else True

    micro_pass_count = sum(1 for _, ok in micro_checks if ok)
    micro_enabled_checks = len(micro_checks)
    micro_mode = str(VWAP_PB_MICRO_CONFIRM_MODE or "soft2").strip().lower()
    if micro_enabled_checks == 0 or micro_mode == "off":
        micro_confirm_ok = True
    elif micro_mode in {"strict", "all"}:
        micro_confirm_ok = all(ok for _, ok in micro_checks)
    elif micro_mode in {"soft1", "one"}:
        micro_confirm_ok = micro_pass_count >= 1
    else:
        required_passes = max(1, min(int(VWAP_PB_SOFT_CONFIRM_MIN_PASSES), micro_enabled_checks))
        micro_confirm_ok = micro_pass_count >= required_passes

    recent_1m_vol = _mean([float(b.get("volume") or 0.0) for b in bars_1m[-3:]]) if len(bars_1m) >= 3 else 0.0
    baseline_1m_vol = _mean([float(b.get("volume") or 0.0) for b in bars_1m[-13:-3]]) if len(bars_1m) >= 13 else _mean([float(b.get("volume") or 0.0) for b in bars_1m[:-3]])
    recent_1m_vol_ratio = recent_1m_vol / max(baseline_1m_vol, 1.0) if baseline_1m_vol > 0 else 1.0
    micro_vol_ok = recent_1m_vol_ratio >= float(VWAP_PB_MIN_RECENT_1M_VOL_RATIO)

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
    if atr_ok:
        score += 8.0
    if day_range_ok:
        score += 6.0
    if micro_vol_ok:
        score += 6.0
    if last_1m_green:
        score += 4.0
    if higher_low_ok:
        score += 4.0
    if entry_confirm_ok:
        score += 6.0
    score += max(0.0, min(relvol, 2.5)) * 6.0
    score += max(0.0, min(recent_1m_vol_ratio, 2.0)) * 4.0
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
    if not atr_ok:
        component_reasons.append("atr_regime_fail")
    if not day_range_ok:
        component_reasons.append("day_range_too_small")
    if not micro_vol_ok:
        component_reasons.append("recent_1m_volume_fail")
    if not last_1m_green:
        component_reasons.append("last_1m_not_green")
    if not higher_low_ok:
        component_reasons.append("micro_higher_low_fail")
    if not entry_confirm_ok:
        component_reasons.append("entry_confirm_fail")
    if not momentum_ok:
        component_reasons.append("bounce_not_confirmed")
    if not micro_confirm_ok:
        component_reasons.append("micro_confirm_fail")

    macro_blockers = [r for r in component_reasons if r in {"price_below_vwap", "ema_stack_fail", "price_below_ema_fast", "slope_fail", "touch_fail", "not_back_above_vwap", "too_extended_from_vwap", "relvol_fail", "atr_regime_fail", "day_range_too_small", "bounce_not_confirmed"}]
    micro_blockers = [r for r in component_reasons if r in {"recent_1m_volume_fail", "last_1m_not_green", "micro_higher_low_fail", "entry_confirm_fail", "micro_confirm_fail"}]

    near = bool(
        (permissive_trend_ok and touched and (not regained_vwap))
        or (touched and regained_vwap and score >= float(VWAP_PB_NEAR_MISS_SCORE_MIN))
        or (price_above_vwap and ema_stack_ok and abs(dist_to_vwap_pct) <= touch_band * 1.35)
    )
    fallback_touch_ok = bool(touched or (bool(VWAP_PB_FALLBACK_ALLOW_TOUCHLESS) and abs(dist_to_vwap_pct) <= touch_band * 1.10))
    fallback_slope_ok = bool(
        ema_slope >= float(VWAP_PB_FALLBACK_MIN_EMA_SLOPE)
        and vwap_slope >= float(VWAP_PB_FALLBACK_MIN_VWAP_SLOPE)
    )
    fallback_bounce_ok = bool(
        regained_vwap and (
            momentum_ok
            or (
                bool(VWAP_PB_FALLBACK_ALLOW_BOUNCE_SLACK)
                and dist_to_vwap_pct >= -float(VWAP_PB_FALLBACK_BOUNCE_SLACK_PCT)
            )
        )
    )
    fallback_distance_ok = bool(abs(dist_to_vwap_pct) <= float(VWAP_PB_FALLBACK_MAX_DIST_VWAP_PCT))
    fallback_ready = bool(
        VWAP_PB_ALLOW_NEAR_MISS_FALLBACK
        and permissive_trend_ok
        and fallback_touch_ok
        and extension_ok
        and near
        and score >= float(VWAP_PB_FALLBACK_SIGNAL_SCORE_MIN)
    )
    fallback_blockers = []
    if not fallback_touch_ok:
        fallback_blockers.append("fallback_touch_fail")
    if not fallback_slope_ok:
        fallback_blockers.append("fallback_slope_fail")
    if not fallback_bounce_ok:
        fallback_blockers.append("fallback_bounce_fail")
    if not fallback_distance_ok:
        fallback_blockers.append("fallback_distance_fail")

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
        "atr_5m": round(atr_5m, 4),
        "atr_pct": round(atr_pct * 100.0, 3),
        "atr_ok": bool(atr_ok),
        "day_range_pct": round(day_range_pct * 100.0, 3),
        "day_range_ok": bool(day_range_ok),
        "recent_1m_vol_ratio": round(recent_1m_vol_ratio, 3),
        "micro_vol_ok": bool(micro_vol_ok),
        "last_1m_green": bool(last_1m_green),
        "higher_low_ok": bool(higher_low_ok),
        "entry_confirm_ok": bool(entry_confirm_ok),
        "raw_last_1m_green": bool(raw_last_1m_green),
        "raw_higher_low_ok": bool(raw_higher_low_ok),
        "raw_entry_confirm_ok": bool(raw_entry_confirm_ok),
        "micro_confirm_mode": micro_mode,
        "micro_confirm_passes": int(micro_pass_count),
        "micro_confirm_enabled_checks": int(micro_enabled_checks),
        "micro_confirm_ok": bool(micro_confirm_ok),
        "score_min": float(VWAP_PB_SCORE_MIN),
        "dist_to_vwap_pct": round(dist_to_vwap_pct * 100.0, 3),
        "score": round(score, 3),
        "component_reasons": component_reasons,
        "blocker_split": {"macro": macro_blockers, "micro": micro_blockers, "fallback": fallback_blockers},
        "fallback_checks": {
            "touch_ok": bool(fallback_touch_ok),
            "slope_ok": bool(fallback_slope_ok),
            "bounce_ok": bool(fallback_bounce_ok),
            "distance_ok": bool(fallback_distance_ok),
            "blockers": fallback_blockers,
        },
        "trend_components": {
            "price_above_vwap": bool(price_above_vwap),
            "ema_stack_ok": bool(ema_stack_ok),
            "price_above_fast": bool(price_above_fast),
            "strict_trend_ok": bool(strict_trend_ok),
            "allow_below_vwap_pct": round(allow_below_vwap_pct * 100.0, 3),
            "ema_stack_slack_pct": round(ema_stack_slack_pct * 100.0, 3),
            "allow_price_below_ema_fast_pct": round(allow_below_fast_pct * 100.0, 3),
            "fallback_ready": bool(fallback_ready),
            "fallback_touch_ok": bool(fallback_touch_ok),
            "fallback_slope_ok": bool(fallback_slope_ok),
            "fallback_bounce_ok": bool(fallback_bounce_ok),
            "fallback_distance_ok": bool(fallback_distance_ok),
            "fallback_min_ema_slope": float(VWAP_PB_FALLBACK_MIN_EMA_SLOPE),
            "fallback_min_vwap_slope": float(VWAP_PB_FALLBACK_MIN_VWAP_SLOPE),
            "fallback_bounce_slack_pct": round(float(VWAP_PB_FALLBACK_BOUNCE_SLACK_PCT) * 100.0, 3),
            "fallback_max_dist_vwap_pct": round(float(VWAP_PB_FALLBACK_MAX_DIST_VWAP_PCT) * 100.0, 3),
            "micro_confirm_mode": micro_mode,
            "micro_confirm_ok": bool(micro_confirm_ok),
            "score_min": float(VWAP_PB_SCORE_MIN),
            "min_5m_atr_pct": round(float(VWAP_PB_MIN_5M_ATR_PCT) * 100.0, 3),
            "max_5m_atr_pct": round(float(VWAP_PB_MAX_5M_ATR_PCT) * 100.0, 3),
            "min_day_range_pct": round(float(VWAP_PB_MIN_DAY_RANGE_PCT) * 100.0, 3),
            "min_recent_1m_vol_ratio": float(VWAP_PB_MIN_RECENT_1M_VOL_RATIO),
        },
        "near_miss": {
            "near": near,
            "near_pct": round(touch_band * 100.0, 3),
            "dist_to_level_pct": round(((vwap - price) / vwap) * 100.0, 3) if vwap else None,
            "fallback_ready": bool(fallback_ready),
            "score": round(score, 3),
        },
    })

    if strict_trend_ok and slope_ok and touched and regained_vwap and extension_ok and relvol_ok and momentum_ok and atr_ok and day_range_ok and micro_vol_ok and micro_confirm_ok and score >= float(VWAP_PB_SCORE_MIN):
        out["reason"] = "ok"
        out["triggered"] = True
    elif fallback_ready and fallback_slope_ok and fallback_bounce_ok and fallback_distance_ok and atr_ok and day_range_ok and micro_vol_ok and micro_confirm_ok and score >= float(VWAP_PB_SCORE_MIN):
        out["reason"] = "fallback_ready"
        out["triggered"] = True
        out["fallback_trigger"] = True
    else:
        if score < float(VWAP_PB_SCORE_MIN):
            out["reason"] = "score_too_low"
        elif not permissive_trend_ok:
            out["reason"] = component_reasons[0] if component_reasons else "trend_fail"
        elif fallback_ready and not fallback_slope_ok:
            out["reason"] = "fallback_slope_fail"
        elif fallback_ready and not fallback_bounce_ok:
            out["reason"] = "fallback_bounce_fail"
        elif fallback_ready and not fallback_distance_ok:
            out["reason"] = "fallback_distance_fail"
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
        elif not atr_ok:
            out["reason"] = "atr_regime_fail"
        elif not day_range_ok:
            out["reason"] = "day_range_too_small"
        elif not micro_vol_ok:
            out["reason"] = "recent_1m_volume_fail"
        elif not micro_confirm_ok:
            if VWAP_PB_REQUIRE_GREEN_LAST_1M and not raw_last_1m_green:
                out["reason"] = "last_1m_not_green"
            elif VWAP_PB_REQUIRE_HIGHER_LOW and not raw_higher_low_ok:
                out["reason"] = "micro_higher_low_fail"
            elif VWAP_PB_ENTRY_CONFIRM_ABOVE_PRIOR_1M_HIGH and not raw_entry_confirm_ok:
                out["reason"] = "entry_confirm_fail"
            else:
                out["reason"] = "micro_confirm_fail"
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
        "system_name": SYSTEM_NAME,
        "env_name": ENV_NAME,
        "strategy_mode": STRATEGY_MODE,
        "live_trading_enabled": LIVE_TRADING_ENABLED,
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
    effective_dry_run = bool(SCANNER_DRY_RUN or (not is_live_trading_permitted("worker_scan")))
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
def diagnostics_orders(request: Request, limit: int = 50, include_broker_status: bool = True):
    require_admin_if_configured(request)
    lim = max(1, min(limit, 500))
    rows = [d for d in DECISIONS if d.get("event") in {"ENTRY", "EXIT", "RECONCILE"}]
    rows = rows[-lim:]
    if include_broker_status:
        tail = rows[-min(len(rows), ORDER_DIAGNOSTIC_LOOKBACK):]
        for row in tail:
            details = row.get("details") or {}
            oid = details.get("order_id") if isinstance(details, dict) else None
            if oid:
                row["broker_order"] = get_order_status(str(oid))
    reconcile_snapshot = build_reconcile_snapshot()
    return {"ok": True, "count": len(rows), "orders": rows, "reconcile_snapshot": reconcile_snapshot}






@app.get("/diagnostics/reconcile")
def diagnostics_reconcile(request: Request):
    require_admin_if_configured(request)
    latest_snapshot = read_positions_snapshot()
    snap = build_reconcile_snapshot()
    return {
        "ok": True,
        "reconcile_order_lookback_limit": RECONCILE_ORDER_LOOKBACK_LIMIT,
        "reconcile_orphan_order_max_age_sec": RECONCILE_ORPHAN_ORDER_MAX_AGE_SEC,
        "reconcile_deactivate_orphan_plans": RECONCILE_DEACTIVATE_ORPHAN_PLANS,
        "reconcile_partial_fill_max_age_sec": RECONCILE_PARTIAL_FILL_MAX_AGE_SEC,
        "startup_state": STARTUP_STATE,
        "latest_snapshot": latest_snapshot,
        "summary": {
            "health_grade": snap.get("health_grade"),
            "max_severity": snap.get("max_severity"),
            "trading_blocked": snap.get("trading_blocked"),
            "issue_total": snap.get("issue_total"),
            "issue_counts": snap.get("issue_counts") or {},
            "recommended_action_count": len(snap.get("recommended_actions") or []),
        },
        **snap,
    }


@app.get("/diagnostics/journal")
def diagnostics_journal(request: Request, limit: int = 100, event: str = "", symbol: str = ""):
    require_admin_if_configured(request)
    rows = _read_journal(limit=limit, event=event, symbol=symbol)
    return {"ok": True, "count": len(rows), "items": rows}


@app.get("/diagnostics/execution")
def diagnostics_execution(request: Request, limit: int = 100):
    require_admin_if_configured(request)
    rows = _read_journal(limit=limit)
    entry_submits = [r for r in rows if r.get("event") == "ENTRY" and r.get("action") == "order_submitted"]
    exit_submits = [r for r in rows if r.get("event") == "EXIT" and r.get("action") == "order_submitted"]
    rejects = [r for r in rows if r.get("action") in {"rejected", "error"}]
    latest_snapshot = read_positions_snapshot()
    return {
        "ok": True,
        "journal_enabled": JOURNAL_ENABLED,
        "journal_path": JOURNAL_PATH,
        "position_snapshot_path": POSITION_SNAPSHOT_PATH,
        "persistence_required": PERSISTENCE_REQUIRED,
        "swing_max_hold_days": SWING_MAX_HOLD_DAYS,
        "swing_allow_same_day_exit": SWING_ALLOW_SAME_DAY_EXIT,
        "recent_count": len(rows),
        "entry_submits": len(entry_submits),
        "exit_submits": len(exit_submits),
        "rejects_or_errors": len(rejects),
        "last_entry": entry_submits[-1] if entry_submits else None,
        "last_exit": exit_submits[-1] if exit_submits else None,
        "last_error": rejects[-1] if rejects else None,
        "latest_snapshot": latest_snapshot,
    }


@app.get("/diagnostics/state")
def diagnostics_state(request: Request):
    require_admin_if_configured(request)
    latest_snapshot = read_positions_snapshot()
    snap = build_reconcile_snapshot()
    return {
        "ok": True,
        "journal_enabled": JOURNAL_ENABLED,
        "journal_path": JOURNAL_PATH,
        "position_snapshot_path": POSITION_SNAPSHOT_PATH,
        "startup_state": STARTUP_STATE,
        "latest_snapshot": latest_snapshot,
        **snap,
    }


@app.get("/diagnostics/rejections")
def diagnostics_rejections(request: Request, limit: int = 100):
    require_admin_if_configured(request)
    lim = max(1, min(int(limit or 100), 1000))
    items = REJECTION_HISTORY[-lim:]
    buckets = Counter([str(x.get("reason_bucket") or "UNKNOWN") for x in items])
    return {"ok": True, "count": len(items), "buckets": dict(buckets), "items": items}


def _is_market_tradable_now(session: dict, data_feed_ok: bool) -> bool:
    """True only when the market is open and the current data feed is usable for trading decisions."""
    market_open = bool((session or {}).get("market_open_now"))
    return market_open and bool(data_feed_ok)


def _readiness_data_feed_state(session: dict, snapshot: dict) -> dict:
    """Classify quote/trade readiness separately from component health."""
    session = session or {}
    snapshot = snapshot or {}
    market_open = bool(session.get("market_open_now"))
    price_present = bool(snapshot.get("price"))
    quote_required = bool(ENTRY_REQUIRE_QUOTE)
    quote_ok = bool(snapshot.get("quote_ok"))
    fresh_required = bool(ENTRY_REQUIRE_FRESH_QUOTE)
    fresh = bool(snapshot.get("fresh"))
    spread_pct = snapshot.get("spread_pct")
    spread_limit = float(ENTRY_MAX_SPREAD_PCT)
    spread_ok = True
    if spread_pct is not None:
        try:
            spread_ok = float(spread_pct) <= spread_limit
        except Exception:
            spread_ok = False

    if not market_open:
        return {
            "ok": False,
            "reason": "market_closed",
            "label": "Market closed",
            "detail": "The market is closed, so quote tradability is not evaluated for promotion.",
        }
    if not price_present:
        return {
            "ok": False,
            "reason": "price_missing",
            "label": "Price missing",
            "detail": "No current trade or midpoint price was available for the readiness symbol.",
        }
    if quote_required and not quote_ok:
        return {
            "ok": False,
            "reason": "quote_missing_or_invalid",
            "label": "Quote missing/invalid",
            "detail": "A tradable two-sided quote was not available for the readiness symbol.",
        }
    if fresh_required and not fresh:
        age = snapshot.get("price_age_sec")
        age_txt = _dashboard_fmt(age) if age is not None else "unknown"
        return {
            "ok": False,
            "reason": "quote_stale",
            "label": "Quote stale",
            "detail": f"The latest readiness price is stale (age_sec={age_txt}).",
        }
    if not spread_ok:
        try:
            spread_txt = f"{float(spread_pct):.6f}"
        except Exception:
            spread_txt = "unknown"
        return {
            "ok": False,
            "reason": "spread_too_wide",
            "label": "Spread too wide",
            "detail": f"Current spread_pct={spread_txt} exceeds ENTRY_MAX_SPREAD_PCT={spread_limit:.6f}.",
        }
    return {
        "ok": True,
        "reason": "tradable",
        "label": "Tradable",
        "detail": "Market is open and the current quote data passes tradability checks.",
    }


def _compute_system_health_ok(
    *,
    scanner_running: bool,
    exit_worker_running: bool,
    broker_connected: bool,
    risk_limits_ok: bool,
    kill_switch: bool,
    daily_halt_active_flag: bool,
    journal_ok: bool,
    require_workers: bool = True,
) -> bool:
    """Pure component/process health, excluding session state, quote freshness, and release gating."""
    workers_ok = (bool(scanner_running) and bool(exit_worker_running)) if require_workers else True
    return all([
        workers_ok,
        bool(broker_connected),
        bool(risk_limits_ok),
        bool(journal_ok),
        not bool(kill_switch),
        not bool(daily_halt_active_flag),
    ])


@app.get("/diagnostics/readiness")
def diagnostics_readiness(request: Request):
    require_admin_if_configured(request)
    now_utc = datetime.now(tz=timezone.utc)
    session = _session_boundary_snapshot()
    market_open = bool(session.get("market_open_now"))
    data_snapshot = get_latest_quote_snapshot(READINESS_SYMBOL)
    data_feed_state = _readiness_data_feed_state(session, data_snapshot)
    data_feed_ok = bool(data_feed_state.get("ok"))
    broker_connected = True
    broker_error = ""
    try:
        trading_client.get_account()
    except Exception as e:
        broker_connected = False
        broker_error = str(e)
    scanner_running = False
    scanner_age_sec = None
    scanner_ref_ts = str((LAST_SCANNER_TELEMETRY.get("last_worker_event_utc") or LAST_SCANNER_TELEMETRY.get("last_event_utc") or LAST_SCANNER_TELEMETRY.get("last_attempt_utc") or LAST_SCANNER_TELEMETRY.get("last_success_utc") or LAST_SCAN.get("ts_utc") or "")).strip()
    if scanner_ref_ts:
        try:
            scanner_ts = datetime.fromisoformat(scanner_ref_ts)
            if scanner_ts.tzinfo is None:
                scanner_ts = scanner_ts.replace(tzinfo=timezone.utc)
            scanner_age_sec = max(0.0, (now_utc - scanner_ts.astimezone(timezone.utc)).total_seconds())
            scanner_running = scanner_age_sec <= max(READINESS_SCANNER_MAX_AGE_SEC, 30)
        except Exception:
            scanner_running = False
    exit_worker_running = False
    exit_age_sec = None
    if LAST_EXIT_HEARTBEAT.get("ts_utc"):
        try:
            exit_ts = datetime.fromisoformat(str(LAST_EXIT_HEARTBEAT.get("ts_utc")))
            if exit_ts.tzinfo is None:
                exit_ts = exit_ts.replace(tzinfo=timezone.utc)
            exit_age_sec = max(0.0, (now_utc - exit_ts.astimezone(timezone.utc)).total_seconds())
            exit_worker_running = exit_age_sec <= max(READINESS_EXIT_MAX_AGE_SEC, 15)
        except Exception:
            exit_worker_running = False
    journal_ok = True
    journal_error = ""
    if JOURNAL_ENABLED:
        try:
            _ensure_parent_dir(JOURNAL_PATH)
            Path(JOURNAL_PATH).parent.mkdir(parents=True, exist_ok=True)
            if PERSISTENCE_REQUIRED:
                try:
                    if str(JOURNAL_PATH).startswith("/tmp") or str(POSITION_SNAPSHOT_PATH).startswith("/tmp"):
                        log("PERSISTENCE_WARNING", journal_path=JOURNAL_PATH, position_snapshot_path=POSITION_SNAPSHOT_PATH, reason="tmp_path_detected")
                except Exception:
                    pass
        except Exception as e:
            journal_ok = False
            journal_error = str(e)
    risk_ok = risk_limits_ok()
    halt_active = daily_halt_active()
    system_health_ok = _compute_system_health_ok(
        scanner_running=scanner_running,
        exit_worker_running=exit_worker_running,
        broker_connected=broker_connected,
        risk_limits_ok=risk_ok,
        kill_switch=KILL_SWITCH,
        daily_halt_active_flag=halt_active,
        journal_ok=journal_ok,
        require_workers=READINESS_REQUIRE_WORKERS,
    )
    market_tradable_now = _is_market_tradable_now(session, data_feed_ok)
    overall = system_health_ok

    lifecycle_counts = _paper_lifecycle_counts()
    lifecycle_events = list(PAPER_LIFECYCLE_HISTORY or [])
    if LAST_PAPER_LIFECYCLE and (not lifecycle_events or lifecycle_events[-1] != LAST_PAPER_LIFECYCLE):
        lifecycle_events.append(dict(LAST_PAPER_LIFECYCLE))

    def _latest_lifecycle_event_for_readiness(stage: str, statuses: set[str] | None = None) -> dict:
        target_stage = str(stage or '').strip().lower()
        target_statuses = {str(s).strip().lower() for s in (statuses or set()) if str(s).strip()}
        for ev in reversed(lifecycle_events):
            if not isinstance(ev, dict):
                continue
            ev_stage = str(ev.get('stage') or '').strip().lower()
            ev_status = str(ev.get('status') or '').strip().lower()
            if ev_stage != target_stage:
                continue
            if target_statuses and ev_status not in target_statuses:
                continue
            return dict(ev)
        return {}

    last_selected_event = _latest_lifecycle_event_for_readiness('candidate', {'selected'})
    last_entry_event = _latest_lifecycle_event_for_readiness('entry', {'planned', 'submitted', 'filled', 'opened'})
    last_exit_event = _latest_lifecycle_event_for_readiness('exit', {'submitted', 'closed', 'filled', 'completed', 'dry_run'})
    trade_path_proven = bool((lifecycle_counts.get('candidate_selected') or 0) > 0 and (lifecycle_counts.get('entry_events') or 0) > 0 and (lifecycle_counts.get('exit_events') or 0) > 0)

    freshness = freshness_snapshot()
    freshness_entries = dict(freshness.get('entries') or {})
    same_session_proven = all(bool((freshness_entries.get(name) or {}).get('same_session')) for name in ('last_scan', 'regime', 'paper_lifecycle', 'scanner_telemetry'))

    release = release_gate_status()
    workflow = dict(release.get('release_workflow') or {})
    promotion_targets = dict(workflow.get('promotion_targets') or {})
    guarded_live_ready = bool((promotion_targets.get('guarded_live_eligible') or {}).get('ready'))
    live_guarded_ready = bool((promotion_targets.get('live_guarded') or {}).get('ready'))
    unmet_conditions = list(release.get('unmet_conditions') or [])
    component_truth = 'ready' if system_health_ok else 'not_ready'
    proof_truth = 'proven' if trade_path_proven else 'not_proven'
    if (not system_health_ok) and ALERT_ON_READINESS_FAIL:
        problems = []
        if not broker_connected:
            problems.append("broker_disconnected")
        if not data_feed_ok:
            problems.append(f"data_feed:{data_feed_state.get('reason') or 'unknown'}")
        if not journal_ok:
            problems.append("journal")
        if not risk_ok:
            problems.append("risk")
        if READINESS_REQUIRE_WORKERS and not scanner_running:
            problems.append("scanner_worker")
        if READINESS_REQUIRE_WORKERS and not exit_worker_running:
            problems.append("exit_worker")
        send_alert(
            kind="readiness_fail",
            title="READINESS CHECK FAILED",
            text=f"readiness=false | problems={','.join(problems) or 'unknown'} | symbol={READINESS_SYMBOL}",
            level="critical",
            dedup_key=f"readiness:{','.join(problems)}",
            event="SYSTEM",
            action="readiness_failed",
            reason=",".join(problems),
            details={
                "scanner_running": scanner_running,
                "exit_worker_running": exit_worker_running,
                "broker_connected": broker_connected,
                "data_feed_ok": data_feed_ok,
                "data_feed_reason": data_feed_state.get("reason"),
                "journal_ok": journal_ok,
                "risk_limits_ok": risk_ok,
            },
        )
    return {
        "ok": True,
        "ready": system_health_ok,
        "ready_scope": "component_only",
        "system_health_ok": system_health_ok,
        "market_tradable_now": market_tradable_now,
        "component_ready": system_health_ok,
        "component_truth": component_truth,
        "trade_path_proven": trade_path_proven,
        "trade_path_truth": proof_truth,
        "same_session_proven": same_session_proven,
        "guarded_live_ready": guarded_live_ready,
        "live_guarded_ready": live_guarded_ready,
        "go_live_eligible": bool(release.get('go_live_eligible')),
        "release_stage": release.get('effective_release_stage') or release.get('system_release_stage'),
        "unmet_conditions": unmet_conditions,
        "proof_counts": lifecycle_counts,
        "proof_events": {
            "last_selected_candidate_utc": last_selected_event.get('ts_utc'),
            "last_entry_event_utc": last_entry_event.get('ts_utc'),
            "last_exit_event_utc": last_exit_event.get('ts_utc'),
        },
        "scanner_running": scanner_running,
        "scanner_age_sec": scanner_age_sec,
        "exit_worker_running": exit_worker_running,
        "exit_worker_age_sec": exit_age_sec,
        "market_open": market_open,
        "data_feed_ok": data_feed_ok,
        "data_feed_reason": data_feed_state.get("reason"),
        "data_feed_label": data_feed_state.get("label"),
        "data_feed_detail": data_feed_state.get("detail"),
        "data_snapshot": data_snapshot,
        "broker_connected": broker_connected,
        "broker_error": broker_error,
        "risk_limits_ok": risk_ok,
        "kill_switch": KILL_SWITCH,
        "daily_halt_active": daily_halt_active(),
        "daily_halt_state": DAILY_HALT_STATE,
        "journal_ok": journal_ok,
        "journal_error": journal_error,
        "require_workers": READINESS_REQUIRE_WORKERS,
        "readiness_symbol": READINESS_SYMBOL,
        "alerts_enabled": ALERTS_ENABLED,
        "alert_webhook_configured": bool(ALERT_WEBHOOK_URL),
        "last_alert_heartbeat": LAST_ALERT_HEARTBEAT,
    }

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



def _plan_risk_per_share(plan: dict) -> float:
    try:
        r = float(plan.get("risk_per_share") or 0.0)
        if r > 0:
            return r
    except Exception:
        pass
    try:
        return abs(float(plan.get("entry_price") or 0.0) - float(plan.get("initial_stop_price") or plan.get("stop_price") or 0.0))
    except Exception:
        return 0.0


def _swing_unrealized_r(plan: dict, px: float) -> float:
    entry = _safe_float((plan or {}).get("entry_price") or 0.0)
    risk = _plan_risk_per_share(plan or {})
    if entry <= 0 or risk <= 0:
        return 0.0
    side = str((plan or {}).get("side") or "buy").lower()
    if side == "buy":
        return (float(px) - entry) / risk
    return (entry - float(px)) / risk


def _calc_swing_dynamic_levels(symbol: str, plan: dict, px: float) -> dict:
    out = {"updates": {}, "flags": [], "stall_exit": False, "stall_r": 0.0}
    if STRATEGY_MODE != "swing":
        return out
    side = str((plan or {}).get("side") or "buy").lower()
    if side != "buy":
        return out
    entry = _safe_float((plan or {}).get("entry_price") or 0.0)
    current_stop = _safe_float((plan or {}).get("stop_price") or 0.0)
    risk = _plan_risk_per_share(plan or {})
    if entry <= 0 or risk <= 0:
        return out
    unrealized_r = _swing_unrealized_r(plan or {}, float(px))
    out["stall_r"] = round(unrealized_r, 4)

    new_stop = current_stop
    if SWING_ENABLE_BREAK_EVEN_STOP and unrealized_r >= float(SWING_BREAK_EVEN_R):
        new_stop = max(new_stop, entry)
        out["flags"].append("break_even_armed")

    if SWING_ENABLE_TRAILING_STOP and unrealized_r >= float(SWING_TRAIL_AFTER_R):
        lookback = max(2, int(SWING_TRAIL_LOOKBACK_DAYS))
        bars = fetch_daily_bars_multi([symbol], lookback_days=max(lookback + 10, 20)).get(symbol, [])
        if len(bars) >= lookback:
            lows = [_safe_float(b.get("low") or 0.0) for b in bars[-lookback:]]
            trail_stop = max(lows) if lows else 0.0
            if trail_stop > 0:
                new_stop = max(new_stop, trail_stop)
                out["flags"].append("trailing_stop_armed")

    if new_stop > current_stop + 1e-9:
        out["updates"]["stop_price"] = round(new_stop, 4)

    hold_days = plan_days_held(plan or {})
    if SWING_STALL_EXIT_DAYS > 0 and hold_days >= int(SWING_STALL_EXIT_DAYS) and unrealized_r < float(SWING_STALL_MIN_R):
        out["stall_exit"] = True
        out["flags"].append("stall_exit_ready")
    return out


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
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="kill_switch_enabled", meta=meta)
        return {"ok": False, "rejected": True, "reason": "kill_switch_enabled"}
    if daily_halt_active():
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="daily_halt_active", meta=meta)
        return {"ok": False, "rejected": True, "reason": "daily_halt_active"}
    if daily_stop_hit():
        activate_daily_halt("daily_stop_hit")
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="daily_stop_hit", meta=meta)
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

        close_out = close_position(symbol, reason="reverse_not_allowed", source=source)
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

    effective_dry_run = bool((SCANNER_DRY_RUN if source == "worker_scan" else False) or (not is_live_trading_permitted(source)))

    try:
        snapshot = get_latest_quote_snapshot(symbol)
        base_price = snapshot.get("price")
        if base_price is None or float(base_price) <= 0:
            raise ValueError("latest_price_missing")
        if ENTRY_REQUIRE_QUOTE and not snapshot.get("quote_ok"):
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="quote_missing", meta={"snapshot": snapshot, "payload": payload, **(meta or {})})
            soften_symbol_lock(symbol, 5)
            return {"ok": False, "rejected": True, "reason": "quote_missing", "symbol": symbol, "signal": signal, "snapshot": snapshot}
        if ENTRY_REQUIRE_FRESH_QUOTE and not snapshot.get("fresh"):
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="price_stale", meta={"snapshot": snapshot, "payload": payload, **(meta or {})})
            soften_symbol_lock(symbol, 5)
            return {"ok": False, "rejected": True, "reason": "price_stale", "symbol": symbol, "signal": signal, "snapshot": snapshot}
        spread_pct = snapshot.get("spread_pct")
        if spread_pct is not None and float(spread_pct) > float(ENTRY_MAX_SPREAD_PCT):
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="spread_too_wide", meta={"snapshot": snapshot, **(meta or {})})
            soften_symbol_lock(symbol, 5)
            return {"ok": False, "rejected": True, "reason": "spread_too_wide", "symbol": symbol, "signal": signal, "snapshot": snapshot}

        qty_signed_post_lock, pos_side_post_lock = get_position(symbol)
        if qty_signed_post_lock != 0:
            desired_side = "long" if side == "buy" else "short"
            reason = f"position_open_after_lock:{pos_side_post_lock}"
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="ignored", reason=reason, meta={"snapshot": snapshot, **(meta or {})})
            return {"ok": True, "ignored": True, "reason": reason, "symbol": symbol, "signal": signal, "snapshot": snapshot}
        if TRADE_PLAN.get(symbol, {}).get("active"):
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="ignored", reason="plan_active_after_lock", meta={"snapshot": snapshot, **(meta or {})})
            return {"ok": True, "ignored": True, "reason": "plan_active_after_lock", "symbol": symbol, "signal": signal, "snapshot": snapshot}

        risk_qty = compute_qty(float(base_price)) if side == "buy" else round(abs(qty_signed), 2)
        qty = risk_qty
        affordability = None
        if side == "buy":
            affordability = clip_qty_for_affordability(float(base_price), float(risk_qty))
            qty = float(affordability.get("submitted_qty") or 0.0)
            if affordability.get("reason"):
                reason = str(affordability.get("reason"))
                record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason=reason, qty=risk_qty, meta={"snapshot": snapshot, "affordability": affordability, **(meta or {})})
                soften_symbol_lock(symbol, 5)
                return {"ok": False, "rejected": True, "reason": reason, "symbol": symbol, "signal": signal, "snapshot": snapshot, "affordability": affordability}
        if qty <= 0:
            raise ValueError("qty_zero")

        payload = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "risk_qty": risk_qty,
            "affordability": affordability,
            "base_price": round(float(base_price), 2),
            "signal": signal,
            "paper": APCA_PAPER,
            "dry_run": effective_dry_run,
            "source": source,
            "snapshot": snapshot,
        }

        if effective_dry_run:
            plan = build_trade_plan(symbol, side, qty, float(base_price), signal, meta=meta)
            plan["source"] = source
            plan["requested_qty"] = float(risk_qty)
            plan["submitted_qty"] = float(qty)
            plan["filled_qty"] = float(qty)
            plan["avg_fill_price"] = float(base_price)
            plan["affordability"] = affordability or {}
            TRADE_PLAN[symbol] = plan
            _ensure_execution_lifecycle_plan(symbol, plan)
            _transition_execution_lifecycle(plan, symbol, "planned", reason="entry_dry_run_plan", details={"source": source, "signal": signal, "qty": qty}, allow_illegal=True)
            persist_positions_snapshot(reason="entry_dry_run_plan", extra={"symbol": symbol, "source": source, "signal": signal})
            record_decision("ENTRY", source, symbol, side=side, signal=signal, action="dry_run_plan_created", reason="", qty=qty, meta={"snapshot": snapshot, **(meta or {})})
            try:
                _record_paper_lifecycle("entry", "planned", symbol=symbol, details={"source": source, "signal": signal, "qty": qty, "dry_run": True, "base_price": round(float(base_price), 4)})
            except Exception:
                pass
            return {"ok": True, "submitted": False, "dry_run": True, "order": payload, "plan": plan}

        order = submit_market_order(symbol, side, qty)
        plan = build_trade_plan(symbol, side, qty, float(base_price), signal, meta=meta)
        plan["source"] = source
        plan["order_id"] = str(getattr(order, "id", ""))
        plan["submitted_at"] = now_ny().isoformat()
        plan["requested_qty"] = float(risk_qty)
        plan["submitted_qty"] = float(qty)
        plan["filled_qty"] = 0.0
        plan["avg_fill_price"] = float(base_price)
        plan["order_status"] = "submitted"
        plan["affordability"] = affordability or {}
        TRADE_PLAN[symbol] = plan
        _ensure_execution_lifecycle_plan(symbol, plan)
        _transition_execution_lifecycle(plan, symbol, "submitted", reason="entry_submitted", details={"source": source, "signal": signal, "qty": qty, "order_id": str(getattr(order, "id", ""))}, allow_illegal=True)
        persist_positions_snapshot(reason="entry_submitted", extra={"symbol": symbol, "order_id": str(getattr(order, "id", "")), "source": source, "signal": signal})
        log("ORDER_SUBMITTED", symbol=symbol, side=side, qty=qty, order_id=str(getattr(order, "id", "")), signal=signal, source=source)
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="order_submitted", reason="", order_id=str(getattr(order, "id", "")), qty=qty, meta={"snapshot": snapshot, **(meta or {})})
        try:
            _record_paper_lifecycle("entry", "submitted", symbol=symbol, details={"source": source, "signal": signal, "qty": qty, "order_id": str(getattr(order, "id", "")), "dry_run": False})
        except Exception:
            pass
        return {"ok": True, "submitted": True, "order_id": str(getattr(order, "id", "")), "order": payload, "plan": plan}
    except Exception as e:
        log("ORDER_REJECTED", symbol=symbol, side=side, err=str(e), signal=signal, source=source)
        record_decision("ENTRY", source, symbol, side=side, signal=signal, action="rejected", reason="alpaca_submit_failed", err=str(e), meta=meta)
        try:
            _record_paper_lifecycle("entry", "rejected", symbol=symbol, details={"source": source, "signal": signal, "reason": "alpaca_submit_failed", "error": str(e)})
        except Exception:
            pass
        try:
            stale_plan = TRADE_PLAN.get(symbol)
            if isinstance(stale_plan, dict):
                _ensure_execution_lifecycle_plan(symbol, stale_plan)
                _transition_execution_lifecycle(stale_plan, symbol, "rejected", reason="alpaca_submit_failed", details={"source": source, "signal": signal, "error": str(e)}, allow_illegal=True)
        except Exception:
            pass
        soften_symbol_lock(symbol, 5)
        return {"ok": False, "rejected": True, "reason": f"alpaca_submit_failed:{e}", "symbol": symbol, "signal": signal, "affordability": affordability if "affordability" in locals() else None}


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
    update_exit_heartbeat(status="started")

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
        update_exit_heartbeat(status="kill_switch", results=len(out))
        return {"ok": True, "mode": "kill_switch", "ts_ny": now_ny().isoformat(), "results": out}

    if daily_stop_hit():
        activate_daily_halt("daily_stop_hit")
        out = flatten_all("daily_stop_hit") if AUTO_FLATTEN_ON_DAILY_STOP else []
        update_exit_heartbeat(status="daily_stop_hit", results=len(out))
        return {"ok": True, "mode": "daily_stop_hit", "daily_halt": DAILY_HALT_STATE, "ts_ny": now_ny().isoformat(), "results": out}

    if ONLY_MARKET_HOURS and not in_market_hours():
        return {"ok": True, "skipped": True, "reason": "outside_market_hours"}

    # Reconcile internal plans from Alpaca positions (protects positions across restarts)
    reconcile_actions = reconcile_trade_plans_from_alpaca()

    now = now_ny()
    now_t = now.time()
    results = []

    # EOD flatten is intraday-only. Swing mode intentionally disables clock-based flattening.
    if EOD_FLATTEN_TIME:
        eod_t = parse_hhmm(EOD_FLATTEN_TIME)
        if now_t >= eod_t:
            for p in list_open_positions_allowed():
                sym = p["symbol"]
                out = close_position(sym, reason="eod_flatten", source="worker_exit")
                if sym in TRADE_PLAN:
                    TRADE_PLAN[sym]["active"] = False
                results.append({"symbol": sym, "action": "flatten_eod", **out})
            return {"ok": True, "ts_ny": now.isoformat(), "reconcile": reconcile_actions, "results": results, "mode": "eod_flatten"}

    # Manage active plans with stop/take
    for symbol, plan in list(TRADE_PLAN.items()):
        if not plan.get("active"):
            continue

        if PLAN_SYNC_ON_WORKER_EXIT:
            sync_info = sync_trade_plan_with_broker(symbol, plan)
            if sync_info.get("changes"):
                results.append({"symbol": symbol, "action": "plan_sync", "changes": sync_info.get("changes"), "order_status": sync_info.get("order_status")})
            if not plan.get("active"):
                results.append({"symbol": symbol, "action": "plan_deactivated", "reason": "sync_rule"})
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

        dynamic_exit = {"updates": {}, "flags": [], "stall_exit": False, "stall_r": 0.0}
        if STRATEGY_MODE == "swing":
            dynamic_exit = _calc_swing_dynamic_levels(symbol, plan, float(px))
            if dynamic_exit.get("updates"):
                plan.update(dynamic_exit.get("updates") or {})
                stop_price = float(plan.get("stop_price"))
                take_price = float(plan.get("take_price"))

        if entry_side == "buy":
            hit_stop = px <= stop_price
            hit_take = px >= take_price
        else:
            hit_stop = px >= stop_price
            hit_take = px <= take_price

        hold_days = plan_days_held(plan)
        plan["days_held"] = hold_days
        max_hold_days = int(plan.get("max_hold_days") or SWING_MAX_HOLD_DAYS or 0)
        if STRATEGY_MODE == "swing" and max_hold_days > 0 and hold_days >= max_hold_days:
            if same_day_exit_blocked(plan, reason="time_exit"):
                results.append({"symbol": symbol, "action": "blocked_same_day_exit", "reason": "time_exit", "days_held": hold_days})
            else:
                plan["last_exit_attempt_ts"] = now_ts
                out = close_position(symbol, reason="time_exit", source="worker_exit")
                if out.get("closed"):
                    plan["active"] = False
                results.append({"symbol": symbol, "action": "time_exit", "days_held": hold_days, "dynamic_flags": dynamic_exit.get("flags", []), **out})
                continue

        if dynamic_exit.get("stall_exit"):
            plan["last_exit_attempt_ts"] = now_ts
            reason = "stall_exit"
            if same_day_exit_blocked(plan, reason=reason):
                results.append({"symbol": symbol, "action": "blocked_same_day_exit", "reason": reason, "days_held": hold_days, "dynamic_flags": dynamic_exit.get("flags", []), "stall_r": dynamic_exit.get("stall_r")})
                continue
            out = close_position(symbol, reason=reason, source="worker_exit")
            if out.get("closed"):
                plan["active"] = False
                results.append({"symbol": symbol, "action": reason, "price": px, "stop": stop_price, "take": take_price, "days_held": hold_days, "dynamic_flags": dynamic_exit.get("flags", []), "stall_r": dynamic_exit.get("stall_r"), **out})
            else:
                results.append({"symbol": symbol, "action": f"{reason}_failed", "price": px, "days_held": hold_days, "dynamic_flags": dynamic_exit.get("flags", []), "stall_r": dynamic_exit.get("stall_r"), **out})
            continue

        if hit_stop or hit_take:
            plan["last_exit_attempt_ts"] = now_ts
            reason = "stop" if hit_stop else "target"
            if same_day_exit_blocked(plan, reason=reason):
                results.append({"symbol": symbol, "action": "blocked_same_day_exit", "reason": reason, "days_held": hold_days})
                continue
            out = close_position(symbol, reason=reason, source="worker_exit")
            if out.get("closed"):
                plan["active"] = False
                results.append({"symbol": symbol, "action": f"exit_{reason}", "price": px, "stop": stop_price, "take": take_price, "days_held": hold_days, "dynamic_flags": dynamic_exit.get("flags", []), **out})
            else:
                results.append({"symbol": symbol, "action": f"exit_{reason}_failed", "price": px, "days_held": hold_days, "dynamic_flags": dynamic_exit.get("flags", []), **out})
        else:
            results.append({"symbol": symbol, "action": "hold", "price": px, "stop": stop_price, "take": take_price, "days_held": hold_days, "dynamic_flags": dynamic_exit.get("flags", []), "stall_r": dynamic_exit.get("stall_r")})


    # --- Trades-Today forcing (optional, emergency) ---
    # Keep this path conservative and self-contained so it cannot crash the exit worker.
    try:
        effective_dry_run = bool(SCANNER_DRY_RUN or (not is_live_trading_permitted("worker_scan")))
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
    if results or reconcile_actions:
        persist_positions_snapshot(reason="worker_exit_cycle", extra={"results_count": len(results), "reconcile_count": len(reconcile_actions)})
    update_exit_heartbeat(status="ok", results=len(results), reconcile=len(reconcile_actions))
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


@app.get("/diagnostics/alerts")
def diagnostics_alerts(request: Request, limit: int = 100, kind: str = ""):
    require_admin_if_configured(request)
    lim = max(1, min(int(limit or 100), 1000))
    rows = ALERT_HISTORY[-lim:]
    if kind:
        k = str(kind or "").strip().lower()
        rows = [r for r in rows if str(r.get("kind") or "").lower() == k]
    return {
        "ok": True,
        "count": len(rows),
        "alerts_enabled": ALERTS_ENABLED,
        "webhook_configured": bool(ALERT_WEBHOOK_URL),
        "last_alert": ALERT_HISTORY[-1] if ALERT_HISTORY else None,
        "heartbeat": LAST_ALERT_HEARTBEAT,
        "items": rows,
    }


@app.post("/test/alert")
def test_alert(request: Request, title: str = "Manual test alert", text: str = "Patch 007 test alert", level: str = "info"):
    require_admin_if_configured(request)
    result = send_alert(kind="manual_test", title=title, text=text, level=level, dedup_key=f"manual:{title}:{text}")
    return {"ok": True, **result}


@app.get("/diagnostics/strategy")
def diagnostics_strategy(request: Request, symbol: str = ""):
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {
            "ok": True,
            "primary_strategy": SCANNER_PRIMARY_STRATEGY,
            "scanner_universe": {
                "provider": SCANNER_UNIVERSE_PROVIDER,
                "max_symbols_per_cycle": SCANNER_MAX_SYMBOLS_PER_CYCLE,
                "candidate_limit": SCANNER_CANDIDATE_LIMIT,
                "dynamic_top_n": SCANNER_DYNAMIC_TOP_N,
                "dynamic_min_price": SCANNER_DYNAMIC_MIN_PRICE,
                "dynamic_min_dollar_vol": SCANNER_DYNAMIC_MIN_DOLLAR_VOL,
                "dynamic_min_relvol": SCANNER_DYNAMIC_MIN_RELVOL,
                "dynamic_min_range_pct": SCANNER_DYNAMIC_MIN_RANGE_PCT,
                "dynamic_keep_anchors": SCANNER_DYNAMIC_KEEP_ANCHORS,
                "anchor_symbols": SCANNER_ANCHOR_SYMBOLS[:10],
                "pool_size": len(_base_scanner_pool()) if SCANNER_UNIVERSE_PROVIDER == "dynamic" else None,
            },
            "vwap_pullback": {
                "enabled": bool(VWAP_PB_ENABLE),
                "score_min": float(VWAP_PB_SCORE_MIN),
                "min_bars_5m": int(VWAP_PB_MIN_BARS_5M),
                "min_bars_5m_source": ("env" if os.getenv("VWAP_PB_MIN_BARS_5M") is not None else "default"),
                "ema_fast": int(VWAP_PB_EMA_FAST),
                "ema_slow": int(VWAP_PB_EMA_SLOW),
                "min_5m_atr_pct": float(VWAP_PB_MIN_5M_ATR_PCT),
                "max_5m_atr_pct": float(VWAP_PB_MAX_5M_ATR_PCT),
                "min_day_range_pct": float(VWAP_PB_MIN_DAY_RANGE_PCT),
                "min_recent_1m_vol_ratio": float(VWAP_PB_MIN_RECENT_1M_VOL_RATIO),
                "require_green_last_1m": bool(VWAP_PB_REQUIRE_GREEN_LAST_1M),
                "require_higher_low": bool(VWAP_PB_REQUIRE_HIGHER_LOW),
                "entry_confirm_above_prior_1m_high": bool(VWAP_PB_ENTRY_CONFIRM_ABOVE_PRIOR_1M_HIGH),
                "entry_confirm_buffer_pct": float(VWAP_PB_ENTRY_CONFIRM_BUFFER_PCT),
                "micro_confirm_mode": str(VWAP_PB_MICRO_CONFIRM_MODE),
                "soft_confirm_min_passes": int(VWAP_PB_SOFT_CONFIRM_MIN_PASSES),
                "fallback_signal_score_min": float(VWAP_PB_FALLBACK_SIGNAL_SCORE_MIN),
                "fallback_min_ema_slope": float(VWAP_PB_FALLBACK_MIN_EMA_SLOPE),
                "fallback_min_vwap_slope": float(VWAP_PB_FALLBACK_MIN_VWAP_SLOPE),
                "fallback_allow_bounce_slack": bool(VWAP_PB_FALLBACK_ALLOW_BOUNCE_SLACK),
                "fallback_bounce_slack_pct": float(VWAP_PB_FALLBACK_BOUNCE_SLACK_PCT),
                "fallback_max_dist_vwap_pct": float(VWAP_PB_FALLBACK_MAX_DIST_VWAP_PCT),
                "fallback_allow_touchless": bool(VWAP_PB_FALLBACK_ALLOW_TOUCHLESS),
            },
        }

    bars = fetch_intraday_bars(sym, SCANNER_LOOKBACK_DAYS) or []
    diag = _vwap_pullback_setup(bars)
    return {
        "ok": True,
        "symbol": sym,
        "primary_strategy": SCANNER_PRIMARY_STRATEGY,
        "vwap_pullback": diag,
    }


@app.get("/diagnostics/gatekeeper")
def diagnostics_gatekeeper(request: Request, symbol: str = "SPY"):
    sym = str(symbol or "SPY").upper().strip() or "SPY"
    quote = get_latest_quote_snapshot(sym)
    bars = fetch_intraday_bars(sym, SCANNER_LOOKBACK_DAYS) or []
    vwap_diag = _vwap_pullback_setup(bars)

    allowed = _is_symbol_allowed(sym)
    market_hours_ok = in_market_hours() if ONLY_MARKET_HOURS else True
    has_position = False
    try:
        pos = get_position_safe(sym)
        has_position = bool(pos and float(pos.get("qty") or 0) > 0)
    except Exception:
        has_position = False
    active_plan = bool((TRADE_PLAN.get(sym) or {}).get("active"))
    lock_until = float(SYMBOL_LOCKS.get(sym, 0) or 0)
    now_ts = time.time()
    symbol_locked = lock_until > now_ts
    spread_ok = True
    if ENTRY_REQUIRE_QUOTE:
        spread_ok = bool(quote.get("quote_ok"))
        spct = quote.get("spread_pct")
        if spct is not None:
            spread_ok = spread_ok and float(spct) <= float(ENTRY_MAX_SPREAD_PCT)
    quote_required_ok = (not ENTRY_REQUIRE_QUOTE) or bool(quote.get("quote_ok"))
    freshness_ok = (not ENTRY_REQUIRE_FRESH_QUOTE) or bool(quote.get("fresh"))

    checks = {
        "symbol_allowed": bool(allowed),
        "market_hours_ok": bool(market_hours_ok),
        "quote_required_ok": bool(quote_required_ok),
        "freshness_ok": bool(freshness_ok),
        "spread_ok": bool(spread_ok),
        "has_position": bool(has_position),
        "active_plan": bool(active_plan),
        "symbol_locked": bool(symbol_locked),
        "strategy_eligible": bool(vwap_diag.get("eligible")),
        "strategy_triggered": bool(vwap_diag.get("triggered")),
    }
    would_pass = all([
        checks["symbol_allowed"],
        checks["market_hours_ok"],
        checks["quote_required_ok"],
        checks["freshness_ok"],
        checks["spread_ok"],
        (not checks["has_position"]),
        (not checks["active_plan"]),
        (not checks["symbol_locked"]),
        checks["strategy_eligible"],
        checks["strategy_triggered"],
    ])

    reasons = []
    if not checks["symbol_allowed"]:
        reasons.append("symbol_not_allowed")
    if not checks["market_hours_ok"]:
        reasons.append("outside_market_hours")
    if not checks["quote_required_ok"]:
        reasons.append("quote_unavailable")
    if not checks["freshness_ok"]:
        reasons.append("stale_quote")
    if not checks["spread_ok"]:
        reasons.append("spread_too_wide")
    if checks["has_position"]:
        reasons.append("position_exists")
    if checks["active_plan"]:
        reasons.append("active_plan_exists")
    if checks["symbol_locked"]:
        reasons.append("symbol_locked")
    if not checks["strategy_eligible"] or not checks["strategy_triggered"]:
        reasons.append(f"strategy:{vwap_diag.get('reason') or 'no_signal'}")

    return {
        "ok": True,
        "symbol": sym,
        "would_pass": bool(would_pass),
        "checks": checks,
        "reasons": reasons,
        "quote": quote,
        "strategy": {"name": "vwap_pullback", **vwap_diag},
        "lock_expires_in_sec": max(0.0, lock_until - now_ts) if symbol_locked else 0.0,
        "max_open_positions": int(MAX_OPEN_POSITIONS),
        "open_positions_count": len([1 for _s,_p in (TRADE_PLAN or {}).items() if (_p or {}).get("active")]),
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

    live_ready = all(v.get("ok") for v in checks.values()) and is_live_trading_permitted("worker_scan")
    return {
        "ok": True,
        "live_ready": live_ready,
        "paper": APCA_PAPER,
        "dry_run": DRY_RUN,
        "scanner_dry_run": SCANNER_DRY_RUN,
        "scanner_allow_live": SCANNER_ALLOW_LIVE,
        "checks": checks,
    }

@app.get("/diagnostics/ranking")
def diagnostics_ranking(limit: int = 25):
    rows = [d for d in DECISIONS if d.get("event") == "SCAN" and d.get("action") == "ignored" and d.get("reason") in {"rank_below_threshold", "fallback_raw_score_too_low", "lower_rank_than_top_slots"}]
    rows = rows[-max(1, min(int(limit or 25), 200)):]
    if (not rows) and LAST_SCAN.get("summary"):
        rows = list((LAST_SCAN.get("summary") or {}).get("top_pre_ranked_candidates") or [])[: max(1, min(int(limit or 25), 200))]
    return {"ok": True, "count": len(rows), "items": rows, "settings": {"enabled": bool(SIGNAL_RANKING_ENABLED), "scanner_rank_min_score": float(SCANNER_RANK_MIN_SCORE), "scanner_fallback_min_rank_score": float(SCANNER_FALLBACK_MIN_RANK_SCORE), "scanner_fallback_min_raw_score": float(SCANNER_FALLBACK_MIN_RAW_SCORE), "micro_confirm_mode": str(VWAP_PB_MICRO_CONFIRM_MODE), "soft_confirm_min_passes": int(VWAP_PB_SOFT_CONFIRM_MIN_PASSES), "fallback_min_ema_slope": float(VWAP_PB_FALLBACK_MIN_EMA_SLOPE), "fallback_min_vwap_slope": float(VWAP_PB_FALLBACK_MIN_VWAP_SLOPE), "fallback_bounce_slack_pct": float(VWAP_PB_FALLBACK_BOUNCE_SLACK_PCT), "fallback_max_dist_vwap_pct": float(VWAP_PB_FALLBACK_MAX_DIST_VWAP_PCT)}}

@app.get("/diagnostics/exits")
def diagnostics_exits(limit: int = 25):
    rows = []
    for symbol, plan in list((TRADE_PLAN or {}).items()):
        if not isinstance(plan, dict) or not plan.get("active"):
            continue
        try:
            px = get_latest_price(symbol)
        except Exception:
            px = None
        rows.append({
            "symbol": symbol,
            "strategy": plan.get("strategy_name") or plan.get("signal"),
            "days_held": plan_days_held(plan),
            "entry_price": plan.get("entry_price"),
            "stop_price": plan.get("stop_price"),
            "take_price": plan.get("take_price"),
            "initial_stop_price": plan.get("initial_stop_price"),
            "risk_per_share": plan.get("risk_per_share"),
            "unrealized_r": round(_swing_unrealized_r(plan, float(px)), 4) if px is not None else None,
            "latest_price": px,
            "max_hold_days": plan.get("max_hold_days"),
            "thesis": plan.get("thesis") or {},
        })
    rows.sort(key=lambda x: (int(x.get("days_held") or 0), float(x.get("unrealized_r") or -999)), reverse=True)
    return {
        "ok": True,
        "strategy_mode": STRATEGY_MODE,
        "exit_rules": {
            "swing_enable_break_even_stop": SWING_ENABLE_BREAK_EVEN_STOP,
            "swing_break_even_r": SWING_BREAK_EVEN_R,
            "swing_enable_trailing_stop": SWING_ENABLE_TRAILING_STOP,
            "swing_trail_after_r": SWING_TRAIL_AFTER_R,
            "swing_trail_lookback_days": SWING_TRAIL_LOOKBACK_DAYS,
            "swing_stall_exit_days": SWING_STALL_EXIT_DAYS,
            "swing_stall_min_r": SWING_STALL_MIN_R,
        },
        "positions": rows[: max(1, min(int(limit or 25), 200))],
        "count": len(rows),
    }


def _diagnostics_swing_blockers() -> dict:
    now = now_ny()
    market_blocked = bool(ONLY_MARKET_HOURS and not in_market_hours())
    scanner_enabled = bool(SCANNER_ENABLED)
    same_day_stats = _same_day_entry_stats()
    remaining_today = max(0, SWING_MAX_NEW_ENTRIES_PER_DAY - int(same_day_stats.get('counted') or 0))
    regime_favorable = LAST_REGIME_SNAPSHOT.get('favorable') if isinstance(LAST_REGIME_SNAPSHOT, dict) else None
    regime_data_complete = bool((LAST_REGIME_SNAPSHOT or {}).get('data_complete')) if isinstance(LAST_REGIME_SNAPSHOT, dict) else False
    regime_blocked = bool(regime_favorable is False and not SWING_ALLOW_NEW_ENTRIES_IN_WEAK_TAPE)
    daily_halt_blocked = bool(daily_halt_active() or daily_stop_hit())
    effective_dry_run = bool(SCANNER_DRY_RUN or (not is_live_trading_permitted("diagnostics_swing")))
    corr_groups = _correlation_groups_list()
    exposure = _current_portfolio_exposure_breakdown()
    open_total = float(exposure.get('total') or 0.0)
    open_strategy = float(exposure.get('strategy_managed') or 0.0)
    open_recovered = float(exposure.get('recovered') or 0.0)
    open_unmanaged = float(exposure.get('unmanaged') or 0.0)
    equity = max(0.0, _current_equity_estimate())
    portfolio_cap = equity * SWING_MAX_PORTFOLIO_EXPOSURE_PCT if equity > 0 else 0.0
    blocked_total_cap = bool(portfolio_cap > 0 and open_total >= portfolio_cap)
    blocked_strategy_cap = bool(portfolio_cap > 0 and open_strategy >= portfolio_cap)
    if SWING_PORTFOLIO_CAP_BLOCK_MODE == 'total':
        over_portfolio_cap = blocked_total_cap
    elif SWING_PORTFOLIO_CAP_BLOCK_MODE == 'strategy':
        over_portfolio_cap = blocked_strategy_cap
    elif SWING_PORTFOLIO_CAP_BLOCK_MODE == 'both':
        over_portfolio_cap = blocked_total_cap or blocked_strategy_cap
    else:
        over_portfolio_cap = False
    return {
        'scanner_enabled': scanner_enabled,
        'market_hours_required': bool(ONLY_MARKET_HOURS),
        'market_open_now': bool(in_market_hours()),
        'blocked_by_market_hours': market_blocked,
        'effective_dry_run': effective_dry_run,
        'daily_halt_active': daily_halt_blocked,
        'regime_known': bool(LAST_REGIME_SNAPSHOT),
        'regime_data_complete': regime_data_complete,
        'regime_favorable': regime_favorable,
        'blocked_by_weak_regime': regime_blocked,
        'remaining_new_entries_today': int(remaining_today),
        'blocked_by_entry_cap': bool(remaining_today <= 0),
        'max_new_entries_per_day': int(SWING_MAX_NEW_ENTRIES_PER_DAY),
        'correlation_groups_count': len(corr_groups),
        'correlation_groups': corr_groups,
        'raw_correlation_groups': SWING_CORRELATION_GROUPS,
        'last_scan_ts': LAST_SCAN.get('ts_utc'),
        'last_scan_reason': LAST_SCAN.get('reason'),
        'last_scan_summary': dict((LAST_SCAN.get('summary') or {})),
        'same_day_entry_count': int(same_day_stats.get('counted') or 0),
        'same_day_entry_details': same_day_stats,
        'portfolio_exposure': round(open_total, 2),
        'strategy_portfolio_exposure': round(open_strategy, 2),
        'recovered_portfolio_exposure': round(open_recovered, 2),
        'unmanaged_portfolio_exposure': round(open_unmanaged, 2),
        'portfolio_exposure_cap': round(portfolio_cap, 2),
        'portfolio_cap_block_mode': SWING_PORTFOLIO_CAP_BLOCK_MODE,
        'blocked_by_portfolio_cap': over_portfolio_cap,
        'blocked_by_total_portfolio_cap': blocked_total_cap,
        'blocked_by_strategy_portfolio_cap': blocked_strategy_cap,
        'recovered_symbols': list(exposure.get('recovered_symbols') or []),
        'strategy_symbols': list(exposure.get('strategy_symbols') or []),
        'unmanaged_symbols': list(exposure.get('unmanaged_symbols') or []),
        'now_ny': now.isoformat(),
    }


def _dashboard_badge(label: str, ok: bool | None) -> str:
    cls = "neutral"
    txt = "UNKNOWN"
    if ok is True:
        cls = "good"
        txt = "YES"
    elif ok is False:
        cls = "bad"
        txt = "NO"
    return f'<span class="badge {cls}">{html.escape(label)}: {txt}</span>'


def _dashboard_fmt(v):
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return f"{v:,.2f}"
    return html.escape(str(v))


def _dashboard_json_block(obj) -> str:
    import json
    return html.escape(json.dumps(obj, indent=2, sort_keys=False, default=str))


def _dashboard_rows(rows: list[tuple[str, object]]) -> str:
    return ''.join(
        f"<tr><th>{html.escape(str(k))}</th><td>{_dashboard_fmt(v)}</td></tr>"
        for k, v in rows
    )


def _dashboard_metric_class(
    status: str | None,
    good_values: set[str] | None = None,
    bad_values: set[str] | None = None,
    neutral_values: set[str] | None = None,
) -> str:
    s = str(status or "").strip().lower()
    if good_values and s in good_values:
        return "good"
    if bad_values and s in bad_values:
        return "bad"
    if neutral_values and s in neutral_values:
        return "neutral"
    return "neutral"


def _dashboard_warning_badges(warnings: list[str] | None) -> str:
    out = []
    for code in list(warnings or [])[:6]:
        out.append(f'<span class="badge bad">{html.escape(str(code))}</span>')
    return ''.join(out)


def _dashboard_source_badge(label: str, source: str) -> str:
    cls = "good" if source == "authoritative" else ("neutral" if source in {"derived", "summary"} else "bad")
    return f'<span class="badge {cls}">{html.escape(label)}: {html.escape(source)}</span>'


def _dashboard_scanner_runtime_hint(scanner: dict | None, worker_snapshot: dict | None) -> dict:
    scanner = dict(scanner or {})
    worker_snapshot = dict(worker_snapshot or {})
    summary = dict(scanner.get("summary") or {})
    last = dict(scanner.get("last") or {})
    derived = dict(scanner.get("derived") or {})
    status = str(summary.get("worker_status") or worker_snapshot.get("scanner_status") or "unknown").strip().lower()
    next_run_in_sec = derived.get("next_run_in_sec")
    next_run_estimate_utc = last.get("next_run_estimate_utc")
    lateness_sec = None
    if isinstance(next_run_in_sec, (int, float)):
        lateness_sec = max(0.0, -float(next_run_in_sec))
    restored_from_state = bool(last.get("restored_from_state"))
    boot_ts = _safe_iso_to_dt(last.get("boot_ts_utc"))
    last_event_ts = _safe_iso_to_dt(last.get("last_event_utc"))
    timeout_floor = max(120, int(worker_snapshot.get("scanner_late_after_sec") or 0) // 10)

    display_status = status
    if status == "late":
        display_status = "late_but_alive"
    elif status in {"stale", "down"}:
        display_status = "down"

    hint_code = "none"
    hint_text = "On schedule."
    if status == "late":
        if restored_from_state and not boot_ts:
            hint_code = "stale_persisted_timing"
            hint_text = "Likely stale persisted timing: restored telemetry has no fresh worker boot timestamp."
        elif restored_from_state and boot_ts and last_event_ts and last_event_ts < boot_ts:
            hint_code = "stale_persisted_timing"
            hint_text = "Likely stale persisted timing: restored event timestamps predate the current worker boot."
        elif isinstance(lateness_sec, (int, float)) and lateness_sec >= timeout_floor:
            hint_code = "missed_wake_up"
            hint_text = f"Likely missed wake-up: scanner is overdue by {_dashboard_fmt(lateness_sec)}s versus next scheduled run."
        else:
            hint_code = "scheduler_drift"
            hint_text = "Likely scheduler drift: worker is alive but has slipped past its expected wake-up window."
    elif status in {"stale", "down"}:
        hint_code = "worker_not_alive"
        hint_text = "Scanner heartbeat is stale/down, not merely late."
    elif status == "unknown":
        hint_code = "worker_status_unknown"
        hint_text = "Scanner worker status is unknown."

    return {
        "display_status": display_status,
        "hint_code": hint_code,
        "hint_text": hint_text,
        "next_run_estimate_utc": next_run_estimate_utc,
        "next_run_in_sec": next_run_in_sec,
        "lateness_sec": lateness_sec,
    }


def _dashboard_scanner_attempt_summary(scanner: dict | None) -> dict:
    scanner = dict(scanner or {})
    summary = dict(scanner.get("summary") or {})
    last = dict(scanner.get("last") or {})
    history = list(scanner.get("history") or [])
    current_boot_ts = str(last.get("boot_ts_utc") or "").strip()

    current_boot_events = []
    for item in history:
        details = dict((item or {}).get("details") or {})
        event_boot_ts = str(details.get("boot_ts_utc") or "").strip()
        if current_boot_ts and event_boot_ts == current_boot_ts:
            current_boot_events.append(item)
    if not current_boot_events and current_boot_ts:
        for item in history:
            if str((item or {}).get("ts_utc") or "").strip() >= current_boot_ts:
                current_boot_events.append(item)

    startup_attempts = 0
    startup_dispatch_errors = 0
    startup_closed_success = 0
    recent_events = []
    for item in current_boot_events[-6:]:
        event = str((item or {}).get("event") or "")
        status = str((item or {}).get("status") or "")
        ts = str((item or {}).get("ts_ny") or (item or {}).get("ts_utc") or "")
        recent_events.append({"ts": ts, "event": event, "status": status})
    for item in current_boot_events:
        details = dict((item or {}).get("details") or {})
        event = str((item or {}).get("event") or "")
        reason = str(details.get("reason") or details.get("requested_reason") or "").strip().lower()
        if event == "scan_attempt" and reason == "startup":
            startup_attempts += 1
        if event in {"scan_dispatch_http_error", "scan_dispatch_error"} and reason == "startup":
            startup_dispatch_errors += 1
        if event == "scan_ok" and str(details.get("scan_reason") or details.get("requested_reason") or "").strip().lower() == "startup":
            startup_closed_success += 1

    return {
        "boot_ts_utc": last.get("boot_ts_utc"),
        "boot_ts_ny": last.get("boot_ts_ny"),
        "last_event": last.get("last_event"),
        "last_event_status": last.get("last_event_status"),
        "last_worker_event": last.get("last_worker_event"),
        "last_success_utc": last.get("last_success_utc"),
        "last_failure_utc": last.get("last_failure_utc"),
        "last_dispatch_failure_utc": last.get("last_dispatch_failure_utc"),
        "current_sleep_sec": last.get("current_sleep_sec"),
        "next_run_estimate_utc": last.get("next_run_estimate_utc"),
        "startup_attempts_current_boot": startup_attempts,
        "startup_dispatch_errors_current_boot": startup_dispatch_errors,
        "startup_successes_current_boot": startup_closed_success,
        "dispatch_failures_today": summary.get("dispatch_failures_today"),
        "attempts_today": summary.get("attempts_today"),
        "success_today": summary.get("success_today"),
        "failure_today": summary.get("failure_today"),
        "closed_runs_today": summary.get("closed_runs_today"),
        "recent_events": recent_events,
    }


@app.post("/worker/scanner_heartbeat")
async def worker_scanner_heartbeat(req: Request):
    body = {}
    try:
        body = await req.json()
    except Exception:
        body = {}
    if WORKER_SECRET:
        if (body.get("worker_secret") or "").strip() != WORKER_SECRET:
            raise HTTPException(status_code=401, detail="Invalid worker secret")
    event = str(body.get("event") or "heartbeat").strip().lower() or "heartbeat"
    status = str(body.get("status") or "ok").strip().lower() or "ok"
    details = dict(body.get("details") or {})
    snapshot = _record_scanner_telemetry(event, status, details=details)
    return {"ok": True, "telemetry": snapshot}


@app.get("/diagnostics/scanner")
def diagnostics_scanner():
    _ensure_runtime_state_loaded()
    now_utc = datetime.now(timezone.utc)
    tel = dict(LAST_SCANNER_TELEMETRY or {})
    last_event_age_sec = None
    last_closed_age_sec = None
    next_run_in_sec = None
    try:
        ts = tel.get("last_event_utc")
        if ts:
            dt = datetime.fromisoformat(str(ts))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            last_event_age_sec = max(0.0, (now_utc - dt.astimezone(timezone.utc)).total_seconds())
    except Exception:
        pass
    try:
        ts = tel.get("last_closed_utc")
        if ts:
            dt = datetime.fromisoformat(str(ts))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            last_closed_age_sec = max(0.0, (now_utc - dt.astimezone(timezone.utc)).total_seconds())
    except Exception:
        pass
    try:
        ts = tel.get("next_run_estimate_utc")
        if ts:
            dt = datetime.fromisoformat(str(ts))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            next_run_in_sec = (dt.astimezone(timezone.utc) - now_utc).total_seconds()
    except Exception:
        pass
    today_prefix = str(now_ny().date())
    summary = _scanner_telemetry_summary(today_prefix=today_prefix)
    last_view = dict(tel)
    last_view.update({
        "attempts_total": summary.get("attempts_total"),
        "success_total": summary.get("success_total"),
        "failure_total": summary.get("failure_total"),
        "skipped_total": summary.get("skipped_total"),
        "attempts_today": summary.get("attempts_today"),
        "success_today": summary.get("success_today"),
        "failure_today": summary.get("failure_today"),
        "skipped_today": summary.get("skipped_today"),
        "closed_runs_total": summary.get("closed_runs_total"),
        "closed_runs_today": summary.get("closed_runs_today"),
        "incomplete_runs_total": summary.get("incomplete_runs_total"),
        "incomplete_runs_today": summary.get("incomplete_runs_today"),
        "historical_incomplete_runs_total": summary.get("historical_incomplete_runs_total"),
        "historical_incomplete_runs_today": summary.get("historical_incomplete_runs_today"),
        "in_flight_run": summary.get("in_flight_run"),
        "warning_codes": summary.get("warning_codes"),
        "active_warning_codes": summary.get("active_warning_codes"),
        "recovered_warning_codes": summary.get("recovered_warning_codes"),
        "historical_warning_codes": summary.get("historical_warning_codes"),
        "has_warnings": summary.get("has_warnings"),
        "has_active_warnings": summary.get("has_active_warnings"),
        "has_recovered_warnings": summary.get("has_recovered_warnings"),
        "last_closed_event": summary.get("last_closed_event"),
        "last_closed_status": summary.get("last_closed_status"),
        "last_closed_utc": summary.get("last_closed_utc"),
    })
    return {
        "ok": True,
        "telemetry_state_path": SCANNER_TELEMETRY_STATE_PATH,
        "state_restore": dict(globals().get("SCANNER_TELEMETRY_STATE_RESTORE") or {}),
        "summary": summary,
        "last": last_view,
        "history_count": len(SCANNER_TELEMETRY_HISTORY),
        "history_limit": SCANNER_TELEMETRY_HISTORY_LIMIT,
        "history": list(SCANNER_TELEMETRY_HISTORY[-50:]),
        "derived": {
            "last_event_age_sec": last_event_age_sec,
            "last_closed_age_sec": last_closed_age_sec,
            "next_run_in_sec": next_run_in_sec,
            "scanner_running": _worker_status_snapshot().get("scanner_running"),
            "scanner_status": _worker_status_snapshot().get("scanner_status"),
            "scanner_age_sec": _worker_status_snapshot().get("scanner_age_sec"),
        },
        "worker": _worker_status_snapshot(),
    }

@app.get("/diagnostics/freshness")
def diagnostics_freshness(request: Request):
    require_admin_if_configured(request)
    _ensure_runtime_state_loaded()
    _refresh_regime_snapshot_if_needed()
    snap = freshness_snapshot()
    snap.update({
        "ok": True,
        "release_max_scan_age_sec": RELEASE_MAX_SCAN_AGE_SEC,
        "readiness_scanner_max_age_sec": READINESS_SCANNER_MAX_AGE_SEC,
        "readiness_exit_max_age_sec": READINESS_EXIT_MAX_AGE_SEC,
    })
    return snap


def _dashboard_should_warn_on_freshness(*, stale_entries: list[str], missing_entries: list[str], session: dict | None = None) -> bool:
    stale = [str(item or "").strip() for item in (stale_entries or []) if str(item or "").strip()]
    missing = [str(item or "").strip() for item in (missing_entries or []) if str(item or "").strip()]
    if missing:
        return True
    if not stale:
        return False
    market_open_now = bool((session or {}).get("market_open_now"))
    if not market_open_now and set(stale) == {"regime"}:
        return False
    return True


def _dashboard_latest_completed_scan_summary() -> dict:
    def _usable_summary(scan_like: dict | None) -> dict:
        summary = dict((scan_like or {}).get("summary") or {})
        if not summary:
            return {}
        if summary.get("skipped"):
            return {}
        if summary.get("skip_reason"):
            return {}
        if summary.get("top_candidates") or summary.get("top_rejection_reasons") or summary.get("rejection_counts"):
            return summary
        return {}

    preferred = _usable_summary(LAST_SCAN if isinstance(LAST_SCAN, dict) else {})
    if preferred:
        return preferred

    for item in reversed(list(SCAN_HISTORY or [])):
        if not isinstance(item, dict):
            continue
        preferred = _usable_summary(item)
        if preferred:
            return preferred
    return {}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    require_admin_if_configured(request)
    _ensure_runtime_state_loaded()
    _refresh_regime_snapshot_if_needed()

    release = release_gate_status()
    continuity = continuity_snapshot(normalize_current=False)
    blockers = _diagnostics_swing_blockers()
    regime = dict(LAST_REGIME_SNAPSHOT or {})
    last_scan = dict(LAST_SCAN or {})
    last_lifecycle = dict(LAST_PAPER_LIFECYCLE or {})
    reconcile = build_reconcile_snapshot()
    freshness = freshness_snapshot()
    continuity_issues = list(continuity.get('issues') or [])
    freshness_entries = freshness.get("entries") or {}
    session = freshness.get("session") or {}
    scanner_summary = _scanner_telemetry_summary()
    worker_snapshot = _worker_status_snapshot()
    scanner_last_success = scanner_summary.get('last_closed_utc') or (LAST_SCANNER_TELEMETRY or {}).get('last_closed_utc') or (LAST_SCANNER_TELEMETRY or {}).get('last_success_utc') or 'none'
    authoritative_state = continuity.get('authoritative_state') or {}
    freshness_stale = list(freshness.get('stale_entries') or [])
    freshness_missing = list(freshness.get('missing_entries') or [])
    scanner_card_warnings = list(scanner_summary.get('active_warning_codes') or scanner_summary.get('warning_codes') or [])
    scanner_card_warnings = list(dict.fromkeys(scanner_card_warnings))
    reconcile_grade = str(reconcile.get('health_grade') or 'healthy')
    reconcile_actions = list(reconcile.get('recommended_actions') or [])
    reconcile_issue_codes = [str((item or {}).get('code') or '') for item in (reconcile.get('issues') or []) if str((item or {}).get('code') or '')]
    dashboard_warnings = []
    if _dashboard_should_warn_on_freshness(stale_entries=freshness_stale, missing_entries=freshness_missing, session=session):
        dashboard_warnings.append('freshness_degraded')
    if scanner_card_warnings:
        dashboard_warnings.extend(scanner_card_warnings)
    if reconcile_grade != 'healthy':
        dashboard_warnings.append(f'reconcile_{reconcile_grade}')
        dashboard_warnings.extend(reconcile_issue_codes[:3])
    if not continuity.get('ok'):
        dashboard_warnings.append('continuity_issues_present')
    if ((release.get('release_workflow') or {}).get('configured_stage_drift')):
        dashboard_warnings.append('release_stage_config_drift')
    scanner_worker_status = str(scanner_summary.get('worker_status') or worker_snapshot.get('scanner_status') or 'unknown').strip().lower()
    scanner_diag = diagnostics_scanner()
    scanner_runtime = _dashboard_scanner_runtime_hint(scanner_diag, worker_snapshot)
    scanner_attempts = _dashboard_scanner_attempt_summary(scanner_diag)
    scanner_display_status = str(scanner_runtime.get('display_status') or scanner_worker_status)
    exit_worker_status = str(worker_snapshot.get('exit_worker_status') or ('up' if worker_snapshot.get('exit_worker_running') else 'unknown')).strip().lower()
    combined_worker_status = scanner_display_status
    if scanner_worker_status in {'down', 'stale', 'unknown'}:
        combined_worker_status = 'down' if scanner_worker_status in {'down', 'stale'} else 'unknown'
    elif scanner_worker_status == 'late':
        combined_worker_status = 'late_but_alive'
    elif exit_worker_status in {'down', 'stale', 'unknown'}:
        combined_worker_status = 'down' if exit_worker_status in {'down', 'stale'} else 'unknown'
    workers_metric_class = _dashboard_metric_class(combined_worker_status, good_values={'up'}, bad_values={'down','unknown'}, neutral_values={'late','late_but_alive'})
    scanner_metric_class = _dashboard_metric_class(scanner_display_status, good_values={'up'}, bad_values={'down','unknown'}, neutral_values={'late','late_but_alive'})

    readiness = diagnostics_readiness(request)
    component_ready = bool(readiness.get('component_ready', readiness.get('ready')))
    system_health_ok = bool(readiness.get('system_health_ok', component_ready))
    market_tradable_now = bool(readiness.get('market_tradable_now', False))
    lifecycle_counts = dict((release.get('lifecycle_counts') or {}))
    lifecycle_events = list(PAPER_LIFECYCLE_HISTORY or [])
    if LAST_PAPER_LIFECYCLE and (not lifecycle_events or lifecycle_events[-1] != LAST_PAPER_LIFECYCLE):
        lifecycle_events.append(dict(LAST_PAPER_LIFECYCLE))

    def _latest_lifecycle_event(stage: str, statuses: set[str] | None = None) -> dict:
        target_stage = str(stage or '').strip().lower()
        target_statuses = {str(s).strip().lower() for s in (statuses or set()) if str(s).strip()}
        for ev in reversed(lifecycle_events):
            if not isinstance(ev, dict):
                continue
            ev_stage = str(ev.get('stage') or '').strip().lower()
            ev_status = str(ev.get('status') or '').strip().lower()
            if ev_stage != target_stage:
                continue
            if target_statuses and ev_status not in target_statuses:
                continue
            return dict(ev)
        return {}

    last_selected_event = _latest_lifecycle_event('candidate', {'selected'})
    last_entry_event = _latest_lifecycle_event('entry', {'planned', 'submitted', 'filled', 'opened'})
    last_exit_event = _latest_lifecycle_event('exit', {'submitted', 'closed', 'filled', 'completed', 'dry_run'})
    trade_path_proven = bool((lifecycle_counts.get('candidate_selected') or 0) > 0 and (lifecycle_counts.get('entry_events') or 0) > 0 and (lifecycle_counts.get('exit_events') or 0) > 0)
    ready_for_guarded_live = bool(((release.get('release_workflow') or {}).get('promotion_targets') or {}).get('guarded_live_eligible', {}).get('ready'))
    readiness_blockers = list((((release.get('release_workflow') or {}).get('promotion_targets') or {}).get('guarded_live_eligible', {}).get('unmet_conditions')) or (release.get('unmet_conditions') or []))
    readiness_metric_class = 'good' if ready_for_guarded_live else ('neutral' if component_ready else 'bad')
    proof_metric_class = 'good' if trade_path_proven else 'neutral'
    readiness_summary_badges = _dashboard_warning_badges(readiness_blockers[:4])
    if not market_tradable_now:
        if not bool(readiness.get('market_open')):
            readiness_blockers.append('market_closed')
        elif not bool(readiness.get('data_feed_ok')):
            readiness_blockers.append(str(readiness.get('data_feed_reason') or 'data_feed_not_tradable'))
    readiness_blockers = list(dict.fromkeys(readiness_blockers))
    readiness_summary_badges = _dashboard_warning_badges(readiness_blockers[:4])
    if system_health_ok and market_tradable_now and trade_path_proven:
        readiness_assessment_label = 'FULLY PROVEN'
        readiness_assessment_class = 'good'
        readiness_assessment_text = 'System health, current tradability, and trade-path proof are all present.'
    elif system_health_ok and not bool(readiness.get('market_open')):
        readiness_assessment_label = 'HEALTHY / MARKET CLOSED'
        readiness_assessment_class = 'neutral'
        readiness_assessment_text = 'Components are healthy, but the market is currently closed.'
    elif system_health_ok and bool(readiness.get('market_open')) and not market_tradable_now:
        readiness_assessment_label = 'HEALTHY / DATA NOT TRADABLE'
        readiness_assessment_class = 'neutral'
        readiness_assessment_text = str(readiness.get('data_feed_detail') or 'Components are healthy, but the current quote data is not tradable.')
    elif system_health_ok and market_tradable_now and not trade_path_proven:
        readiness_assessment_label = 'HEALTHY / PATH NOT PROVEN'
        readiness_assessment_class = 'neutral'
        readiness_assessment_text = 'Components are healthy and the market is tradable, but end-to-end trade path is not yet proven.'
    else:
        readiness_assessment_label = 'NOT READY'
        readiness_assessment_class = 'bad'
        readiness_assessment_text = 'Component health is not sufficient for promotion.'


    scan_summary = dict(last_scan.get('summary') or {})
    if (not scan_summary) or scan_summary.get('skipped') or scan_summary.get('skip_reason'):
        scan_summary = _dashboard_latest_completed_scan_summary()
    top_candidates = list((scan_summary.get('top_candidates') or []))[:5]
    rejection_counts = dict(scan_summary.get('rejection_counts') or {})
    if not rejection_counts:
        for item in (scan_summary.get('top_rejection_reasons') or []):
            if not isinstance(item, dict):
                continue
            reason = str(item.get('reason') or '').strip()
            count = item.get('count')
            if not reason:
                continue
            try:
                rejection_counts[reason] = int(count)
            except (TypeError, ValueError):
                continue
    top_rejections = sorted(rejection_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:8]

    candidate_rows = ''.join(
        '<tr>'
        f'<td>{html.escape(str(item.get("symbol") or ""))}</td>'
        f'<td>{_dashboard_fmt(item.get("rank_score"))}</td>'
        f'<td>{_dashboard_fmt(item.get("close"))}</td>'
        f'<td>{_dashboard_fmt(item.get("breakout_distance_pct"))}</td>'
        f'<td>{html.escape(", ".join(item.get("rejection_reasons") or []))}</td>'
        '</tr>'
        for item in top_candidates
    )
    if not candidate_rows:
        candidate_rows = '<tr><td colspan="5" class="muted">No recent candidate set available.</td></tr>'

    rejection_rows = ''.join(
        f'<tr><td>{html.escape(str(reason))}</td><td>{count}</td></tr>'
        for reason, count in top_rejections
    )
    if not rejection_rows:
        rejection_rows = '<tr><td colspan="2" class="muted">No rejection data available.</td></tr>'

    freshness_rows = ''.join(
        '<tr>'
        f'<td>{html.escape(str(name))}</td>'
        f'<td>{html.escape(str((row or {}).get("status") or ""))}</td>'
        f'<td>{_dashboard_fmt((row or {}).get("age_sec"))}</td>'
        f'<td>{html.escape(str((row or {}).get("same_session")))}</td>'
        f'<td>{html.escape(str((row or {}).get("source") or ""))}</td>'
        '</tr>'
        for name, row in freshness_entries.items()
    )
    if not freshness_rows:
        freshness_rows = '<tr><td colspan="5" class="muted">No freshness data available.</td></tr>'

    continuity_rows = ''.join(
        '<tr>'
        f'<td>{html.escape(str((item or {}).get("code") or ""))}</td>'
        f'<td>{html.escape(str((item or {}).get("severity") or ""))}</td>'
        f'<td>{html.escape(str((item or {}).get("symbol") or ""))}</td>'
        '</tr>'
        for item in continuity_issues[:10]
    )
    if not continuity_rows:
        continuity_rows = '<tr><td colspan="3" class="muted">No continuity issues detected.</td></tr>'

    html_doc = f'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Trading Webhook Dashboard</title>
  <meta http-equiv="refresh" content="30">
  <style>
    :root {{ color-scheme: dark; }}
    body {{ font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:#0b1220; color:#e5e7eb; margin:0; padding:20px; }}
    .wrap {{ max-width: 1400px; margin:0 auto; }}
    h1,h2,h3 {{ margin:0 0 10px; }}
    .sub {{ color:#94a3b8; margin:0 0 18px; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(260px,1fr)); gap:16px; }}
    .card {{ background:#111827; border:1px solid #1f2937; border-radius:14px; padding:16px; box-shadow:0 2px 8px rgba(0,0,0,.25); }}
    .metric {{ font-size:28px; font-weight:700; margin-top:8px; }}
    .muted {{ color:#94a3b8; }}
    .good {{ color:#86efac; }}
    .bad {{ color:#fca5a5; }}
    .neutral {{ color:#fcd34d; }}
    .badge {{ display:inline-block; padding:4px 8px; border-radius:999px; font-size:12px; font-weight:700; background:#1f2937; margin:4px 6px 0 0; }}
    .badge.good {{ background:#14532d; color:#bbf7d0; }}
    .badge.bad {{ background:#7f1d1d; color:#fecaca; }}
    .badge.neutral {{ background:#78350f; color:#fde68a; }}
    table {{ width:100%; border-collapse: collapse; }}
    th,td {{ text-align:left; padding:8px 10px; border-bottom:1px solid #1f2937; vertical-align:top; }}
    th {{ color:#93c5fd; width:34%; font-weight:600; }}
    pre {{ white-space:pre-wrap; word-break:break-word; background:#0f172a; border:1px solid #1f2937; border-radius:10px; padding:12px; overflow:auto; }}
    .section {{ margin-top:18px; }}
    .links a {{ color:#93c5fd; text-decoration:none; margin-right:14px; }}
    .links a:hover {{ text-decoration:underline; }}
  </style>
</head>
<body>
<div class="wrap">
  <h1>Read-Only Monitoring Dashboard</h1>
  <p class="sub">Auto-refresh every 30 seconds. This page reads in-memory state only and does not place, modify, or cancel orders.</p>
  <div class="links">
    <a href="/diagnostics/swing">swing</a>
    <a href="/diagnostics/candidates">candidates</a>
    <a href="/diagnostics/regime">regime</a>
    <a href="/diagnostics/release">release</a>
    <a href="/diagnostics/release_workflow">release_workflow</a>
    <a href="/diagnostics/reconcile">reconcile</a>
    <a href="/diagnostics/paper_lifecycle">paper_lifecycle</a>
    <a href="/diagnostics/execution_lifecycle">execution_lifecycle</a>
    <a href="/diagnostics/freshness">freshness</a>
    <a href="/diagnostics/scanner">scanner</a>
    <a href="/diagnostics/continuity">continuity</a>
    <a href="/diagnostics/readiness">readiness</a>
  </div>

  {('<div class="section"><div class="card"><h2>Operator Warnings</h2>' + _dashboard_warning_badges(dashboard_warnings) + '</div></div>') if dashboard_warnings else ''}

  <div class="section grid">
    <div class="card"><div class="muted">Release stage</div><div class="metric">{_dashboard_fmt(release.get('effective_release_stage') or release.get('system_release_stage'))}</div><div class="muted">Configured: {_dashboard_fmt(release.get('configured_release_stage') or release.get('system_release_stage'))}</div>{_dashboard_badge('Live orders permitted', release.get('live_orders_permitted'))}{_dashboard_badge('Workflow enforced', ((release.get('release_workflow') or {}).get('workflow_enforced')))}{_dashboard_warning_badges(['release_stage_config_drift'] if ((release.get('release_workflow') or {}).get('configured_stage_drift')) else [])}{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Market hours</div><div class="metric {'good' if session.get('market_open_now') else 'neutral'}">{'OPEN' if session.get('market_open_now') else ('WEEKEND' if session.get('market_closed_reason') == 'weekend' else 'CLOSED')}</div><div class="muted">Now NY: {_dashboard_fmt(blockers.get('now_ny'))}</div>{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Regime</div><div class="metric {'bad' if regime.get('favorable') is False else 'good' if regime.get('favorable') is True else 'neutral'}">{_dashboard_fmt(regime.get('favorable'))}</div>{_dashboard_badge('Data complete', regime.get('data_complete'))}{_dashboard_badge('Fresh', (freshness_entries.get('regime') or {}).get('fresh'))}{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Last scan</div><div class="metric">{_dashboard_fmt(last_scan.get('reason') or 'none')}</div><div class="muted">{_dashboard_fmt(last_scan.get('ts_utc'))}</div>{_dashboard_badge('Fresh', (freshness_entries.get('last_scan') or {}).get('fresh'))}{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Workers</div><div class="metric {workers_metric_class}">{html.escape(str(combined_worker_status).upper())}</div><div class="muted">Scanner: {html.escape(str(scanner_display_status).upper())} ({_dashboard_fmt(worker_snapshot.get('scanner_age_sec'))}s)</div><div class="muted">Next run: {_dashboard_fmt(scanner_runtime.get('next_run_estimate_utc'))} / in {_dashboard_fmt(scanner_runtime.get('next_run_in_sec'))}s</div><div class="muted">Late by: {_dashboard_fmt(scanner_runtime.get('lateness_sec'))}s / hint: {html.escape(str(scanner_runtime.get('hint_text') or 'none'))}</div><div class="muted">Exit: {html.escape(str(exit_worker_status).upper())} ({_dashboard_fmt(worker_snapshot.get('exit_worker_age_sec'))}s)</div>{_dashboard_warning_badges([scanner_runtime.get('hint_code')] if scanner_runtime.get('hint_code') not in {'none', ''} else [])}{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Scanner telemetry</div><div class="metric {scanner_metric_class}">{_dashboard_fmt(scanner_summary.get('closed_runs_today') or 0)}</div><div class="muted">Closed today / last closed: {_dashboard_fmt(scanner_last_success)}</div><div class="muted">Worker: {html.escape(str(scanner_display_status))} / last source: {html.escape(str(scanner_summary.get('last_request_source_kind') or 'unknown'))}</div><div class="muted">Manual today: {_dashboard_fmt(scanner_summary.get('manual_request_today') or 0)} / External today: {_dashboard_fmt(scanner_summary.get('external_request_today') or 0)}</div><div class="muted">Hint: {html.escape(str(scanner_runtime.get('hint_text') or 'none'))}</div>{_dashboard_warning_badges(scanner_card_warnings)}{_dashboard_warning_badges([scanner_runtime.get('hint_code')] if scanner_runtime.get('hint_code') not in {'none', ''} else [])}{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Session date</div><div class="metric">{html.escape(str(session.get('today_ny') or 'unknown'))}</div><div class="muted">Open / close: {html.escape(str(session.get('market_open_ny') or ''))} / {html.escape(str(session.get('market_close_ny') or ''))}</div>{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Freshness</div><div class="metric {'bad' if (freshness_stale or freshness_missing) else 'good'}">{len(freshness_stale)} stale / {len(freshness_missing)} missing</div><div class="muted">All fresh: {'YES' if freshness.get('all_fresh') else 'NO'}</div>{_dashboard_warning_badges(freshness_stale + freshness_missing)}{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Reconcile</div><div class="metric {_dashboard_metric_class(reconcile_grade, good_values={'healthy'}, bad_values={'blocking','critical'}, neutral_values={'degraded'})}">{html.escape(str(reconcile_grade).upper())}</div><div class="muted">Plans / orders / broker positions: {reconcile.get('active_plan_count',0)} / {reconcile.get('open_order_count',0)} / {reconcile.get('broker_positions_count',0)}</div><div class="muted">Issues: {_dashboard_fmt(reconcile.get('issue_total') or 0)} / blocked: {'YES' if reconcile.get('trading_blocked') else 'NO'}</div>{_dashboard_warning_badges(reconcile_issue_codes[:3])}{_dashboard_warning_badges(['action_required'] if reconcile_actions else [])}{_dashboard_source_badge('Source', 'authoritative')}</div>
    <div class="card"><div class="muted">Continuity</div><div class="metric {'good' if continuity.get('ok') else 'bad'}">{'OK' if continuity.get('ok') else 'ISSUES'}</div><div class="muted">{len(continuity.get('issues') or [])} issues / idle: {authoritative_state.get('idle')}</div>{_dashboard_warning_badges(continuity.get('issue_codes') or [])}{_dashboard_source_badge('Source', 'authoritative')}</div>
  </div>

  <div class="section grid">
    <div class="card">
      <h2>Readiness Evidence</h2>
      <table>{_dashboard_rows([
        ('system_health_ok', system_health_ok),
        ('market_tradable_now', market_tradable_now),
        ('component_ready', component_ready),
        ('ready_scope', readiness.get('ready_scope')),
        ('same_session_proven', readiness.get('same_session_proven')),
        ('trade_path_proven', trade_path_proven),
        ('scan_completed', lifecycle_counts.get('scan_completed')),
        ('candidate_selected', lifecycle_counts.get('candidate_selected')),
        ('entry_events', lifecycle_counts.get('entry_events')),
        ('exit_events', lifecycle_counts.get('exit_events')),
        ('last_selected_candidate_utc', last_selected_event.get('ts_utc')),
        ('last_entry_event_utc', last_entry_event.get('ts_utc')),
        ('last_exit_event_utc', last_exit_event.get('ts_utc')),
      ])}</table>
      <h3 style="margin-top:14px;">Assessment</h3>
      <div class="metric {readiness_assessment_class}">{readiness_assessment_label}</div>
      <div class="muted">{html.escape(readiness_assessment_text)}</div>
    </div>

    <div class="card">
      <h2>Guarded Live Path</h2>
      <table>{_dashboard_rows([
        ('guarded_live_ready', ready_for_guarded_live),
        ('component_ready', component_ready),
        ('system_health_ok', system_health_ok),
        ('market_tradable_now', market_tradable_now),
        ('trade_path_proven', trade_path_proven),
        ('same_session_proven', readiness.get('same_session_proven')),
        ('release_stage', release.get('effective_release_stage') or release.get('system_release_stage')),
        ('workflow_live_activation_armed', ((release.get('release_workflow') or {}).get('live_activation_armed'))),
        ('live_orders_permitted', release.get('live_orders_permitted')),
        ('scanner_running', readiness.get('scanner_running')),
        ('exit_worker_running', readiness.get('exit_worker_running')),
        ('broker_connected', readiness.get('broker_connected')),
        ('data_feed_ok', readiness.get('data_feed_ok')),
        ('data_feed_reason', readiness.get('data_feed_reason')),
        ('journal_ok', readiness.get('journal_ok')),
        ('risk_limits_ok', readiness.get('risk_limits_ok')),
      ])}</table>
      <h3 style="margin-top:14px;">Current blockers</h3>
      <pre>{html.escape(chr(10).join(readiness_blockers or ['none']))}</pre>
      <div class="metric {readiness_metric_class}">{'READY' if ready_for_guarded_live else 'BLOCKED'}</div>
      {readiness_summary_badges}
    </div>
  </div>

  <div class="section grid">
    <div class="card">
      <h2>Release Gates</h2>
      <table>{_dashboard_rows([
        ('go_live_eligible', release.get('go_live_eligible')),
        ('live_orders_permitted', release.get('live_orders_permitted')),
        ('effective_release_stage', release.get('effective_release_stage') or release.get('system_release_stage')),
        ('configured_release_stage', release.get('configured_release_stage') or release.get('system_release_stage')),
        ('workflow_live_activation_armed', ((release.get('release_workflow') or {}).get('live_activation_armed'))),
        ('recent_market_scan_ok', release.get('recent_market_scan_ok')),
        ('last_scan_age_sec', release.get('last_scan_age_sec')),
        ('scan_completed', ((release.get('lifecycle_counts') or {}).get('scan_completed'))),
        ('candidate_selected', ((release.get('lifecycle_counts') or {}).get('candidate_selected'))),
        ('entry_events', ((release.get('lifecycle_counts') or {}).get('entry_events'))),
        ('exit_events', ((release.get('lifecycle_counts') or {}).get('exit_events'))),
      ])}</table>
      <h3 style="margin-top:14px;">Unmet conditions</h3>
      <pre>{html.escape(chr(10).join(release.get('unmet_conditions') or ['none']))}</pre>
    </div>

    <div class="card">
      <h2>Current Regime</h2>
      <table>{_dashboard_rows([
        ('index_symbol', regime.get('index_symbol')),
        ('index_close', regime.get('index_close')),
        ('index_fast_ma', regime.get('index_fast_ma')),
        ('index_slow_ma', regime.get('index_slow_ma')),
        ('index_trend_ok', regime.get('index_trend_ok')),
        ('breadth', regime.get('breadth')),
        ('ret_breadth', regime.get('ret_breadth')),
        ('breadth_total', regime.get('breadth_total')),
        ('score', regime.get('score')),
        ('favorable', regime.get('favorable')),
        ('data_complete', regime.get('data_complete')),
      ])}</table>
      <h3 style="margin-top:14px;">Reasons</h3>
      <pre>{html.escape(chr(10).join(regime.get('reasons') or ['none']))}</pre>
    </div>
  </div>

  <div class="section grid">
    <div class="card">
      <h2>Session Boundaries</h2>
      <table>{_dashboard_rows([
        ('today_ny', session.get('today_ny')),
        ('now_ny', session.get('now_ny')),
        ('market_open_ny', session.get('market_open_ny')),
        ('market_close_ny', session.get('market_close_ny')),
        ('market_open_now', session.get('market_open_now')),
        ('scan_same_session', ((freshness_entries.get('last_scan') or {}).get('same_session'))),
        ('regime_same_session', ((freshness_entries.get('regime') or {}).get('same_session'))),
        ('lifecycle_same_session', ((freshness_entries.get('paper_lifecycle') or {}).get('same_session'))),
      ])}</table>
    </div>
    <div class="card">
      <h2>Freshness Diagnostics</h2>
      <table>
        <thead><tr><th>Source</th><th>Status</th><th>Age sec</th><th>Same session</th><th>Source type</th></tr></thead>
        <tbody>{freshness_rows}</tbody>
      </table>
      <h3 style="margin-top:14px;">Stale / missing</h3>
      <pre>{html.escape(chr(10).join((freshness.get('stale_entries') or []) + (freshness.get('missing_entries') or []) or ['none']))}</pre>
    </div>
  </div>

  <div class="section grid">
    <div class="card">
      <h2>Reconcile Actions</h2>
      <table>{_dashboard_rows([
        ('health_grade', reconcile.get('health_grade')),
        ('max_severity', reconcile.get('max_severity')),
        ('trading_blocked', reconcile.get('trading_blocked')),
        ('issue_total', reconcile.get('issue_total')),
      ])}</table>
      <h3 style="margin-top:14px;">Recommended actions</h3>
      <pre>{html.escape(chr(10).join(reconcile_actions or ['none']))}</pre>
    </div>
    <div class="card">
      <h2>Reconcile Issues</h2>
      <table>
        <thead><tr><th>Code</th><th>Severity</th><th>Symbols</th></tr></thead>
        <tbody>{''.join('<tr><td>' + html.escape(str((item or {}).get('code') or '')) + '</td><td>' + html.escape(str((item or {}).get('severity') or '')) + '</td><td>' + html.escape(', '.join((item or {}).get('symbols') or [])) + '</td></tr>' for item in (reconcile.get('issues') or [])[:8]) or '<tr><td colspan="3" class="muted">No reconcile issues detected.</td></tr>'}</tbody>
      </table>
    </div>
  </div>

  <div class="section grid">
    <div class="card">
      <h2>Top Candidate Rejections</h2>
      <table>
        <thead><tr><th>Symbol</th><th>Rank</th><th>Close</th><th>Breakout %</th><th>Reasons</th></tr></thead>
        <tbody>{candidate_rows}</tbody>
      </table>
    </div>
    <div class="card">
      <h2>Rejection Totals</h2>
      <table>
        <thead><tr><th>Reason</th><th>Count</th></tr></thead>
        <tbody>{rejection_rows}</tbody>
      </table>
    </div>
  </div>

  <div class="section grid">
    <div class="card">
      <h2>Blockers</h2>
      <pre>{_dashboard_json_block(blockers)}</pre>
    </div>
    <div class="card">
      <h2>Last Lifecycle Event</h2>
      <pre>{_dashboard_json_block(last_lifecycle)}</pre>
    </div>
  </div>

</div>
</body>
</html>'''
    return HTMLResponse(content=html_doc)


@app.get("/diagnostics/swing")
def diagnostics_swing():
    _ensure_runtime_state_loaded()
    _refresh_regime_snapshot_if_needed()
    return {
        'ok': True,
        'strategy_mode': STRATEGY_MODE,
        'strategy_name': SWING_STRATEGY_NAME,
        'scanner': {
            'enabled': SCANNER_ENABLED,
            'dry_run': SCANNER_DRY_RUN,
            'allow_live': SCANNER_ALLOW_LIVE,
            'live_trading_enabled': LIVE_TRADING_ENABLED,
        },
        'risk': {
            'swing_max_open_positions': MAX_OPEN_POSITIONS,
            'swing_risk_per_trade_dollars': RISK_DOLLARS,
            'swing_max_hold_days': SWING_MAX_HOLD_DAYS,
            'swing_max_portfolio_exposure_pct': SWING_MAX_PORTFOLIO_EXPOSURE_PCT,
            'swing_max_symbol_exposure_pct': SWING_MAX_SYMBOL_EXPOSURE_PCT,
            'swing_allow_same_day_exit': SWING_ALLOW_SAME_DAY_EXIT,
            'swing_portfolio_cap_block_mode': SWING_PORTFOLIO_CAP_BLOCK_MODE,
        },
        'regime': dict(LAST_REGIME_SNAPSHOT),
        'recovery': {
            'startup_state_restore': dict(globals().get('STARTUP_STATE') or {}),
            'recovered_active_symbols': [str(sym or '').upper() for sym, plan in (TRADE_PLAN or {}).items() if isinstance(plan, dict) and bool(plan.get('active')) and _plan_is_recovered(plan)],
        },
        'persistence': {
            'scan_state_path': SCAN_STATE_PATH,
            'regime_state_path': REGIME_STATE_PATH,
            'paper_lifecycle_state_path': PAPER_LIFECYCLE_STATE_PATH,
            'scan_state_restore': dict(globals().get('SCAN_STATE_RESTORE') or {}),
            'regime_state_restore': dict(globals().get('REGIME_STATE_RESTORE') or {}),
            'paper_lifecycle_state_restore': dict(globals().get('PAPER_LIFECYCLE_STATE_RESTORE') or {}),
            'scanner_telemetry_state_path': SCANNER_TELEMETRY_STATE_PATH,
            'scanner_telemetry_state_restore': dict(globals().get('SCANNER_TELEMETRY_STATE_RESTORE') or {}),
            'last_scan_state_source': 'memory' if LAST_SCAN else ('restored' if (globals().get('SCAN_STATE_RESTORE') or {}).get('last_scan_restored') else 'empty'),
            'last_regime_state_source': 'memory' if LAST_REGIME_SNAPSHOT else ('restored' if (globals().get('REGIME_STATE_RESTORE') or {}).get('current_restored') else 'empty'),
            'last_paper_lifecycle_source': 'memory' if LAST_PAPER_LIFECYCLE else ('restored' if (globals().get('PAPER_LIFECYCLE_STATE_RESTORE') or {}).get('last_event_restored') else 'empty'),
            'last_scanner_telemetry_source': 'memory' if LAST_SCANNER_TELEMETRY else ('restored' if (globals().get('SCANNER_TELEMETRY_STATE_RESTORE') or {}).get('last_restored') else 'empty'),
        },
        'release': release_gate_status(),
        'session': _session_boundary_snapshot(),
        'freshness': freshness_snapshot(),
        'blockers': _diagnostics_swing_blockers(),
        'scan_history_size': len(SCAN_HISTORY),
        'last_scan': {
            'ts_utc': LAST_SCAN.get('ts_utc'),
            'reason': LAST_SCAN.get('reason'),
            'scanned': LAST_SCAN.get('scanned'),
            'signals': LAST_SCAN.get('signals'),
            'would_trade': LAST_SCAN.get('would_trade'),
            'blocked': LAST_SCAN.get('blocked'),
            'summary': dict((LAST_SCAN.get('summary') or {})),
        },
    }


@app.get("/diagnostics/regime")
def diagnostics_regime(limit: int = 20):
    _ensure_runtime_state_loaded()
    lim = max(1, min(int(limit or 20), 200))
    parsed_groups = _correlation_groups_list()
    return {
        'ok': True,
        'strategy_mode': STRATEGY_MODE,
        'settings': {
            'swing_regime_filter_enabled': SWING_REGIME_FILTER_ENABLED,
            'swing_regime_fast_ma_days': SWING_REGIME_FAST_MA_DAYS,
            'swing_regime_slow_ma_days': SWING_REGIME_SLOW_MA_DAYS,
            'swing_regime_min_breadth': SWING_REGIME_MIN_BREADTH,
            'swing_allow_new_entries_in_weak_tape': SWING_ALLOW_NEW_ENTRIES_IN_WEAK_TAPE,
            'swing_weak_tape_max_new_entries': SWING_WEAK_TAPE_MAX_NEW_ENTRIES,
            'swing_max_group_positions': SWING_MAX_GROUP_POSITIONS,
            'swing_portfolio_cap_block_mode': SWING_PORTFOLIO_CAP_BLOCK_MODE,
            'swing_correlation_groups': SWING_CORRELATION_GROUPS,
            'swing_correlation_groups_normalized': _normalize_correlation_groups_raw(SWING_CORRELATION_GROUPS),
            'swing_correlation_groups_parsed': parsed_groups,
        },
        'current': dict(LAST_REGIME_SNAPSHOT),
        'history': REGIME_HISTORY[-lim:],
        'blockers': _diagnostics_swing_blockers(),
    }




@app.get("/diagnostics/release")
def diagnostics_release(request: Request):
    require_admin_if_configured(request)
    status = release_gate_status()
    status.update({
        "ok": True,
        "dry_run": DRY_RUN,
        "live_trading_enabled": LIVE_TRADING_ENABLED,
        "scanner_allow_live": SCANNER_ALLOW_LIVE,
        "kill_switch": KILL_SWITCH,
        "daily_halt_active": daily_halt_active(),
        "requirements": {
            "release_require_regime_complete": RELEASE_REQUIRE_REGIME_COMPLETE,
            "release_require_regime_favorable": RELEASE_REQUIRE_REGIME_FAVORABLE,
            "release_require_recent_market_scan": RELEASE_REQUIRE_RECENT_MARKET_SCAN,
            "release_max_scan_age_sec": RELEASE_MAX_SCAN_AGE_SEC,
            "release_min_completed_scans": RELEASE_MIN_COMPLETED_SCANS,
            "release_min_selected_candidates": RELEASE_MIN_SELECTED_CANDIDATES,
            "release_min_entry_events": RELEASE_MIN_ENTRY_EVENTS,
            "release_min_exit_events": RELEASE_MIN_EXIT_EVENTS,
        },
    })
    return status


@app.get("/diagnostics/release_workflow")
def diagnostics_release_workflow(request: Request):
    require_admin_if_configured(request)
    return {
        "ok": True,
        "release_state_path": RELEASE_STATE_PATH,
        "workflow": _release_workflow_snapshot(include_gate=True),
        "dry_run": DRY_RUN,
        "live_trading_enabled": LIVE_TRADING_ENABLED,
        "scanner_allow_live": SCANNER_ALLOW_LIVE,
    }


@app.post("/admin/release/promote/{target_stage}")
async def admin_release_promote(request: Request, target_stage: str):
    require_admin(request)
    actor = request.headers.get("x-admin-actor", "admin").strip() or "admin"
    body = {}
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}
    reason = str(body.get("reason") or request.headers.get("x-release-reason") or "manual_transition")
    workflow = release_stage_transition(target_stage=target_stage, actor=actor, reason=reason)
    return {"ok": True, "workflow": workflow}


@app.get("/diagnostics/continuity")
def diagnostics_continuity(request: Request):
    require_admin_if_configured(request)
    _ensure_runtime_state_loaded()
    _refresh_regime_snapshot_if_needed()
    snap = continuity_snapshot(normalize_current=True)
    snap["ok"] = bool(snap.get("ok"))
    return snap


@app.get("/diagnostics/paper_lifecycle")
def diagnostics_paper_lifecycle():
    _ensure_runtime_state_loaded()
    return {
        "ok": True,
        "state_path": PAPER_LIFECYCLE_STATE_PATH,
        "state_restore": dict(globals().get("PAPER_LIFECYCLE_STATE_RESTORE") or {}),
        "last_event": dict(LAST_PAPER_LIFECYCLE or {}),
        "history_limit": PAPER_LIFECYCLE_HISTORY_LIMIT,
        "history_count": len(PAPER_LIFECYCLE_HISTORY or []),
        "history": list(PAPER_LIFECYCLE_HISTORY or []),
    }

@app.get("/diagnostics/execution_lifecycle")
def diagnostics_execution_lifecycle(limit: int = 100):
    return execution_lifecycle_snapshot(limit=limit)

@app.get("/diagnostics/candidates")
def diagnostics_candidates(limit: int = 25):
    _ensure_runtime_state_loaded()
    _refresh_regime_snapshot_if_needed()
    lim = max(1, min(int(limit or 25), 200))
    latest = LAST_SWING_CANDIDATES[-lim:] if LAST_SWING_CANDIDATES else []
    hist = CANDIDATE_HISTORY[-5:]
    return {
        'ok': True,
        'strategy_mode': STRATEGY_MODE,
        'strategy_name': SWING_STRATEGY_NAME,
        'regime': dict(LAST_REGIME_SNAPSHOT),
        'blockers': _diagnostics_swing_blockers(),
        'count': len(latest),
        'items': latest,
        'history': hist,
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

    user_agent = str(req.headers.get("user-agent", "") or "")
    scanner_source_header = str(req.headers.get("X-Scanner-Source", "") or "").strip().lower()
    ua_l = user_agent.lower()
    source_kind = "unknown"
    if scanner_source_header == "worker" or ua_l.startswith("equities-scanner/"):
        source_kind = "worker"
    elif "postmanruntime" in ua_l or "insomnia" in ua_l:
        source_kind = "manual"
    elif ua_l:
        source_kind = "external"
    worker_source = source_kind == "worker"
    source_meta = {"requested_reason": str(body.get("reason") or ""), "user_agent": user_agent, "source_ip": getattr(req.client, "host", ""), "source_kind": source_kind, "worker_source": worker_source}

    try:
        _record_scanner_telemetry("scan_request", "received", details=source_meta)
    except Exception:
        pass

    effective_dry_run = bool(SCANNER_DRY_RUN or (not is_live_trading_permitted("worker_scan")))
    requested_reason = str(body.get("reason") or "").strip() or None

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
        if requested_reason and not LAST_SCAN.get('reason'):
            LAST_SCAN['reason'] = requested_reason
        try:
            if isinstance(LAST_SCAN.get('summary'), dict) and requested_reason and not LAST_SCAN['summary'].get('scan_reason'):
                LAST_SCAN['summary']['scan_reason'] = requested_reason
        except Exception:
            pass
        try:
            persist_scan_runtime_state(reason=str(LAST_SCAN.get('reason') or kwargs.get('reason') or "set_last_scan"))
        except Exception:
            pass

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
            try:
                _record_scanner_telemetry("scan_skip", "skipped", details={"reason": "scanner_disabled", "duration_ms": _elapsed_ms(), "scan_reason": requested_reason or "scheduled", **source_meta})
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
            try:
                _record_scanner_telemetry("scan_skip", "skipped", details={"reason": "outside_market_hours", "duration_ms": _elapsed_ms(), "scan_reason": requested_reason or "scheduled", **source_meta})
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
            try:
                _record_scanner_telemetry("scan_skip", "skipped", details={"reason": "outside_scanner_session", "duration_ms": _elapsed_ms(), "scan_reason": requested_reason or "scheduled", **source_meta})
            except Exception:
                pass
            return {"ok": True, "skipped": True, "reason": "outside_scanner_session", **LAST_SCAN}

# Reconcile first: never place entries against stale internal state.
        reconcile_actions = reconcile_trade_plans_from_alpaca()

        if STRATEGY_MODE == "swing":
            swing_resp = run_swing_daily_scan(effective_dry_run, _set_last_scan, _elapsed_ms, reconcile_actions=reconcile_actions)
            try:
                scanner_payload = swing_resp.get("scanner") if isinstance(swing_resp, dict) else {}
                _record_scanner_telemetry(
                    "scan_ok",
                    "success",
                    details={
                        "status": 200,
                        "symbols_scanned": int((scanner_payload or {}).get("symbols_scanned") or 0),
                        "signals": int((scanner_payload or {}).get("signals") or 0),
                        "blocked": int((scanner_payload or {}).get("blocked") or 0),
                        "duration_ms": int((scanner_payload or {}).get("duration_ms") or _elapsed_ms() or 0),
                        "scan_reason": requested_reason or "scheduled",
                        **source_meta,
                    },
                )
            except Exception:
                pass
            return swing_resp

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
            filtered_candidate_info = list(candidate_info)
            if SCANNER_UNIVERSE_PROVIDER == "dynamic":
                filtered_candidate_info = [
                    row for row in candidate_info
                    if float(row.get("price", 0.0) or 0.0) >= float(SCANNER_DYNAMIC_MIN_PRICE)
                    and float(row.get("dollar_vol", 0.0) or 0.0) >= float(SCANNER_DYNAMIC_MIN_DOLLAR_VOL)
                    and float(row.get("relvol", 0.0) or 0.0) >= float(SCANNER_DYNAMIC_MIN_RELVOL)
                    and (float(row.get("range_pct", 0.0) or 0.0) / 100.0) >= float(SCANNER_DYNAMIC_MIN_RANGE_PCT)
                ]
                if not filtered_candidate_info:
                    filtered_candidate_info = list(candidate_info)

            selected_n = max(1, int(SCANNER_CANDIDATE_LIMIT))
            syms = [row["symbol"] for row in filtered_candidate_info[:selected_n]]

            if SCANNER_UNIVERSE_PROVIDER == "dynamic" and SCANNER_DYNAMIC_KEEP_ANCHORS:
                anchors = [s for s in SCANNER_ANCHOR_SYMBOLS if s in bars_map]
                syms = _dedupe_keep_order(syms + anchors)[: max(selected_n, len(syms))]

        vol_rank_info = {
            "enabled": True,
            "mode": "activity_rank",
            "primary_strategy": SCANNER_PRIMARY_STRATEGY,
            "selected_n": len(syms),
            "universe_n": len(candidate_info),
            "top": candidate_info[: min(10, len(candidate_info))],
            "provider": SCANNER_UNIVERSE_PROVIDER,
            "dynamic_filters": {
                "top_n": int(SCANNER_DYNAMIC_TOP_N),
                "min_price": float(SCANNER_DYNAMIC_MIN_PRICE),
                "min_dollar_vol": float(SCANNER_DYNAMIC_MIN_DOLLAR_VOL),
                "min_relvol": float(SCANNER_DYNAMIC_MIN_RELVOL),
                "min_range_pct": float(SCANNER_DYNAMIC_MIN_RANGE_PCT),
                "keep_anchors": bool(SCANNER_DYNAMIC_KEEP_ANCHORS),
            } if SCANNER_UNIVERSE_PROVIDER == "dynamic" else {},
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
                            vp_diag_for_rank = diag.get("vwap_pullback", {}) if isinstance(diag.get("vwap_pullback"), dict) else {}
                            rank_score, rank_meta = compute_signal_rank(signal_name, vp_diag_for_rank)
                            local_signals.append({"symbol": sym, "action": action, "side": side, "price": price, "signal": signal_name, "score": float(vp_diag_for_rank.get("score", 0.0)), "rank_score": rank_score, "signal_family": rank_meta.get("family"), "rank_meta": rank_meta, "plan_preview": preview_plan})

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

        signals.sort(key=lambda r: float(r.get("rank_score", r.get("score", 0.0))), reverse=True)

        component_counts = Counter()
        macro_counts = Counter()
        micro_counts = Counter()
        fallback_counts = Counter()
        fallback_ready_count = 0
        fallback_tradable_count = 0
        fallback_only_fail_count = 0
        near_miss_symbols = []
        pre_ranked_candidates = []
        for r in results:
            try:
                vp = (((r.get("diagnostics") or {}).get("vwap_pullback")) or {})
                for reason in (vp.get("component_reasons") or []):
                    component_counts[str(reason)] += 1
                split = vp.get("blocker_split") or {}
                for reason in (split.get("macro") or []):
                    macro_counts[str(reason)] += 1
                for reason in (split.get("micro") or []):
                    micro_counts[str(reason)] += 1
                for reason in (split.get("fallback") or []):
                    fallback_counts[str(reason)] += 1
                rank_score, rank_meta = compute_signal_rank("VWAP_PULLBACK_FALLBACK" if bool((vp.get("trend_components") or {}).get("fallback_ready")) else "VWAP_PULLBACK", vp)
                pre_ranked_candidates.append({
                    "symbol": r.get("symbol"),
                    "reason": vp.get("reason"),
                    "signal_family": rank_meta.get("family"),
                    "raw_score": rank_meta.get("raw_score"),
                    "rank_score": rank_meta.get("rank_score"),
                    "dist_to_vwap_pct": vp.get("dist_to_vwap_pct"),
                    "fallback_ready": bool((vp.get("trend_components") or {}).get("fallback_ready")),
                })
                nm = vp.get("near_miss") or {}
                if bool(nm.get("near")):
                    near_miss_symbols.append({
                        "symbol": r.get("symbol"),
                        "score": vp.get("score"),
                        "dist_to_vwap_pct": vp.get("dist_to_vwap_pct"),
                        "reason": vp.get("reason"),
                    })
                tc = vp.get("trend_components") or {}
                if bool(tc.get("fallback_ready")):
                    fallback_ready_count += 1
                    if bool(tc.get("fallback_slope_ok")) and bool(tc.get("fallback_bounce_ok")) and bool(tc.get("fallback_distance_ok")):
                        fallback_tradable_count += 1
                    elif (vp.get("reason") or "").startswith("fallback_"):
                        fallback_only_fail_count += 1
            except Exception:
                pass
        pre_ranked_candidates.sort(key=lambda x: float(x.get("rank_score", 0.0) or 0.0), reverse=True)

        scan_summary = {
            "actions": dict(action_counts),
            "no_signal_total": int(sum(no_signal_counts.values())),
            "top_no_signal_reasons": top_no_signal,
            "near_miss_total": int(sum(near_miss_counts.values())),
            "near_miss_by_strategy": {k: int(v) for k, v in near_miss_counts.items()},
            "strategy_breakdown": strategy_breakdown,
            "top_component_blockers": [{"reason": k, "count": int(v)} for k, v in component_counts.most_common(10)],
            "top_macro_blockers": [{"reason": k, "count": int(v)} for k, v in macro_counts.most_common(10)],
            "top_micro_blockers": [{"reason": k, "count": int(v)} for k, v in micro_counts.most_common(10)],
            "fallback_ready_total": int(fallback_ready_count),
            "fallback_tradable_total": int(fallback_tradable_count),
            "fallback_only_fail_total": int(fallback_only_fail_count),
            "top_fallback_blockers": [{"reason": k, "count": int(v)} for k, v in fallback_counts.most_common(10)],
            "near_miss_symbols": near_miss_symbols[: min(10, len(near_miss_symbols))],
            "top_pre_ranked_candidates": pre_ranked_candidates[: min(10, len(pre_ranked_candidates))],
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

        # Execute (optional): submit up to N live orders per scan using rank-aware slot allocation.
        would_submit = []
        ignored_ranked_out = []
        candidate_slots = candidate_slots_available()
        ranked_candidates = []
        if signals:
            for plan in signals:
                rank_score = float(plan.get("rank_score", plan.get("score", 0.0)) or 0.0)
                raw_score = float(plan.get("score", 0.0) or 0.0)
                family = str(plan.get("signal_family") or "primary")
                min_rank = SCANNER_FALLBACK_MIN_RANK_SCORE if family == "fallback" else SCANNER_RANK_MIN_SCORE
                if SIGNAL_RANKING_ENABLED and rank_score < float(min_rank):
                    record_decision("SCAN", "worker_scan", symbol=plan.get("symbol", ""), side=plan.get("side", ""), signal=plan.get("signal", ""), action="ignored", reason="rank_below_threshold", meta={"rank_score": rank_score, "min_rank": float(min_rank), "signal_family": family})
                    ignored_ranked_out.append({"symbol": plan.get("symbol"), "signal": plan.get("signal"), "rank_score": rank_score, "reason": "rank_below_threshold"})
                    continue
                if SIGNAL_RANKING_ENABLED and family == "fallback" and raw_score < float(SCANNER_FALLBACK_MIN_RAW_SCORE):
                    record_decision("SCAN", "worker_scan", symbol=plan.get("symbol", ""), side=plan.get("side", ""), signal=plan.get("signal", ""), action="ignored", reason="fallback_raw_score_too_low", meta={"raw_score": raw_score, "min_raw_score": float(SCANNER_FALLBACK_MIN_RAW_SCORE)})
                    ignored_ranked_out.append({"symbol": plan.get("symbol"), "signal": plan.get("signal"), "rank_score": rank_score, "reason": "fallback_raw_score_too_low"})
                    continue
                ranked_candidates.append(plan)

            allowed_submits = ranked_candidates[: max(0, min(candidate_slots, int(max(1, SCANNER_MAX_ENTRIES_PER_SCAN))))]
            for skipped in ranked_candidates[len(allowed_submits):]:
                record_decision("SCAN", "worker_scan", symbol=skipped.get("symbol", ""), side=skipped.get("side", ""), signal=skipped.get("signal", ""), action="ignored", reason="lower_rank_than_top_slots", meta={"rank_score": float(skipped.get("rank_score", skipped.get("score", 0.0)) or 0.0), "candidate_slots": candidate_slots})
                ignored_ranked_out.append({"symbol": skipped.get("symbol"), "signal": skipped.get("signal"), "rank_score": float(skipped.get("rank_score", skipped.get("score", 0.0)) or 0.0), "reason": "lower_rank_than_top_slots"})

            for plan in allowed_submits:
                sym = plan.get("symbol")
                side = plan.get("side") or "buy"
                sig = plan.get("signal") or "scan"
                payload = {"symbol": sym, "side": side, "signal": sig, "rank_score": float(plan.get("rank_score", plan.get("score", 0.0)) or 0.0), "signal_family": plan.get("signal_family", "primary")}
                if SCANNER_ALLOW_LIVE and (not SCANNER_DRY_RUN) and (not effective_dry_run):
                    resp = submit_scan_trade(sym, side, sig, meta={"rank_score": payload["rank_score"], "signal_family": payload["signal_family"]})
                    would_submit.append({**payload, **resp})
                else:
                    would_submit.append({**payload, "ok": True, "action": "dry_run", "reason": "scanner_not_live_or_dry_run"})

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
                    "candidate_slots": candidate_slots,
                    "ignored_ranked_out": ignored_ranked_out,
                    "would_submit": would_submit,
                })
                if len(SCAN_HISTORY) > SCAN_HISTORY_SIZE:
                    del SCAN_HISTORY[: len(SCAN_HISTORY) - SCAN_HISTORY_SIZE]
                persist_scan_runtime_state(reason="worker_scan_entries")
                try:
                    _record_scanner_telemetry("scan_ok", "success", details={"status": 200, "symbols_scanned": len(syms), "signals": len(signals), "blocked": blocked, "duration_ms": duration_ms, "scan_reason": requested_reason or "scheduled", **source_meta})
                except Exception:
                    pass
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
            try:
                _record_scanner_telemetry("scan_error", "exception", details={"error": str(e), "duration_ms": duration_ms, "scan_reason": requested_reason or "scheduled", **source_meta})
            except Exception:
                pass
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