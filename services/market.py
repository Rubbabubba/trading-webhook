# services/market.py
# Version: 2025-09-12-CT
# Minimal market abstraction for S3 (Opening Range) and S4 (RSI Pullback).
# - Uses Alpaca REST for account/buying power, latest trade, and minute bars.
# - Exchange timezone (NYSE) handled in America/New_York for session math.
# - Local "now" is returned in America/Chicago so S3's CT windows work out-of-the-box.

from __future__ import annotations

import os
import math
import datetime as dt
from dataclasses import dataclass
from typing import List, Any, Dict, Optional

import requests
from zoneinfo import ZoneInfo


# -----------------------------
# Configuration / Environment
# -----------------------------
ALPACA_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID", "")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY", "")

ALPACA_TRADING_BASE = (os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")).rstrip("/")
ALPACA_DATA_BASE = (os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")).rstrip("/")

# Free plan usually requires feed=iex; if you have SIP entitlement, set ALPACA_FEED=sip
ALPACA_FEED = os.getenv("ALPACA_FEED", "iex")

# Minute timeframe used for bars
TIMEFRAME = os.getenv("INTRADAY_TIMEFRAME", "1Min")

# Timezones
EXCHANGE_TZ = ZoneInfo("America/New_York")      # NYSE
LOCAL_TZ = ZoneInfo(os.getenv("TZ", "America/Chicago"))  # for S3 window checks

# Debug printing to Render logs when set to "1"
MARKET_DEBUG = os.getenv("MARKET_DEBUG", "0") == "1"

# Shared HTTP session + headers
SESSION = requests.Session()
HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY or "",
    "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
    # Content-Type added per request when needed
}


# -----------------------------
# Helpers
# -----------------------------
def _iso_utc(ts: dt.datetime) -> str:
    """Return an RFC3339/ISO8601 UTC string for Alpaca (Z suffix)."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    ts = ts.astimezone(dt.timezone.utc)
    return ts.isoformat().replace("+00:00", "Z")


def _ema(values: List[float], length: int) -> float:
    if not values:
        return 0.0
    if len(values) < length:
        # Fallback: simple average over what we have
        return float(sum(values) / len(values))
    k = 2.0 / (length + 1.0)
    ema = float(values[0])
    for v in values[1:]:
        ema = float(v) * k + ema * (1.0 - k)
    return ema


def _get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: float = 10.0) -> Dict[str, Any]:
    """GET JSON with Alpaca auth headers; raises for HTTP errors with useful text."""
    r = SESSION.get(url, headers=HEADERS, params=params or {}, timeout=timeout)
    # If auth headers missing or entitlement wrong, Alpaca returns 401/403
    r.raise_for_status()
    return r.json()


# -----------------------------
# Data structures
# -----------------------------
@dataclass
class Bar:
    t: dt.datetime  # timestamp in EXCHANGE_TZ (aware)
    o: float
    h: float
    l: float
    c: float
    v: float


# -----------------------------
# Market implementation
# -----------------------------
class Market:
    """
    Methods required by S3/S4:
      - now() -> datetime (aware, LOCAL_TZ)
      - session_start(now) -> datetime (aware, EXCHANGE_TZ)  # 9:30 ET of 'today'
      - buying_power() -> float
      - last_price(symbol) -> float
      - position_sizer(symbol, risk_dollars, sl_pct) -> int
      - get_intraday(symbol) -> list[Bar]  # minute bars from session open → now
      - indicators(symbol) -> object with closes (List[float]), ema20, ema50
    """

    # ----- clock -----
    def now(self) -> dt.datetime:
        """Local 'now' in America/Chicago for S3's trade window checks."""
        return dt.datetime.now(tz=LOCAL_TZ)

    def session_start(self, now: dt.datetime) -> dt.datetime:
        """
        9:30 AM ET on the calendar day corresponding to 'now' (converted to ET).
        Returns an aware datetime in EXCHANGE_TZ.
        """
        if now.tzinfo is None:
            # Assume local timezone if naive
            now = now.replace(tzinfo=LOCAL_TZ)
        et_now = now.astimezone(EXCHANGE_TZ)
        open_et = et_now.replace(hour=9, minute=30, second=0, microsecond=0)
        return open_et

    # ----- account / pricing -----
    def buying_power(self) -> float:
        url = f"{ALPACA_TRADING_BASE}/v2/account"
        data = _get_json(url, timeout=8)
        bp = float(data.get("buying_power", 0.0))
        if MARKET_DEBUG:
            print(f"[market] buying_power=${bp:,.2f}")
        return bp

    def last_price(self, symbol: str) -> float:
        # Add feed param to avoid entitlement issues
        url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/trades/latest"
        params = {"feed": ALPACA_FEED}
        data = _get_json(url, params=params, timeout=6)
        px = float(data["trade"]["p"])
        if MARKET_DEBUG:
            print(f"[market] last_price {symbol} -> {px}")
        return px

    # ----- sizing -----
    def position_sizer(self, symbol: str, risk_dollars: float, sl_pct: float) -> int:
        """
        Simple risk model: shares = floor(risk $ / (price * sl_pct)).
        Returns an integer share qty >= 0.
        """
        px = max(self.last_price(symbol), 1e-6)
        denom = max(px * max(sl_pct, 1e-6), 1e-6)
        qty = int(math.floor(risk_dollars / denom))
        if MARKET_DEBUG:
            print(f"[market] size {symbol}: risk=${risk_dollars:.2f}, sl={sl_pct:.4f}, px={px:.2f} -> qty={qty}")
        return max(qty, 0)

    # ----- bars / indicators -----
    def get_intraday(self, symbol: str) -> List[Bar]:
        """
        Minute bars from today's session open (9:30 ET) to now.
        Returns Bar.t as aware EXCHANGE_TZ datetimes.
        """
        now_local = self.now()  # aware LOCAL_TZ
        start_et = self.session_start(now_local)         # aware EXCHANGE_TZ
        start_utc = start_et.astimezone(dt.timezone.utc) # aware UTC
        end_utc = now_local.astimezone(dt.timezone.utc)

        url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars"
        params = {
            "timeframe": TIMEFRAME,
            "start": _iso_utc(start_utc),
            "end": _iso_utc(end_utc),
            "adjustment": "raw",
            "limit": 10000,
            "feed": ALPACA_FEED,
        }
        data = _get_json(url, params=params, timeout=10)

        out: List[Bar] = []
        for b in data.get("bars", []):
            # Alpaca times are UTC ISO strings; convert to EXCHANGE_TZ for session math
            ts_utc = dt.datetime.fromisoformat(b["t"].replace("Z", "+00:00"))
            ts_et = ts_utc.astimezone(EXCHANGE_TZ)
            out.append(Bar(
                t=ts_et,
                o=float(b["o"]),
                h=float(b["h"]),
                l=float(b["l"]),
                c=float(b["c"]),
                v=float(b.get("v", 0.0)),
            ))

        if MARKET_DEBUG:
            print(f"[market] {symbol} bars since {start_et.isoformat()} ET -> {len(out)} bars")
        return out

    def indicators(self, symbol: str):
        """
        Returns closes list and EMAs for S4.
        Uses the same feed/timeframe as intraday; limit extended for better EMA stability.
        """
        url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars"
        params = {
            "timeframe": TIMEFRAME,
            "limit": 400,         # more history for RSI/EMA
            "adjustment": "raw",
            "feed": ALPACA_FEED,
        }
        data = _get_json(url, params=params, timeout=8)
        closes = [float(b["c"]) for b in data.get("bars", [])]

        ema20 = _ema(closes[-200:], 20) if closes else 0.0
        ema50 = _ema(closes[-200:], 50) if closes else 0.0

        if MARKET_DEBUG:
            print(f"[market] {symbol} indicators: closes={len(closes)}, ema20={ema20:.2f}, ema50={ema50:.2f}")

        # Return a lightweight object with attributes closes/ema20/ema50
        return type("Series", (), {"closes": closes, "ema20": ema20, "ema50": ema50})
