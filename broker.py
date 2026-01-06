#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
broker.py — Alpaca adapter for equities

Implements the same public API shape used by br_router:

- get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300) -> List[{t,o,h,l,c,v}]
- last_price(symbol: str) -> float
- last_trade_map(symbols: list[str]) -> dict[SYMBOL, {"price": float}]
- market_notional(symbol: str, side: "buy"|"sell", notional: float,
                  price: float|None = None, strategy: str|None = None, **kwargs) -> dict
- orders() -> list[dict]
- positions() -> list[dict]
- trades_history(count: int = 20) -> dict   # best-effort fills view

This uses Alpaca’s Trading + Market Data v2 REST API via requests.

Env it expects (matching your existing webhook):

  APCA_API_BASE_URL     e.g. https://paper-api.alpaca.markets
  APCA_API_KEY_ID
  APCA_API_SECRET_KEY
  APCA_DATA_BASE_URL    e.g. https://data.alpaca.markets
  APCA_DATA_FEED        e.g. iex (or sip)
"""

from __future__ import annotations

import datetime as dt
import os
import time
from typing import Any, Dict, List, Optional

import requests

# --- Env / config -----------------------------------------------------------

API_BASE = os.getenv("APCA_API_BASE_URL", os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"))
DATA_BASE = os.getenv("APCA_DATA_BASE_URL", os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"))
API_KEY  = os.getenv("APCA_API_KEY_ID", os.getenv("ALPACA_API_KEY"))
API_SECRET = os.getenv("APCA_API_SECRET_KEY", os.getenv("ALPACA_API_SECRET_KEY", os.getenv("ALPACA_API_SECRET")))
DATA_FEED = os.getenv("APCA_DATA_FEED", os.getenv("ALPACA_FEED", "iex"))

DEFAULT_TIMEOUT = float(os.getenv("ALPACA_HTTP_TIMEOUT", "10.0"))

if not API_KEY or not API_SECRET:
    # In dev you might be running with dry-mode; we still keep this check
    # so real deploys fail loudly if keys are missing.
    pass


def _headers() -> Dict[str, str]:
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Missing Alpaca API keys (APCA_API_KEY_ID / APCA_API_SECRET_KEY)")
    return {
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET,
    }


# --- Helpers ----------------------------------------------------------------

def _parse_bar_time(t: Any) -> int:
    """
    Alpaca v2 stock bars generally return ISO8601 strings in 't'.
    Convert to epoch seconds.
    """
    if isinstance(t, (int, float)):
        return int(t)
    if isinstance(t, str):
        # Normalize '2025-01-01T13:30:00Z' -> aware datetime
        s = t.replace("Z", "+00:00")
        try:
            dt_obj = dt.datetime.fromisoformat(s)
        except Exception:
            # Last resort: just return 0
            return 0
        return int(dt_obj.timestamp())
    return 0


def _normalize_symbol(symbol: str) -> str:
    """
    For equities we basically use the UI symbol as-is (SPY, QQQ, TSLA, ...),
    uppercased, with whitespace stripped.
    """
    return symbol.strip().upper()


# --- Market data ------------------------------------------------------------

def get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300) -> List[Dict[str, Any]]:
    """
    Return a list of bars: [{t,o,h,l,c,v}] newest last, using Alpaca v2 stock bars.

    Maps directly from /v2/stocks/bars:
      GET {DATA_BASE}/v2/stocks/bars?symbols=SPY&timeframe=5Min&limit=300
    """
    sym = _normalize_symbol(symbol)

    url = f"{DATA_BASE}/v2/stocks/bars"
    params = {
        "symbols": sym,
        "timeframe": timeframe,  # Alpaca accepts "1Min", "5Min", "1Hour", "1Day", etc.
        "limit": int(limit),
        "feed": DATA_FEED,
    }

    # Alpaca's bars endpoint can legitimately return a very short slice if you don't
    # specify a time range (sometimes only the most recent window).
    # To make the strategy logic stable, we request an explicit lookback window.
    try:
        lookback_days = float(os.getenv("ALPACA_BARS_LOOKBACK_DAYS", "10"))
    except Exception:
        lookback_days = 10.0

    # Only apply for intraday timeframes.
    if "Min" in str(timeframe) or "Hour" in str(timeframe):
        end_dt = datetime.datetime.now(datetime.timezone.utc)
        start_dt = end_dt - datetime.timedelta(days=lookback_days)
        params["start"] = start_dt.isoformat().replace("+00:00", "Z")
        params["end"] = end_dt.isoformat().replace("+00:00", "Z")


    resp = requests.get(url, headers=_headers(), params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    data = resp.json() or {}

    # According to docs, v2 returns { "bars": { "SPY": [ {t,o,h,l,c,v,...}, ... ] } }
    raw_bars = (data.get("bars") or {}).get(sym, [])

    out: List[Dict[str, Any]] = []
    for b in raw_bars:
        t = _parse_bar_time(b.get("t"))
        out.append({
            "t": t,
            "o": float(b.get("o", 0.0)),
            "h": float(b.get("h", 0.0)),
            "l": float(b.get("l", 0.0)),
            "c": float(b.get("c", 0.0)),
            "v": float(b.get("v", 0.0)),
        })
    return out


def last_trade_map(symbols: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Return {SYMBOL: {"price": last_price}} using latest bars.

    For simplicity we call the "latest bars" endpoint with 1-min bars:
      GET {DATA_BASE}/v2/stocks/bars/latest?symbols=SPY,QQQ&timeframe=1Min
    """
    syms = [_normalize_symbol(s) for s in symbols if s]
    if not syms:
        return {}

    url = f"{DATA_BASE}/v2/stocks/bars/latest"
    params = {
        "symbols": ",".join(syms),
        "timeframe": "1Min",
        "feed": DATA_FEED,
    }

    resp = requests.get(url, headers=_headers(), params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    data = resp.json() or {}

    # Shape: { "bars": { "SPY": {t,o,h,l,c,v,...}, ... } }
    bars = data.get("bars") or {}
    out: Dict[str, Dict[str, float]] = {}
    for sym in syms:
        b = bars.get(sym) or {}
        try:
            price = float(b.get("c", 0.0))
        except Exception:
            price = 0.0
        out[sym] = {"price": price}
    return out


def last_price(symbol: str) -> float:
    mp = last_trade_map([symbol])
    try:
        return float((mp.get(_normalize_symbol(symbol)) or {}).get("price", 0.0))
    except Exception:
        return 0.0


# --- Trading (market_notional) ----------------------------------------------

def market_notional(
    symbol: str,
    side: str,
    notional: float,
    price: Optional[float] = None,
    strategy: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Place a market order by **notional** dollar amount, matching the modern
    market_notional signature that br_router expects.

    Uses:
      POST {API_BASE}/v2/orders
      { "symbol": "SPY", "side": "buy", "type": "market",
        "time_in_force": "day", "notional": 100.0 }
    """
    sym = _normalize_symbol(symbol)
    side_l = side.lower()
    if side_l not in ("buy", "sell"):
        return {"ok": False, "error": f"invalid side: {side}"}

    url = f"{API_BASE}/v2/orders"

    # Optional: tag strategy in client_order_id so you can correlate in Alpaca UI/logs
    client_order_id = kwargs.get("client_order_id")
    if not client_order_id and strategy:
        client_order_id = f"{strategy}-{int(time.time())}"

    payload: Dict[str, Any] = {
        "symbol": sym,
        "side": side_l,
        "type": "market",
        "time_in_force": kwargs.get("time_in_force", "day"),
        "notional": float(notional),
    }
    if client_order_id:
        payload["client_order_id"] = client_order_id

    # You could also pass "order_class", "take_profit", "stop_loss" here if you want OCO
    # behavior to live at the Alpaca layer later.

    resp = requests.post(url, headers=_headers(), json=payload, timeout=DEFAULT_TIMEOUT)
    try:
        resp.raise_for_status()
    except Exception as exc:
        return {
            "ok": False,
            "status": resp.status_code,
            "error": f"alpaca order failed: {exc}",
            "body": resp.text,
        }

    order = resp.json() or {}
    return {
        "ok": True,
        "order": order,
    }


# --- Orders / positions / trades --------------------------------------------

def orders(status: str = "open", limit: int = 50) -> List[Dict[str, Any]]:
    """
    List orders (simple pass-through of Alpaca /v2/orders).
    """
    url = f"{API_BASE}/v2/orders"
    params = {
        "status": status,
        "limit": int(limit),
    }
    resp = requests.get(url, headers=_headers(), params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json() or []


def positions() -> List[Dict[str, Any]]:
    """
    List current positions via /v2/positions.
    """
    url = f"{API_BASE}/v2/positions"
    resp = requests.get(url, headers=_headers(), timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json() or []


def trades_history(count: int = 20) -> Dict[str, Any]:
    """
    Best-effort recent fills.

    We use /v2/account/activities?activity_types=FILL&direction=desc&date=YYYY-MM-DD
    and then slice the most recent `count` fills.
    """
    today = dt.date.today().isoformat()
    url = f"{API_BASE}/v2/account/activities"
    params = {
        "activity_types": "FILL",
        "date": today,
        "direction": "desc",
        "page_size": min(int(count), 100),
    }
    resp = requests.get(url, headers=_headers(), params=params, timeout=DEFAULT_TIMEOUT)
    if not resp.ok:
        # Don't break the whole app if fills endpoint fails; just return a stub.
        return {"ok": False, "error": f"alpaca activities error {resp.status_code}", "activities": []}

    acts = resp.json() or []
    return {
        "ok": True,
        "activities": acts[:count],
    }
