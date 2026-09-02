"""Minimal Alpaca stock-bar transport for the regime-intraday service."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


NY = ZoneInfo("America/New_York")


def _credentials() -> tuple[str, str]:
    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_PAPER_API_KEY_ID") or ""
    secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_PAPER_API_SECRET_KEY") or ""
    return key.strip(), secret.strip()


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_minute_bars(symbols: list[str], *, start: datetime, end: datetime, max_pages: int = 12) -> tuple[dict[str, list[dict]], dict]:
    symbols = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
    output = {symbol: [] for symbol in symbols}
    key, secret = _credentials()
    debug = {"method": "alpaca_rest", "feed": os.getenv("DATA_FEED", "iex"), "pages": 0, "count": 0}
    if not key or not secret:
        return output, {**debug, "error": "alpaca_market_data_credentials_missing"}
    token = None
    try:
        for _ in range(max(1, min(50, max_pages))):
            params = {
                "symbols": ",".join(symbols), "timeframe": "1Min", "start": _iso(start), "end": _iso(end),
                "limit": "10000", "adjustment": os.getenv("DATA_ADJUSTMENT", "raw"),
                "feed": os.getenv("DATA_FEED", "iex"), "sort": "asc",
            }
            if token:
                params["page_token"] = token
            request = Request(
                f"https://data.alpaca.markets/v2/stocks/bars?{urlencode(params)}",
                headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret, "User-Agent": "trading-webhook/regime-intraday-v2"},
            )
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
            for symbol, rows in dict(payload.get("bars") or {}).items():
                for row in rows or []:
                    stamp = datetime.fromisoformat(str(row.get("t") or "").replace("Z", "+00:00"))
                    output.setdefault(symbol.upper(), []).append({
                        "ts_utc": stamp, "ts_ny": stamp.astimezone(NY), "open": float(row.get("o") or 0),
                        "high": float(row.get("h") or 0), "low": float(row.get("l") or 0),
                        "close": float(row.get("c") or 0), "volume": float(row.get("v") or 0), "vwap": float(row.get("vw") or 0),
                    })
            debug["pages"] += 1
            token = payload.get("next_page_token")
            if not token:
                break
        debug.update({"count": sum(len(rows) for rows in output.values()), "truncated": bool(token)})
    except Exception as exc:
        debug["error"] = str(exc)[:300]
    return output, debug


def fetch_recent_minute_bars(symbols: list[str], lookback_days: int = 1) -> tuple[dict[str, list[dict]], dict]:
    end = datetime.now(timezone.utc)
    return fetch_minute_bars(symbols, start=end - timedelta(days=max(1, lookback_days)), end=end)
