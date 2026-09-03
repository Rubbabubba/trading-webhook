"""Alpaca historical crypto-bar transport for research and replay."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_crypto_bars(
    symbols: list[str], *, start: datetime, end: datetime, timeframe: str = "1Hour", max_pages: int = 500
) -> tuple[dict[str, list[dict]], dict]:
    """Fetch paginated Alpaca US crypto bars without mutating broker state."""
    normalized = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
    output = {symbol: [] for symbol in normalized}
    key = (os.getenv("OPPORTUNITY_ALPACA_API_KEY_ID") or "").strip()
    secret = (os.getenv("OPPORTUNITY_ALPACA_API_SECRET_KEY") or "").strip()
    debug = {"method": "alpaca_crypto_rest", "timeframe": timeframe, "requested_start": _iso(start), "requested_end": _iso(end), "pages": 0, "count": 0}
    if not key or not secret:
        return output, {**debug, "error": "opportunity_alpaca_market_data_credentials_missing"}
    token = None
    try:
        for _ in range(max(1, min(500, int(max_pages)))):
            params = {"symbols": ",".join(normalized), "timeframe": timeframe, "start": _iso(start), "end": _iso(end), "limit": "10000", "sort": "asc"}
            if token:
                params["page_token"] = token
            request = Request(
                f"https://data.alpaca.markets/v1beta3/crypto/us/bars?{urlencode(params)}",
                headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret, "User-Agent": "trading-webhook/opportunity-lab-v1"},
            )
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
            for symbol, rows in dict(payload.get("bars") or {}).items():
                for row in rows or []:
                    output.setdefault(symbol.upper(), []).append({
                        "ts_utc": datetime.fromisoformat(str(row.get("t") or "").replace("Z", "+00:00")),
                        "open": float(row.get("o") or 0), "high": float(row.get("h") or 0),
                        "low": float(row.get("l") or 0), "close": float(row.get("c") or 0),
                        "volume": float(row.get("v") or 0), "vwap": float(row.get("vw") or 0),
                    })
            debug["pages"] += 1
            token = payload.get("next_page_token")
            if not token:
                break
        all_rows = [row for rows in output.values() for row in rows]
        debug.update({"count": len(all_rows), "truncated": bool(token)})
        if all_rows:
            earliest = min(row["ts_utc"] for row in all_rows)
            latest = max(row["ts_utc"] for row in all_rows)
            debug.update({"actual_start": _iso(earliest), "actual_end": _iso(latest), "coverage_days": round((latest - earliest).total_seconds() / 86400, 2)})
    except Exception as exc:
        debug["error"] = str(exc)[:300]
    return output, debug
