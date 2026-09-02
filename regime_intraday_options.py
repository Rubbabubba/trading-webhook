"""Alpaca option-chain reader and pure defined-risk spread selector.

This module has no order-submission function.  Live execution is intentionally
implemented behind a separate gate after paper evidence exists.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


def fetch_option_chain(api_key: str, api_secret: str, underlying: str, *, feed: str = "indicative", timeout: int = 20) -> dict:
    query = urlencode({"feed": feed, "limit": 1000})
    url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{underlying}?{query}"
    request = Request(url, headers={"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_occ(symbol: str) -> dict[str, Any] | None:
    match = OCC_RE.match(str(symbol or "").upper())
    if not match:
        return None
    root, expiry, cp, strike = match.groups()
    return {
        "symbol": symbol,
        "root": root,
        "expiration": datetime.strptime(expiry, "%y%m%d").date(),
        "option_type": "call" if cp == "C" else "put",
        "strike": int(strike) / 1000.0,
    }


def _quote(snapshot: dict) -> tuple[float, float]:
    quote = dict(snapshot.get("latestQuote") or snapshot.get("latest_quote") or {})
    return float(quote.get("bp") or quote.get("bid_price") or 0.0), float(quote.get("ap") or quote.get("ask_price") or 0.0)


def _delta(snapshot: dict) -> float:
    return float(dict(snapshot.get("greeks") or {}).get("delta") or 0.0)


def select_debit_spread(
    chain: dict,
    intent: dict,
    *,
    as_of: date | None = None,
    max_loss_dollars: float = 100.0,
    width: float = 1.0,
) -> dict[str, Any]:
    """Select one liquid long leg and a same-expiry farther-OTM short leg."""
    today = as_of or datetime.now(timezone.utc).date()
    snapshots = dict(chain.get("snapshots") or chain)
    option_type = str(intent.get("option_type") or "").lower()
    dte_low = int(intent.get("min_dte") or 7)
    dte_high = int(intent.get("max_dte") or 21)
    delta_low, delta_high = [float(v) for v in (intent.get("target_delta_range") or [0.55, 0.70])]
    max_spread = float(intent.get("max_bid_ask_spread_pct") or 0.08)
    candidates = []
    for symbol, snapshot in snapshots.items():
        parsed = parse_occ(symbol)
        if not parsed or parsed["option_type"] != option_type:
            continue
        dte = (parsed["expiration"] - today).days
        delta = abs(_delta(dict(snapshot or {})))
        bid, ask = _quote(dict(snapshot or {}))
        mid = (bid + ask) / 2.0
        spread_pct = (ask - bid) / mid if mid > 0 else 999.0
        if dte_low <= dte <= dte_high and delta_low <= delta <= delta_high and bid > 0 and ask > bid and spread_pct <= max_spread:
            candidates.append({**parsed, "dte": dte, "delta": delta, "bid": bid, "ask": ask, "spread_pct": spread_pct})
    candidates.sort(key=lambda row: (abs(row["delta"] - ((delta_low + delta_high) / 2.0)), row["spread_pct"], row["dte"]))
    for long_leg in candidates:
        target_strike = long_leg["strike"] + width if option_type == "call" else long_leg["strike"] - width
        shorts = []
        for symbol, snapshot in snapshots.items():
            parsed = parse_occ(symbol)
            if not parsed or parsed["option_type"] != option_type or parsed["expiration"] != long_leg["expiration"]:
                continue
            if abs(parsed["strike"] - target_strike) > 0.001:
                continue
            bid, ask = _quote(dict(snapshot or {}))
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid > 0 else 999.0
            if bid > 0 and ask >= bid and spread_pct <= max_spread:
                shorts.append({**parsed, "bid": bid, "ask": ask, "delta": abs(_delta(dict(snapshot or {}))), "spread_pct": spread_pct})
        if not shorts:
            continue
        short_leg = min(shorts, key=lambda row: row["spread_pct"])
        debit = round(long_leg["ask"] - short_leg["bid"], 2)
        if debit <= 0 or debit * 100 > max_loss_dollars or debit >= width:
            continue
        return {
            "status": "selected",
            "underlying": intent.get("underlying"),
            "option_type": option_type,
            "order_class": "mleg",
            "quantity": 1,
            "limit_debit": debit,
            "max_loss_dollars": round(debit * 100, 2),
            "max_profit_dollars": round((width - debit) * 100, 2),
            "expiration": long_leg["expiration"].isoformat(),
            "legs": [
                {"symbol": long_leg["symbol"], "side": "buy", "ratio_qty": 1, "position_intent": "buy_to_open"},
                {"symbol": short_leg["symbol"], "side": "sell", "ratio_qty": 1, "position_intent": "sell_to_open"},
            ],
            "quote_basis": {"long_ask": long_leg["ask"], "short_bid": short_leg["bid"], "long_delta": long_leg["delta"], "feed": "indicative_or_configured"},
            "live_submission": False,
        }
    return {"status": "no_eligible_defined_risk_spread", "live_submission": False, "candidate_count": len(candidates)}
