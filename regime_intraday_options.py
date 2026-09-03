"""Alpaca option-chain reader and pure defined-risk spread selector.

This module has no order-submission function.  Live execution is intentionally
implemented behind a separate gate after paper evidence exists.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


def fetch_option_chain(api_key: str, api_secret: str, underlying: str, *, feed: str = "indicative", timeout: int = 20,
                       intent: dict | None = None, as_of: date | None = None, max_pages: int = 10,
                       expiration: str | None = None) -> dict:
    params = {"feed": feed, "limit": 1000}
    if intent:
        today = as_of or datetime.now(timezone.utc).date()
        params.update({"type": intent["option_type"],
                       "expiration_date_gte": (today + timedelta(days=int(intent.get("min_dte") or 7))).isoformat(),
                       "expiration_date_lte": (today + timedelta(days=int(intent.get("max_dte") or 21))).isoformat()})
        # Do not filter strikes: delta-eligible legs and their partners must remain available.
    if expiration:
        params["expiration_date"] = expiration
    snapshots, seen = {}, set()
    deadline = time.monotonic() + timeout
    pages, reason = 0, "page_limit"
    for _ in range(max_pages):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            reason = "time_budget"
            break
        url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{underlying}?{urlencode(params)}"
        request = Request(url, headers={"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret})
        try:
            with urlopen(request, timeout=remaining) as response:
                page = json.loads(response.read().decode("utf-8"))
            if not isinstance(page.get("snapshots"), dict):
                reason = "invalid_response"
                break
        except Exception:
            reason = "request_failed"
            break
        pages += 1
        snapshots.update(page["snapshots"])
        token = page.get("next_page_token")
        if not token:
            reason = "complete"
            break
        if token in seen:
            reason = "repeated_page_token"
            break
        seen.add(token)
        params["page_token"] = token
    return {"snapshots": snapshots, "chain_diagnostics": {"complete": reason == "complete", "reason": reason,
            "pages": pages, "snapshot_count": len(snapshots), "feed": feed}}


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
    diagnostics = {"chain": dict(chain.get("chain_diagnostics") or {}), "rejections": {}}
    if diagnostics["chain"].get("complete") is False or chain.get("next_page_token"):
        return {"status": "incomplete_option_chain", "live_submission": False, "diagnostics": diagnostics}
    snapshots = dict(chain.get("snapshots", chain))
    diagnostics["snapshot_count"] = len(snapshots)
    def reject(reason: str) -> None:
        counts = diagnostics["rejections"]
        counts[reason] = counts.get(reason, 0) + 1
    option_type = str(intent.get("option_type") or "").lower()
    dte_low = int(intent.get("min_dte") or 7)
    dte_high = int(intent.get("max_dte") or 21)
    delta_low, delta_high = [float(v) for v in (intent.get("target_delta_range") or [0.55, 0.70])]
    max_spread = float(intent.get("max_bid_ask_spread_pct") or 0.08)
    underlying_price = float(intent.get("underlying_price") or 0.0)
    candidates = []
    for symbol, snapshot in snapshots.items():
        parsed = parse_occ(symbol)
        if not parsed or parsed["option_type"] != option_type:
            reject("invalid_symbol_or_wrong_type")
            continue
        dte = (parsed["expiration"] - today).days
        delta = abs(_delta(dict(snapshot or {})))
        bid, ask = _quote(dict(snapshot or {}))
        mid = (bid + ask) / 2.0
        spread_pct = (ask - bid) / mid if mid > 0 else 999.0
        delta_eligible = delta_low <= delta <= delta_high
        moneyness_eligible = (
            delta == 0
            and underlying_price > 0
            and (
                (option_type == "call" and underlying_price * 0.99 <= parsed["strike"] <= underlying_price)
                or (option_type == "put" and underlying_price <= parsed["strike"] <= underlying_price * 1.01)
            )
        )
        if not dte_low <= dte <= dte_high:
            reject("expiration_out_of_range")
        elif not (delta_eligible or moneyness_eligible):
            reject("delta_or_moneyness")
        elif not (bid > 0 and ask > bid):
            reject("invalid_long_quote")
        elif spread_pct > max_spread:
            reject("long_bid_ask_spread")
        else:
            candidates.append({**parsed, "dte": dte, "delta": delta, "delta_source": "greeks" if delta else "near_money_fallback", "bid": bid, "ask": ask, "spread_pct": spread_pct})
    diagnostics["eligible_long_legs"] = len(candidates)
    candidates.sort(key=lambda row: (0 if row["delta_source"] == "greeks" else 1, abs(row["delta"] - ((delta_low + delta_high) / 2.0)) if row["delta"] else abs(row["strike"] - underlying_price), row["spread_pct"], row["dte"]))
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
            reject("missing_or_ineligible_short_leg")
            continue
        short_leg = min(shorts, key=lambda row: row["spread_pct"])
        debit = round(long_leg["ask"] - short_leg["bid"], 2)
        if debit <= 0 or debit * 100 > max_loss_dollars or debit >= width:
            reject("debit_or_risk_limit")
            continue
        return {
            "status": "selected",
            "diagnostics": diagnostics,
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
            "quote_basis": {"long_ask": long_leg["ask"], "short_bid": short_leg["bid"], "long_delta": long_leg["delta"] or None, "selection_source": long_leg["delta_source"], "feed": "indicative_or_configured"},
            "live_eligible": long_leg["delta_source"] == "greeks",
            "live_submission": False,
        }
    return {"status": "no_eligible_defined_risk_spread", "live_submission": False, "candidate_count": len(candidates), "diagnostics": diagnostics}


def value_debit_spread(chain: dict, plan: dict) -> dict[str, Any]:
    """Conservative liquidation value: sell long at bid, buy short at ask."""
    snapshots = dict(chain.get("snapshots") or chain)
    legs = list(plan.get("legs") or [])
    if len(legs) != 2:
        return {"status": "invalid_plan"}
    long_snapshot = dict(snapshots.get(str(legs[0].get("symbol") or "")) or {})
    short_snapshot = dict(snapshots.get(str(legs[1].get("symbol") or "")) or {})
    long_bid, long_ask = _quote(long_snapshot)
    short_bid, short_ask = _quote(short_snapshot)
    if min(long_bid, long_ask, short_bid, short_ask) <= 0:
        return {"status": "missing_leg_quote"}
    credit = round(max(0.01, long_bid - short_ask), 2)
    debit = float(plan.get("limit_debit") or 0.0)
    return {
        "status": "valued",
        "liquidation_credit": credit,
        "entry_debit": debit,
        "unrealized_dollars": round((credit - debit) * 100, 2),
        "unrealized_return_pct": round((credit / debit) - 1.0, 4) if debit > 0 else None,
        "quote_basis": {"long_bid": long_bid, "short_ask": short_ask},
    }


def spread_exit_decision(plan: dict, valuation: dict, *, minutes_to_close: int, take_profit_fraction: float = 0.50, stop_loss_fraction: float = 0.50) -> dict[str, Any]:
    if valuation.get("status") != "valued":
        return {"exit": False, "reason": "spread_not_valued"}
    debit = float(plan.get("limit_debit") or 0.0)
    credit = float(valuation.get("liquidation_credit") or 0.0)
    max_profit = float(plan.get("max_profit_dollars") or 0.0)
    pnl = float(valuation.get("unrealized_dollars") or 0.0)
    reason = None
    if minutes_to_close <= 15:
        reason = "end_of_day"
    elif pnl >= max_profit * take_profit_fraction:
        reason = "profit_target"
    elif credit <= debit * (1.0 - stop_loss_fraction):
        reason = "option_stop"
    return {"exit": bool(reason), "reason": reason or "hold", "limit_credit": credit, "unrealized_dollars": pnl}
