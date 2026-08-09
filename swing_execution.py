"""Pure swing execution helper functions.

This module must stay broker-client-free and FastAPI-free. It can build order
payloads and calculate protective limit-entry previews, but app.py still owns
actual Alpaca submission.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SWING_EXECUTION_MODULE_VERSION = "patch-354-selected-candidate-submit-completion-marketable-protective-limit-pricing"


@dataclass(frozen=True)
class SwingLimitEntryConfig:
    enabled: bool
    max_spread_pct: float
    max_trade_mid_deviation_pct: float
    spread_fraction: float
    fractional_enabled: bool
    marketable_enabled: bool = True
    marketable_max_slippage_pct: float = 0.0035
    marketable_min_adv: float = 50000000.0

def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def format_order_qty(qty: float) -> str:
    q = float(qty)
    if q.is_integer():
        return str(int(q))
    return (f"{q:.6f}").rstrip("0").rstrip(".")


def build_market_order_payload(symbol: str, side: str, qty: float, client_order_id: str) -> dict:
    return {
        "symbol": str(symbol).upper(),
        "side": str(side).lower(),
        "type": "market",
        "time_in_force": "day",
        "qty": format_order_qty(qty),
        "client_order_id": str(client_order_id),
    }


def build_limit_order_payload(symbol: str, side: str, qty: float, limit_price: float, client_order_id: str) -> dict:
    return {
        "symbol": str(symbol).upper(),
        "side": str(side).lower(),
        "type": "limit",
        "time_in_force": "day",
        "qty": format_order_qty(qty),
        "limit_price": f"{float(limit_price):.2f}",
        "client_order_id": str(client_order_id),
    }


def limit_entry_preview(
    symbol: str,
    side: str,
    snapshot: dict | None,
    *,
    config: SwingLimitEntryConfig,
) -> dict:
    snap = dict(snapshot or {})
    if not bool(config.enabled):
        return {"allowed": False, "reason": "limit_entry_disabled"}

    if str(side or "").lower() != "buy":
        return {"allowed": False, "reason": "limit_entry_buy_only"}

    bid = safe_float(snap.get("bid"))
    ask = safe_float(snap.get("ask"))
    mid = safe_float(snap.get("mid") or snap.get("price"))

    if bid <= 0 or ask <= 0 or mid <= 0 or ask <= bid:
        return {
            "allowed": False,
            "reason": "quote_not_limitable",
            "bid": bid,
            "ask": ask,
            "mid": mid,
        }

    spread_pct = safe_float(snap.get("spread_pct"), (ask - bid) / mid)
    if spread_pct > float(config.max_spread_pct):
        return {
            "allowed": False,
            "reason": "spread_above_limit_entry_max",
            "spread_pct": spread_pct,
            "max_spread_pct": float(config.max_spread_pct),
        }

    deviation = snap.get("trade_mid_deviation_pct")
    if deviation is not None:
        try:
            if abs(float(deviation)) > float(config.max_trade_mid_deviation_pct):
                return {
                    "allowed": False,
                    "reason": "trade_mid_deviation_above_limit_entry_max",
                    "trade_mid_deviation_pct": float(deviation),
                    "max_trade_mid_deviation_pct": float(config.max_trade_mid_deviation_pct),
                }
        except Exception:
            pass

    avg_dollar_volume = safe_float(
        snap.get("avg_dollar_volume_20d")
        or snap.get("avg_dollar_volume")
        or (snap.get("quote_debug") or {}).get("avg_dollar_volume_20d")
    )
    price = safe_float(snap.get("price") or mid)
    marketable_allowed = (
        bool(getattr(config, "marketable_enabled", False))
        and avg_dollar_volume >= float(getattr(config, "marketable_min_adv", 0.0))
        and price > 0
        and ask > 0
    )
    if marketable_allowed:
        max_slippage_pct = max(0.0, float(getattr(config, "marketable_max_slippage_pct", 0.0)))
        max_price = round(price * (1.0 + max_slippage_pct), 2)
        marketable_limit = round(min(ask, max_price), 2)
        if marketable_limit >= ask or ask <= max_price:
            return {
                "allowed": True,
                "reason": "marketable_protective_limit_entry_allowed",
                "symbol": str(symbol or "").upper(),
                "side": "buy",
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "price": price,
                "spread_pct": spread_pct,
                "spread_fraction": 1.0,
                "limit_price": marketable_limit,
                "marketable": True,
                "marketable_max_price": max_price,
                "marketable_max_slippage_pct": max_slippage_pct,
                "avg_dollar_volume": avg_dollar_volume,
                "fractional_enabled": bool(config.fractional_enabled),
            }

    fraction = max(0.0, min(float(config.spread_fraction), 1.0))
    limit_price = round(bid + ((ask - bid) * fraction), 2)

    return {
        "allowed": True,
        "reason": "limit_entry_allowed",
        "symbol": str(symbol or "").upper(),
        "side": "buy",
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "price": price,
        "spread_pct": spread_pct,
        "spread_fraction": fraction,
        "limit_price": limit_price,
        "marketable": False,
        "avg_dollar_volume": avg_dollar_volume,
        "fractional_enabled": bool(config.fractional_enabled),
    }