"""Strict Alpaca multi-leg order transport for the isolated intraday sleeve."""

from __future__ import annotations

import json
import hashlib
import re
from typing import Any
from urllib.request import Request, urlopen


def paper_client_order_id(signal_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "-", str(signal_id or "")).strip("-")[:20]
    digest = hashlib.sha256(str(signal_id or "").encode("utf-8")).hexdigest()[:16]
    return f"ri-{normalized or 'signal'}-{digest}"[:48]


def build_mleg_limit_order(plan: dict[str, Any], *, client_order_id: str | None = None) -> dict[str, Any]:
    if plan.get("status") != "selected" or plan.get("order_class") != "mleg":
        raise ValueError("eligible selected mleg plan required")
    if int(plan.get("quantity") or 0) != 1:
        raise ValueError("pilot quantity must equal one spread")
    debit = float(plan.get("limit_debit") or 0.0)
    max_loss = float(plan.get("max_loss_dollars") or 0.0)
    if debit <= 0 or max_loss <= 0 or max_loss > 100.0:
        raise ValueError("pilot max loss must be between $0 and $100")
    legs = list(plan.get("legs") or [])
    if len(legs) != 2 or [leg.get("side") for leg in legs] != ["buy", "sell"]:
        raise ValueError("one long and one short leg required")
    payload = {
        "order_class": "mleg",
        "qty": "1",
        "type": "limit",
        "time_in_force": "day",
        "limit_price": f"{debit:.2f}",
        "legs": [
            {
                "symbol": str(leg["symbol"]),
                "ratio_qty": "1",
                "side": str(leg["side"]),
                "position_intent": str(leg["position_intent"]),
            }
            for leg in legs
        ],
    }
    if client_order_id:
        payload["client_order_id"] = str(client_order_id)[:48]
    return payload


def build_mleg_close_order(plan: dict[str, Any], limit_credit: float, *, client_order_id: str | None = None) -> dict[str, Any]:
    legs = list(plan.get("legs") or [])
    if len(legs) != 2 or float(limit_credit or 0.0) <= 0:
        raise ValueError("valid two-leg plan and positive closing credit required")
    payload = {
        "order_class": "mleg",
        "qty": "1",
        "type": "limit",
        "time_in_force": "day",
        "limit_price": f"{-abs(float(limit_credit)):.2f}",
        "legs": [
            {"symbol": str(legs[0]["symbol"]), "ratio_qty": "1", "side": "sell", "position_intent": "sell_to_close"},
            {"symbol": str(legs[1]["symbol"]), "ratio_qty": "1", "side": "buy", "position_intent": "buy_to_close"},
        ],
    }
    if client_order_id:
        payload["client_order_id"] = str(client_order_id)[:48]
    return payload


def submit_mleg_limit_order(
    api_key: str,
    api_secret: str,
    plan: dict[str, Any],
    *,
    paper: bool,
    live_enabled: bool = False,
    timeout: int = 20,
    client_order_id: str | None = None,
) -> dict[str, Any]:
    if not api_key or not api_secret:
        raise ValueError("Alpaca credentials are required")
    if not paper and not live_enabled:
        raise PermissionError("live regime-intraday submission gate is closed")
    if not paper and not bool(plan.get("live_eligible")):
        raise PermissionError("live options require a verified Greeks-based contract selection")
    payload = build_mleg_limit_order(plan, client_order_id=client_order_id)
    base = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"
    request = Request(
        f"{base}/v2/orders",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json", "APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret},
    )
    with urlopen(request, timeout=timeout) as response:
        result = json.loads(response.read().decode("utf-8"))
    return {"submitted": True, "paper": bool(paper), "order_id": result.get("id"), "client_order_id": result.get("client_order_id") or client_order_id, "status": result.get("status"), "symbol": result.get("symbol"), "order_class": result.get("order_class")}


def submit_mleg_close_order(
    api_key: str,
    api_secret: str,
    plan: dict[str, Any],
    limit_credit: float,
    *,
    paper: bool,
    live_enabled: bool = False,
    timeout: int = 20,
    client_order_id: str | None = None,
) -> dict[str, Any]:
    if not api_key or not api_secret:
        raise ValueError("Alpaca credentials are required")
    if not paper and not live_enabled:
        raise PermissionError("live regime-intraday close gate is closed")
    payload = build_mleg_close_order(plan, limit_credit, client_order_id=client_order_id)
    base = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"
    request = Request(
        f"{base}/v2/orders",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json", "APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret},
    )
    with urlopen(request, timeout=timeout) as response:
        result = json.loads(response.read().decode("utf-8"))
    return {"submitted": True, "paper": bool(paper), "order_id": result.get("id"), "client_order_id": result.get("client_order_id") or client_order_id, "status": result.get("status"), "order_class": result.get("order_class"), "action": "close"}


def get_order(api_key: str, api_secret: str, order_id: str, *, paper: bool = True, timeout: int = 20) -> dict[str, Any]:
    base = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"
    request = Request(f"{base}/v2/orders/{order_id}?nested=true", headers={"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret})
    with urlopen(request, timeout=timeout) as response:
        row = json.loads(response.read().decode("utf-8"))
    return {key: row.get(key) for key in ("id", "status", "created_at", "submitted_at", "filled_at", "canceled_at", "expired_at", "filled_qty", "filled_avg_price", "limit_price", "order_class", "legs")}


def get_order_by_client_id(api_key: str, api_secret: str, client_order_id: str, *, paper: bool = True, timeout: int = 20) -> dict[str, Any]:
    base = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"
    request = Request(f"{base}/v2/orders:by_client_order_id?client_order_id={client_order_id}", headers={"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret})
    with urlopen(request, timeout=timeout) as response:
        row = json.loads(response.read().decode("utf-8"))
    return {key: row.get(key) for key in ("id", "client_order_id", "status", "created_at", "submitted_at", "filled_at", "filled_qty", "filled_avg_price", "limit_price", "order_class", "legs")}


def cancel_order(api_key: str, api_secret: str, order_id: str, *, paper: bool = True, timeout: int = 20) -> None:
    base = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"
    request = Request(f"{base}/v2/orders/{order_id}", method="DELETE", headers={"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret})
    with urlopen(request, timeout=timeout):
        return None
