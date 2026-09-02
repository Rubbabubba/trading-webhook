"""Strict Alpaca multi-leg order transport for the isolated intraday sleeve."""

from __future__ import annotations

import json
from typing import Any
from urllib.request import Request, urlopen


def build_mleg_limit_order(plan: dict[str, Any]) -> dict[str, Any]:
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
    return {
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


def submit_mleg_limit_order(
    api_key: str,
    api_secret: str,
    plan: dict[str, Any],
    *,
    paper: bool,
    live_enabled: bool = False,
    timeout: int = 20,
) -> dict[str, Any]:
    if not api_key or not api_secret:
        raise ValueError("Alpaca credentials are required")
    if not paper and not live_enabled:
        raise PermissionError("live regime-intraday submission gate is closed")
    if not paper and not bool(plan.get("live_eligible")):
        raise PermissionError("live options require a verified Greeks-based contract selection")
    payload = build_mleg_limit_order(plan)
    base = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"
    request = Request(
        f"{base}/v2/orders",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json", "APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret},
    )
    with urlopen(request, timeout=timeout) as response:
        result = json.loads(response.read().decode("utf-8"))
    return {"submitted": True, "paper": bool(paper), "order_id": result.get("id"), "status": result.get("status"), "symbol": result.get("symbol"), "order_class": result.get("order_class")}
