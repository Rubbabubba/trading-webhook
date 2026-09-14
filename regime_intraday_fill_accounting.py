"""Verified multi-leg fill accounting; quotes are never treated as realized fills."""

from __future__ import annotations

from typing import Any


def _side(value: Any) -> str:
    text = str(value or "").lower()
    return "buy" if text.startswith("buy") else ("sell" if text.startswith("sell") else text)


def _aggregate(rows: list[dict[str, Any]], expected: dict[str, str]) -> dict[str, Any]:
    totals: dict[str, dict[str, float]] = {}
    problems = []
    for row in rows:
        symbol = str(row.get("symbol") or "")
        side = _side(row.get("side"))
        try:
            qty, price = float(row.get("qty") or row.get("filled_qty") or 0), abs(float(row.get("price") or row.get("filled_avg_price") or 0))
        except (TypeError, ValueError):
            qty = price = 0
        if not symbol or qty <= 0 or price <= 0:
            continue
        bucket = totals.setdefault(symbol, {"qty": 0.0, "notional": 0.0, "side": side})
        if bucket["side"] != side:
            problems.append(f"mixed_sides:{symbol}")
        bucket["qty"] += qty
        bucket["notional"] += qty * price
    legs = []
    for symbol, expected_side in expected.items():
        row = totals.get(symbol)
        if not row:
            problems.append(f"missing_fill:{symbol}")
            continue
        if row["side"] != expected_side:
            problems.append(f"unexpected_side:{symbol}:{row['side']}")
        legs.append({"symbol": symbol, "side": row["side"], "qty": row["qty"],
                     "average_price": round(row["notional"] / row["qty"], 6)})
    unexpected = sorted(set(totals) - set(expected))
    problems.extend(f"unexpected_symbol:{symbol}" for symbol in unexpected)
    quantities = [leg["qty"] for leg in legs]
    if len(quantities) == len(expected) and max(quantities) != min(quantities):
        problems.append("unequal_leg_quantities")
    cashflow = sum((1 if leg["side"] == "sell" else -1) * leg["average_price"] * leg["qty"] for leg in legs)
    spread_qty = min(quantities) if quantities else 0
    return {"complete": not problems and len(legs) == len(expected), "problems": problems, "legs": legs,
            "spread_quantity": spread_qty, "net_cashflow_per_spread": round(cashflow / spread_qty, 6) if spread_qty else None}


def verified_order_fill(order: dict[str, Any], plan: dict[str, Any], *, closing: bool) -> dict[str, Any]:
    plan_legs = list(plan.get("legs") or [])
    if len(plan_legs) != 2:
        return {"complete": False, "source": None, "problems": ["two_leg_plan_required"]}
    expected = {}
    for index, leg in enumerate(plan_legs):
        side = _side(leg.get("side"))
        if closing:
            side = "sell" if side == "buy" else "buy"
        expected[str(leg.get("symbol") or "")] = side
    activities = [dict(row) for row in list(order.get("fill_activities") or [])]
    nested = [dict(row) for row in list(order.get("legs") or [])]
    if activities:
        result = _aggregate(activities, expected)
        result["source"] = "account_fill_activities"
    elif nested:
        result = _aggregate(nested, expected)
        result["source"] = "nested_order_leg_fills"
    else:
        result = {"complete": False, "problems": ["verified_leg_fills_missing"], "legs": [],
                  "spread_quantity": None, "net_cashflow_per_spread": None, "source": None}
    net = result.get("net_cashflow_per_spread")
    if result.get("complete") and ((closing and float(net) <= 0) or (not closing and float(net) >= 0)):
        result["complete"] = False
        result.setdefault("problems", []).append("cashflow_direction_invalid")
    result["entry_debit"] = round(-float(net), 6) if result.get("complete") and not closing else None
    result["exit_credit"] = round(float(net), 6) if result.get("complete") and closing else None
    return result


def verified_roundtrip(record: dict[str, Any]) -> dict[str, Any]:
    plan = dict(record.get("plan") or {})
    entry = verified_order_fill(dict(record.get("broker") or {}), plan, closing=False)
    close = verified_order_fill(dict(dict(record.get("close_order") or {}).get("broker") or {}), plan, closing=True)
    complete = bool(entry.get("complete") and close.get("complete"))
    realized = round((float(close["exit_credit"]) - float(entry["entry_debit"])) * 100, 2) if complete else None
    return {"complete": complete, "entry": entry, "close": close, "entry_debit": entry.get("entry_debit"),
            "exit_credit": close.get("exit_credit"), "realized_dollars": realized,
            "problems": [*list(entry.get("problems") or []), *list(close.get("problems") or [])],
            "rule": "Verified leg fills only; parent prices and quotes are not actual P/L."}
