# services/alpaca_exec.py
# Version: 2025-09-12-EXEC
# Places MARKET bracket orders on Alpaca using OrderPlan inputs.

from __future__ import annotations
import time
from typing import List, Dict, Any

from services.market import (
    Market, ALPACA_TRADING_BASE, HEADERS, SESSION
)
from strategies.base import OrderPlan


def _tp_sl_prices(side: str, last: float, tp_pct: float, sl_pct: float):
    # Round to 2 decimals for stocks
    if side == "buy":
        tp = round(last * (1.0 + float(tp_pct)), 2)
        sl = round(last * (1.0 - float(sl_pct)), 2)
    else:  # sell/short
        tp = round(last * (1.0 - float(tp_pct)), 2)
        sl = round(last * (1.0 + float(sl_pct)), 2)
    return tp, sl


def _client_order_id(system: str, symbol: str) -> str:
    return f"{system}:{symbol}:{int(time.time())}"


def execute_orderplan_batch(plans: List[OrderPlan], system: str) -> List[Dict[str, Any]]:
    """
    Submit MARKET bracket orders for each plan.
    Returns list of {symbol, qty, side, status, id|error}.
    """
    out: List[Dict[str, Any]] = []
    if not plans:
        return out

    mkt = Market()
    for p in plans:
        try:
            sym = p.symbol.upper()
            side = p.side.lower()
            qty  = int(p.qty)
            if qty <= 0:
                out.append({"symbol": sym, "side": side, "qty": qty, "status": "skipped", "reason": "qty<=0"})
                continue

            last = mkt.last_price(sym)
            tp_price, sl_price = _tp_sl_prices(side, last, p.tp_pct, p.sl_pct)

            payload = {
                "symbol": sym,
                "qty": qty,
                "side": side,                 # "buy" or "sell"
                "type": "market",
                "time_in_force": "day",
                "order_class": "bracket",
                "take_profit": {"limit_price": tp_price},
                "stop_loss": {"stop_price": sl_price},
                "client_order_id": _client_order_id(system, sym),
                "extended_hours": False,
            }

            r = SESSION.post(f"{ALPACA_TRADING_BASE}/v2/orders", headers=HEADERS, json=payload, timeout=10)
            if r.status_code >= 300:
                out.append({
                    "symbol": sym, "side": side, "qty": qty,
                    "status": "error", "error": f"{r.status_code} {r.text[:300]}"
                })
                continue

            data = r.json()
            out.append({
                "symbol": sym, "side": side, "qty": qty,
                "status": "submitted", "id": data.get("id"), "tp": tp_price, "sl": sl_price
            })
        except Exception as e:
            out.append({"symbol": getattr(p, 'symbol', '?'), "status": "error", "error": str(e)})
    return out
