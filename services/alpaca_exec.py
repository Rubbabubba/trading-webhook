import os
import uuid
import math
from typing import List, Dict, Any
import requests

ALPACA_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET", "")
ALPACA_TRADING_BASE = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")
TIF = os.getenv("ALPACA_TIME_IN_FORCE", "day")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
    "Content-Type": "application/json",
}

def _round2(x: float) -> float:
    return float(f"{x:.2f}")

def _latest(symbol: str) -> float:
    # lightweight latest trade via data API
    data_base = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
    url = f"{data_base}/v2/stocks/{symbol}/trades/latest"
    r = requests.get(url, headers={k: v for k, v in HEADERS.items() if k != "Content-Type"}, timeout=8)
    r.raise_for_status()
    return float(r.json()["trade"]["p"])

def _build_prices(side: str, price: float, tp_pct: float, sl_pct: float) -> Dict[str, float]:
    if side == "buy":
        tp = price * (1.0 + tp_pct)
        sl = price * (1.0 - sl_pct)
    else:  # sell / short
        tp = price * (1.0 - tp_pct)
        sl = price * (1.0 + sl_pct)
    return {"tp": _round2(tp), "sl": _round2(sl)}

def execute_orderplan_batch(plans: List[Any], system: str) -> List[Dict[str, Any]]:
    """
    Submit bracket market orders for a batch of OrderPlans.
    Each plan must have: symbol, side ("buy"|"sell"), qty (int/float), tp_pct, sl_pct.
    """
    results: List[Dict[str, Any]] = []
    orders_url = f"{ALPACA_TRADING_BASE}/v2/orders"

    for plan in plans:
        symbol = plan.symbol
        side = plan.side
        qty = int(math.floor(float(plan.qty)))
        if qty < 1:
            results.append({"symbol": symbol, "ok": False, "error": "qty<1", "skipped": True})
            continue

        try:
            px = _latest(symbol)
            prices = _build_prices(side, px, plan.tp_pct, plan.sl_pct)

            payload = {
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "type": "market",
                "time_in_force": TIF,
                "order_class": "bracket",
                "take_profit": {"limit_price": prices["tp"]},
                "stop_loss": {"stop_price": prices["sl"]},
                "client_order_id": f"{system}-{symbol}-{uuid.uuid4().hex[:8]}",
            }

            r = requests.post(orders_url, headers=HEADERS, json=payload, timeout=12)
            if r.status_code >= 400:
                results.append({
                    "symbol": symbol, "ok": False, "status": r.status_code,
                    "error": r.text[:300]
                })
                continue

            j = r.json()
            results.append({
                "symbol": symbol,
                "ok": True,
                "id": j.get("id"),
                "status": j.get("status"),
                "submitted_at": j.get("submitted_at"),
                "client_order_id": j.get("client_order_id"),
                "tp": prices["tp"],
                "sl": prices["sl"],
            })
        except Exception as e:
            results.append({"symbol": symbol, "ok": False, "error": str(e)})

    return results
