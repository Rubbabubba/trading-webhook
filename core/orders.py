from typing import Optional, Dict
from .config import (ATTACH_OCO_ON_PLAIN, OCO_TIF, OCO_REQUIRE_POSITION, log)
from .alpaca import get_position, post_order, cancel_open_orders, can_open_new_position
from .utils import _cid

def attach_oco_exit_if_position(symbol: str, qty_hint: Optional[int],
                                take_profit: float, stop_loss: float,
                                tif: str, system: str) -> Optional[Dict]:
    pos = get_position(symbol)
    if not pos:
        log.info(f"OCO skip: no position for {symbol}")
        return None
    try:
        qty = qty_hint or int(float(pos.get("qty", "0")))
    except Exception:
        qty = qty_hint or 0
    if qty <= 0:
        log.info(f"OCO skip: qty<=0 for {symbol}")
        return None

    payload = {
        "symbol": symbol, "qty": str(qty), "side": "sell",
        "type": "limit", "time_in_force": tif if tif in ("day","gtc") else "gtc",
        "order_class": "oco",
        "take_profit": {"limit_price": f"{take_profit:.2f}"},
        "stop_loss": {"stop_price": f"{stop_loss:.2f}"},
        "client_order_id": _cid("OCO", system, symbol),
    }
    try:
        return post_order(payload)
    except Exception as e:
        log.error(f"OCO_POST_ERROR {e} sent={payload}")
        return None