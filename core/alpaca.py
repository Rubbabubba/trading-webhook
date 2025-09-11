import json
from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta, timezone

from .config import (SESSION, HEADERS, DATA_BASE, DATA_FEED, ALPACA_BASE,
                     MAX_OPEN_POSITIONS, MAX_POSITIONS_PER_SYMBOL, log)

def is_market_open_now() -> bool:
    try:
        r = SESSION.get(f"{ALPACA_BASE}/clock", headers=HEADERS, timeout=3)
        r.raise_for_status()
        return bool(r.json().get("is_open"))
    except Exception:
        return False

def bars(symbol: str, timeframe: str, limit: int = 200, start: Optional[str] = None, end: Optional[str] = None):
    params = {"timeframe": timeframe, "limit": limit, "feed": DATA_FEED}
    if start: params["start"] = start
    if end:   params["end"] = end
    r = SESSION.get(f"{DATA_BASE}/stocks/{symbol}/bars", headers=HEADERS, params=params, timeout=5)
    r.raise_for_status()
    return r.json().get("bars", [])

def last_close(symbol: str) -> Optional[float]:
    try:
        b = bars(symbol, "1Min", limit=1)
        return float(b[-1]["c"]) if b else None
    except Exception:
        return None

def get_position(symbol: str) -> Optional[dict]:
    try:
        r = SESSION.get(f"{ALPACA_BASE}/positions/{symbol}", headers=HEADERS, timeout=3)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def list_positions() -> List[dict]:
    try:
        r = SESSION.get(f"{ALPACA_BASE}/positions", headers=HEADERS, timeout=4)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []

def list_open_orders(symbol: Optional[str] = None) -> List[dict]:
    try:
        r = SESSION.get(f"{ALPACA_BASE}/orders?status=open&nested=false&limit=200", headers=HEADERS, timeout=4)
        r.raise_for_status()
        orders = r.json()
        if symbol:
            orders = [o for o in orders if o.get("symbol") == symbol]
        return orders
    except Exception:
        return []

def cancel_open_orders(symbol: Optional[str] = None):
    for o in list_open_orders(symbol):
        oid = o.get("id")
        if not oid: continue
        try:
            SESSION.delete(f"{ALPACA_BASE}/orders/{oid}", headers=HEADERS, timeout=3)
        except Exception:
            pass

def post_order(payload: Dict) -> Dict:
    r = SESSION.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(payload), timeout=6)
    if not r.ok:
        log.error(f"POST /orders {r.status_code} {r.text} payload={payload}")
    r.raise_for_status()
    return r.json()

def can_open_new_position(symbol: str) -> Tuple[bool, str]:
    positions = list_positions()
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False, f"MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS}"
    sym_count = sum(1 for p in positions if p.get("symbol") == symbol)
    if sym_count >= MAX_POSITIONS_PER_SYMBOL:
        return False, f"MAX_POSITIONS_PER_SYMBOL={MAX_POSITIONS_PER_SYMBOL}"
    return True, "ok"