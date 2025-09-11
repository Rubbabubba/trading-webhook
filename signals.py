from typing import Dict, Tuple, Optional
from dataclasses import dataclass

from .alpaca import last_close, post_order, get_position, cancel_open_orders, can_open_new_position
from .orders import attach_oco_exit_if_position
from .config import CANCEL_OPEN_ORDERS_BEFORE_PLAIN, ATTACH_OCO_ON_PLAIN, OCO_TIF
from .utils import _cid

@dataclass
class StrategyConfig:
    qty: int = 1
    order_type: str = "bracket"  # 'bracket' for first entries, 'plain' for adds
    tif: str = "day"
    tp_pct: float = 0.010
    sl_pct: float = 0.007

def _envf(system: str, key: str, default: Optional[str]) -> str:
    import os
    return os.getenv(f"{system}_{key}", default if default is not None else "")

def load_strategy_config(system: str) -> StrategyConfig:
    return StrategyConfig(
        qty=int(_envf(system, "QTY", "1") or "1"),
        order_type=_envf(system, "ORDER_TYPE", "bracket") or "bracket",
        tif=_envf(system, "TIF", "day") or "day",
        tp_pct=float(_envf(system, "TP_PCT", "0.010") or "0.010"),
        sl_pct=float(_envf(system, "SL_PCT", "0.007") or "0.007"),
    )

def _ref_price(symbol: str, given: Optional[float]) -> float:
    if given and float(given) > 0:
        return float(given)
    lc = last_close(symbol)
    return float(lc) if lc else 0.0

def _first_entry_should_be_bracket(has_pos: bool, order_type: str) -> bool:
    return (not has_pos) and (order_type.lower() == "bracket")

def _compute_exits_from_price(entry_px: float, side: str, tp_pct: float, sl_pct: float):
    if side.lower() == "buy":
        tp = round(entry_px * (1 + tp_pct), 2)
        sl = round(entry_px * (1 - sl_pct), 2)
    else:
        tp = round(entry_px * (1 - tp_pct), 2)
        sl = round(entry_px * (1 + sl_pct), 2)
    return tp, sl

def process_signal(data: Dict) -> Tuple[int, Dict]:
    system = (data.get("system") or "UNKNOWN").strip()
    symbol = (data.get("ticker") or "").upper().strip()
    side = (data.get("side") or "buy").lower().strip()
    price = _ref_price(symbol, data.get("price"))

    if not symbol or side not in ("buy", "sell"):
        return 400, {"ok": False, "error": "invalid payload"}

    ok, reason = can_open_new_position(symbol)
    if not ok:
        return 200, {"ok": False, "blocked": reason}

    cfg = load_strategy_config(system)
    has_pos = get_position(symbol) is not None
    tif = cfg.tif

    if (cfg.order_type.lower() == "plain") and CANCEL_OPEN_ORDERS_BEFORE_PLAIN:
      cancel_open_orders(symbol)

    tp, sl = _compute_exits_from_price(price, side, cfg.tp_pct, cfg.sl_pct)

    try:
        if _first_entry_should_be_bracket(has_pos, cfg.order_type):
            payload = {
                "symbol": symbol, "qty": str(cfg.qty), "side": side,
                "type": "market", "time_in_force": tif,
                "order_class": "bracket",
                "take_profit": {"limit_price": f"{tp:.2f}"},
                "stop_loss": {"stop_price": f"{sl:.2f}"},
                "client_order_id": _cid("BRK", system, symbol),
            }
            order = post_order(payload)
            out = {"ok": True, "placed": "bracket", "order": order}
        else:
            payload = {
                "symbol": symbol, "qty": str(cfg.qty), "side": side,
                "type": "market", "time_in_force": tif,
                "client_order_id": _cid("PLN", system, symbol),
            }
            order = post_order(payload)
            out = {"ok": True, "placed": "plain", "order": order}

            if ATTACH_OCO_ON_PLAIN:
                attach_oco_exit_if_position(symbol, cfg.qty, tp, sl, OCO_TIF, system)
        return 200, out
    except Exception as e:
        return 200, {"ok": False, "error": str(e)}