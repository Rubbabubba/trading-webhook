# order_mapper.py
from dataclasses import dataclass
from typing import Optional, Dict, Any
import os, time, random

@dataclass
class StrategyConfig:
    name: str
    qty: int = 1
    tp_pct: float = 0.01     # 1% default as decimal (0.01 = 1%)
    sl_pct: float = 0.005    # 0.5% default (0.005 = 0.5%)
    order_type: str = "market"  # 'market' or 'limit'
    tif: str = "day"            # 'day' or 'gtc'

def _pct_from_env_decimal(key: str, default: float) -> float:
    """Interpret env as DECIMAL FRACTION (0.01 = 1%)."""
    try:
        return float(os.getenv(key, default))
    except Exception:
        return default

def load_strategy_config(strategy: str) -> StrategyConfig:
    upper = strategy.upper()
    return StrategyConfig(
        name=strategy,
        qty=int(os.getenv(f"{upper}_QTY", os.getenv("DEFAULT_QTY", 1))),
        tp_pct=_pct_from_env_decimal(f"{upper}_TP_PCT", 0.01),
        sl_pct=_pct_from_env_decimal(f"{upper}_SL_PCT", 0.005),
        order_type=os.getenv(f"{upper}_ORDER_TYPE", os.getenv("DEFAULT_ORDER_TYPE", "market")),
        tif=os.getenv(f"{upper}_TIF", os.getenv("DEFAULT_TIF", "day"))
    )

def compute_bracket_prices(side: str, entry_price: float, tp_pct: float, sl_pct: float) -> Dict[str, float]:
    side = side.upper()
    if side == "LONG":
        tp = entry_price * (1 + tp_pct)
        sl = entry_price * (1 - sl_pct)
    elif side == "SHORT":
        tp = entry_price * (1 - tp_pct)
        sl = entry_price * (1 + sl_pct)
    else:
        raise ValueError(f"Invalid side: {side}")
    return {"tp": float(f"{tp:.2f}"), "sl": float(f"{sl:.2f}")}

def build_client_order_id(system: str, symbol: str) -> str:
    return f"{system}-{symbol}-{int(time.time()*1000)}-{random.randint(100000,999999)}"

def build_entry_order_payload(
    symbol: str,
    side: str,
    price: float,
    strategy: str,
    qty: Optional[int] = None
) -> Dict[str, Any]:
    """
    Returns a valid Alpaca ENTRY bracket order.
    LONG -> 'buy' entry
    SHORT -> 'sell' entry
    """
    cfg = load_strategy_config(strategy)
    q = int(qty if qty is not None else cfg.qty)
    bracket = compute_bracket_prices(side, float(price), cfg.tp_pct, cfg.sl_pct)

    return {
        "symbol": symbol,
        "qty": q,
        "side": "buy" if side.upper() == "LONG" else "sell",
        "type": cfg.order_type,
        "time_in_force": cfg.tif,
        "order_class": "bracket",
        "take_profit": {"limit_price": f"{bracket['tp']:.2f}"},
        "stop_loss": {"stop_price": f"{bracket['sl']:.2f}"},
        "client_order_id": build_client_order_id(strategy, symbol),
    }

def under_position_caps(open_positions: Dict[str, int], symbol: str, strategy: str) -> bool:
    """
    open_positions shape example:
    {
      "TOTAL": 2,
      "BY_SYMBOL": {"SPY": 1, "COIN": 1},
      "BY_STRATEGY": {"SPY_VWAP_EMA20": 1, "SMA10D_MACD": 1}
    }
    """
    max_total = int(os.getenv("MAX_OPEN_POSITIONS", 5))
    max_per_symbol = int(os.getenv("MAX_POSITIONS_PER_SYMBOL", 1))
    max_per_strategy = int(os.getenv("MAX_POSITIONS_PER_STRATEGY", 3))

    total_ok = open_positions.get("TOTAL", 0) < max_total
    sym_ok = open_positions.get("BY_SYMBOL", {}).get(symbol, 0) < max_per_symbol
    strat_ok = open_positions.get("BY_STRATEGY", {}).get(strategy, 0) < max_per_strategy
    return total_ok and sym_ok and strat_ok
