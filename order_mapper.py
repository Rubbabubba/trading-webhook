# order_mapper.py
import os
from dataclasses import dataclass

# ---------- Config model ----------
@dataclass
class StrategyConfig:
    qty: int
    order_type: str   # "market" or "limit"
    tif: str          # "day" or "gtc"
    tp_pct: float     # take-profit percent, e.g., 0.012 = 1.2%
    sl_pct: float     # stop-loss percent, e.g., 0.006 = 0.6%

# ---------- Helpers ----------
def _get_pct(var: str, default: float) -> float:
    try:
        v = os.getenv(var, "")
        return float(v) if v != "" else default
    except Exception:
        return default

def _get_int(var: str, default: int) -> int:
    try:
        v = os.getenv(var, "")
        return int(v) if v != "" else default
    except Exception:
        return default

def _norm_order_type(val: str) -> str:
    v = (val or "").strip().lower()
    return "limit" if v == "limit" else "market"

def _norm_tif(val: str) -> str:
    v = (val or "").strip().lower()
    return "gtc" if v == "gtc" else "day"

# ---------- Strategy defaults ----------
def _defaults_for(system: str) -> StrategyConfig:
    sys = (system or "").upper()

    # Base knobs
    qty = _get_int(f"{sys}_QTY", _get_int("DEFAULT_QTY", 1))
    order_type = _norm_order_type(os.getenv(f"{sys}_ORDER_TYPE", os.getenv("DEFAULT_ORDER_TYPE", "market")))
    tif = _norm_tif(os.getenv(f"{sys}_TIF", os.getenv("DEFAULT_TIF", "day")))

    # Exit percents (global for the system)
    if sys == "SMA10D_MACD":
        tp = _get_pct("SMA10D_MACD_TP_PCT", 0.012)
        sl = _get_pct("SMA10D_MACD_SL_PCT", 0.006)
    elif sys == "SPY_VWAP_EMA20":
        tp = _get_pct("SPY_VWAP_EMA20_TP_PCT", 0.0025)
        sl = _get_pct("SPY_VWAP_EMA20_SL_PCT", 0.0020)
    else:
        tp = _get_pct(f"{sys}_TP_PCT", 0.01)
        sl = _get_pct(f"{sys}_SL_PCT", 0.005)

    return StrategyConfig(qty=qty, order_type=order_type, tif=tif, tp_pct=tp, sl_pct=sl)

def _symbol_overrides(system: str, symbol: str, base: StrategyConfig) -> StrategyConfig:
    """Apply env overrides like SMA10D_MACD_TP_PCT_TSLA / SMA10D_MACD_SL_PCT_TSLA if present."""
    sys = (system or "").upper()
    sym = (symbol or "").upper()

    tp = os.getenv(f"{sys}_TP_PCT_{sym}")
    sl = os.getenv(f"{sys}_SL_PCT_{sym}")

    tp_pct = base.tp_pct if not tp else float(tp)
    sl_pct = base.sl_pct if not sl else float(sl)

    # Optional per-symbol qty/type/tif if you ever want them:
    qty = _get_int(f"{sys}_QTY_{sym}", base.qty)
    otype = _norm_order_type(os.getenv(f"{sys}_ORDER_TYPE_{sym}", base.order_type))
    tif = _norm_tif(os.getenv(f"{sys}_TIF_{sym}", base.tif))

    return StrategyConfig(qty=qty, order_type=otype, tif=tif, tp_pct=tp_pct, sl_pct=sl_pct)

# ---------- Public API (imported by app.py) ----------
def load_strategy_config(system: str, symbol: str | None = None) -> StrategyConfig:
    """Load strategy config; applies per-symbol TP/SL if symbol is provided."""
    base = _defaults_for(system)
    return _symbol_overrides(system, symbol, base) if symbol else base

def build_entry_order_payload(symbol: str, side: str, price: float, system: str) -> dict:
    """
    Build a BRACKET entry (used when flat). TP/SL use per-symbol overrides if defined.
    """
    cfg = load_strategy_config(system, symbol)
    side_norm = (side or "").upper()
    if side_norm not in ("LONG", "SHORT"):
        raise ValueError(f"invalid side: {side}")

    qty = int(cfg.qty) if int(cfg.qty) > 0 else 1
    entry_type = cfg.order_type  # "market" | "limit"

    # Compute TP/SL around the provided price (app.py will retry with Alpaca base_price if needed)
    p = float(price)
    if side_norm == "LONG":
        tp = p * (1 + cfg.tp_pct)
        sl = p * (1 - cfg.sl_pct)
        entry_side = "buy"
    else:
        tp = p * (1 - cfg.tp_pct)
        sl = p * (1 + cfg.sl_pct)
        entry_side = "sell"

    payload = {
        "symbol": symbol.upper(),
        "qty": qty,
        "side": entry_side,
        "type": entry_type,
        "time_in_force": cfg.tif,
        "order_class": "bracket",
        "take_profit": {"limit_price": f"{tp:.2f}"},
        "stop_loss":  {"stop_price":  f"{sl:.2f}"},
        "client_order_id": f"{system}-{symbol}-bracket",
    }
    if entry_type == "limit":
        payload["limit_price"] = f"{p:.2f}"
    return payload

def under_position_caps(open_snapshot: dict, symbol: str, system: str) -> bool:
    """
    Enforce MAX_* caps. open_snapshot is produced by app.get_open_positions_snapshot().
    """
    total = int(open_snapshot.get("TOTAL", 0))
    by_symbol = open_snapshot.get("BY_SYMBOL", {}) or {}
    by_strategy = open_snapshot.get("BY_STRATEGY", {}) or {}

    max_total = _get_int("MAX_OPEN_POSITIONS", 5)
    max_per_symbol = _get_int("MAX_POSITIONS_PER_SYMBOL", 1)
    max_per_strategy = _get_int("MAX_POSITIONS_PER_STRATEGY", 3)

    if total >= max_total:
        return False
    if by_symbol.get(symbol.upper(), 0) >= max_per_symbol:
        return False
    if by_strategy.get(system, 0) >= max_per_strategy:
        return False
    return True
