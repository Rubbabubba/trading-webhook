# strategies/s4.py
# CHANGELOG:
# v1.0.0 (2025-09-11): Initial release: RSI pullback with trend filter, optional shorts, risk, TP/SL.

from dataclasses import dataclass
from typing import List
from .base import BaseStrategy, OrderPlan
import os

__version__ = "1.0.0"

@dataclass
class S4Config:
    length: int
    lower: float
    upper: float
    allow_shorts: bool
    max_trades: int
    tp_pct: float
    sl_pct: float
    risk_per_trade: float
    trend_filter: str  # e.g., "EMA20>EMA50"

def load_s4_config():
    return S4Config(
        length=int(os.getenv("RSI_PB_LENGTH", "14")),
        lower=float(os.getenv("RSI_PB_LOWER", "35")),
        upper=float(os.getenv("RSI_PB_UPPER", "65")),
        allow_shorts=os.getenv("RSI_PB_ALLOW_SHORTS", "false").lower() == "true",
        max_trades=int(os.getenv("RSI_PB_MAX_TRADES", "3")),
        tp_pct=float(os.getenv("S4_TP_PCT", "0.006")),
        sl_pct=float(os.getenv("S4_SL_PCT", "0.003")),
        risk_per_trade=float(os.getenv("S4_RISK_PER_TRADE", "0.003")),
        trend_filter=os.getenv("RSI_PB_TREND_FILTER", "EMA20>EMA50"),
    )

def rsi(closes, length):
    if len(closes) < length + 1:
        return None
    gains, losses = 0.0, 0.0
    for i in range(-length, 0):
        chg = closes[i] - closes[i - 1]
        (gains if chg > 0 else losses).__iadd__(abs(chg))
    avg_gain = gains / length
    avg_loss = losses / length
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

class S4RSIPullback(BaseStrategy):
    system = "RSI_PB"

    def __init__(self, market):
        self.cfg = load_s4_config()
        self.market = market

    def _trend_ok(self, series) -> bool:
        # Evaluate simple boolean like "EMA20>EMA50" using latest values
        expr = self.cfg.trend_filter.replace("EMA20", str(series.ema20)).replace("EMA50", str(series.ema50))
        try:
            return bool(eval(expr))
        except Exception:
            return True  # fail-open

    def scan(self, now, market, symbols: List[str]) -> List[OrderPlan]:
        plans: List[OrderPlan] = []
        for symbol in symbols:
            series = market.indicators(symbol)  # requires: closes (list[float]), ema20, ema50
            val = rsi(series.closes, self.cfg.length)
            if val is None:
                continue

            bp = market.buying_power()
            risk_dollars = bp * self.cfg.risk_per_trade

            def build(side: str, trigger: str):
                qty = market.position_sizer(symbol, risk_dollars, self.cfg.sl_pct)
                if qty > 0:
                    return OrderPlan(
                        symbol=symbol,
                        side=side,
                        qty=qty,
                        tp_pct=self.cfg.tp_pct,
                        sl_pct=self.cfg.sl_pct,
                        meta={"trigger": trigger, "rsi": round(val, 1), "ver": __version__, "system": self.system},
                    )

            # Long pullback
            if val <= self.cfg.lower and self._trend_ok(series):
                plan = build("buy", "RSI<=lower")
                if plan: plans.append(plan)

            # Optional short
            if self.cfg.allow_shorts and val >= self.cfg.upper and not self._trend_ok(series):
                plan = build("sell", "RSI>=upper")
                if plan: plans.append(plan)

        return plans[: self.cfg.max_trades]
