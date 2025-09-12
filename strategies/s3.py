# strategies/s3.py
# CHANGELOG:
# v1.0.0 (2025-09-11): Initial release: Opening Range Breakout with window, buffer, dir, risk, TP/SL.

from dataclasses import dataclass
from typing import List
from .base import BaseStrategy, OrderPlan
import datetime as dt
import os

__version__ = "1.0.0"

@dataclass
class S3Config:
    minutes: int
    buffer: float
    max_trades: int
    dir: str  # "LONG"|"SHORT"|"BOTH"
    tp_pct: float
    sl_pct: float
    risk_per_trade: float
    window_start: str
    window_end: str

def load_s3_config():
    return S3Config(
        minutes=int(os.getenv("OPENING_RANGE_MINUTES", "5")),
        buffer=float(os.getenv("OPENING_RANGE_BUFFER", "0.0")),
        max_trades=int(os.getenv("OPENING_RANGE_MAX_TRADES", "3")),
        dir=os.getenv("OPENING_RANGE_DIR", "BOTH").upper(),
        tp_pct=float(os.getenv("S3_TP_PCT", "0.006")),
        sl_pct=float(os.getenv("S3_SL_PCT", "0.003")),
        risk_per_trade=float(os.getenv("S3_RISK_PER_TRADE", "0.003")),
        window_start=os.getenv("S3_TRADE_WINDOW_START", "09:00"),
        window_end=os.getenv("S3_TRADE_WINDOW_END", "11:00"),
    )

class S3OpeningRange(BaseStrategy):
    system = "OPENING_RANGE"

    def __init__(self, market):
        self.cfg = load_s3_config()
        self.market = market  # DI: OHLCV access, clock, buying_power, position_sizer

    def _in_window(self, now):
        s = dt.datetime.strptime(self.cfg.window_start, "%H:%M").time()
        e = dt.datetime.strptime(self.cfg.window_end, "%H:%M").time()
        return s <= now.time() <= e

    def scan(self, now, market, symbols: List[str]) -> List[OrderPlan]:
        if not self._in_window(now):
            return []
        plans: List[OrderPlan] = []

        for symbol in symbols:
            ohlcv = market.get_intraday(symbol)  # minute bars with fields t,h,l
            session = market.session_start(now)
            range_end = session + dt.timedelta(minutes=self.cfg.minutes)
            bars = [b for b in ohlcv if session <= b.t < range_end]
            if not bars:
                continue

            hi = max(b.h for b in bars)
            lo = min(b.l for b in bars)
            up_break = hi * (1 + self.cfg.buffer)
            dn_break = lo * (1 - self.cfg.buffer)
            last = market.last_price(symbol)
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
                        meta={"trigger": trigger, "hi": hi, "lo": lo, "ver": __version__, "system": self.system},
                    )

            if self.cfg.dir in ("LONG", "BOTH") and last > up_break:
                plan = build("buy", "ORUP")
                if plan: plans.append(plan)
            if self.cfg.dir in ("SHORT", "BOTH") and last < dn_break:
                plan = build("sell", "ORDN")
                if plan: plans.append(plan)

        return plans[: self.cfg.max_trades]
