# strategies/s4.py
# RSI Pullback (S4)
# Version: 1.0.1

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import time
from typing import List

from .base import BaseStrategy, OrderPlan


__version__ = "1.0.1"


def _to_bool(val, default=False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def _parse_hhmm(s: str) -> time:
    s = (s or "").strip()
    if not s:
        return time(0, 0)
    hh, mm = s.split(":")[:2]
    return time(int(hh), int(mm))


def _rsi(closes: List[float], length: int) -> float | None:
    """
    Simple RSI (average gains/losses over the last `length` deltas).
    Robust to short input; no in-place magic — just straight math.
    """
    if not closes or len(closes) < length + 1:
        return None
    window = closes[-(length + 1) :]  # length+1 prices → length deltas
    gain_sum = 0.0
    loss_sum = 0.0
    for i in range(1, len(window)):
        d = float(window[i]) - float(window[i - 1])
        if d >= 0:
            gain_sum += d
        else:
            loss_sum += -d
    avg_gain = gain_sum / length
    avg_loss = loss_sum / length
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


@dataclass
class _Cfg:
    # Ops / window (America/Chicago; Market.now() should already be CT)
    start: time
    end: time
    allow_shorts: bool
    max_trades: int

    # RSI / trend
    length: int
    lower: float
    upper: float
    trend_filter: str  # "EMA20>EMA50", "EMA20<EMA50", or "NONE"

    # Risk / brackets
    risk_per_trade: float  # fraction of buying power
    tp_pct: float
    sl_pct: float


def _load_cfg() -> _Cfg:
    return _Cfg(
        start=_parse_hhmm(os.getenv("S4_TRADE_WINDOW_START", "09:00")),
        end=_parse_hhmm(os.getenv("S4_TRADE_WINDOW_END", "14:00")),
        allow_shorts=_to_bool(os.getenv("RSI_PB_ALLOW_SHORTS", "false")),
        max_trades=int(os.getenv("RSI_PB_MAX_TRADES", "3")),
        length=int(os.getenv("RSI_PB_LENGTH", "14")),
        lower=float(os.getenv("RSI_PB_LOWER", "35")),
        upper=float(os.getenv("RSI_PB_UPPER", "65")),
        trend_filter=(os.getenv("RSI_PB_TREND_FILTER", "EMA20>EMA50") or "NONE").strip().upper(),
        risk_per_trade=float(os.getenv("S4_RISK_PER_TRADE", "0.003")),
        tp_pct=float(os.getenv("S4_TP_PCT", "0.006")),
        sl_pct=float(os.getenv("S4_SL_PCT", "0.003")),
    )


class S4RSIPullback(BaseStrategy):
    """
    Longs in uptrend on RSI pullback (RSI <= lower). Optional shorts in downtrend (RSI >= upper).
    Trend filter via EMA20 vs EMA50. Position sizing via risk dollars and SL%.
    """

    system = "RSI_PB"

    def __init__(self, market):
        self.mkt = market
        self.cfg = _load_cfg()

    def _in_window(self, now) -> bool:
        st, en = self.cfg.start, self.cfg.end
        # `now` is timezone-aware (CT). Compare times only.
        nt = now.timetz().replace(tzinfo=None)
        return (st <= nt <= en) if st <= en else (nt >= st or nt <= en)

    def _trend_allows_long(self, ema20: float, ema50: float) -> bool:
        tf = self.cfg.trend_filter
        if tf == "NONE":
            return True
        if tf in ("EMA20>EMA50", "UP"):
            return ema20 > ema50
        if tf in ("EMA20<EMA50", "DOWN"):
            return ema20 < ema50
        # Unknown filter → be conservative
        return False

    def _trend_allows_short(self, ema20: float, ema50: float) -> bool:
        tf = self.cfg.trend_filter
        if tf == "NONE":
            return True
        if tf in ("EMA20<EMA50", "DOWN"):
            return ema20 < ema50
        if tf in ("EMA20>EMA50", "UP"):
            return ema20 > ema50
        return False

    def scan(self, now, market, symbols: List[str]) -> List[OrderPlan]:
        if not self._in_window(now):
            return []

        cfg = self.cfg
        bp = max(float(market.buying_power()), 0.0)
        risk_dollars = bp * cfg.risk_per_trade

        plans: List[OrderPlan] = []

        for symbol in symbols:
            series = market.indicators(symbol)
            closes = list(series.closes or [])
            if len(closes) < cfg.length + 1:
                continue

            ema20 = float(series.ema20)
            ema50 = float(series.ema50)
            rsi = _rsi(closes, cfg.length)
            if rsi is None:
                continue

            # Long setup: uptrend + RSI <= lower
            if self._trend_allows_long(ema20, ema50) and rsi <= cfg.lower:
                qty = market.position_sizer(symbol, risk_dollars, cfg.sl_pct)
                if qty >= 1:
                    plans.append(
                        OrderPlan(
                            symbol=symbol,
                            side="buy",
                            qty=qty,
                            tp_pct=cfg.tp_pct,
                            sl_pct=cfg.sl_pct,
                            meta={
                                "rsi": round(rsi, 2),
                                "ema20": round(ema20, 4),
                                "ema50": round(ema50, 4),
                                "reason": "rsi_pullback_long",
                            },
                        )
                    )

            # Short setup (optional): downtrend + RSI >= upper
            if cfg.allow_shorts and self._trend_allows_short(ema20, ema50) and rsi >= cfg.upper:
                qty = market.position_sizer(symbol, risk_dollars, cfg.sl_pct)
                if qty >= 1:
                    plans.append(
                        OrderPlan(
                            symbol=symbol,
                            side="sell",
                            qty=qty,
                            tp_pct=cfg.tp_pct,
                            sl_pct=cfg.sl_pct,
                            meta={
                                "rsi": round(rsi, 2),
                                "ema20": round(ema20, 4),
                                "ema50": round(ema50, 4),
                                "reason": "rsi_pullback_short",
                            },
                        )
                    )

            if len(plans) >= cfg.max_trades:
                break

        return plans
