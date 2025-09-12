# tests/test_s3_s4.py
import datetime as dt
from types import SimpleNamespace
from collections import namedtuple

from strategies.s3 import S3OpeningRange
from strategies.s4 import S4RSIPullback

Bar = namedtuple("Bar", "t o h l c v")

class FakeMarket:
    def __init__(self, now=None, last=500.0, ema20=510.0, ema50=500.0, closes=None):
        self._now = now or dt.datetime(2025, 9, 11, 9, 30)
        self._last = last
        self._ema20 = ema20
        self._ema50 = ema50
        self._closes = closes or [100 + i for i in range(30)]

    def now(self): return self._now
    def session_start(self, now): return now.replace(hour=9, minute=30, second=0, microsecond=0)
    def last_price(self, symbol): return self._last
    def buying_power(self): return 100000.0
    def position_sizer(self, symbol, risk_dollars, sl_pct):
        # naive: risk_dollars / (last * sl_pct)
        denom = max(self._last * max(sl_pct, 1e-6), 1e-6)
        return round(risk_dollars / denom, 2)

    def get_intraday(self, symbol):
        ss = self.session_start(self._now)
        # 5-minute range bars: hi=500, lo=495
        return [
            Bar(t=ss + dt.timedelta(minutes=m), o=498, h=500, l=495, c=499, v=1000)
            for m in range(0, 6)
        ]

    def indicators(self, symbol):
        return SimpleNamespace(closes=self._closes, ema20=self._ema20, ema50=self._ema50)

def test_s3_breaks_up_range():
    fm = FakeMarket(last=501.0)  # > hi (500) → breakout long
    plans = S3OpeningRange(fm).scan(fm.now(), fm, ["SPY"])
    assert len(plans) == 1
    p = plans[0]
    assert p.side == "buy"
    assert p.meta["trigger"] == "ORUP"
    assert "ver" in p.meta

def test_s4_rsi_pullback_uptrend_long():
    closes = [100]*15 + [99,98,98,97,97,97,97,97,97,97,97,97,97,97]  # depressed RSI
    fm = FakeMarket(closes=closes, ema20=510, ema50=500)
    plans = S4RSIPullback(fm).scan(fm.now(), fm, ["SPY"])
    assert len(plans) == 1
    assert plans[0].side == "buy"
    assert plans[0].meta["trigger"].startswith("RSI")
