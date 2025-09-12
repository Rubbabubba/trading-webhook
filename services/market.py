import os
import math
import datetime as dt
from dataclasses import dataclass
from typing import List, Any, Dict
import requests

ALPACA_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET", "")
ALPACA_TRADING_BASE = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")
ALPACA_DATA_BASE = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
SESSION_TZ = os.getenv("SESSION_TZ", "America/New_York")  # Exchange time
TIMEFRAME = os.getenv("INTRADAY_TIMEFRAME", "1Min")       # Used for bars

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
}

@dataclass
class Bar:
    t: dt.datetime
    o: float
    h: float
    l: float
    c: float
    v: float

def _iso(ts: dt.datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")

def _ema(values: List[float], length: int) -> float:
    if not values or len(values) < length:
        return float(values[-1]) if values else 0.0
    k = 2.0 / (length + 1.0)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return float(ema)

class Market:
    """
    Minimal market abstraction used by S3/S4.
    Pulls latest trade, minute bars, and account buying power from Alpaca REST.
    """

    def now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)

    # US stocks open 9:30 ET; we keep everything in local naive time for range computation.
    def session_start(self, now: dt.datetime) -> dt.datetime:
        # default to 9:30 AM in the *same* date as `now` but timezone-naive
        local = now.astimezone(dt.timezone(dt.timedelta(hours=-4)))  # rough ET (handles DST roughly on server)
        start = local.replace(hour=9, minute=30, second=0, microsecond=0)
        # return naive (strategies only compare times)
        return start.replace(tzinfo=None)

    def _get(self, url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        r = requests.get(url, headers=HEADERS, params=params or {}, timeout=10)
        r.raise_for_status()
        return r.json()

    def buying_power(self) -> float:
        url = f"{ALPACA_TRADING_BASE}/v2/account"
        data = self._get(url)
        return float(data.get("buying_power", 0.0))

    def last_price(self, symbol: str) -> float:
        url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/trades/latest"
        data = self._get(url)
        return float(data["trade"]["p"])

    def get_intraday(self, symbol: str) -> List[Bar]:
        """Return minute bars for today’s session window → now."""
        now = self.now()
        session = self.session_start(now)
        # Start at session start in UTC
        start_utc = session.replace(tzinfo=dt.timezone.utc)
        url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars"
        params = {
            "timeframe": TIMEFRAME,             # "1Min"
            "start": _iso(start_utc),
            "end": _iso(now),
            "adjustment": "raw",
            "limit": 10000,
        }
        data = self._get(url, params)
        out: List[Bar] = []
        for b in data.get("bars", []):
            ts = dt.datetime.fromisoformat(b["t"].replace("Z", "+00:00")).replace(tzinfo=None)
            out.append(Bar(
                t=ts,
                o=float(b["o"]),
                h=float(b["h"]),
                l=float(b["l"]),
                c=float(b["c"]),
                v=float(b.get("v", 0)),
            ))
        return out

    def indicators(self, symbol: str):
        """Provide closes list + ema20/ema50 for RSI_PB strategy."""
        url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars"
        params = {"timeframe": TIMEFRAME, "limit": 200, "adjustment": "raw"}
        data = self._get(url, params)
        closes = [float(b["c"]) for b in data.get("bars", [])]
        ema20 = _ema(closes[-100:], 20) if closes else 0.0
        ema50 = _ema(closes[-100:], 50) if closes else 0.0
        return type("Series", (), {"closes": closes, "ema20": ema20, "ema50": ema50})

    def position_sizer(self, symbol: str, risk_dollars: float, sl_pct: float) -> int:
        price = self.last_price(symbol)
        denom = max(price * max(sl_pct, 1e-6), 1e-6)
        qty = math.floor(risk_dollars / denom)
        return max(qty, 0)
