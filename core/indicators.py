from typing import List, Optional

def sma(values: List[float], length: int) -> List[Optional[float]]:
    out = []
    q: List[float] = []
    s = 0.0
    for v in values:
        q.append(v); s += v
        if len(q) > length: s -= q.pop(0)
        out.append(s / length if len(q) == length else None)
    return out

def ema(values: List[float], length: int) -> List[float]:
    k = 2 / (length + 1.0)
    out = []
    e = None
    for v in values:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out

def macd(values: List[float], fast=12, slow=26, signal=9):
    ef = ema(values, fast)
    es = ema(values, slow)
    line = [f - s for f, s in zip(ef, es)]
    sig = ema(line, signal)
    hist = [m - s for m, s in zip(line, sig)]
    return line, sig, hist