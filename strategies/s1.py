# strategies/s1.py
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone, time as dtime
import math

from core.indicators import ema, sma
from core.alpaca import bars
from core.utils import _iso
from core.signals import process_signal

def _parse_iso(ts: str) -> datetime:
    # Alpaca data returns ISO (Z). Be forgiving.
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)

def _is_rth_utc(ts: datetime) -> bool:
    # RTH 09:30–16:00 ET = 13:30–20:00 UTC (no DST math here since ET->UTC offset is handled by Alpaca timestamps)
    t = ts.time()
    return (dtime(13, 30) <= t <= dtime(20, 0))

def _vwap_series(h: List[float], l: List[float], c: List[float], v: List[float]) -> List[float]:
    vwap = []
    cum_vp = 0.0
    cum_v = 0.0
    for hi, lo, cl, vol in zip(h, l, c, v):
        tp = (hi + lo + cl) / 3.0
        cum_vp += tp * vol
        cum_v  += vol
        vwap.append(cum_vp / max(cum_v, 1e-9))
    return vwap

def _rolling_std(vals: List[float], length: int) -> List[float]:
    out = []
    q: List[float] = []
    s = 0.0
    s2 = 0.0
    for x in vals:
        q.append(x); s += x; s2 += x * x
        if len(q) > length:
            old = q.pop(0); s -= old; s2 -= old * old
        if len(q) < max(2, length//2):
            out.append(float("nan"))
        else:
            n = len(q)
            mu = s / n
            var = max(0.0, (s2 / n) - mu * mu)
            out.append(math.sqrt(var))
    return out

def _macd_up_dummy(closes: List[float]) -> bool:
    # Not strictly needed, but sometimes helpful as a micro-filter. Keep off by default.
    return True

def _slope(vals: List[float], lookback: int = 5) -> float:
    if len(vals) <= lookback: return 0.0
    return vals[-1] - vals[-1 - lookback]

def scan_s1_symbols(
    symbols: List[str],
    tf: str = "1",
    mode: str = "either",           # reversion|trend|either
    band: float = 0.6,              # VWAP sigma multiplier
    slope_min: float = 0.0,         # EMA20 slope minimum (price units over lookback)
    use_rth: bool = True,
    use_vol: bool = False,
    vol_mult: float = 1.0,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Returns {checked: [...], triggers: [...], timeframe: "1Min"|"5Min"}
    """
    tf_map = {"1": "1Min", "5": "5Min"}
    tf_s = tf_map.get(str(tf), "1Min")

    checked: List[Dict[str, Any]] = []
    triggers: List[Dict[str, Any]] = []

    for sym in symbols:
        s = sym.strip().upper()
        try:
            # Fetch intraday bars
            lim = 300 if tf_s == "1Min" else 400  # generous history
            raw = bars(s, tf_s, limit=lim)
            if len(raw) < 60:
                checked.append({"symbol": s, "skip": "not_enough_bars"}); continue

            # Extract arrays; filter RTH if requested
            H, L, C, V, T = [], [], [], [], []
            for b in raw:
                cl = float(b["c"]); hi = float(b["h"]); lo = float(b["l"]); vol = float(b["v"])
                ts = _parse_iso(b.get("t") or b.get("time") or b.get("T") or "")
                if use_rth and not _is_rth_utc(ts): 
                    continue
                H.append(hi); L.append(lo); C.append(cl); V.append(vol); T.append(ts)

            if len(C) < 40:
                checked.append({"symbol": s, "skip": "not_enough_rth_bars"}); continue

            vwap = _vwap_series(H, L, C, V)
            ema20 = ema(C, 20)
            dist = [c - vw for c, vw in zip(C, vwap)]
            sig = _rolling_std(dist, 50)
            last_sigma = sig[-1] if not math.isnan(sig[-1]) else 0.0
            thr = abs(last_sigma) * band
            e_slope = _slope(ema20, lookback=5)

            # Volume confirmation
            vol_ok = True
            if use_vol and len(V) >= 21:
                v_sma20 = sma(V, 20)[-1]
                vol_ok = (v_sma20 is not None) and (V[-1] > (v_sma20 * max(0.1, vol_mult)))

            # Signals
            above_now = C[-1] > vwap[-1]
            above_prev = C[-2] > vwap[-2]
            crossed_up = (C[-2] < vwap[-2]) and above_now
            touched_band = (C[-2] <= vwap[-2] - thr) or (C[-1] <= vwap[-1] - thr)

            want_reversion = crossed_up and (e_slope >= slope_min)
            want_trend = above_now and (e_slope >= slope_min) and (touched_band or above_prev)  # at/above vwap w/ recent kiss

            should_long = vol_ok and (
                (mode == "reversion" and want_reversion) or
                (mode == "trend" and want_trend) or
                (mode == "either" and (want_reversion or want_trend))
            )

            checked.append({
                "symbol": s, "tf": tf_s, "mode": mode, "rth": use_rth, "vol_ok": vol_ok,
                "ema20_slope": round(e_slope, 4),
                "dist": round(dist[-1], 4),
                "sigma": round(last_sigma, 4),
                "band_thr": round(thr, 4),
                "crossed_up": crossed_up,
                "touched_band": touched_band,
                "above_vwap": above_now,
            })

            if should_long:
                payload = {
                    "system": "SPY_VWAP_EMA20",
                    "side": "buy",
                    "ticker": s,
                    "price": C[-1],
                    "time": _iso(T[-1]),
                }
                if dry_run:
                    triggers.append(payload)
                else:
                    # Fire immediately; let shared processor handle risk/oco
                    process_signal(payload)
                    triggers.append(payload)

        except Exception as e:
            checked.append({"symbol": s, "error": str(e)})

    return {"checked": checked, "triggers": triggers, "timeframe": tf_s}
