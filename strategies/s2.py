from typing import List, Dict, Any
from datetime import datetime, timezone

from core.indicators import sma, macd
from core.alpaca import bars
from core.utils import _iso
from core.signals import process_signal

def scan_s2_symbols(symbols: List[str], tf: str, mode: str, use_rth: bool, use_vol: bool, dry_run: bool) -> Dict[str, Any]:
    tf_c = {"30": "30Min", "60": "1Hour", "120": "2Hour", "day": "1Day"}.get(tf, "1Hour")
    checked = []
    triggers = []

    for sym in symbols:
        s = sym.strip().upper()
        try:
            d_bars = bars(s, "1Day", limit=60)
            if len(d_bars) < 12:
                checked.append({"symbol": s, "skip": "not_enough_daily"}); continue
            closes_d = [float(b["c"]) for b in d_bars]
            sma10 = sma(closes_d, 10)[-1]
            trend_ok = (closes_d[-1] > sma10) if sma10 is not None else False

            i_bars = bars(s, tf_c, limit=200)
            if len(i_bars) < 35:
                checked.append({"symbol": s, "skip": "not_enough_intraday"}); continue
            closes = [float(b["c"]) for b in i_bars]
            vols   = [float(b["v"]) for b in i_bars]

            m_line, s_line, _ = macd(closes, 12, 26, 9)
            prev_cross = m_line[-2] - s_line[-2]
            last_cross = m_line[-1] - s_line[-1]
            macd_up    = (prev_cross <= 0 and last_cross > 0)

            vol_ok = True
            if use_vol and len(vols) >= 20:
                from core.indicators import sma as sma_func
                v_sma20 = sma_func(vols, 20)[-1]
                vol_ok = v_sma20 is not None and vols[-1] > v_sma20

            should_long = macd_up and (trend_ok if mode == "strict" else True) and vol_ok

            checked.append({"symbol": s, "trend_ok": trend_ok, "macd_up": macd_up, "vol_ok": vol_ok})
            if should_long:
                ref = closes[-1]
                payload = {"system": "SMA10D_MACD", "side": "buy", "ticker": s, "price": ref, "time": _iso(datetime.now(timezone.utc))}
                if dry_run:
                    triggers.append(payload)
                else:
                    status, out = process_signal(payload)  # fire
                    triggers.append(payload)
        except Exception as e:
            checked.append({"symbol": s, "error": str(e)})
    return {"checked": checked, "triggers": triggers, "timeframe": tf_c}