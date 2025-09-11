import uuid
from datetime import datetime, timezone

from .config import CURRENT_STRATEGIES

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _canon_timeframe(tf: str) -> str:
    return {"30": "30Min", "60": "1Hour", "120": "2Hour", "day": "1Day"}.get(tf, "1Hour")

def _cid(prefix: str, system: str, symbol: str) -> str:
    return f"{prefix}-{system}-{symbol}-{uuid.uuid4().hex[:8]}"

def _scrub_nans(x):
    import math
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return 0.0
    if isinstance(x, list):
        return [_scrub_nans(v) for v in x]
    if isinstance(x, dict):
        return {k: _scrub_nans(v) for k, v in x.items()}
    return x

def only_current_strats(by_strategy: dict) -> dict:
    base = {"realized_pnl": 0.0, "trades": 0, "win_rate": 0.0}
    out = {}
    for s in CURRENT_STRATEGIES:
        v = by_strategy.get(s)
        out[s] = v if isinstance(v, dict) else dict(base)
    return out