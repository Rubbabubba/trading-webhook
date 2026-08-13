#!/usr/bin/env python3
"""Direct Alpaca bar probe utility.

Usage:
  python bar_probe.py --symbol AAPL --days 1 --limit 500
"""

import argparse
import json
from datetime import datetime, timedelta, timezone

from app import _DATA_FEED_RAW, _fetch_bars_via_rest, _iso_utc


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe Alpaca historical 1m bars directly via REST.")
    parser.add_argument("--symbol", default="AAPL", help="Ticker symbol")
    parser.add_argument("--days", type=int, default=1, help="Lookback days")
    parser.add_argument("--limit", type=int, default=500, help="Per-request bar limit")
    args = parser.parse_args()

    sym = (args.symbol or "AAPL").strip().upper()
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=max(1, int(args.days)))

    rows, debug = _fetch_bars_via_rest([sym], start_utc, end_utc, feed_override=_DATA_FEED_RAW, limit=max(1, int(args.limit)))
    seq = rows.get(sym, [])

    payload = {
        "ok": bool(seq),
        "symbol": sym,
        "request_start_utc": _iso_utc(start_utc),
        "request_end_utc": _iso_utc(end_utc),
        "bars": len(seq),
        "first_ts": seq[0].get("ts_ny").isoformat() if seq else None,
        "last_ts": seq[-1].get("ts_ny").isoformat() if seq else None,
        "debug": debug,
    }
    print(json.dumps(payload, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
