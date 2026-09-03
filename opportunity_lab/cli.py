"""Command-line entry points for Opportunity Lab research."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone

from .catalog import candidate_catalog
from .crypto_market_data import fetch_crypto_bars
from .crypto_regime import crypto_research_suite


def main() -> int:
    parser = argparse.ArgumentParser(prog="opportunity-lab")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("catalog")
    crypto = sub.add_parser("backtest-crypto")
    crypto.add_argument("--symbol", default="BTC/USD")
    crypto.add_argument("--days", type=int, default=730)
    crypto.add_argument("--timeframe", default="1Hour")
    args = parser.parse_args()
    if args.command == "catalog":
        print(json.dumps(candidate_catalog(), indent=2))
        return 0
    end = datetime.now(timezone.utc)
    bars, transport = fetch_crypto_bars([args.symbol], start=end - timedelta(days=max(30, args.days)), end=end, timeframe=args.timeframe)
    result = {"symbol": args.symbol, "transport": transport, "research": crypto_research_suite(bars.get(args.symbol.upper(), []))}
    print(json.dumps(result, indent=2, default=str))
    return 0 if not transport.get("error") else 2


if __name__ == "__main__":
    raise SystemExit(main())
