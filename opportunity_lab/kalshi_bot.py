"""Standalone, public-data-only Kalshi paper bot. No order submission transport."""

from __future__ import annotations

import argparse
import itertools
import json
import math
import sqlite3
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from urllib.parse import quote

from .kalshi_market_data import _get, fetch_open_events


def number(value):
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except InvalidOperation:
        return None


def active(market, now):
    try:
        close = datetime.fromisoformat(market["close_time"].replace("Z", "+00:00"))
        return (market.get("status") in {"active", "open"}
                and market.get("market_type") == "binary"
                and (close - now).total_seconds() >= 300)
    except (KeyError, TypeError, ValueError):
        return False


def discover(events, now):
    """Only same-contract complements and explicitly exclusive NO pairs."""
    rows, seen = [], set()
    for event in events:
        markets = [m for m in event.get("markets", []) if active(m, now) and m.get("ticker")]
        pairs = [(m, "yes", m, "no", "complement") for m in markets]
        if event.get("mutually_exclusive") is True:
            pairs.extend((a, "no", b, "no", "exclusive_no_pair")
                         for a, b in itertools.combinations(markets, 2) if a["ticker"] != b["ticker"])
        for a, sa, b, sb, strategy in pairs:
            prices = [number(m.get(f"{s}_ask_dollars")) for m, s in ((a, sa), (b, sb))]
            if any(p is None or not 0 < p < 1 for p in prices) or sum(prices) >= 1:
                continue
            legs = sorted([{"ticker": a["ticker"], "side": sa}, {"ticker": b["ticker"], "side": sb}],
                          key=lambda leg: (leg["ticker"], leg["side"]))
            key = json.dumps(legs, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            rows.append({"key": key, "event_ticker": event.get("event_ticker"),
                         "category": event.get("category"), "strategy": strategy,
                         "legs": legs, "displayed_cost": float(sum(prices))})
    return sorted(rows, key=lambda row: row["displayed_cost"])


def best_ask(payload, side):
    """An ask is one minus the opposing bid. Reject malformed levels."""
    levels = payload.get("orderbook_fp", {}).get("no_dollars" if side == "yes" else "yes_dollars", [])
    valid = []
    for level in levels:
        if not isinstance(level, list) or len(level) != 2:
            return None
        price, size = map(number, level)
        if price is None or size is None or not 0 < price < 1 or size <= 0:
            return None
        valid.append((price, size))
    if not valid:
        return None
    bid = max(p for p, _ in valid)
    return Decimal(1) - bid, sum(s for p, s in valid if p == bid)


def size_trade(candidate, books, *, budget_cents, max_contracts=10,
               fee_coefficient=Decimal("0.07"), slippage=Decimal("0.01"), min_profit_cents=5):
    asks = [best_ask(books.get(leg["ticker"], {}), leg["side"]) for leg in candidate["legs"]]
    if any(ask is None for ask in asks):
        return None
    max_size = min(max_contracts, *(math.floor(size) for _, size in asks))
    best = None
    for count in range(1, max_size + 1):
        # Full per-leg fee rounding; slippage is charged as a paper cost.
        prices = [price for price, _ in asks]
        fees = sum((fee_coefficient * count * p * (1 - p) * 100).to_integral_value(rounding=ROUND_CEILING)
                   for p in prices)
        cost = int((sum(prices) * count * 100).to_integral_value(rounding=ROUND_CEILING) + fees
                   + (slippage * count * len(prices) * 100).to_integral_value(rounding=ROUND_CEILING))
        profit = count * 100 - cost
        if cost <= budget_cents and profit >= min_profit_cents:
            if best is None or profit > best["estimated_min_profit_cents"]:
                best = {**candidate, "contracts": count, "cost_cents": cost,
                        "estimated_fee_cents": int(fees), "estimated_min_profit_cents": profit,
                        "asks": [str(p) for p in prices], "fee_coefficient": str(fee_coefficient),
                        "slippage_per_contract_per_leg": str(slippage),
                        "fill_model": "hypothetical simultaneous fills at displayed top depth",
                        "fees_verified": False}
    return best


def connect(path, initial_cents):
    db = sqlite3.connect(path, timeout=30)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS account(id INTEGER PRIMARY KEY CHECK(id=1), initial_cents INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS trades(
            key TEXT PRIMARY KEY, event_ticker TEXT NOT NULL, opened TEXT NOT NULL,
            detail TEXT NOT NULL, cost_cents INTEGER NOT NULL, payout_cents INTEGER, settled TEXT);
        CREATE TABLE IF NOT EXISTS runs(at TEXT NOT NULL, detail TEXT NOT NULL);
    """)
    with db:
        db.execute("INSERT OR IGNORE INTO account VALUES(1, ?)", (initial_cents,))
    return db


def summary(db):
    initial = db.execute("SELECT initial_cents FROM account WHERE id=1").fetchone()[0]
    spent, returned, locked, settled_cost, opened, settled = db.execute("""
        SELECT coalesce(sum(cost_cents),0), coalesce(sum(payout_cents),0),
        coalesce(sum(CASE WHEN payout_cents IS NULL THEN cost_cents ELSE 0 END),0),
        coalesce(sum(CASE WHEN payout_cents IS NOT NULL THEN cost_cents ELSE 0 END),0),
        count(CASE WHEN payout_cents IS NULL THEN 1 END), count(payout_cents) FROM trades
    """).fetchone()
    return {"initial_cents": initial, "available_cents": initial - spent + returned,
            "open_cost_cents": locked, "realized_paper_profit_cents": returned - settled_cost,
            "open_trades": opened, "settled_trades": settled}


def record_trade(db, trade, max_event_cents, max_total_cents):
    # Recheck cash and exposure while holding the write lock (including parallel processes).
    with db:
        db.execute("BEGIN IMMEDIATE")
        state = summary(db)
        event_cost = db.execute("SELECT coalesce(sum(cost_cents),0) FROM trades WHERE event_ticker=? AND payout_cents IS NULL",
                                (trade["event_ticker"],)).fetchone()[0]
        if (trade["cost_cents"] > state["available_cents"]
                or event_cost + trade["cost_cents"] > max_event_cents
                or state["open_cost_cents"] + trade["cost_cents"] > max_total_cents):
            return False
        # Avoid counting the same visible liquidity in overlapping simulated trades.
        tickers = {leg["ticker"] for leg in trade["legs"]}
        for (detail,) in db.execute("SELECT detail FROM trades WHERE payout_cents IS NULL"):
            if tickers.intersection(leg["ticker"] for leg in json.loads(detail)["legs"]):
                return False
        result = db.execute("INSERT OR IGNORE INTO trades VALUES(?,?,?,?,?,NULL,NULL)",
                            (trade["key"], trade["event_ticker"], datetime.now(timezone.utc).isoformat(),
                             json.dumps(trade), trade["cost_cents"]))
        return result.rowcount == 1


def reconcile(db, get=_get):
    errors, markets = [], {}
    for key, detail in db.execute("SELECT key, detail FROM trades WHERE payout_cents IS NULL").fetchall():
        trade, payouts = json.loads(detail), []
        for leg in trade["legs"]:
            ticker = leg["ticker"]
            if ticker not in markets:
                payload, transport = get(f"/markets/{quote(ticker, safe='')}", {})
                markets[ticker] = payload.get("market", {})
                if transport.get("error"):
                    errors.append({"ticker": ticker, "error": transport["error"]})
            market = markets[ticker]
            # Non-binary/void settlements stay unresolved for manual review.
            if market.get("status") != "finalized" or market.get("result") not in {"yes", "no"}:
                break
            payouts.append(100 * trade["contracts"] if market["result"] == leg["side"] else 0)
        if len(payouts) == len(trade["legs"]):
            with db:
                db.execute("UPDATE trades SET payout_cents=?, settled=? WHERE key=? AND payout_cents IS NULL",
                           (sum(payouts), datetime.now(timezone.utc).isoformat(), key))
    return errors


def run_once(db, args):
    report = {"at": datetime.now(timezone.utc).isoformat(), "mode": "paper", "execution_enabled": False,
              "settlement_errors": reconcile(db), "entered": [], "rejected": []}
    if Path(args.kill_file).exists():
        report["paused"] = True
    else:
        events, transport = fetch_open_events(pages=args.pages)
        report["discovery"] = transport
        # Never open trades on a partial response caused by a transport failure.
        candidates = discover(events, datetime.now(timezone.utc)) if not transport.get("error") else []
        report["snapshot_candidates"] = len(candidates)
        report["recheck_limit"] = args.rechecks
        for candidate in candidates[:args.rechecks]:
            if Path(args.kill_file).exists():
                report["paused"] = True
                break
            books, valid = {}, True
            started = time.monotonic()
            for ticker in dict.fromkeys(leg["ticker"] for leg in candidate["legs"]):
                path = f"/markets/{quote(ticker, safe='')}"
                payload, status = _get(path, {})
                if status.get("error") or not active(payload.get("market", {}), datetime.now(timezone.utc)):
                    valid = False
                    break
                book, status = _get(path + "/orderbook", {"depth": 1})
                if status.get("error"):
                    valid = False
                    break
                books[ticker] = book
            trade = None
            if valid and time.monotonic() - started <= 10:
                trade = size_trade(candidate, books, budget_cents=min(args.max_trade_cents, summary(db)["available_cents"]),
                                   max_contracts=args.max_contracts, fee_coefficient=number(args.fee_coefficient),
                                   slippage=number(args.slippage), min_profit_cents=args.min_profit_cents)
            if (trade and not Path(args.kill_file).exists()
                    and record_trade(db, trade, args.max_event_cents, args.max_total_cents)):
                report["entered"].append(trade)
            else:
                report["rejected"].append({"key": candidate["key"], "reason": "quote, edge, freshness, duplicate, or exposure gate"})
    report["account"] = summary(db)
    with db:
        db.execute("INSERT INTO runs VALUES(?,?)", (report["at"], json.dumps(report)))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="kalshi_paper.sqlite3")
    parser.add_argument("--watch", action="store_true", help="Repeat until Ctrl+C")
    parser.add_argument("--status", action="store_true", help="Print saved paper account without network requests")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--rechecks", type=int, default=10)
    parser.add_argument("--initial-cents", type=int, default=100000)
    parser.add_argument("--max-trade-cents", type=int, default=1000)
    parser.add_argument("--max-event-cents", type=int, default=2500)
    parser.add_argument("--max-total-cents", type=int, default=10000)
    parser.add_argument("--max-contracts", type=int, default=10)
    parser.add_argument("--min-profit-cents", type=int, default=5)
    parser.add_argument("--fee-coefficient", default="0.07")
    parser.add_argument("--slippage", default="0.01")
    parser.add_argument("--kill-file", default="KALSHI_STOP")
    args = parser.parse_args()
    for name in ("initial_cents", "max_trade_cents", "max_event_cents", "max_total_cents", "min_profit_cents"):
        if getattr(args, name) <= 0:
            parser.error(f"{name} must be positive")
    if not (1 <= args.pages <= 10 and 1 <= args.rechecks <= 100 and 1 <= args.max_contracts <= 1000 and args.interval >= 10):
        parser.error("pages: 1..10; rechecks: 1..100; max-contracts: 1..1000; interval: >=10")
    for name in ("fee_coefficient", "slippage"):
        value = number(getattr(args, name))
        if value is None or not 0 <= value <= 1:
            parser.error(f"{name} must be a finite number between 0 and 1")
    db = connect(args.db, args.initial_cents)
    try:
        if args.status:
            print(json.dumps(summary(db), indent=2))
            return 0
        while True:
            report = run_once(db, args)
            print(json.dumps(report, indent=2), flush=True)
            if not args.watch:
                return 2 if report.get("discovery", {}).get("error") else 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
