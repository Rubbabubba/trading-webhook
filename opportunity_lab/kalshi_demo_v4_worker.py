"""Hosted one-contract V4 canary for the isolated Kalshi demo environment.

This worker is incapable of production trading: the client and journal are both
hard-wired to demo. It holds at most one contract, enters an event once, and
stops rather than guessing when exchange state cannot be reconciled.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from fractions import Fraction
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

from .kalshi_binary_broker import BinaryDemoBroker
from .kalshi_binary_journal import BinaryJournal
from .kalshi_demo_broker import DemoClient, check_exchange
from .kalshi_demo_market_data import DemoMarkets
from .kalshi_momentum_v4 import momentum_entry_signal
from .kalshi_process_lock import acquire
from .kalshi_shadow import cost, price_book


STRATEGY_ID = "portfolio_liquidity_momentum_v4"
CONFIG = {
    "strategy_id": STRATEGY_ID, "take_profit_cents": 12,
    "stop_loss_cents": 6, "max_hold_seconds": 300,
    "latency_seconds": 2, "slippage_cents": 1,
}
CAPITAL_LIMIT_CENTS = 160
ORDER_LIMIT_CENTS = 110
DAILY_LOSS_CENTS = 100
FEE_RESERVE_CENTS = 5


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def event_id(market):
    value = market.get("event_ticker")
    if not isinstance(value, str) or not value:
        raise ValueError("event_identity_missing")
    return value


def one_contract_frame(quote, *, book_id):
    return {
        "ticker": quote["ticker"], "book_id": book_id,
        "received_at": quote["observed_at"], "fee_coefficient": ".07",
        "orderbook_fp": quote["orderbook_fp"],
    }


def limit_price_cents(frame, outcome, action):
    bid, ask, bid_size, ask_size = price_book(frame, outcome)
    if action == "buy":
        if ask_size < 1:
            raise ValueError("entry_depth_missing")
        value = int((Decimal(ask.numerator) / Decimal(ask.denominator) * 100)
                    .to_integral_value(rounding=ROUND_CEILING)) + 1
    elif action == "sell":
        if bid_size < 1:
            raise ValueError("exit_depth_missing")
        value = int((Decimal(bid.numerator) / Decimal(bid.denominator) * 100)
                    .to_integral_value(rounding=ROUND_FLOOR)) - 1
    else:
        raise ValueError("invalid_action")
    if not 1 <= value <= 99:
        raise ValueError("executable_limit_out_of_range")
    return value


def select_markets(markets, *, now, limit=6, maximum_book_checks=8):
    """Return deterministic active shard-0 demo binaries with two-sided books."""
    selected = []; checked = 0
    for params in (
        {"status": "open", "limit": 200, "mve_filter": "exclude", "series_ticker": "KXFEDDECISION"},
        {"status": "open", "limit": 200, "mve_filter": "exclude"},
    ):
        page, _started, _observed = markets.get(params=params)
        rows = sorted(page.get("markets", []), key=lambda row: (
            -float(row.get("volume_24h_fp") or row.get("volume_24h") or 0),
            str(row.get("ticker", "")),
        ))
        for market in rows:
            if (market.get("status") != "active" or market.get("market_type") != "binary"
                    or market.get("exchange_index", 0) != 0):
                continue
            try:
                close = datetime.fromisoformat(market["close_time"].replace("Z", "+00:00")).timestamp()
            except (KeyError, TypeError, ValueError):
                continue
            if close <= now + 1800 or any(row["ticker"] == market.get("ticker") for row in selected):
                continue
            if checked >= maximum_book_checks:
                return selected
            checked += 1
            try:
                quote = markets.quote({"ticker": market["ticker"]})
                book = quote.get("orderbook_fp", {})
                if book.get("yes_dollars") and book.get("no_dollars"):
                    selected.append(market)
            except (ValueError, KeyError, TypeError):
                continue
            if len(selected) >= limit:
                return selected
    return selected


class WorkerState:
    def __init__(self, path):
        self.db = sqlite3.connect(path, isolation_level=None, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS settings(name TEXT PRIMARY KEY,detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS history(at REAL NOT NULL,ticker TEXT NOT NULL,mid TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS intent_meta(
            client_id TEXT PRIMARY KEY,kind TEXT NOT NULL,event_id TEXT NOT NULL,
            ticker TEXT NOT NULL,outcome TEXT NOT NULL,created_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS entered_events(event_id TEXT PRIMARY KEY,entered_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS actions(at REAL NOT NULL,ticker TEXT,detail TEXT NOT NULL);
        """)
        saved = self.load("protocol")
        protocol = {"strategy_id": STRATEGY_ID, "config": CONFIG,
                    "capital_limit_cents": CAPITAL_LIMIT_CENTS,
                    "order_limit_cents": ORDER_LIMIT_CENTS,
                    "daily_loss_cents": DAILY_LOSS_CENTS,
                    "one_contract": True, "environment": "demo"}
        if saved is not None and saved != protocol:
            raise ValueError("demo_protocol_changed")
        self.save("protocol", protocol)

    def save(self, name, value):
        self.db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)",
                        (name, json.dumps(value, sort_keys=True)))

    def load(self, name, default=None):
        row = self.db.execute("SELECT detail FROM settings WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    def record(self, ticker, detail):
        self.db.execute("INSERT INTO actions VALUES(?,?,?)", (time.time(), ticker, json.dumps(detail)))

    def close(self):
        self.db.close()


def recover(journal, broker):
    """Resolve durable states without ever resubmitting an uncertain mutation."""
    for row in journal.records():
        cid = row["payload"]["client_order_id"]
        if row["state"] == "reserved":
            journal.abandon_reserved(cid)
            continue
        if row["state"] == "uncertain":
            row = broker.refresh(cid)
        if row["state"] == "working":
            broker.cancel(cid)
    broker.reconcile_positions(allow_reserved=True)


def open_position(journal, state):
    accounting = journal.accounting()
    if len(accounting["positions"]) > 1 or any(abs(value) != 1 for value in accounting["positions"].values()):
        raise ValueError("demo_inventory_limit_breached")
    if not accounting["positions"]:
        return None, accounting
    ticker, signed = next(iter(accounting["positions"].items()))
    outcome = "yes" if signed > 0 else "no"
    row = state.db.execute(
        "SELECT event_id,created_at FROM intent_meta WHERE ticker=? AND outcome=? AND kind='entry' "
        "ORDER BY created_at DESC LIMIT 1", (ticker, outcome)).fetchone()
    if row is None:
        raise ValueError("position_metadata_missing")
    state.db.execute("INSERT OR IGNORE INTO entered_events VALUES(?,?)", (row[0], row[1]))
    return {"ticker": ticker, "outcome": outcome, "event_id": row[0], "opened_at": row[1],
            "basis_cents": float(accounting["open_basis"] * 100)}, accounting


def submit_intent(state, journal, broker, markets, market, outcome, action, price_cents):
    client_id = "v4-demo-" + uuid.uuid4().hex
    kind = "entry" if action == "buy" else "exit"
    at = time.time()
    state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                     (client_id, kind, event_id(market), market["ticker"], outcome, at))
    snapshot = broker.snapshot()
    journal.reserve(client_id, market["ticker"], 1, price_cents, FEE_RESERVE_CENTS,
                    outcome=outcome, action=action, account_snapshot=snapshot)
    try:
        result = broker.submit(client_id, quote_provider=markets.quote)
    except Exception:
        # If validation failed before the durable uncertainty transition, the
        # exchange mutation was definitely not sent and the reservation can be
        # closed. Uncertain state is preserved for reconciliation.
        if journal.get(client_id)["state"] == "reserved":
            journal.abandon_reserved(client_id)
        raise
    if result["state"] == "working":
        result = broker.cancel(client_id)
    if result["state"] != "terminal":
        raise ValueError("demo_ioc_not_terminal")
    if result["filled"] not in (0, 1):
        raise ValueError("demo_fill_count_invalid")
    if kind == "entry" and result["filled"] == 1:
        state.db.execute("INSERT OR IGNORE INTO entered_events VALUES(?,?)", (event_id(market), at))
    state.record(market["ticker"], {"action": kind, "outcome": outcome,
                                    "filled": result["filled"], "client_order_id": client_id,
                                    "environment": "demo"})
    return result


def write_status(path, **values):
    payload = {"at": now_iso(), "environment": "demo", "production_execution_enabled": False,
               "strategy_id": STRATEGY_ID, **values}
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def run(data_root, *, cycles=None):
    root = Path(data_root).resolve(); root.mkdir(parents=True, exist_ok=True)
    lock = acquire(root / "worker.lock")
    state = WorkerState(root / "worker.sqlite3")
    journal = BinaryJournal(root / "journal.sqlite3", order_limit_cents=ORDER_LIMIT_CENTS,
                            capital_limit_cents=CAPITAL_LIMIT_CENTS,
                            daily_loss_cents=DAILY_LOSS_CENTS)
    client = DemoClient(os.environ["KALSHI_DEMO_API_KEY_ID"],
                        os.environ["KALSHI_DEMO_PRIVATE_KEY_PATH"])
    markets = DemoMarkets(); broker = BinaryDemoBroker(journal, client)
    cohort = state.load("cohort", []); cohort_at = state.load("cohort_at", 0)
    scan = int(state.load("scan", 0)); cycle = 0
    try:
        check_exchange(client)
        recover(journal, broker)
        while cycles is None or cycle < cycles:
            errors = []
            try:
                position, accounting = open_position(journal, state)
                if time.time() - cohort_at >= 1800 or not cohort:
                    cohort = select_markets(markets, now=time.time())
                    if not cohort:
                        raise ValueError("no_eligible_demo_markets")
                    cohort_at = time.time(); state.save("cohort", cohort); state.save("cohort_at", cohort_at)
                if position:
                    market = next((row for row in cohort if row["ticker"] == position["ticker"]), None)
                    if market is None:
                        raw, _a, _b = markets.get(position["ticker"]); market = raw["market"]
                    quote = markets.quote({"ticker": position["ticker"]})
                    frame = one_contract_frame(quote, book_id=str(quote["observed_at"]))
                    bid, _ask, _bid_size, _ask_size = price_book(frame, position["outcome"])
                    proceeds = cost(bid, Decimal(".07"), CONFIG["slippage_cents"], False)
                    net = proceeds - position["basis_cents"]
                    reason = ("stop_loss" if net <= -CONFIG["stop_loss_cents"] else
                              "take_profit" if net >= CONFIG["take_profit_cents"] else
                              "time_limit" if time.time() - position["opened_at"] >= CONFIG["max_hold_seconds"] else None)
                    if reason:
                        price = limit_price_cents(frame, position["outcome"], "sell")
                        result = submit_intent(state, journal, broker, markets, market,
                                               position["outcome"], "sell", price)
                        state.record(position["ticker"], {"action": "exit_result", "reason": reason,
                                                         "filled": result["filled"], "modeled_net_cents": net})
                else:
                    market = cohort[scan % len(cohort)]; scan += 1; state.save("scan", scan)
                    quote = markets.quote({"ticker": market["ticker"]})
                    frame = one_contract_frame(quote, book_id=str(quote["observed_at"]))
                    rows = state.db.execute("SELECT at,mid FROM history WHERE ticker=? AND at>=? ORDER BY at",
                                            (market["ticker"], time.time() - 600)).fetchall()
                    history = [(at, Fraction(mid)) for at, mid in rows]
                    signal = momentum_entry_signal(history, frame, CONFIG)
                    locked = state.db.execute("SELECT 1 FROM entered_events WHERE event_id=?",
                                              (event_id(market),)).fetchone()
                    if signal and not locked:
                        price = limit_price_cents(frame, signal["side"], "buy")
                        submit_intent(state, journal, broker, markets, market,
                                      signal["side"], "buy", price)
                    yes_bid, yes_ask, _x, _y = price_book(frame, "yes")
                    state.db.execute("INSERT INTO history VALUES(?,?,?)",
                                     (frame["received_at"], market["ticker"], str((yes_bid + yes_ask) / 2)))
                    state.db.execute("DELETE FROM history WHERE at<?", (time.time() - 600,))
                final = broker.snapshot()
                position, accounting = open_position(journal, state)
                write_status(root / "status.json", phase="running", errors=[],
                             demo_balance_cents=final["balance"]["balance"],
                             open_positions=len(accounting["positions"]), position=position,
                             entered_events=state.db.execute("SELECT count(*) FROM entered_events").fetchone()[0],
                             cohort_size=len(cohort), mutations=len(journal.records()),
                             mutation_unknown=any(row["state"] == "uncertain" for row in journal.records()))
            except Exception as exc:
                errors.append(type(exc).__name__)
                state.record(None, {"action": "cycle_error", "error_type": type(exc).__name__})
                write_status(root / "status.json", phase="blocked", errors=errors,
                             mutation_unknown=any(row["state"] == "uncertain" for row in journal.records()),
                             mutations=len(journal.records()))
                if any(row["state"] == "uncertain" for row in journal.records()):
                    journal.stop(); raise
            cycle += 1
            if cycles is None or cycle < cycles:
                time.sleep(2)
    finally:
        journal.close(); state.close(); lock.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="/var/data/kalshi-demo-v4")
    parser.add_argument("--cycles", type=int)
    args = parser.parse_args(argv)
    run(args.data_root, cycles=args.cycles)


if __name__ == "__main__":
    main()
