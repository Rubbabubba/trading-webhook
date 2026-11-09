from fractions import Fraction
import json
import sqlite3

import pytest

from opportunity_lab.kalshi_wallet_monitor import monitor_tick, price_book


def test_production_book_keeps_displayed_depth_above_accounting_limit():
    quote = {"orderbook_fp": {
        "yes_dollars": [["0.6100", "263844.13"]],
        "no_dollars": [["0.3700", "2644760.78"], ["0.3800", "5779587.72"]],
    }}
    assert price_book(quote, "yes") == tuple(map(
        Fraction, ("0.61", "0.62", "263844.13", "5779587.72")))
    assert price_book(quote, "no") == tuple(map(
        Fraction, ("0.38", "0.39", "5779587.72", "263844.13")))


@pytest.mark.parametrize("no_levels", [
    [],
    [["0.38", "0"]],
    [["0.38", "NaN"]],
    [["0.38", "1"], ["0.38", "2"]],
    [["0.39", "10"]],
])
def test_invalid_public_depth_stays_unavailable(no_levels):
    quote = {"orderbook_fp": {"yes_dollars": [["0.61", "10"]], "no_dollars": no_levels}}
    with pytest.raises(ValueError):
        price_book(quote, "yes")


def test_capture_deduplicates_sells_without_spending_buy_quote_budget():
    wallet = "0x" + "a" * 40
    trade = {"type": "TRADE", "proxy_wallet": wallet, "condition_id": "0x" + "b" * 64,
             "token_id": "123", "timestamp": 100, "transaction_hash": "0x" + "c" * 64,
             "price": 0.42, "size": 20, "event_slug": "nfl-buf-min-2026-11-10"}

    def read(url, params):
        assert url.endswith("/activity")
        return ({"data": [{**trade, "side": "BUY"}, {**trade, "side": "SELL"}],
                 "pagination": {"next_cursor": None}}, 100, 101, None, None)

    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE sells(id TEXT PRIMARY KEY, trade TEXT)")
        db.execute("CREATE TABLE buys(id TEXT PRIMARY KEY, trade TEXT, state TEXT, evidence TEXT)")
        for _ in range(2):
            assert monitor_tick(db, [wallet], {}, 90, read) == {}
        assert db.execute("SELECT state FROM buys").fetchall() == [("unmapped",)]
        row, = db.execute("SELECT trade FROM sells").fetchall()
        assert json.loads(row[0])["side"] == "SELL"
