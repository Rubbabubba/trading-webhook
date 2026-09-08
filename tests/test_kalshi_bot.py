from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from opportunity_lab.kalshi_bot import (best_ask, connect, discover, number, record_trade,
                                       reconcile, run_once, size_trade, summary)


NOW = datetime(2026, 9, 7, tzinfo=timezone.utc)


def market(ticker):
    return {"ticker": ticker, "market_type": "binary", "status": "active",
            "close_time": "2026-09-08T00:00:00Z", "yes_ask_dollars": ".70", "no_ask_dollars": ".40"}


def candidate():
    return discover([{"event_ticker": "EVENT", "mutually_exclusive": True,
                      "markets": [market("A"), market("B")]}], NOW)[0]


def books():
    return {ticker: {"orderbook_fp": {"yes_dollars": [[".6", "10"]]}} for ticker in ("A", "B")}


def trade():
    return size_trade(candidate(), books(), budget_cents=1000)


def test_discovery_requires_explicit_exclusivity_and_open_binary_markets():
    event = {"event_ticker": "EVENT", "markets": [market("A"), market("B")]}
    assert discover([event], NOW) == []
    event["mutually_exclusive"] = True
    assert len(discover([event, event], NOW)) == 1
    event["markets"][0]["status"] = "closed"
    assert discover([event], NOW) == []
    event["markets"][0] = {**market("A"), "market_type": "scalar"}
    assert discover([event], NOW) == []


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", None, "bad"])
def test_invalid_numbers_fail_closed(value):
    assert number(value) is None


def test_book_uses_opposite_side_and_best_price_regardless_of_order():
    payload = {"orderbook_fp": {"yes_dollars": [[".5", "8"], [".6", "2.5"]]}}
    assert best_ask(payload, "no") == (Decimal(".4"), Decimal("2.5"))
    assert best_ask(payload, "yes") is None
    payload["orderbook_fp"]["yes_dollars"].append(["NaN", "10"])
    assert best_ask(payload, "no") is None


def test_size_accounts_for_fees_slippage_depth_and_budget():
    result = trade()
    assert result["contracts"] == 10
    assert result["estimated_fee_cents"] == 34
    assert result["cost_cents"] == 854
    assert result["estimated_min_profit_cents"] == 146
    smaller = size_trade(candidate(), books(), budget_cents=100)
    assert smaller["contracts"] == 1
    assert smaller["cost_cents"] == 86
    shallow = books()
    shallow["A"]["orderbook_fp"]["yes_dollars"] = [[".6", ".99"]]
    assert size_trade(candidate(), shallow, budget_cents=1000) is None
    assert size_trade(candidate(), books(), budget_cents=50) is None


def test_fees_can_remove_displayed_edge():
    expensive = {ticker: {"orderbook_fp": {"yes_dollars": [[".51", "10"]]}} for ticker in ("A", "B")}
    assert size_trade(candidate(), expensive, budget_cents=1000) is None


def test_persistent_cash_limits_deduplication_and_settlement(tmp_path):
    path = tmp_path / "paper.db"
    db = connect(path, 1000)
    assert not record_trade(db, trade(), 800, 1000)
    assert not record_trade(db, trade(), 1000, 800)
    assert record_trade(db, trade(), 1000, 1000)
    assert not record_trade(db, trade(), 1000, 1000)
    assert summary(db)["available_cents"] == 146
    assert summary(db)["realized_paper_profit_cents"] == 0
    db.close()
    db = connect(path, 99999)
    assert summary(db)["initial_cents"] == 1000

    def unsettled(path, params):
        return {"market": {"status": "closed", "result": "yes"}}, {}

    reconcile(db, unsettled)
    assert summary(db)["open_trades"] == 1

    def settled(path, params):
        return {"market": {"status": "finalized", "result": "yes" if path.endswith("A") else "no"}}, {}

    reconcile(db, settled)
    reconcile(db, settled)
    state = summary(db)
    assert state["available_cents"] == 1146
    assert state["realized_paper_profit_cents"] == 146
    assert state["settled_trades"] == 1
    assert state["open_cost_cents"] == 0
    db.close()


def test_reconcile_errors_and_nonbinary_results_do_not_create_profit(tmp_path):
    db = connect(tmp_path / "paper.db", 1000)
    record_trade(db, trade(), 1000, 1000)
    errors = reconcile(db, lambda path, params: ({}, {"error": "offline"}))
    assert errors
    reconcile(db, lambda path, params: ({"market": {"status": "finalized", "result": "scalar"}}, {}))
    assert summary(db)["available_cents"] == 146
    assert summary(db)["realized_paper_profit_cents"] == 0
    db.close()


def test_scan_opens_only_after_book_recheck_and_persists_audit(tmp_path, monkeypatch):
    from opportunity_lab import kalshi_bot as bot

    future = "2099-01-01T00:00:00Z"
    event = {"event_ticker": "EVENT", "mutually_exclusive": True,
             "markets": [{**market(ticker), "close_time": future} for ticker in ("A", "B")]}
    monkeypatch.setattr(bot, "fetch_open_events", lambda **kw: ([event], {"pages": 1}))

    def get(path, params):
        ticker = path.split("/")[2]
        if path.endswith("/orderbook"):
            return books()[ticker], {}
        return {"market": {**market(ticker), "close_time": future}}, {}

    monkeypatch.setattr(bot, "_get", get)
    args = SimpleNamespace(kill_file=str(tmp_path / "STOP"), pages=1, rechecks=2, max_trade_cents=1000,
                           max_contracts=10, fee_coefficient=".07", slippage=".01", min_profit_cents=5,
                           max_event_cents=2500, max_total_cents=10000)
    db = connect(tmp_path / "paper.db", 100000)
    report = run_once(db, args)
    assert len(report["entered"]) == 1
    assert report["account"]["realized_paper_profit_cents"] == 0
    assert db.execute("SELECT count(*) FROM runs").fetchone()[0] == 1
    db.close()


def test_pause_and_partial_discovery_errors_prevent_new_entries(tmp_path, monkeypatch):
    from opportunity_lab import kalshi_bot as bot

    db = connect(tmp_path / "paper.db", 1000)
    kill = tmp_path / "STOP"
    kill.touch()
    args = SimpleNamespace(kill_file=str(kill), pages=1, rechecks=2)
    monkeypatch.setattr(bot, "fetch_open_events", lambda **kw: pytest.fail("paused discovery"))
    assert run_once(db, args)["paused"]
    kill.unlink()
    monkeypatch.setattr(bot, "fetch_open_events", lambda **kw: ([{}], {"error": "offline"}))
    report = run_once(db, args)
    assert report["snapshot_candidates"] == 0
    assert report["entered"] == []
    assert summary(db)["available_cents"] == 1000
    db.close()
