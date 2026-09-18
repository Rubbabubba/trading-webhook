from datetime import datetime, timezone

from opportunity_lab.kalshi_binary_journal import BinaryJournal
from opportunity_lab.kalshi_demo_v5_maker_worker import (
    MAKER_SERIES,
    MakerState,
    current_position,
    evidence,
    refresh_cohort,
    select_maker_markets,
)


def snapshot(cash=50000):
    now = datetime.now(timezone.utc).timestamp()
    return {"environment": "demo", "started_at": now, "observed_at": now,
            "balance": {"balance": cash}, "positions": [], "resting_orders": []}


def test_empty_maker_evidence_is_demo_only_and_flat(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        result = evidence(state, journal)
        assert result["environment"] == "demo" and result["post_only"] is True
        assert result["post_only_attempts"] == result["maker_fills"] == 0
        assert result["ending_position_contracts"] == 0
    finally:
        journal.close(); state.close()


def test_current_position_requires_matching_maker_metadata(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        journal.reserve("m1", "TEST", 1, 40, 5, outcome="yes", action="buy",
                        account_snapshot=snapshot(), order_mode="post_only_gtc")
        state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                         ("m1", "maker_entry", "EVENT", "TEST", "yes", 100.0))
        position, accounting = current_position(journal, state)
        assert position is None and accounting["positions"] == {}
    finally:
        journal.close(); state.close()


class FakeMarkets:
    def __init__(self, rows):
        self.rows = rows
        self.series = []

    def get(self, *, params):
        self.series.append(params["series_ticker"])
        return {"markets": self.rows.get(params["series_ticker"], [])}, 1.0, 1.1


def candidate(ticker, event, *, bid=".40", ask=".44", bid_size="20", ask_size="18", volume="1"):
    return {
        "ticker": ticker, "event_ticker": event, "status": "active",
        "market_type": "binary", "exchange_index": 0,
        "close_time": "2030-01-01T00:00:00Z", "yes_bid_dollars": bid,
        "yes_ask_dollars": ask, "yes_bid_size_fp": bid_size,
        "yes_ask_size_fp": ask_size, "volume_24h_fp": volume,
    }


def test_maker_selector_queries_each_series_and_enforces_event_diversity():
    rows = {
        "KXMLBGAME": [candidate("MLB-A", "GAME-1", volume="10"),
                       candidate("MLB-B", "GAME-1", volume="9")],
        "KXNFLGAME": [candidate("NFL-A", "GAME-2", volume="8")],
        "KXNCAAFGAME": [candidate("NCAAF-WIDE", "GAME-3", bid=".30", ask=".50")],
        "KXEPLGAME": [candidate("EPL-THIN", "GAME-4", bid_size="1")],
        "KXFEDDECISION": [candidate("FED-A", "FED-1", volume="7")],
    }
    markets = FakeMarkets(rows)
    result = select_maker_markets(markets, now=1_700_000_000)
    assert markets.series == list(MAKER_SERIES)
    assert [row["ticker"] for row in result] == ["MLB-A", "NFL-A", "FED-A"]
    assert len({row["event_ticker"] for row in result}) == len(result)


class FakeState:
    def __init__(self):
        self.saved = {}
        self.actions = []

    def save(self, name, value):
        self.saved[name] = value

    def record(self, ticker, detail):
        self.actions.append((ticker, detail))


def test_failed_discovery_refresh_keeps_existing_cohort_and_backs_off():
    state = FakeState()
    markets = FakeMarkets({})
    existing = [candidate("MLB-A", "GAME-1")]
    cohort, cohort_at = refresh_cohort(
        state, markets, existing, 100.0, now=2000.0
    )
    assert cohort == existing
    assert cohort_at == 500.0
    assert state.saved["cohort_at"] == 500.0
    assert state.actions[-1][1]["action"] == "cohort_refresh_deferred"
