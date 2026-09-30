from datetime import datetime, timezone
import json
import sqlite3

from opportunity_lab.kalshi_external_sleeves_worker import (
    FAVORITE_MAKER_ID, FLB_ID, STRUCTURAL_ID, advance_mve_coverage, collect_generation,
    maker_snapshot, open_db, write_status,
)


def market(strike, yes_ask, no_ask):
    return {
        "ticker": f"E-{strike}", "event_ticker": "E", "status": "active",
        "market_type": "binary", "exchange_index": 0, "strike_type": "greater",
        "floor_strike": strike, "yes_ask_dollars": yes_ask,
        "no_ask_dollars": no_ask, "rules_primary": f"Value is above {strike} units",
        "rules_secondary": "Same source", "category": "Economics",
        "close_time": "2030-01-01T00:00:00Z",
        "expiration_time": "2030-01-02T00:00:00Z",
        "occurrence_datetime": "2030-01-01T00:00:00Z",
    }


class Client:
    def __init__(self, rows): self.rows = {row["ticker"]: row for row in rows}
    def quote(self, payload):
        row = self.rows[payload["ticker"]]
        book = ({"no_dollars": [[".70", "2"]]} if row["floor_strike"] == 10 else
                {"yes_dollars": [[".80", "3"]]})
        observed = 100.0 if row["floor_strike"] == 10 else 104.0
        return {"market": row, "orderbook_fp": book, "observed_at": observed}


class MveClient:
    def __init__(self):
        self.calls = []

    def get(self, *, params):
        self.calls.append(dict(params))
        row = {
            "ticker": "MVE-A", "event_ticker": "KXCOMBO-1", "category": "Politics",
            "status": "active", "market_type": "binary", "exchange_index": 0,
            "close_time": "2030-01-01T00:00:00Z", "yes_bid_dollars": ".40",
            "yes_ask_dollars": ".45", "yes_bid_size_fp": "10",
            "yes_ask_size_fp": "10", "volume_24h_fp": "5",
            "mve_collection_ticker": "KXCOMBO",
        }
        return {"markets": [row], "cursor": ""}, 1.0, 1.1


def test_multivariate_inventory_is_separate_and_shadow_only(tmp_path):
    db = open_db(tmp_path / "research.sqlite3")
    client = MveClient()
    result = advance_mve_coverage(
        db, client, datetime(2026, 9, 28, tzinfo=timezone.utc))
    assert client.calls == [{"status": "open", "limit": 200, "mve_filter": "only"}]
    assert result["in_progress"] is False
    assert result["markets_scanned"] == result["active_binary_markets"] == 1
    assert result["maker_screen_candidates"] == 1
    assert result["market_families"] == {"Politics": 1}
    db.close()


def test_snapshot_collection_and_comparison_packet(tmp_path, monkeypatch):
    rows = [market(10, ".05", ".71"), market(20, ".80", ".05")]
    maker = sqlite3.connect(tmp_path / "worker.sqlite3")
    maker.execute("CREATE TABLE research_market_universe(ticker,event_id,generation,tags,detail)")
    for row in rows:
        maker.execute("INSERT INTO research_market_universe VALUES(?,?,?,?,?)",
                      (row["ticker"], "E", 7, "[]", json.dumps(row)))
    maker.commit(); maker.close()
    generation, snapshot, coverage = maker_snapshot(tmp_path / "worker.sqlite3")
    assert generation == 7 and len(snapshot) == 2
    assert coverage == {}

    db = open_db(tmp_path / "research_sleeves.sqlite3")
    monkeypatch.setattr("opportunity_lab.kalshi_external_sleeves_worker.time.time", lambda: 105.0)
    collect_generation(db, Client(rows), generation, snapshot,
                       datetime.fromtimestamp(105, timezone.utc))
    assert db.execute("SELECT count(*) FROM structural_signals").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM calibration_observations").fetchone()[0] == 2
    packet = write_status(tmp_path, db, generation, len(snapshot))
    indexed = {row["strategy_id"]: row for row in packet["sleeves"]}
    assert indexed[STRUCTURAL_ID]["complete_observations"] == 1
    assert indexed[FLB_ID]["complete_observations"] == 0
    assert indexed[FAVORITE_MAKER_ID]["complete_observations"] == 0
    assert packet["favorite_maker_gate"]["candidate_observations"] == 0
    assert packet["execution_enabled"] is False
    assert packet["coverage"]["coverage_complete"] is False
    db.close()
