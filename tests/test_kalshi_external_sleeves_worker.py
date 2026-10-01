from datetime import datetime, timezone
import json
import sqlite3

from opportunity_lab.kalshi_external_sleeves_worker import (
    FAVORITE_MAKER_ID, FLB_ID, STRUCTURAL_ID, advance_mve_coverage, collect_generation,
    maker_snapshot, open_db, parent_event_calibration_observations, write_status,
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
    stale = market(5, ".03", ".98")
    stale["unused_large_payload"] = "x" * 10000
    maker.execute("INSERT INTO research_market_universe VALUES(?,?,?,?,?)",
                  (stale["ticker"], "OLD", 6, "[]", json.dumps(stale)))
    for row in rows:
        maker.execute("INSERT INTO research_market_universe VALUES(?,?,?,?,?)",
                      (row["ticker"], "E", 7, "[]", json.dumps(row)))
    maker.commit(); maker.close()
    generation, snapshot, coverage = maker_snapshot(tmp_path / "worker.sqlite3")
    assert generation == 7 and len(snapshot) == 2
    assert all("unused_large_payload" not in row for row in snapshot)
    assert coverage == {}

    db = open_db(tmp_path / "research_sleeves.sqlite3")
    monkeypatch.setattr("opportunity_lab.kalshi_external_sleeves_worker.time.time", lambda: 105.0)
    collect_generation(db, Client(rows), generation, snapshot,
                       datetime.fromtimestamp(105, timezone.utc))
    assert db.execute("SELECT count(*) FROM structural_signals").fetchone()[0] == 1
    assert db.execute(
        "SELECT count(*) FROM calibration_parent_observations").fetchone()[0] == 1
    detail = json.loads(db.execute(
        "SELECT detail FROM calibration_parent_observations").fetchone()[0])
    assert detail["observation_id"] == "E:longshot:2-5"
    db.execute(
        "INSERT INTO weather_observations VALUES(?,?,?,?,?,NULL)",
        ("bad-weather", "WX", 1, "2026-09-30T00:00:00+00:00", "{bad-json"),
    )
    packet = write_status(tmp_path, db, generation, len(snapshot))
    indexed = {row["strategy_id"]: row for row in packet["sleeves"]}
    assert indexed[STRUCTURAL_ID]["complete_observations"] == 1
    assert indexed[FLB_ID]["complete_observations"] == 0
    assert packet["favorite_longshot_gate"][
        "evidence_scope"] == "parent_event_canonical_only"
    assert indexed[FAVORITE_MAKER_ID]["complete_observations"] == 0
    assert packet["favorite_maker_gate"]["candidate_observations"] == 0
    assert packet["weather_ensemble_gate"]["observations"] == 1
    assert packet["weather_ensemble_gate"]["candidate_observations"] == 0
    assert packet["weather_ensemble_gate"]["malformed_observations_excluded"] == 1
    assert packet["execution_enabled"] is False
    assert packet["coverage"]["coverage_complete"] is False
    db.close()


def test_calibration_groups_correlated_contracts_by_parent_event_and_bin():
    rows = [market(10, ".05", ".71"), market(20, ".80", ".05")]
    rows[0]["volume_24h_fp"] = "5"
    rows[1]["volume_24h_fp"] = "10"
    observations = parent_event_calibration_observations(
        rows, "2026-09-30T12:15:00+00:00")
    assert len(observations) == 1
    assert observations[0]["ticker"] == "E-20"
    assert observations[0]["observation_id"] == "E:longshot:2-5"


def test_parent_event_migration_preserves_legacy_rows_and_seeds_canonical(tmp_path):
    path = tmp_path / "research.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.execute("""CREATE TABLE calibration_observations(
        observation_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,
        decision_bucket INTEGER NOT NULL,observed_at TEXT NOT NULL,
        detail TEXT NOT NULL,resolution TEXT)""")
    detail = json.dumps({"classification": "favorite", "price_bin": "90-95"})
    legacy.executemany(
        "INSERT INTO calibration_observations VALUES(?,?,?,?,?,NULL)",
        [("E:favorite:90-95", "E", 1, "2026-09-30T00:00:00+00:00", detail),
         ("E-1:yes:1", "E", 1, "2026-09-30T00:00:00+00:00", detail)],
    )
    legacy.commit(); legacy.close()
    db = open_db(path)
    assert db.execute("SELECT count(*) FROM calibration_observations").fetchone()[0] == 2
    assert db.execute(
        "SELECT count(*) FROM calibration_parent_observations").fetchone()[0] == 1
    assert db.execute(
        "SELECT 1 FROM coverage_settings WHERE name='parent_event_calibration_v1'"
    ).fetchone() == (1,)
    db.close()


def test_snapshot_preserves_last_complete_generation_during_scan(tmp_path):
    maker = sqlite3.connect(tmp_path / "worker.sqlite3")
    maker.execute("CREATE TABLE research_market_universe(ticker,event_id,generation,tags,detail)")
    maker.execute("CREATE TABLE settings(name PRIMARY KEY,detail)")
    complete = market(10, ".20", ".81")
    partial = market(20, ".30", ".71")
    maker.execute("INSERT INTO research_market_universe VALUES(?,?,?,?,?)",
                  (complete["ticker"], "COMPLETE", 7, "[]", json.dumps(complete)))
    maker.execute("INSERT INTO research_market_universe VALUES(?,?,?,?,?)",
                  (partial["ticker"], "PARTIAL", 8, "[]", json.dumps(partial)))
    maker.execute("INSERT INTO settings VALUES(?,?)", (
        "market_discovery", json.dumps({"generation": 8, "in_progress": True})))
    maker.commit(); maker.close()

    generation, snapshot, coverage = maker_snapshot(tmp_path / "worker.sqlite3")
    assert generation == 7
    assert [row["ticker"] for row in snapshot] == [complete["ticker"]]
    assert coverage == {"generation": 8, "in_progress": True}
