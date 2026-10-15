from datetime import datetime, timezone
import json

import pytest

from opportunity_lab.kalshi_external_sleeves_worker import open_db, resolve_one_weather
from opportunity_lab.kalshi_weather_ensemble import score_resolution


NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)


def observation(ticker):
    return {"expiration_time": "2026-10-08T14:00:00Z",
            "event_structure": "binary_contracts", "fee_and_model_stress": .03,
            "contracts": [{"ticker": ticker, "bucket": [80, float("inf")],
                           "consensus_probability": .8, "market_mid_probability": .5}],
            "signal": {"ticker": ticker, "side": "yes", "ask": .55}}


def insert(db, key, row):
    db.execute("INSERT INTO weather_observations VALUES(?,?,?,?,?,NULL)",
               (key, key, 1, "2026-09-30T00:00:00Z", json.dumps(row)))


class Client:
    def __init__(self, results):
        self.results = results
        self.calls = []

    def get(self, ticker):
        self.calls.append(ticker)
        return {"market": dict(ticker=ticker, expiration_value="",
                               **self.results[ticker])}, 0, 0


def test_finalized_binary_results_equal_temperature_scoring():
    row = observation("WX")
    numeric = score_resolution(row, 82)
    binary = score_resolution(row, None, contract_results={"WX": "yes"})
    assert {k: v for k, v in numeric.items() if k != "expiration_value"} == {
        k: v for k, v in binary.items() if k != "expiration_value"}
    assert binary["expiration_value"] is None
    with pytest.raises(ValueError, match="incomplete_contract_results"):
        score_resolution(row, None, contract_results={"WX": ""})


def test_unavailable_market_does_not_starve_later_rows_and_cache_survives_restart(tmp_path):
    path = tmp_path / "research.sqlite3"
    db = open_db(path)
    for i in range(21):
        insert(db, f"a{i:02}", observation("UNSETTLED"))
    insert(db, "z", observation("FINAL"))
    client = Client({"UNSETTLED": {"status": "closed", "result": ""},
                     "FINAL": {"status": "finalized", "result": "yes"}})
    first = resolve_one_weather(db, client, NOW)
    assert first["rows_scanned"] == 20 and client.calls == ["UNSETTLED"]
    second = resolve_one_weather(db, client, NOW)
    assert second["scored"] == 1 and client.calls == ["UNSETTLED", "FINAL"]
    result = json.loads(db.execute(
        "SELECT resolution FROM weather_observations WHERE observation_id='z'").fetchone()[0])
    assert result["outcome_source"] == "kalshi_finalized_contract_results"
    assert result["cost_stressed_net_dollars"] == pytest.approx(.42)
    assert result["hypothetical_only"] is True
    db.close()
    db = open_db(path)
    insert(db, "zz", observation("FINAL"))
    assert resolve_one_weather(db, client, NOW)["scored"] == 1
    assert client.calls == ["UNSETTLED", "FINAL"]
    db.close()


def test_nonfinal_outcomes_never_score(tmp_path):
    db = open_db(tmp_path / "research.sqlite3")
    insert(db, "a", observation("WX"))
    client = Client({"WX": {"status": "determined", "result": "yes"}})
    assert resolve_one_weather(db, client, NOW)["scored"] == 0
    assert db.execute("SELECT resolution FROM weather_observations").fetchone()[0] is None
    db.close()


def test_wrong_ticker_response_never_scores(tmp_path):
    db = open_db(tmp_path / "research.sqlite3")
    insert(db, "a", observation("WX"))

    class WrongClient:
        def get(self, ticker):
            return {"market": {"ticker": "OTHER", "status": "finalized", "result": "yes"}}, 0, 0

    assert resolve_one_weather(db, WrongClient(), NOW)["scored"] == 0
    assert db.execute("SELECT resolution FROM weather_observations").fetchone()[0] is None
    db.close()


def test_malformed_record_preserved_and_multicontract_fetch_is_bounded(tmp_path):
    db = open_db(tmp_path / "research.sqlite3")
    insert(db, "a", "not an object")
    row = observation("A")
    row["contracts"].append(dict(row["contracts"][0], ticker="B"))
    insert(db, "b", row)
    client = Client({"A": {"status": "finalized", "result": "yes"},
                     "B": {"status": "finalized", "result": "no"}})
    first = resolve_one_weather(db, client, NOW)
    assert first["malformed"] == 1 and first["scored"] == 0
    assert client.calls == ["A"]
    second = resolve_one_weather(db, client, NOW)
    assert second["scored"] == 1 and client.calls == ["A", "B"]
    assert db.execute("SELECT resolution FROM weather_observations WHERE observation_id='a'").fetchone()[0] is None
    db.close()
