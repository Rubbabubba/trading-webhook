from datetime import datetime, timezone
import hashlib
import json
import sqlite3

import pytest

from opportunity_lab.kalshi_weather_historical_audit import audit


def source(path):
    db = sqlite3.connect(path)
    db.executescript("""CREATE TABLE weather_observations(observation_id TEXT PRIMARY KEY,event_id TEXT,
      decision_bucket INTEGER,observed_at TEXT,detail TEXT,resolution TEXT);
      CREATE TABLE weather_forecasts(id TEXT);""")
    row = {"observed_at": "2026-09-30T12:00:00Z", "expiration_time": "2026-10-02T12:00:00Z",
           "station": "KMIA", "target_date": "2026-10-01", "event_structure": "binary_contracts",
           "fee_and_model_stress": .03,
           "contracts": [{"ticker": "WX", "bucket": [80, float("inf")],
                          "consensus_probability": .8, "market_mid_probability": .5}],
           "signal": {"ticker": "WX", "side": "yes", "ask": .55}}
    db.execute("INSERT INTO weather_observations VALUES('a','E',1,?,?,NULL)",
               (row["observed_at"], json.dumps(row)))
    db.commit(); db.close()


class Client:
    def __init__(self): self.calls = 0
    def get(self, ticker):
        self.calls += 1
        return {"market": {"ticker": ticker, "status": "finalized", "result": "no",
                           "expiration_value": "", "settlement_ts": "2026-10-02T13:00:00Z"}}, 0, 0


def test_historical_loss_scored_without_mutating_source_or_inventing_fills(tmp_path):
    path = tmp_path / "source.sqlite3"; source(path)
    before = hashlib.sha256(path.read_bytes()).hexdigest(); client = Client()
    report = audit(path, client=client, now=datetime(2026, 10, 15, tzinfo=timezone.utc))
    assert client.calls == 1
    assert report["after"]["complete_signal_observations"] == 1
    assert report["after"]["cost_stressed_net_cents"] == pytest.approx(-58)
    assert report["execution_enabled"] is report["fill_assumed"] is report["profitability_proven"] is False
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_zero_budget_missing_settlement_stays_incomplete(tmp_path):
    path = tmp_path / "source.sqlite3"; source(path); client = Client()
    report = audit(path, client=client, maximum_requests=0,
                   now=datetime(2026, 10, 15, tzinfo=timezone.utc))
    assert report["request_budget_exhausted"] and client.calls == 0
    assert report["after"]["complete_signal_observations"] == 0
    with pytest.raises(ValueError, match="maximum_20"):
        audit(path, client=client, maximum_requests=21)
