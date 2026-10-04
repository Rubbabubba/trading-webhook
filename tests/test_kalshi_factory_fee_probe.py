from datetime import datetime, timedelta, timezone
import json
import sqlite3

from opportunity_lab.kalshi_factory_fee_probe import fee_basis, probe_next, status


def _row(at):
    return {"observation_id": "obs-future", "event_id": "SERIES-1",
            "ticker": "SERIES-1-MKT", "price_cents": 8,
            "observed_at": at.isoformat(), "market_type": "binary",
            "exchange_index": 0}


def test_fee_basis_uses_series_or_event_override_and_marks_estimate():
    now = datetime.now(timezone.utc)
    row = _row(now)
    event = {"event_ticker": "SERIES-1", "series_ticker": "SERIES"}
    series = {"ticker": "SERIES", "fee_type": "quadratic", "fee_multiplier": 1}
    basis = fee_basis(row, event, series, fetched_at=now.timestamp() + 4)
    assert basis["model_fee_upper_bound_cents"] == 1
    assert basis["modeled_entry_cost_cents"] == 9
    assert basis["actual_fee_verified"] is False
    override = fee_basis(row, {**event, "fee_type_override": "quadratic",
                               "fee_multiplier_override": 3},
                         series, fetched_at=now.timestamp() + 4)
    assert override["fee_source"] == "event_override"
    assert override["model_fee_upper_bound_cents"] == 2


def test_probe_is_forward_only_and_uses_fresh_identity(tmp_path):
    db = sqlite3.connect(tmp_path / "research.sqlite3", isolation_level=None)
    db.executescript("""
      CREATE TABLE strategy_factory_candidates(strategy_id TEXT,spec_hash TEXT,
        registered_at TEXT,state TEXT);
      CREATE TABLE calibration_parent_observations(observation_id TEXT,event_id TEXT,
        observed_at TEXT,detail TEXT,resolution TEXT);
      CREATE TABLE strategy_factory_events(strategy_id TEXT,observation_id TEXT);
      CREATE TABLE strategy_factory_holdout_events(strategy_id TEXT,observation_id TEXT);
    """)
    now = datetime.now(timezone.utc)
    db.execute("INSERT INTO strategy_factory_candidates VALUES(?,?,?,?)",
               ("factory-one", "a" * 64, (now - timedelta(days=1)).isoformat(), "shadow"))
    assert probe_next(db, None, now=now)["reason"] == "no_new_observation"
    future = now + timedelta(seconds=1)
    row = _row(future)
    db.execute("INSERT INTO calibration_parent_observations VALUES(?,?,?,?,NULL)",
               ("obs-future", "SERIES-1", future.isoformat(), json.dumps(row)))
    db.execute("INSERT INTO strategy_factory_events VALUES(?,?)", ("factory-one", "obs-future"))

    class Client:
        def get_event(self, ticker):
            assert ticker == "SERIES-1"
            return {"event": {"event_ticker": ticker, "series_ticker": "SERIES"}}, 0, future.timestamp() + 2
        def get_series(self, ticker):
            assert ticker == "SERIES"
            return {"series": {"ticker": ticker, "fee_type": "quadratic",
                               "fee_multiplier": 1}}, 0, future.timestamp() + 3
        def get(self, ticker):
            assert ticker == "SERIES-1-MKT"
            return {"market": {"ticker": ticker, "event_ticker": "SERIES-1",
                               "market_type": "binary", "exchange_index": 0}}, 0, future.timestamp() + 4

    try:
        result = probe_next(db, Client(), now=future)
        assert result["probed"] is True
        assert status(db)["versions"][0]["observations"] == 1
        db.execute("UPDATE calibration_parent_observations SET resolution=?",
                   (json.dumps({"payout_cents": 100, "hypothetical_only": True}),))
        summary = status(db)["versions"][0]
        assert summary["resolved_independent_events"] == 1
        assert summary["modeled_net_cents"] == 91
        assert summary["prospective_fee_coverage"] == {
            "resolved_events": 1, "fully_modeled_fee_events": 1,
            "missing_modeled_fee_events": 0}
        assert summary["holdout_fee_coverage"]["resolved_events"] == 0
        assert probe_next(db, Client(), now=future)["reason"] == "no_new_observation"
    finally:
        db.close()
