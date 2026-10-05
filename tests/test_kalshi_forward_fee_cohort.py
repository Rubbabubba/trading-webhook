from datetime import datetime, timedelta, timezone
import json
import sqlite3

from opportunity_lab.kalshi_strategy_factory import cycle, capture_future
from opportunity_lab.kalshi_factory_fee_probe import probe_next


def setup():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE calibration_parent_observations(observation_id TEXT PRIMARY KEY,"
               "event_id TEXT,decision_bucket INTEGER,observed_at TEXT,detail TEXT,resolution TEXT)")
    at = datetime(2026, 10, 4, tzinfo=timezone.utc)
    report = cycle(db, now=at, parallel=True)
    chosen = next(c for c in report["candidates"] if c["spec"]["stratum"] == "non_sports" and c["spec"]["side"] == "yes")
    # Register fee collection before future acquisition, independently of outcomes.
    probe_next(db, None, now=at)
    return db, at, chosen


def add(db, at, candidate, name, *, resolved=False, event="SERIES-1"):
    low, high = map(int, candidate["spec"]["price_bin"].split("-"))
    row = {**candidate["spec"], "observation_id": name, "event_id": event,
           "ticker": event + "-MKT", "price_cents": (low + high) // 2,
           "observed_at": at.isoformat(), "fill_assumed": False, "execution_enabled": False}
    db.execute("INSERT INTO calibration_parent_observations VALUES(?,?,?,?,?,?)",
               (name, event, 0, at.isoformat(), json.dumps(row), "{}" if resolved else None))


class Client:
    def __init__(self, at, multiplier=1, price=3): self.at, self.multiplier, self.calls, self.price = at, multiplier, 0, price
    def get_event(self, ticker):
        self.calls += 1
        return {"event": {"event_ticker": ticker, "series_ticker": "SERIES"}}, 0, self.at.timestamp() + 1
    def get_series(self, ticker):
        return {"series": {"ticker": ticker, "fee_type": "quadratic", "fee_multiplier": self.multiplier}}, 0, self.at.timestamp() + 2
    def get(self, ticker):
        return {"market": {"ticker": ticker, "event_ticker": ticker.removesuffix("-MKT"), "market_type": "binary", "exchange_index": 0}}, 0, self.at.timestamp() + 3
    def quote(self, payload):
        # Only tests the fresh acquisition path; actual transport is GET-only.
        self.calls += 1
        return {"environment": "demo", "observed_at": self.at.timestamp(), "started_at": self.at.timestamp(),
                "market": {"ticker": payload["ticker"], "event_ticker": payload["ticker"].removesuffix("-MKT"),
                           "market_type": "binary", "status": "active", "exchange_index": 0},
                "orderbook_fp": {"yes_dollars": [[str((self.price - 2) / 100), "1"]], "no_dollars": [[str((100 - self.price) / 100), "1"]]}}


def test_unverified_quotes_never_enter_and_cannot_be_repaired_after_settlement():
    db, at, c = setup(); future = at + timedelta(seconds=10)
    add(db, future, c, "missing")
    assert capture_future(db) == 0
    db.execute("UPDATE calibration_parent_observations SET resolution='{}'")
    client = Client(future)
    assert probe_next(db, client, now=future)["probed"] is False
    assert client.calls == 0
    assert db.execute("SELECT count(*) FROM strategy_factory_events").fetchone()[0] == 0


def test_audit_before_membership_one_quote_per_event_and_unchanged_old_records():
    db, at, c = setup(); future = at + timedelta(seconds=10)
    low, high = map(int, c["spec"]["price_bin"].split("-")); price = (low + high) // 2
    add(db, future, c, "audited")
    assert probe_next(db, Client(future, price=price), now=future)["probed"] is True
    assert capture_future(db) > 0
    assert db.execute("SELECT count(*) FROM strategy_factory_events WHERE strategy_id=?", (c["strategy_id"],)).fetchone()[0] == 1
    later = future + timedelta(seconds=10)
    add(db, later, c, "later")
    probe_next(db, Client(later, price=price), now=later)
    capture_future(db)
    assert db.execute("SELECT count(*) FROM strategy_factory_events WHERE strategy_id=?", (c["strategy_id"],)).fetchone()[0] == 1
    old = json.loads(json.dumps(c["spec"])); old["evaluation_protocol"] = "tournament_v1"
    from opportunity_lab.kalshi_strategy_factory import fingerprint
    db.execute("INSERT INTO strategy_factory_candidates VALUES(?,?,?,?,?,?)",
               ("old", fingerprint(old), json.dumps(old), at.isoformat(), "shadow", "original"))
    db.execute("INSERT INTO strategy_tournament_protocols(strategy_id,spec_hash,registered_at,protocol) VALUES(?,?,?,?)",
               ("old", fingerprint(old), at.isoformat(), "tournament_v1"))
    cycle(db, now=later, parallel=True)
    assert db.execute("SELECT state,reason,spec_json FROM strategy_factory_candidates WHERE strategy_id='old'").fetchone() == (
        "rejected", "superseded_by_forward_fee_protocol", json.dumps(old))


def test_old_snapshot_is_only_a_hint_new_quote_is_measured_after_registration():
    db, at, c = setup(); future = at + timedelta(seconds=10)
    low, high = map(int, c["spec"]["price_bin"].split("-")); price = (low + high) // 2
    add(db, at - timedelta(days=1), c, "old-indicative")
    assert capture_future(db) == 0
    assert probe_next(db, Client(future, price=price), now=future)["probed"] is True
    capture_future(db)
    record = db.execute("SELECT o.observation_id,o.observed_at FROM strategy_factory_events e "
                        "JOIN calibration_parent_observations o USING(observation_id) WHERE e.strategy_id=?", (c["strategy_id"],)).fetchone()
    assert record[0].startswith("fee-v2:") and record[1] == future.isoformat()
    assert db.execute("SELECT observed_at FROM calibration_parent_observations WHERE observation_id='old-indicative'").fetchone()[0] == (at - timedelta(days=1)).isoformat()
