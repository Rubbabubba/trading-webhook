from datetime import datetime, timedelta, timezone
import json
import sqlite3

from opportunity_lab.kalshi_strategy_factory import cycle, evaluate, fingerprint, specs


def _db():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE calibration_parent_observations("
               "observation_id TEXT PRIMARY KEY,event_id TEXT,decision_bucket INTEGER,"
               "observed_at TEXT,detail TEXT,resolution TEXT)")
    return db


def _observation(db, event, at, *, net=None, spec=None):
    spec = spec or tuple(specs())[0]
    detail = {"stratum": spec["stratum"], "price_bin": spec["price_bin"],
              "execution_enabled": False, "fill_assumed": False}
    resolution = None if net is None else {"cost_stressed_net_cents": net}
    db.execute("INSERT INTO calibration_parent_observations VALUES(?,?,?,?,?,?)",
               (event, event, 0, at.isoformat(), json.dumps(detail),
                json.dumps(resolution) if resolution else None))


def test_factory_registers_before_future_evidence_and_rotates_after_rejection():
    db = _db(); start = datetime(2026, 10, 3, tzinfo=timezone.utc)
    first = cycle(db, now=start)
    assert len(first["candidates"]) == 1
    candidate = first["candidates"][0]
    assert candidate["prospective_signals"] == 0
    chosen = candidate["spec"]
    assert candidate["spec_hash"] == fingerprint(chosen)
    _observation(db, "old", start - timedelta(days=1), net=99, spec=chosen)
    for index in range(15):
        _observation(db, f"future-{index}", start + timedelta(minutes=index + 1),
                     net=-20, spec=chosen)
    second = cycle(db, now=start + timedelta(days=1))
    original = next(row for row in second["candidates"] if row["strategy_id"] == candidate["strategy_id"])
    assert original["state"] == "rejected"
    assert original["complete_independent_events"] == 15
    assert len(second["candidates"]) == 2
    assert second["active_strategy_id"] != candidate["strategy_id"]
    assert all(row["execution_enabled"] is False and row["live_promotion_eligible"] is False
               for row in second["candidates"])


def test_factory_positive_shadow_only_becomes_demo_candidate():
    db = _db(); start = datetime(2026, 10, 3, tzinfo=timezone.utc)
    candidate = cycle(db, now=start)["candidates"][0]
    for index in range(30):
        _observation(db, f"win-{index}", start + timedelta(hours=index + 1),
                     net=20, spec=candidate["spec"])
    report = cycle(db, now=start + timedelta(days=15))["candidates"][0]
    assert report["state"] == "demo_trial_candidate"
    assert report["event_cluster_lower_bound_cents"] > 0
    assert report["execution_enabled"] is False
    assert report["fill_assumed"] is False
    assert report["live_promotion_eligible"] is False
