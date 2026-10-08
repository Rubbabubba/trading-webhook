from datetime import datetime, timedelta, timezone
import json
import hashlib
import sqlite3

from opportunity_lab.kalshi_strategy_factory import cycle, evaluate, fingerprint, specs, register_ideas


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
    assert report["holdout_started_at"] is not None
    for index in range(20):
        _observation(db, f"held-{index}", start + timedelta(days=16, hours=index),
                     net=18, spec=candidate["spec"])
    held = cycle(db, now=start + timedelta(days=18))["candidates"][0]
    assert held["holdout_complete_independent_events"] == 20
    assert held["holdout_event_cluster_lower_bound_cents"] > 0
    assert held["complete_independent_events"] == 30


def test_failed_future_holdout_stops_candidate_without_reusing_old_events():
    db = _db(); start = datetime(2026, 10, 3, tzinfo=timezone.utc)
    candidate = cycle(db, now=start)["candidates"][0]
    for index in range(30):
        _observation(db, f"first-{index}", start + timedelta(hours=index + 1),
                     net=20, spec=candidate["spec"])
    cycle(db, now=start + timedelta(days=15))
    for index in range(10):
        _observation(db, f"bad-{index}", start + timedelta(days=16, hours=index),
                     net=-20, spec=candidate["spec"])
    report = cycle(db, now=start + timedelta(days=17))["candidates"][0]
    assert report["holdout_state"] == "rejected"
    assert report["holdout_reason"] == "negative_holdout_mean"
    assert report["complete_independent_events"] == 30
    assert report["holdout_complete_independent_events"] == 10


def test_generated_idea_registers_only_supported_frozen_rule_and_future_side():
    db = _db(); start = datetime(2026, 10, 3, tzinfo=timezone.utc)
    raw = {"stratum": "non_sports", "price_bin": "5-10", "side": "no", "family": "*"}
    digest = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    idea = {"id": "12345678-1234-1234-1234-123456789abc", "spec_hash": digest, "spec": raw}
    first = cycle(db, now=start, ideas=[idea])
    generated = next(row for row in first["candidates"] if row["strategy_id"].startswith("kalshi_idea_"))
    assert generated["prospective_signals"] == 0
    assert generated["spec_hash"] == fingerprint(generated["spec"])
    for side in ("yes", "no"):
        detail = {"stratum": "non_sports", "price_bin": "5-10", "side": side,
                  "family": "test", "execution_enabled": False, "fill_assumed": False}
        db.execute("INSERT INTO calibration_parent_observations VALUES(?,?,?,?,?,?)",
                   (side, side, 0, (start + timedelta(minutes=1)).isoformat(),
                    json.dumps(detail), json.dumps({"cost_stressed_net_cents": 10})))
    second = cycle(db, now=start + timedelta(hours=1), ideas=[idea])
    updated = next(row for row in second["candidates"] if row["strategy_id"] == generated["strategy_id"])
    assert updated["prospective_signals"] == 1
    assert updated["complete_independent_events"] == 1
    assert sum(row["strategy_id"].startswith("kalshi_idea_") for row in second["candidates"]) == 1
    assert updated["execution_enabled"] is False


def test_generated_idea_rejects_tampered_hash_and_unbounded_spec():
    db = _db(); cycle(db, now=datetime(2026, 10, 3, tzinfo=timezone.utc))
    raw = {"stratum": "sports", "price_bin": "all", "side": "either", "family": "*"}
    idea = {"id": "12345678-1234-1234-1234-123456789abc", "spec_hash": "0" * 64, "spec": raw}
    assert register_ideas(db, [idea]) is None
    assert db.execute("SELECT count(*) FROM strategy_factory_candidates WHERE strategy_id LIKE 'kalshi_idea_%'").fetchone()[0] == 0


def test_rejected_idea_does_not_block_next_queued_hypothesis():
    db = _db(); start = datetime(2026, 10, 3, tzinfo=timezone.utc)
    cycle(db, now=start)
    def idea(identifier, side):
        raw = {"stratum": "non_sports", "price_bin": "5-10", "side": side, "family": "*"}
        digest = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return {"id": identifier, "spec_hash": digest, "spec": raw}
    first = idea("12345678-1234-1234-1234-123456789abc", "no")
    second = idea("12345678-1234-1234-1234-123456789abd", "yes")
    registered = register_ideas(db, [first], now=start)
    assert registered is not None
    db.execute("UPDATE strategy_factory_candidates SET state='rejected' WHERE strategy_id=?", (registered,))
    next_id = register_ideas(db, [first, second], now=start + timedelta(days=1))
    assert next_id is not None and next_id != registered
    assert db.execute("SELECT count(*) FROM strategy_factory_candidates WHERE strategy_id LIKE 'kalshi_idea_%'").fetchone()[0] == 2


def test_new_idea_id_cannot_repeat_rejected_scope_under_same_protocol():
    db = _db(); start = datetime(2026, 10, 3, tzinfo=timezone.utc)
    cycle(db, now=start)
    raw = {"stratum": "non_sports", "price_bin": "5-10", "side": "no", "family": "*"}
    digest = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    first = {"id": "12345678-1234-1234-1234-123456789abc", "spec_hash": digest, "spec": raw}
    renamed = {**first, "id": "12345678-1234-1234-1234-123456789abd"}
    registered = register_ideas(db, [first], now=start)
    assert registered is not None
    db.execute("UPDATE strategy_factory_candidates SET state='rejected',reason='negative_prospective_mean' "
               "WHERE strategy_id=?", (registered,))
    assert register_ideas(db, [renamed], now=start + timedelta(days=1)) is None
    assert db.execute("SELECT count(*) FROM strategy_factory_candidates WHERE strategy_id LIKE 'kalshi_idea_%'").fetchone()[0] == 1
