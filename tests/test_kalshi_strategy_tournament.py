from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3

from opportunity_lab.kalshi_strategy_factory import cycle
from opportunity_lab import kalshi_strategy_tournament as tournament


def database():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE calibration_parent_observations(observation_id TEXT PRIMARY KEY,"
               "event_id TEXT,decision_bucket INTEGER,observed_at TEXT,detail TEXT,resolution TEXT)")
    return db


def observation(db, name, at, spec, net):
    detail = {**spec, "fill_assumed": False, "execution_enabled": False}
    db.execute("INSERT INTO calibration_parent_observations VALUES(?,?,?,?,?,?)",
               (name, name, 0, at.isoformat(), json.dumps(detail),
                json.dumps({"cost_stressed_net_cents": net})))
    # This fixture supplies forward acquisition separately; absence is tested
    # in the fee-cohort tests. It never refetches fees after resolution.
    if spec.get("evaluation_protocol") == tournament.PROTOCOL:
        for (strategy,) in db.execute("SELECT strategy_id FROM strategy_factory_candidates WHERE spec_json=?", (json.dumps(spec, sort_keys=True),)):
            raw = json.dumps({"fixture_fee_proof": True})
            db.execute("INSERT INTO factory_fee_observations VALUES(?,?,?,?,?)",
                       (strategy, name, name, raw, hashlib.sha256(raw.encode()).hexdigest()))


def test_parallel_admission_replay_not_validation_and_replacement():
    db = database(); at = datetime(2026, 10, 4, tzinfo=timezone.utc)
    spec = {"stratum": "sports", "price_bin": "5-10", "side": "yes", "family": "*"}
    observation(db, "historical", at - timedelta(days=1), spec, 50)
    report = cycle(db, now=at, parallel=True)
    assert report["tournament"]["active"] == 8
    assert report["tournament"]["variants_screened"] == 16
    chosen = next(row for row in report["candidates"] if row["spec"]["stratum"] == "sports"
                  and row["spec"]["side"] == "yes" and row["historical_events"])
    assert chosen["complete_independent_events"] == 0
    assert chosen["historical_net_cents"] == 47
    for index in range(15):
        observation(db, f"loss{index}", at + timedelta(minutes=index + 1), chosen["spec"], -20)
    report = cycle(db, now=at + timedelta(days=1), parallel=True)
    assert report["tournament"]["active"] == 8
    assert report["tournament"]["registered"] == 9
    assert report["tournament"]["rejected"] == 1
    assert next(row for row in report["candidates"] if row["strategy_id"] == chosen["strategy_id"])["state"] == "rejected"
    again = cycle(db, now=at + timedelta(days=1), parallel=True)
    assert again["tournament"]["registered"] == 9


def test_ai_parallel_cap_and_no_repeat_of_legacy_idea():
    db = database(); at = datetime(2026, 10, 4, tzinfo=timezone.utc)
    ideas = []
    for index, price in enumerate(("2-5", "5-10", "90-95")):
        spec = {"stratum": "non_sports", "price_bin": price, "side": "no", "family": "*"}
        ideas.append({"id": f"12345678-1234-1234-1234-{index:012d}", "spec": spec,
                      "spec_hash": hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()})
    cycle(db, now=at, ideas=ideas[:1])
    report = cycle(db, now=at, ideas=ideas, parallel=True)
    assert len([row for row in report["candidates"] if row["strategy_id"].startswith("kalshi_idea_")]) == 2


def test_checkpoints_are_frozen_and_multiple_searches_raise_threshold():
    db = database(); tournament.init(db); at = datetime(2026, 10, 4, tzinfo=timezone.utc)
    tournament.register_protocol(db, "one", "hash", at)
    values = {f"e{i}": (12 if i % 2 else -2) for i in range(30)}
    first = tournament.checkpoint(db, "one", "hash", "prospective", values, now=at)
    assert first["alpha"] < .025
    modified = {name: 100 for name in values}
    assert tournament.checkpoint(db, "one", "hash", "prospective", modified, now=at) == first
    tournament.register_protocol(db, "two", "hash2", at)
    second = tournament.checkpoint(db, "two", "hash2", "prospective", values, now=at)
    assert second["alpha"] < first["alpha"]
    assert second["adjusted_lower_bound_cents"] < first["adjusted_lower_bound_cents"]
    db.execute("UPDATE strategy_tournament_checkpoints SET snapshot_json='{}'")
    try:
        tournament.checkpoint(db, "one", "hash", "prospective", values, now=at)
        assert False, "changed checkpoint accepted"
    except ValueError as error:
        assert str(error) == "tournament_checkpoint_changed"
