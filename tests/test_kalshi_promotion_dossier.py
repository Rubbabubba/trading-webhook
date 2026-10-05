from datetime import datetime, timezone, timedelta
import hashlib
import json
import sqlite3
import pytest

from opportunity_lab.kalshi_promotion_dossier import build, _fee_audit
from opportunity_lab.kalshi_factory_fee_probe import fee_basis


def test_missing_proof_never_exports_promotion(tmp_path):
    assert build(tmp_path, {})["blockers"] == ["no_shadow_passed_version"]
    result = build(tmp_path, {"factory_demo_trial": {"protocol": {"strategy_id": "missing"}}})
    assert not result["ready"] and result["evidence"] is None


def test_fee_audit_requires_contemporaneous_unchanged_evidence():
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE strategy_factory_events(strategy_id,observation_id);"
                     "CREATE TABLE calibration_parent_observations(observation_id,detail,resolution);"
                     "CREATE TABLE factory_fee_observations(strategy_id,observation_id,detail,sha256);")
    at = datetime.now(timezone.utc)
    row = {"event_id": "EVENT", "observed_at": at.isoformat(), "price_cents": 8, "market_type": "binary", "exchange_index": 0}
    outcome = {"hypothetical_only": True, "payout_cents": 100, "cost_stressed_net_cents": 90}
    db.execute("INSERT INTO strategy_factory_events VALUES('candidate','observation')")
    db.execute("INSERT INTO calibration_parent_observations VALUES(?,?,?)", ("observation", json.dumps(row), json.dumps(outcome)))
    with pytest.raises(ValueError, match="fee_evidence_missing"):
        _fee_audit(db, "candidate", "strategy_factory_events")
    event = {"event_ticker": "EVENT", "series_ticker": "SERIES"}
    series = {"ticker": "SERIES", "fee_type": "quadratic", "fee_multiplier": "1"}
    basis = fee_basis(row, event, series, fetched_at=at.timestamp())
    raw = json.dumps(basis, sort_keys=True, separators=(",", ":"))
    db.execute("INSERT INTO factory_fee_observations VALUES(?,?,?,?)", ("candidate", "observation", raw, hashlib.sha256(raw.encode()).hexdigest()))
    _fee_audit(db, "candidate", "strategy_factory_events")
    basis["fee_multiplier"] = "10"; basis["modeled_entry_cost_cents"] = 14
    raw = json.dumps(basis, sort_keys=True, separators=(",", ":"))
    db.execute("UPDATE factory_fee_observations SET detail=?", (raw,))
    with pytest.raises(ValueError, match="fee_evidence_missing"):
        _fee_audit(db, "candidate", "strategy_factory_events")
    db.execute("UPDATE factory_fee_observations SET sha256=?", (hashlib.sha256(raw.encode()).hexdigest(),))
    with pytest.raises(ValueError, match="new_protocol_required"):
        _fee_audit(db, "candidate", "strategy_factory_events")
    db.close()


def test_complete_frozen_shadow_and_reconciled_demo_export(tmp_path):
    from opportunity_lab.kalshi_strategy_factory import init, evaluate, fingerprint
    from opportunity_lab.kalshi_strategy_tournament import register_protocol
    from opportunity_lab.kalshi_factory_fee_probe import init as init_fees
    from opportunity_lab.kalshi_demo_v5_maker_worker import MakerState, submit
    from opportunity_lab.kalshi_factory_demo_trial import trial_allowed
    from opportunity_lab.kalshi_binary_journal import BinaryJournal
    from opportunity_lab.kalshi_binary_broker import BinaryDemoBroker
    from test_kalshi_binary_journal import Exchange, quote
    now = datetime.now(timezone.utc); registered = now - timedelta(days=40)
    spec = {"primitive": "buy_at_observed_ask_to_settlement", "stratum": "non_sports", "price_bin": "5-10",
            "side": "yes", "family": "*", "evaluation_protocol": "tournament_v1",
            "extra_fee_stress_cents": 3, "execution_enabled": False}
    strategy = "candidate-dossier"; digest = fingerprint(spec)
    db = sqlite3.connect(tmp_path / "research_sleeves.sqlite3")
    db.execute("CREATE TABLE calibration_parent_observations(observation_id TEXT PRIMARY KEY,event_id TEXT,decision_bucket INTEGER,observed_at TEXT,detail TEXT,resolution TEXT)")
    init(db); init_fees(db)
    db.execute("INSERT INTO strategy_factory_candidates VALUES(?,?,?,?,?,?)", (strategy, digest, json.dumps(spec), registered.isoformat(), "demo_trial_candidate", "prospective_shadow_gate_passed"))
    register_protocol(db, strategy, digest, registered)
    holdout_at = registered + timedelta(days=20)
    db.execute("INSERT INTO strategy_factory_holdouts VALUES(?,?,?,?)", (strategy, holdout_at.isoformat(), "collecting", "future_holdout_started"))
    for split, count, start in (("prospective", 30, registered), ("holdout", 20, holdout_at)):
        table = "strategy_factory_events" if split == "prospective" else "strategy_factory_holdout_events"
        for index in range(count):
            name = f"{split}:{index}"; at = start + timedelta(minutes=index + 1)
            row = {**spec, "event_id": name, "observed_at": at.isoformat(), "price_cents": 8, "market_type": "binary", "exchange_index": 0, "fill_assumed": False}
            outcome = {"hypothetical_only": True, "payout_cents": 100, "cost_stressed_net_cents": 90}
            db.execute("INSERT INTO calibration_parent_observations VALUES(?,?,?,?,?,?)", (name, name, 0, at.isoformat(), json.dumps(row), json.dumps(outcome)))
            db.execute(f"INSERT INTO {table} VALUES(?,?,?,?)", (strategy, name, name, at.isoformat()))
            basis = fee_basis(row, {"event_ticker": name, "series_ticker": "SERIES"}, {"ticker": "SERIES", "fee_type": "quadratic", "fee_multiplier": "1"}, fetched_at=at.timestamp())
            raw = json.dumps(basis, sort_keys=True, separators=(",", ":"))
            db.execute("INSERT INTO factory_fee_observations VALUES(?,?,?,?,?)", (strategy, name, name, raw, hashlib.sha256(raw.encode()).hexdigest()))
    evaluate(db, strategy, now=now); db.commit()
    state = MakerState(tmp_path / "worker.sqlite3"); j = BinaryJournal(tmp_path / "journal.sqlite3")
    x = Exchange(j); b = BinaryDemoBroker(j, x)
    class Markets:
        def quote(self, payload): return quote(j, payload["client_order_id"])
    candidate = {"strategy_id": strategy, "spec_hash": digest}
    assert trial_allowed(state, j, candidate, flat_balance_cents=50000)[0]
    result = submit(state, j, b, Markets(), {"ticker": "T", "event_ticker": "LIVE-DEMO-EVENT"}, "yes", "buy", 8,
                    client_id_prefix="factory-demo-", entry_kind="factory_trial_entry", strategy_id=strategy)
    x.positions = {}
    x.settlements = [dict(ticker="T", market_result="yes", revenue=100, yes_count_fp="1", no_count_fp="0",
                         yes_total_cost_dollars=".08", no_total_cost_dollars="0", fee_cost=".01", value=100,
                         exchange_index=0, settled_time=datetime.now(timezone.utc).isoformat())]
    b.reconcile_settlements(); b.reconcile_positions()
    packet = {"factory_demo_trial": {"protocol": state.load("factory_trial_protocol"),
              "restart_reconciliation": {"environment": "demo", "positions_verified": True, "reconciled_at": now.isoformat()}}}
    state.close(); j.close()
    exported = build(tmp_path, packet)
    assert exported["ready"], exported["blockers"]
    evidence = exported["evidence"]
    assert evidence["demo"]["fills"] == 1 and evidence["demo"]["net_after_fees_cents"] == 91
    assert len(evidence["prospective_events"]) == 30 and len(evidence["holdout_events"]) == 20
    assert evidence["strategy_spec"] == spec
    db.execute("UPDATE strategy_tournament_checkpoints SET snapshot_json='{}'"); db.commit()
    assert not build(tmp_path, packet)["ready"]
    db.close()
