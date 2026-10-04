from datetime import datetime, timedelta, timezone
import json
import sqlite3

from opportunity_lab.kalshi_binary_journal import BinaryJournal
from opportunity_lab.kalshi_demo_v5_maker_worker import MakerState, register_fill, submit
from opportunity_lab.kalshi_factory_demo_trial import (
    eligible_candidate, filled_fees_reconciled, recent_signal, trial_allowed, trial_counts,
)
from opportunity_lab.kalshi_strategy_factory import fingerprint, specs


def _research_db(path):
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE calibration_parent_observations("
               "observation_id TEXT,event_id TEXT,observed_at TEXT,detail TEXT,resolution TEXT)")
    db.execute("CREATE TABLE strategy_factory_candidates("
               "strategy_id TEXT,spec_hash TEXT,spec_json TEXT,registered_at TEXT,state TEXT,reason TEXT)")
    db.execute("CREATE TABLE strategy_factory_events("
               "strategy_id TEXT,observation_id TEXT,event_id TEXT,observed_at TEXT)")
    db.execute("CREATE TABLE strategy_factory_holdouts("
               "strategy_id TEXT,started_at TEXT,state TEXT,reason TEXT)")
    db.execute("CREATE TABLE strategy_factory_holdout_events("
               "strategy_id TEXT,observation_id TEXT,event_id TEXT,observed_at TEXT)")
    return db


def test_demo_trial_fails_closed_without_reproduced_shadow_gate(tmp_path):
    db = _research_db(tmp_path / "research_sleeves.sqlite3")
    try:
        spec = next(row for row in specs() if row["stratum"] == "non_sports" and row["price_bin"] == "5-10")
        db.execute("INSERT INTO strategy_factory_candidates VALUES(?,?,?,?,?,?)",
                   ("candidate-one", fingerprint(spec), json.dumps(spec),
                    (datetime.now(timezone.utc) - timedelta(days=20)).isoformat(),
                    "shadow", "prospective_registration"))
        db.commit()
        assert eligible_candidate(tmp_path) is None
        db.execute("UPDATE strategy_factory_candidates SET state='demo_trial_candidate',"
                   "reason='prospective_shadow_gate_passed'")
        db.commit()
        try:
            eligible_candidate(tmp_path)
        except ValueError as error:
            assert str(error) == "factory_shadow_gate_not_reproducible"
        else:
            assert False, "A forged pass must not enable a Demo order"
    finally:
        db.close()


def test_filled_trial_fee_attestation_requires_broker_detail():
    fills = [("filled-one", "EVT-1", 0)]
    assert not filled_fees_reconciled(fills, {})
    assert not filled_fees_reconciled(fills, {"filled-one": {"fees_dollars": "0.00"}})
    assert filled_fees_reconciled(fills, {"filled-one": {
        "fees_dollars": "0.00", "fills": [{"fill_id": "f1"}],
    }})


def test_demo_trial_binds_one_version_and_loss_stop(tmp_path):
    state = MakerState(tmp_path / "worker.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    candidate = {"strategy_id": "candidate-one", "spec_hash": "a" * 64}
    try:
        assert trial_allowed(state, journal, None, flat_balance_cents=1000) == (False, "no_shadow_pass")
        assert trial_allowed(state, journal, candidate, flat_balance_cents=1000)[0]
        assert state.load("factory_trial_protocol")["strategy_id"] == "candidate-one"
        assert trial_allowed(state, journal, {**candidate, "strategy_id": "another-one"},
                             flat_balance_cents=1000)[0]
        assert state.load("factory_trial_protocol")["strategy_id"] == "another-one"
        assert trial_allowed(state, journal, candidate, flat_balance_cents=None) == (
            False, "balance_unavailable")
    finally:
        journal.close(); state.close()


def test_trial_attempts_are_attributed_by_version_and_day(tmp_path):
    state = MakerState(tmp_path / "worker.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    candidate = {"strategy_id": "candidate-one", "spec_hash": "a" * 64}
    try:
        assert trial_allowed(state, journal, candidate, flat_balance_cents=1000)[0]
        at = datetime.now(timezone.utc).timestamp()
        for index in range(3):
            cid = f"attempt-{index}"
            state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                             (cid, "factory_trial_entry", f"EVT-{index}", f"T-{index}", "yes", at))
            state.db.execute("INSERT INTO factory_trial_assignments VALUES(?,?)",
                             (cid, "candidate-one"))
        assert trial_counts(state, journal, strategy_id="candidate-one")["attempts_today"] == 3
        assert trial_allowed(state, journal, candidate, flat_balance_cents=1000) == (
            False, "daily_attempt_cap")
        assert trial_counts(state, journal, strategy_id="candidate-two")["attempts"] == 0
    finally:
        journal.close(); state.close()


def test_trial_requotes_and_rejects_changed_ask_bucket(tmp_path):
    db = _research_db(tmp_path / "research_sleeves.sqlite3")
    state = MakerState(tmp_path / "worker.sqlite3")
    now = datetime.now(timezone.utc)
    spec = next(row for row in specs() if row["stratum"] == "non_sports" and row["price_bin"] == "5-10")
    candidate = {"strategy_id": "candidate-one", "spec": spec,
                 "registered_at": (now - timedelta(days=1)).isoformat(),
                 "holdout_started_at": (now - timedelta(minutes=1)).isoformat()}
    row = {"event_id": "TEST-1", "ticker": "TEST-1-MKT", "side": "yes",
           "stratum": "non_sports", "price_bin": "5-10"}
    db.execute("INSERT INTO calibration_parent_observations VALUES(?,?,?,?,?)",
               ("obs-1", "TEST-1", now.isoformat(), json.dumps(row), None))
    db.commit()

    class Markets:
        ask = "0.93"
        def quote(self, payload):
            return {"ticker": payload["ticker"], "observed_at": now.timestamp(),
                    "orderbook_fp": {"yes_dollars": [["0.05", "5"]],
                                     "no_dollars": [[self.ask, "5"]]},
                    "market": {"ticker": payload["ticker"], "event_ticker": "TEST-1",
                               "expiration_time": (now + timedelta(hours=1)).isoformat()}}
    markets = Markets()
    try:
        signal = recent_signal(tmp_path, candidate, state, markets, now=now.timestamp())
        assert signal["outcome"] == "yes" and signal["price_cents"] == 8
        assert recent_signal(tmp_path, candidate, state, markets, now=now.timestamp(),
                             max_total_risk_cents=12) is None
        markets.ask = "0.85"  # 15-cent ask is outside the registered bucket.
        assert recent_signal(tmp_path, candidate, state, markets, now=now.timestamp()) is None
    finally:
        state.close(); db.close()


def test_factory_demo_entry_is_versioned_and_uses_ioc(tmp_path):
    state = MakerState(tmp_path / "worker.sqlite3")
    class Journal:
        order_mode = None
        def reserve(self, *args, **kwargs):
            self.order_mode = kwargs["order_mode"]
    class Broker:
        def snapshot(self):
            return {"environment": "demo"}
        def submit(self, client_id, *, quote_provider):
            assert callable(quote_provider)
            return {"state": "terminal", "filled": 0}
    class Markets:
        def quote(self, payload):
            return {}
    journal = Journal()
    try:
        result = submit(state, journal, Broker(), Markets(),
                        {"ticker": "TEST-MKT", "event_ticker": "TEST-EVENT"},
                        "yes", "buy", 8, entry_kind="factory_trial_entry",
                        strategy_id="candidate-one")
        assert result["filled"] == 0 and journal.order_mode == "ioc"
        assert state.db.execute("SELECT kind FROM intent_meta").fetchone()[0] == "factory_trial_entry"
        assert state.db.execute("SELECT strategy_id FROM factory_trial_assignments").fetchone()[0] == "candidate-one"
        client_id = state.db.execute("SELECT client_id FROM factory_trial_assignments").fetchone()[0]
        register_fill(state, {"filled": 1, "payload": {"client_order_id": client_id}}, {})
        assert state.db.execute("SELECT count(*) FROM entered_events").fetchone()[0] == 1
        assert state.db.execute("SELECT count(*) FROM maker_fills").fetchone()[0] == 0
    finally:
        state.close()
