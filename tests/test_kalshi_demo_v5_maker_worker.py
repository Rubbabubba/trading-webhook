from datetime import datetime, timedelta, timezone
from fractions import Fraction

from opportunity_lab.kalshi_binary_journal import BinaryJournal
from opportunity_lab.kalshi_demo_v5_maker_worker import (
    LEGACY_UNCERTAINTY_STOP_RECOVERY,
    FRESH_FLAT_RECOVERY,
    MAKER_SERIES,
    MakerState,
    current_position,
    evidence,
    observe_working_quote,
    preferred_outcome,
    quarantine_stale_unresolved,
    recover_or_report,
    refresh_cohort,
    recover,
    release_legacy_uncertainty_stop,
    safe_cycle_error,
    select_maker_markets,
    QUOTE_TTL_SECONDS,
)


def snapshot(cash=50000):
    now = datetime.now(timezone.utc).timestamp()
    return {"environment": "demo", "started_at": now, "observed_at": now,
            "balance": {"balance": cash}, "positions": [], "resting_orders": []}


def test_empty_maker_evidence_is_demo_only_and_flat(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        result = evidence(state, journal)
        assert result["environment"] == "demo" and result["post_only"] is True
        assert result["post_only_attempts"] == result["maker_fills"] == 0
        assert result["working_quote_records"] == 0
        assert result["ending_position_contracts"] == 0
        state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                         ("orphan", "maker_entry", "E", "T", "yes", 1.0))
        result = evidence(state, journal)
        assert result["markets"] == 0
        assert result["side_attempts"] == {"yes": 0, "no": 0}
        assert state.load("protocol")["quote_ttl_seconds"] == QUOTE_TTL_SECONDS == 900
    finally:
        journal.close(); state.close()


def working_frame(yes_bid, yes_ask, bid_depth=10, ask_depth=10, *, at=100.0):
    yes_bid = Fraction(yes_bid)
    yes_ask = Fraction(yes_ask)
    return {
        "received_at": at,
        "orderbook_fp": {
            "yes_dollars": [[str(float(yes_bid)), str(bid_depth)]],
            "no_dollars": [[str(float(1 - yes_ask)), str(ask_depth)]],
        },
    }


def working_record(client_id="m1"):
    return {"payload": {"client_order_id": client_id}}


def seed_working_quote(state, *, outcome="yes", initial_mid="1/2"):
    state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                     ("m1", "maker_entry", "EVENT", "TEST", outcome, 1.0))
    state.db.execute("INSERT INTO flow_context VALUES(?,?)",
                     ("m1", __import__("json").dumps({"yes_mid": initial_mid})))


def test_working_quote_requires_sustained_two_cent_adverse_move(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    try:
        seed_working_quote(state)
        frame = working_frame(".45", ".51")  # midpoint .48: two cents adverse for YES
        assert observe_working_quote(state, working_record(), frame) is None
        assert observe_working_quote(state, working_record(), frame) is None
        assert observe_working_quote(state, working_record(), frame) == \
            "sustained_adverse_midpoint"
    finally:
        state.close()


def test_working_quote_cancels_immediate_three_cent_move_for_no(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    try:
        seed_working_quote(state, outcome="no")
        frame = working_frame(".52", ".54")  # YES up three cents is adverse for NO
        assert observe_working_quote(state, working_record(), frame) == \
            "immediate_adverse_midpoint"
    finally:
        state.close()


def test_working_quote_requires_sustained_against_side_depth(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    try:
        seed_working_quote(state)
        frame = working_frame(".48", ".52", bid_depth=2, ask_depth=7)
        assert observe_working_quote(state, working_record(), frame) is None
        assert observe_working_quote(state, working_record(), frame) is None
        assert observe_working_quote(state, working_record(), frame) == \
            "sustained_against_side_depth"
    finally:
        state.close()


def test_preferred_outcome_balances_new_tickers_and_never_flips_existing(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        assert preferred_outcome(state, journal, "A") == "yes"
        state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                         ("a1", "maker_entry", "EA", "A", "yes", 1.0))
        journal.reserve("a1", "A", 1, 40, 5, outcome="yes", action="buy",
                        account_snapshot=snapshot(), order_mode="post_only_gtc")
        assert preferred_outcome(state, journal, "B") == "no"
        state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                         ("b1", "maker_entry", "EB", "B", "no", 2.0))
        journal.reserve("b1", "B", 1, 40, 5, outcome="no", action="buy",
                        account_snapshot=snapshot(), order_mode="post_only_gtc")
        assert preferred_outcome(state, journal, "A") == "yes"
        assert preferred_outcome(state, journal, "B") == "no"
        assert preferred_outcome(state, journal, "C") == "yes"
    finally:
        journal.close(); state.close()


def test_current_position_requires_matching_maker_metadata(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        journal.reserve("m1", "TEST", 1, 40, 5, outcome="yes", action="buy",
                        account_snapshot=snapshot(), order_mode="post_only_gtc")
        state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                         ("m1", "maker_entry", "EVENT", "TEST", "yes", 100.0))
        position, accounting = current_position(journal, state)
        assert position is None and accounting["positions"] == {}
    finally:
        journal.close(); state.close()


class FakeMarkets:
    def __init__(self, rows):
        self.rows = rows
        self.series = []

    def get(self, *, params):
        self.series.append(params["series_ticker"])
        return {"markets": self.rows.get(params["series_ticker"], [])}, 1.0, 1.1


def candidate(ticker, event, *, bid=".40", ask=".44", bid_size="20", ask_size="18", volume="1"):
    return {
        "ticker": ticker, "event_ticker": event, "status": "active",
        "market_type": "binary", "exchange_index": 0,
        "close_time": "2030-01-01T00:00:00Z", "yes_bid_dollars": bid,
        "yes_ask_dollars": ask, "yes_bid_size_fp": bid_size,
        "yes_ask_size_fp": ask_size, "volume_24h_fp": volume,
    }


def test_maker_selector_queries_each_series_and_enforces_event_diversity():
    rows = {
        "KXMLBGAME": [candidate("MLB-A", "GAME-1", volume="10"),
                       candidate("MLB-B", "GAME-1", volume="9")],
        "KXNFLGAME": [candidate("NFL-A", "GAME-2", volume="8")],
        "KXNCAAFGAME": [candidate("NCAAF-WIDE", "GAME-3", bid=".30", ask=".50")],
        "KXEPLGAME": [candidate("EPL-THIN", "GAME-4", bid_size="1")],
        "KXFEDDECISION": [candidate("FED-A", "FED-1", volume="7")],
    }
    markets = FakeMarkets(rows)
    result = select_maker_markets(markets, now=1_700_000_000)
    assert markets.series == list(MAKER_SERIES)
    assert [row["ticker"] for row in result] == ["MLB-A", "NFL-A", "FED-A"]
    assert len({row["event_ticker"] for row in result}) == len(result)


class FakeState:
    def __init__(self):
        self.saved = {}
        self.actions = []

    def save(self, name, value):
        self.saved[name] = value

    def record(self, ticker, detail):
        self.actions.append((ticker, detail))


def test_failed_discovery_refresh_keeps_existing_cohort_and_backs_off():
    state = FakeState()
    markets = FakeMarkets({})
    existing = [candidate("MLB-A", "GAME-1")]
    cohort, cohort_at = refresh_cohort(
        state, markets, existing, 100.0, now=2000.0
    )
    assert cohort == existing
    assert cohort_at == 500.0
    assert state.saved["cohort_at"] == 500.0
    assert state.actions[-1][1]["action"] == "cohort_refresh_deferred"


def test_safe_cycle_error_only_exposes_bounded_internal_codes():
    assert safe_cycle_error(ValueError("fills_not_reconciled")) == "fills_not_reconciled"
    assert safe_cycle_error(ValueError("secret path C:/keys/private.pem")) == "ValueError"
    assert safe_cycle_error(RuntimeError("secret")) == "RuntimeError"


class UnresolvedBroker:
    def __init__(self):
        self.refreshes = 0

    def refresh(self, _client_id):
        self.refreshes += 1
        raise ValueError("submission_unresolved")


def uncertain_maker(state, journal):
    journal.reserve("m1", "TEST", 1, 40, 5, outcome="yes", action="buy",
                    account_snapshot=snapshot(), order_mode="post_only_gtc")
    now = datetime.now(timezone.utc).timestamp()
    quote = {"environment": "demo", "ticker": "TEST", "started_at": now,
             "observed_at": now,
             "orderbook_fp": {"yes_dollars": [[".39", "10"]],
                              "no_dollars": [[".50", "10"]]}}
    journal.mark_submission_started("m1", account_snapshot=snapshot(),
                                    quote_snapshot=quote)
    state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                     ("m1", "maker_entry", "EVENT", "TEST", "yes", 100.0))


def test_unresolved_startup_stays_healthy_read_only_and_writes_status(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        uncertain_maker(state, journal)
        broker = UnresolvedBroker()
        assert recover_or_report(tmp_path, state, journal, broker) is False
        status = __import__("json").loads((tmp_path / "status.json").read_text())
        assert status["phase"] == "reconciling"
        assert status["new_submissions_enabled"] is False
        assert status["errors"] == ["submission_unresolved"]
        assert status["evidence"]["unresolved_orders"] == 1
        assert broker.refreshes == 1
        assert journal.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0] == 0
    finally:
        journal.close(); state.close()


def test_only_legacy_uncertainty_crash_stop_is_released(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        uncertain_maker(state, journal)
        journal.stop()
        state.record(None, {"action": "cycle_error", "error_code": "BrokerError"})
        assert release_legacy_uncertainty_stop(state, journal) is True
        assert journal.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0] == 0
        assert state.load(LEGACY_UNCERTAINTY_STOP_RECOVERY) is True
        assert release_legacy_uncertainty_stop(state, journal) is False
    finally:
        journal.close(); state.close()


class FlatRecoveryBroker:
    def __init__(self):
        self.reconciled = 0

    def reconcile_positions(self, *, allow_reserved=False):
        assert allow_reserved is True
        self.reconciled += 1
        return {"positions_match": True, "position_count": 0}


def test_fresh_flat_v7_ledger_recovers_cutover_stop_once(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        journal.stop(); broker = FlatRecoveryBroker()
        recover(state, journal, broker)
        assert broker.reconciled == 1
        assert journal.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0] == 0
        assert state.load(FRESH_FLAT_RECOVERY) is True
    finally:
        journal.close(); state.close()


class NegativeEvidenceClient:
    def __init__(self, *, resting=()):
        self.resting = list(resting)
        self.calls = []

    def pages(self, path, field, **params):
        self.calls.append((path, field, params))
        if path == "/portfolio/orders" and params.get("status") == "resting":
            return self.resting
        return []


class NegativeEvidenceBroker(UnresolvedBroker):
    def __init__(self, *, resting=()):
        super().__init__()
        self.client = NegativeEvidenceClient(resting=resting)


def age_submission(journal, seconds=13 * 60 * 60):
    import json
    import time
    detail = json.loads(journal.db.execute(
        "SELECT detail FROM submission_quotes WHERE client_id='m1'"
    ).fetchone()[0])
    detail["started_at"] = detail["observed_at"] = time.time() - seconds
    journal.db.execute(
        "UPDATE submission_quotes SET detail=? WHERE client_id='m1'",
        (json.dumps(detail, sort_keys=True),),
    )


def test_old_unresolved_zero_exposure_is_quarantined_with_audit_proof(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    clock = [datetime.now(timezone.utc) + timedelta(seconds=1)]
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100,
                            clock=lambda: clock[0])
    try:
        uncertain_maker(state, journal); age_submission(journal)
        broker = NegativeEvidenceBroker()
        assert quarantine_stale_unresolved(state, journal, broker, "m1") is False
        clock[0] += timedelta(seconds=61)
        assert quarantine_stale_unresolved(state, journal, broker, "m1") is True
        assert journal.records() == []
        row = journal.db.execute(
            "SELECT evidence FROM uncertain_quarantine WHERE id='m1'"
        ).fetchone()
        proof = __import__("json").loads(row[0])
        assert proof["all_positions"] == 0
        assert len(proof["negative_observations"]) == 2
        assert proof["negative_observations"][1]["observed_at"] \
            - proof["negative_observations"][0]["observed_at"] >= 60
        assert state.load("last_uncertain_quarantine")["client_order_id"] == "m1"
    finally:
        journal.close(); state.close()


def test_unresolved_with_resting_order_is_not_quarantined(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        uncertain_maker(state, journal); age_submission(journal)
        broker = NegativeEvidenceBroker(resting=[{"order_id": "other"}])
        assert quarantine_stale_unresolved(state, journal, broker, "m1") is False
        assert journal.get("m1")["state"] == "uncertain"
        assert journal.db.execute("SELECT count(*) FROM uncertain_quarantine").fetchone()[0] == 0
    finally:
        journal.close(); state.close()
