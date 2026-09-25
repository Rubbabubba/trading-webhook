from datetime import datetime, timedelta, timezone
from fractions import Fraction
import json

from opportunity_lab.kalshi_binary_journal import BinaryJournal
from opportunity_lab.kalshi_demo_v5_maker_worker import (
    EXECUTION_DISABLED_REASON,
    EXECUTION_ENABLED,
    EXECUTION_POLICY_ID,
    LEGACY_UNCERTAINTY_STOP_RECOVERY,
    FRESH_FLAT_RECOVERY,
    MakerState,
    advance_market_discovery,
    current_position,
    evidence,
    observe_v10_shadow,
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
    TERMINAL_FLAT_STOP_RECOVERY,
    v9_submission_allowed,
)


def snapshot(cash=50000):
    now = datetime.now(timezone.utc).timestamp()
    return {"environment": "demo", "started_at": now, "observed_at": now,
            "balance": {"balance": cash}, "positions": [], "resting_orders": []}


def test_v9_is_retired_from_new_demo_submissions(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    try:
        policy = state.load("execution_policy")
        assert EXECUTION_ENABLED is False
        assert policy == {
            "policy_id": EXECUTION_POLICY_ID,
            "strategy_id": "stable_balanced_maker_v9",
            "execution_enabled": False,
            "reason": EXECUTION_DISABLED_REASON,
            "replacement_candidate": "queue_toxicity_maker_v10_shadow",
            "replacement_execution_enabled": False,
        }
        assert v9_submission_allowed({"side": "yes"}, event_locked=False) is False
        assert v9_submission_allowed(None, event_locked=False) is False
        assert v9_submission_allowed({"side": "yes"}, event_locked=True) is False
    finally:
        state.close()


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
        assert state.load("protocol")["quote_ttl_seconds"] == QUOTE_TTL_SECONDS == 180
    finally:
        journal.close(); state.close()


def test_evidence_keeps_reconciled_fill_when_observation_was_interrupted(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        now = datetime.now(timezone.utc)
        journal.reserve("m1", "TEST", 1, 40, 5, outcome="yes", action="buy",
                        account_snapshot=snapshot(), order_mode="post_only_gtc")
        quote_snapshot = {
            "environment": "demo", "ticker": "TEST",
            "started_at": now.timestamp(), "observed_at": now.timestamp(),
            "orderbook_fp": {
                "yes_dollars": [[".39", "10"]],
                "no_dollars": [[".59", "10"]],
            },
        }
        journal.mark_submission_started(
            "m1", account_snapshot=snapshot(), quote_snapshot=quote_snapshot
        )
        fill = {
            "fill_id": "fill-1", "order_id": "broker-1", "ticker": "TEST",
            "outcome_side": "yes", "book_side": "bid", "subaccount_number": 0,
            "count_fp": "1", "yes_price_dollars": ".40", "fee_cost": "0",
            "created_time": now.isoformat(),
        }
        journal.reconcile(
            "m1", broker_id="broker-1", filled=1, remaining=0, terminal=True,
            evidence={"fills": [fill], "gross_dollars": ".40", "fees_dollars": "0"},
        )
        state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                         ("m1", "maker_entry", "EVENT", "TEST", "yes", now.timestamp()))
        state.db.execute("INSERT INTO flow_context VALUES(?,?)",
                         ("m1", '{"yes_mid":"79/200"}'))

        result = evidence(state, journal)

        assert result["maker_fills"] == 1
        assert result["ending_position_contracts"] == 1
        assert type(result["ending_position_contracts"]) is int
        __import__("json").dumps(result)
        assert result["actual_fee_records"] == 1
        assert result["flow_context_records"] == 1
        assert state.db.execute("SELECT count(*) FROM maker_fills").fetchone()[0] == 0
    finally:
        journal.close(); state.close()


def test_v10_shadow_signals_and_markouts_never_enable_execution(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        history = [(100, Fraction(".40")), (200, Fraction(".405")),
                   (300, Fraction(".41"))]
        signal_frame = working_frame(".40", ".46", 12, 6, at=400)
        signal = observe_v10_shadow(state, "TEST", history, signal_frame)
        assert signal["outcome"] == "yes"
        assert state.db.execute("SELECT count(*) FROM v10_shadow_signals").fetchone()[0] == 1

        observe_v10_shadow(
            state, "TEST", history,
            working_frame(".42", ".48", 12, 6, at=705),
        )
        result = evidence(state, journal)["v10_shadow"]
        assert result["execution_enabled"] is False
        assert result["signals"] >= 1
        assert result["evaluations"] == 2
        assert result["last_evaluation_at"] == 705
        assert result["rejection_reasons"] == {"insufficient_history": 1}
        assert result["complete_signals"] == 1
        assert result["independent_events"] == 1
        assert result["markout_records"] == {"5": 1, "30": 1, "300": 1}
        assert result["event_cluster_lcb_cents"] == {"5": None, "30": None, "300": None}
    finally:
        journal.close(); state.close()


def test_v10_shadow_records_rejection_reason(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        assert observe_v10_shadow(
            state, "TEST", [(100, Fraction(".40")), (200, Fraction(".405"))],
            working_frame(".40", ".46", 12, 6, at=400),
        ) is None
        result = evidence(state, journal)["v10_shadow"]
        assert result["evaluations"] == 1
        assert result["last_evaluation_at"] == 400
        assert result["rejection_reasons"] == {"insufficient_history": 1}
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


def test_working_quote_depth_imbalance_requires_adverse_midpoint_move(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    try:
        seed_working_quote(state)
        frame = working_frame(".48", ".52", bid_depth=2, ask_depth=7)
        assert observe_working_quote(state, working_record(), frame) is None
        assert observe_working_quote(state, working_record(), frame) is None
        assert observe_working_quote(state, working_record(), frame) is None
        adverse = working_frame(".47", ".51", bid_depth=2, ask_depth=7)
        assert observe_working_quote(state, working_record(), adverse) is None
        assert observe_working_quote(state, working_record(), adverse) is None
        assert observe_working_quote(state, working_record(), adverse) == \
            "sustained_depth_and_adverse_midpoint"
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
    def __init__(self, pages=(), *, error=None):
        self.pages = list(pages)
        self.error = error
        self.calls = []

    def get(self, *, params):
        self.calls.append(dict(params))
        if self.error:
            raise ValueError(self.error)
        index = int(params.get("cursor") or 0)
        cursor = str(index + 1) if index + 1 < len(self.pages) else ""
        return {"markets": self.pages[index], "cursor": cursor}, 1.0, 1.1


def candidate(ticker, event, *, bid=".40", ask=".44", bid_size="20", ask_size="18", volume="1"):
    return {
        "ticker": ticker, "event_ticker": event, "status": "active",
        "market_type": "binary", "exchange_index": 0,
        "close_time": "2030-01-01T00:00:00Z", "yes_bid_dollars": bid,
        "yes_ask_dollars": ask, "yes_bid_size_fp": bid_size,
        "yes_ask_size_fp": ask_size, "volume_24h_fp": volume,
    }


def test_maker_selector_accepts_all_categories_and_enforces_event_diversity():
    rows = [
        candidate("MLB-A", "GAME-1", volume="10"),
        candidate("MLB-B", "GAME-1", volume="9"),
        candidate("NFL-A", "GAME-2", volume="8"),
        candidate("POLITICS-WIDE", "EVENT-3", bid=".30", ask=".50"),
        candidate("WEATHER-THIN", "EVENT-4", bid_size="1"),
        candidate("FED-A", "FED-1", volume="7"),
    ]
    result = select_maker_markets(rows, now=1_700_000_000)
    assert [row["ticker"] for row in result] == ["MLB-A", "NFL-A", "FED-A"]
    assert len({row["event_ticker"] for row in result}) == len(result)


def test_market_discovery_paginates_full_universe_before_rotating_cohort(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    markets = FakeMarkets([
        [candidate("SPORT-A", "SPORT-1", volume="5")],
        [candidate("WEATHER-A", "WEATHER-1", volume="9"),
         candidate("ECON-A", "ECON-1", volume="7")],
    ])
    try:
        selected, scan = advance_market_discovery(
            state, markets, now=1_700_000_000, limit=2
        )
        assert selected is None and scan["in_progress"] is True
        selected, scan = advance_market_discovery(
            state, markets, now=1_700_000_001, limit=2
        )
        assert [row["ticker"] for row in selected] == ["WEATHER-A", "ECON-A"]
        assert scan["markets_scanned"] == 3
        assert scan["eligible_markets"] == scan["eligible_events"] == 3
        assert all("series_ticker" not in call for call in markets.calls)
        assert markets.calls[1]["cursor"] == "1"
    finally:
        state.close()


class FakeState:
    def __init__(self):
        self.saved = {}
        self.actions = []

    def save(self, name, value):
        self.saved[name] = value

    def load(self, name, default=None):
        return self.saved.get(name, default)

    def record(self, ticker, detail):
        self.actions.append((ticker, detail))


def test_failed_discovery_refresh_keeps_existing_cohort_and_backs_off():
    state = FakeState()
    markets = FakeMarkets(error="stale_demo_market_data")
    existing = [candidate("MLB-A", "GAME-1")]
    cohort, cohort_at = refresh_cohort(
        state, markets, existing, 100.0, now=2000.0
    )
    assert cohort == existing
    assert cohort_at == 100.0


def test_long_discovery_rotates_from_partial_eligible_universe(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    existing = [candidate("OLD-A", "OLD-1")]
    markets = FakeMarkets([
        [candidate("SPORT-A", "SPORT-1", volume="5")],
        [candidate("WEATHER-A", "WEATHER-1", volume="9")],
    ])
    try:
        cohort, cohort_at = refresh_cohort(
            state, markets, existing, 100.0, now=2000.0
        )
        assert [row["ticker"] for row in cohort] == ["SPORT-A"]
        assert cohort_at == 2000.0
        assert state.load("market_discovery")["in_progress"] is True
        action = state.db.execute(
            "SELECT detail FROM actions ORDER BY at DESC LIMIT 1"
        ).fetchone()
        assert json.loads(action[0])["action"] == (
            "partial_market_discovery_cohort_rotated"
        )
    finally:
        state.close()


def test_long_discovery_rotates_previous_universe_before_new_candidates(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    existing = [candidate("OLD-A", "OLD-1")]
    previous = candidate("WEATHER-A", "WEATHER-1", volume="9")
    state.db.execute(
        "INSERT INTO market_universe VALUES(?,?,?,?,?,?,?)",
        (previous["ticker"], previous["event_ticker"], 1,
         json.dumps(previous), "9", "18", ".04"),
    )
    state.save("market_discovery", {
        "in_progress": True, "generation": 2, "cursor": "1",
        "started_at": 100.0, "pages": 1, "markets_scanned": 1,
        "eligible_markets": 0,
    })
    markets = FakeMarkets([[], [], []])
    try:
        cohort, cohort_at = refresh_cohort(
            state, markets, existing, 100.0, now=2000.0
        )
        assert [row["ticker"] for row in cohort] == ["WEATHER-A"]
        assert cohort_at == 2000.0
        action = json.loads(state.db.execute(
            "SELECT detail FROM actions ORDER BY at DESC LIMIT 1"
        ).fetchone()[0])
        assert action["generation"] == 2
        assert action["source_generation"] == 1
    finally:
        state.close()


def test_long_discovery_expands_underfilled_cohort_before_rotation_ttl(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    existing = [candidate(f"OLD-{index}", f"OLD-{index}") for index in range(6)]
    for index in range(20):
        market = candidate(f"SPORT-{index}", f"EVENT-{index}", volume=str(100 - index))
        state.db.execute(
            "INSERT INTO market_universe VALUES(?,?,?,?,?,?,?)",
            (market["ticker"], market["event_ticker"], 2,
             json.dumps(market), str(100 - index), "18", ".04"),
        )
    state.save("market_discovery", {
        "in_progress": True, "generation": 2, "cursor": "1",
        "started_at": 100.0, "pages": 1, "markets_scanned": 200,
        "eligible_markets": 20,
    })
    markets = FakeMarkets([[], [], []])
    try:
        cohort, cohort_at = refresh_cohort(
            state, markets, existing, 1900.0, now=2000.0
        )
        assert len(cohort) == 16
        assert len({row["event_ticker"] for row in cohort}) == 16
        assert cohort_at == 2000.0
        action = json.loads(state.db.execute(
            "SELECT detail FROM actions ORDER BY at DESC LIMIT 1"
        ).fetchone()[0])
        assert action["action"] == "partial_market_discovery_cohort_rotated"
        assert action["selected_markets"] == 16
    finally:
        state.close()


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


def uncertain_maker(state, journal, client_id="m1"):
    journal.reserve(client_id, "TEST", 1, 40, 5, outcome="yes", action="buy",
                    account_snapshot=snapshot(), order_mode="post_only_gtc")
    now = datetime.now(timezone.utc).timestamp()
    quote = {"environment": "demo", "ticker": "TEST", "started_at": now,
             "observed_at": now,
             "orderbook_fp": {"yes_dollars": [[".39", "10"]],
                              "no_dollars": [[".50", "10"]]}}
    journal.mark_submission_started(client_id, account_snapshot=snapshot(),
                                    quote_snapshot=quote)
    state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                     (client_id, "maker_entry", "EVENT", "TEST", "yes", 100.0))


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

    def reconcile_settlements(self):
        return {"reconciled_settlements": 0}

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


def test_terminal_flat_ledger_recovers_reconciled_safety_stop(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        now = datetime.now(timezone.utc)
        journal.reserve("m1", "TEST", 1, 40, 5, outcome="yes", action="buy",
                        account_snapshot=snapshot(), order_mode="post_only_gtc")
        quote = {
            "environment": "demo", "ticker": "TEST",
            "started_at": now.timestamp(), "observed_at": now.timestamp(),
            "orderbook_fp": {
                "yes_dollars": [[".39", "10"]],
                "no_dollars": [[".59", "10"]],
            },
        }
        journal.mark_submission_started(
            "m1", account_snapshot=snapshot(), quote_snapshot=quote
        )
        fill = {
            "fill_id": "fill-1", "order_id": "broker-1", "ticker": "TEST",
            "outcome_side": "yes", "book_side": "bid", "subaccount_number": 0,
            "count_fp": "1", "yes_price_dollars": ".40", "fee_cost": "0",
            "created_time": (now - timedelta(seconds=2)).isoformat(),
        }
        journal.reconcile(
            "m1", broker_id="broker-1", filled=1, remaining=0, terminal=True,
            evidence={"fills": [fill], "gross_dollars": ".40", "fees_dollars": "0"},
        )
        journal.record_settlement({
            "ticker": "TEST", "market_result": "yes", "revenue": 100,
            "yes_count_fp": "1", "no_count_fp": "0",
            "yes_total_cost_dollars": ".40", "no_total_cost_dollars": "0",
            "fee_cost": "0", "value": 100, "exchange_index": 0,
            "settled_time": (now - timedelta(seconds=1)).isoformat(),
        })
        journal.stop()

        broker = FlatRecoveryBroker()
        recover(state, journal, broker)

        assert broker.reconciled == 1
        assert journal.db.execute(
            "SELECT stopped FROM controls WHERE id=1"
        ).fetchone()[0] == 0
        assert state.load(TERMINAL_FLAT_STOP_RECOVERY) is True
        assert state.db.execute(
            "SELECT count(*) FROM actions WHERE detail LIKE '%verified_terminal_flat%'"
        ).fetchone()[0] == 1
    finally:
        journal.close(); state.close()


class NegativeEvidenceClient:
    def __init__(self, *, resting=(), fills=()):
        self.resting = list(resting)
        self.fills = list(fills)
        self.calls = []

    def pages(self, path, field, **params):
        self.calls.append((path, field, params))
        if path == "/portfolio/orders" and params.get("status") == "resting":
            return self.resting
        if path == "/portfolio/fills":
            return self.fills
        return []


class NegativeEvidenceBroker(UnresolvedBroker):
    def __init__(self, *, resting=(), fills=()):
        super().__init__()
        self.client = NegativeEvidenceClient(resting=resting, fills=fills)


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


def test_unresolved_ignores_fills_from_a_known_journaled_order(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    clock = [datetime.now(timezone.utc) + timedelta(seconds=1)]
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100,
                            clock=lambda: clock[0])
    try:
        uncertain_maker(state, journal, client_id="known")
        journal.acknowledge("known", "known-order", 0)
        journal.reconcile(
            "known", broker_id="known-order", filled=0, remaining=0, terminal=True,
            evidence={"fills": [], "gross_dollars": "0", "fees_dollars": "0"},
        )
        uncertain_maker(state, journal); age_submission(journal)
        # The fill belongs to the earlier acknowledged order on this ticker.
        broker = NegativeEvidenceBroker(fills=[{"order_id": "known-order"}])
        assert quarantine_stale_unresolved(state, journal, broker, "m1") is False
        clock[0] += timedelta(seconds=61)
        assert quarantine_stale_unresolved(state, journal, broker, "m1") is True
    finally:
        journal.close(); state.close()


def test_unresolved_with_unattributed_fill_is_not_quarantined(tmp_path):
    state = MakerState(tmp_path / "state.sqlite3")
    journal = BinaryJournal(tmp_path / "journal.sqlite3", order_limit_cents=110,
                            capital_limit_cents=160, daily_loss_cents=100)
    try:
        uncertain_maker(state, journal); age_submission(journal)
        broker = NegativeEvidenceBroker(fills=[{"order_id": "unknown-order"}])
        assert quarantine_stale_unresolved(state, journal, broker, "m1") is False
        assert state.db.execute(
            "SELECT count(*) FROM uncertainty_checks WHERE client_id='m1'"
        ).fetchone()[0] == 0
    finally:
        journal.close(); state.close()
