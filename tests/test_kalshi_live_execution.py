from copy import deepcopy
from datetime import datetime, timezone, timedelta
import hashlib
import pytest

from opportunity_lab.kalshi_live_execution import LiveBinaryJournal, LiveBinaryBroker, LiveClient, validate_grant
from opportunity_lab.kalshi_strategy_factory import fingerprint
from test_kalshi_binary_journal import Exchange, quote


def grant():
    spec = {"primitive": "buy_at_observed_ask_to_settlement", "stratum": "non_sports",
            "price_bin": "5-10", "extra_fee_stress_cents": 3, "execution_enabled": False}
    return {"schema": "kalshi_live_grant_v1", "approval_id": "owner-decision", "strategy_id": "candidate-test",
            "spec": spec, "spec_hash": fingerprint(spec), "evidence_sha256": "b" * 64,
            "account_key_sha256": hashlib.sha256(b"live-test-key").hexdigest(), "limit_revision": 1,
            "limits": {"capital_cents": 50000, "daily_loss_cents": 500, "total_loss_cents": 2500,
                       "event_exposure_cents": 500, "portfolio_exposure_cents": 2500,
                       "order_contracts": 1, "open_orders": 1, "quote_age_seconds": 5, "spread_cents": 3}}


def snapshot(j, positions=None, cash=50000):
    at = j.clock().timestamp()
    return {"environment": "production", "started_at": at, "observed_at": at,
            "account_key_sha256": grant()["account_key_sha256"], "balance": {"balance": cash},
            "positions": positions or [], "resting_orders": []}


class LiveExchange(Exchange, LiveClient):
    def __init__(self, journal):
        Exchange.__init__(self, journal)
        self.key_id = "live-test-key"


def live_quote(j, cid):
    q = quote(j, cid); q["environment"] = "production"
    q["market"] = {"ticker": "T", "event_ticker": "EVENT", "market_type": "binary", "status": "active"}
    q["market"]["close_time"] = (j.clock() + timedelta(hours=1)).isoformat()
    if j.get(cid)["intent"]["outcome"] == "yes":
        q["orderbook_fp"] = {"yes_dollars": [["0.06", "1"]], "no_dollars": [["0.92", "1"]]}
    else:
        q["orderbook_fp"] = {"yes_dollars": [["0.92", "1"]], "no_dollars": [["0.06", "1"]]}
    return q


def test_production_cannot_reuse_demo_or_unverified_grant(tmp_path):
    with pytest.raises(ValueError, match="verified_owner"):
        LiveBinaryJournal(tmp_path / "live.db", grant(), approval_verifier=None)
    j = LiveBinaryJournal(tmp_path / "live.db", grant(), approval_verifier=lambda digest: True)
    with pytest.raises(ValueError, match="production_journal"):
        j.bind_environment("demo")
    changed = grant(); changed["limits"]["total_loss_cents"] = 3000
    j.close()
    with pytest.raises(ValueError, match="live_grant_changed|binary_policy_changed"):
        LiveBinaryJournal(tmp_path / "live.db", changed, approval_verifier=lambda digest: True)


@pytest.mark.parametrize("outcome", ["yes", "no"])
def test_live_fill_settlement_and_restart(outcome, tmp_path):
    path = tmp_path / "live.db"
    j = LiveBinaryJournal(path, grant(), approval_verifier=lambda digest: True)
    x = LiveExchange(j); b = LiveBinaryBroker(j, x)
    j.reserve("one", "T", 1, 8, 1, event_id="EVENT", outcome=outcome, action="buy", account_snapshot=b.snapshot())
    assert b.submit("one", quote_snapshot=live_quote(j, "one"))["state"] == "terminal"
    assert b.reconcile_positions()["positions_match"]
    x.positions = {}
    x.settlements = [dict(ticker="T", market_result=outcome, revenue=100,
                         yes_count_fp="1" if outcome == "yes" else "0",
                         no_count_fp="1" if outcome == "no" else "0",
                         yes_total_cost_dollars=".08" if outcome == "yes" else "0",
                         no_total_cost_dollars=".08" if outcome == "no" else "0",
                         fee_cost=".01", value=100 if outcome == "yes" else 0,
                         exchange_index=0, settled_time=datetime.now(timezone.utc).isoformat())]
    j.close(); j = LiveBinaryJournal(path, grant(), approval_verifier=lambda digest: True)
    x.j = j; b = LiveBinaryBroker(j, x)
    assert b.recover()["positions_match"]
    assert not j.accounting()["positions"]
    assert float(j.accounting()["realized"]) == .91
    assert x.posts == 1
    with pytest.raises(ValueError, match="event_already"):
        j.reserve("two", "OTHER", 1, 8, 1, event_id="EVENT", outcome=outcome, action="buy", account_snapshot=b.snapshot())
    j.close()


@pytest.mark.parametrize("failure", ["stale", "scope", "revoked", "spread", "changed_quote"])
def test_live_submission_rechecks_limits_before_network(failure, tmp_path):
    j = LiveBinaryJournal(tmp_path / "live.db", grant(), approval_verifier=lambda digest: True)
    x = LiveExchange(j); b = LiveBinaryBroker(j, x)
    j.reserve("one", "T", 1, 8, 1, event_id="EVENT", outcome="yes", action="buy", account_snapshot=b.snapshot())
    q = live_quote(j, "one")
    if failure == "stale": q["observed_at"] -= 10
    if failure == "scope": q["market"]["event_ticker"] = "OTHER"
    if failure == "revoked": j.approval_verifier = lambda digest: False
    if failure == "spread": q["orderbook_fp"]["yes_dollars"] = [[".01", "1"]]
    if failure == "changed_quote": q["orderbook_fp"]["no_dollars"] = [[".93", "1"]]
    with pytest.raises(ValueError): b.submit("one", quote_snapshot=q)
    assert x.posts == 0 and j.get("one")["state"] == "reserved"
    j.close()


def test_live_recovery_cancels_resting_order_after_revocation(tmp_path):
    j = LiveBinaryJournal(tmp_path / "live.db", grant(), approval_verifier=lambda digest: True)
    x = LiveExchange(j); x.resting_only = True; b = LiveBinaryBroker(j, x)
    j.reserve("one", "T", 1, 8, 1, event_id="EVENT", outcome="yes", action="buy", account_snapshot=b.snapshot())
    assert b.submit("one", quote_snapshot=live_quote(j, "one"))["state"] == "working"
    j.approval_verifier = lambda digest: False
    assert b.recover()["positions_match"]
    assert j.get("one")["state"] == "terminal" and x.posts == 1
    j.close()


def test_live_timeout_restart_never_resubmits_and_checks_scope(tmp_path):
    path = tmp_path / "live.db"; j = LiveBinaryJournal(path, grant(), approval_verifier=lambda digest: True)
    x = LiveExchange(j); x.timeout = True; b = LiveBinaryBroker(j, x)
    j.reserve("one", "T", 1, 8, 1, event_id="EVENT", outcome="yes", action="buy", account_snapshot=snapshot(j))
    with pytest.raises(TimeoutError):
        b.submit("one", quote_snapshot=live_quote(j, "one"))
    assert j.get("one")["state"] == "uncertain"
    j.close(); j = LiveBinaryJournal(path, grant(), approval_verifier=lambda digest: True)
    x.j = j; b = LiveBinaryBroker(j, x)
    with pytest.raises(ValueError, match="submission_not_allowed"):
        b.submit("one", quote_snapshot=live_quote(j, "one"))
    assert x.posts == 1
    with pytest.raises(ValueError):
        j.reserve("two", "OTHER", 1, 8, 1, event_id="EVENT2", outcome="yes", action="buy", account_snapshot=snapshot(j))
    j.close()


def test_funding_external_positions_loss_budget_and_revocation(tmp_path):
    j = LiveBinaryJournal(tmp_path / "live.db", grant(), approval_verifier=lambda digest: True)
    with pytest.raises(ValueError, match="funding"):
        j.reserve("one", "T", 1, 8, 1, event_id="EVENT", outcome="yes", action="buy", account_snapshot=snapshot(j, cash=4500))
    with pytest.raises(ValueError, match="external_or_unreconciled"):
        j.reserve("one", "T", 1, 8, 1, event_id="EVENT", outcome="yes", action="buy",
                  account_snapshot=snapshot(j, positions=[{"ticker": "MANUAL", "position_fp": "1"}]))
    with pytest.raises(ValueError, match="live_loss_budget"):
        j.reserve("one", "T", 1, 8, 500, event_id="EVENT", outcome="yes", action="buy", account_snapshot=snapshot(j))
    j.approval_verifier = lambda digest: False
    with pytest.raises(ValueError, match="approval_revoked"):
        j.reserve("one", "T", 1, 8, 1, event_id="EVENT", outcome="yes", action="buy", account_snapshot=snapshot(j))
    j.close()
