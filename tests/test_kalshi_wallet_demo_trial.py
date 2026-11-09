from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from opportunity_lab.kalshi_binary_broker import BinaryDemoBroker
from opportunity_lab.kalshi_demo_broker import DemoClient
from opportunity_lab.kalshi_wallet_demo_trial import CHECKS, CLIENT_ID, evaluate, run_once


@pytest.fixture
def example():
    now = datetime(2026, 11, 9, 5, tzinfo=timezone.utc).timestamp()
    rules = {"rules_primary": "Synthetic exact contract", "rules_secondary": "Same terms",
             "close_time": "2026-11-09T06:00:00+00:00"}
    mapping = {"demo_rules": rules, "ticker": "DEMO-TICKER", "event_ticker": "DEMO",
               "side": "yes", "decision": "exact", "checks": dict.fromkeys(CHECKS, True),
               "rules_receipt_sha256": "0" * 64, "fee_receipt_sha256": "1" * 64,
               "fee_ceiling_cents": 2, "condition_id": "0x" + "2" * 64, "token_id": "3"}
    registration = {"environment": "demo", "mapping": mapping,
                    "wallets": ["0x" + str(i) * 40 for i in range(4)],
                    "start_utc": "2026-11-09T04:00:00+00:00",
                    "end_utc": "2026-11-09T06:00:00+00:00"}
    trade = {"proxy_wallet": registration["wallets"][0], "type": "TRADE", "side": "BUY",
             "transaction_hash": "0x" + "3" * 64, "condition_id": mapping["condition_id"],
             "token_id": "3", "timestamp": int(now - 15), "observed_at": now - 3,
             "price": "0.42", "size": "100"}
    quote = {"ticker": mapping["ticker"], "environment": "demo",
             "market": {**rules, "ticker": mapping["ticker"], "event_ticker": "DEMO",
                        "market_type": "binary", "status": "active"},
             "orderbook_fp": {"yes_dollars": [["0.35", "2"]], "no_dollars": [["0.60", "3"]]},
             "started_at": now - 1, "observed_at": now - .5}
    return registration, trade, quote, now


@pytest.mark.parametrize("wallet_price", ["0.38", "0.42", "0.46"])
def test_exact_demo_quote_including_both_band_endpoints(example, wallet_price):
    registration, trade, quote, now = example
    trade["price"] = wallet_price
    assert evaluate(registration, trade, quote, now=now, offset_low=-.5, offset_high=.5) == (40, 2)


def test_buffalo_price_difference_fits_band(example):
    registration, trade, quote, now = example
    trade["price"] = "0.61"
    quote["orderbook_fp"] = {"yes_dollars": [["0.61", "2"]], "no_dollars": [["0.38", "1330.88"]]}
    assert evaluate(registration, trade, quote, now=now, offset_low=-.5, offset_high=.5) == (62, 2)


@pytest.mark.parametrize("bad_input,reason", [
    ("diagnostic", "wallet_demo_exact_admission_required"),
    ("stale", "wallet_demo_signal_stale_or_unmatched"),
    ("other_wallet", "wallet_demo_signal_stale_or_unmatched"),
    ("old_rules", "wallet_demo_quote_or_rules_unverified"),
    ("production", "wallet_demo_quote_or_rules_unverified"),
    ("wide", "wallet_demo_price_or_depth_guard"),
    ("low", "wallet_demo_price_or_depth_guard"),
    ("small", "wallet_demo_price_or_depth_guard"),
])
def test_rejects_unqualified_input(example, bad_input, reason):
    registration, trade, quote, now = example
    if bad_input == "diagnostic":
        registration["mapping"]["decision"] = "diagnostic"
    elif bad_input == "stale":
        trade["timestamp"] -= 30
    elif bad_input == "other_wallet":
        trade["proxy_wallet"] = "0x" + "e" * 40
    elif bad_input == "old_rules":
        quote["market"]["rules_secondary"] = "Different cancellation terms"
    elif bad_input == "production":
        quote["environment"] = "production"
    elif bad_input == "wide":
        trade["price"] = "0.3799"
    elif bad_input == "low":
        trade["price"] = "0.4601"
    elif bad_input == "small":
        quote["orderbook_fp"]["no_dollars"][0][1] = "0.5"
    with pytest.raises(ValueError, match=reason):
        evaluate(registration, trade, quote, now=now, offset_low=-.5, offset_high=.5)


def test_uncertain_intent_only_reconciles(tmp_path):
    broker = Mock(spec=BinaryDemoBroker)
    broker.client = object.__new__(DemoClient)
    broker.journal = Mock()
    broker.journal.environment = "demo"
    broker.journal.records.return_value = [{"payload": {"client_order_id": CLIENT_ID}, "state": "uncertain"}]
    broker.refresh.side_effect = ValueError("submission_unresolved")
    with pytest.raises(ValueError, match="submission_unresolved"):
        run_once(broker, tmp_path / "lock", {}, {}, quote_provider=Mock(), offsets=None)
    broker.refresh.assert_called_once_with(CLIENT_ID)
    broker.submit.assert_not_called()


def test_submits_one_demo_contract_and_reconciles_fee(example, tmp_path):
    registration, trade, quote, now = example
    broker = Mock(spec=BinaryDemoBroker)
    broker.client = object.__new__(DemoClient)
    broker.journal = Mock()
    broker.journal.environment = "demo"
    broker.journal.clock.return_value = datetime.fromtimestamp(now, timezone.utc)
    broker.journal.records.return_value = []
    broker.journal.db.execute.return_value.fetchone.return_value = ('{"fees_dollars":"0.017"}',)

    def submit(client_id, *, quote_provider):
        assert client_id == CLIENT_ID
        snapshot = quote_provider({"ticker": "DEMO-TICKER"})
        assert snapshot["wallet_signal"] == trade
        assert snapshot["wallet_exact_mapping"] == registration["mapping"]
        return {"state": "terminal", "broker_id": "demo-fill", "filled": 1}

    broker.submit.side_effect = submit
    result = run_once(broker, tmp_path / "lock", registration, trade,
                      quote_provider=lambda payload: quote, offsets=(-.5, .5))
    broker.journal.reserve.assert_called_once_with(
        CLIENT_ID, "DEMO-TICKER", 1, 40, 2, action="buy", outcome="yes",
        account_snapshot=broker.snapshot.return_value)
    broker.reconcile_positions.assert_called_once()
    assert result == {"state": "terminal", "fill_verified": True, "filled": 1,
                      "order_id": "demo-fill", "fees_dollars": "0.017"}


def test_production_transport_rejected(tmp_path):
    broker = Mock(spec=BinaryDemoBroker)
    broker.client = object.__new__(DemoClient)
    broker.client.BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
    with pytest.raises(ValueError, match="wallet_demo_broker_required"):
        run_once(broker, tmp_path / "lock", {}, {}, quote_provider=Mock(), offsets=None)
    broker.submit.assert_not_called()
