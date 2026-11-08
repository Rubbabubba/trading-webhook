import json

import pytest

from opportunity_lab.kalshi_binary_broker import BinaryDemoBroker
from opportunity_lab.kalshi_binary_journal import BinaryJournal
from opportunity_lab.kalshi_demo_broker import DemoClient
from opportunity_lab.kalshi_demo_test_inventory import verify_test_inventory


@pytest.fixture
def inventory(tmp_path):
    client = object.__new__(DemoClient)
    entry = dict(ticker="TEST", order_id="order", fill_id="fill",
                 client_order_id="leaderboard-demo-20261106-test", outcome="no",
                 fee_cost="0.01", price_dollars="0.80")
    identity = dict(ticker="TEST", order_id="order", subaccount_number=0,
                    book_side="ask", outcome_side="no")
    order = dict(identity, client_order_id=entry["client_order_id"], status="executed",
                 initial_count_fp="1.00", fill_count_fp="1.00", remaining_count_fp="0.00")
    fill = dict(identity, fill_id="fill", count_fp="1.00", fee_cost="0.01", yes_price_dollars="0.20")
    position = {"ticker": "TEST", "position_fp": "-1.00"}

    def request(method, path, **kwargs):
        assert method == "GET"
        if path == "/portfolio/orders/order":
            return {"order": order}
        assert path == "/portfolio/balance"
        return {"balance": 100}

    def pages(path, key, **params):
        assert params["subaccount"] == 0
        if path == "/portfolio/fills":
            assert params["ticker"] == "TEST"
            return [fill]
        if path == "/portfolio/positions":
            return [position]
        assert path == "/portfolio/orders" and params["status"] == "resting"
        return []

    client.request = request
    client.pages = pages
    receipt = tmp_path / "inventory.json"
    receipt.write_text(json.dumps(dict(environment="demo", subaccount=0, positions=[entry])))
    return receipt, client, fill, position


def test_exact_inventory_is_read_only_and_unexpected_quantity_stops(tmp_path, inventory):
    receipt, client, _, position = inventory
    verified = verify_test_inventory(receipt, client)
    journal = BinaryJournal(tmp_path / "journal.sqlite3")
    try:
        journal.stop()
        broker = BinaryDemoBroker(journal, client, test_inventory=verified)
        assert broker.reconcile_positions()["positions_match"]
        assert broker.snapshot()["positions"] == [position]
        assert journal.accounting()["positions"] == {}
        assert journal.db.execute("SELECT stopped FROM controls").fetchone() == (1,)
        with pytest.raises(ValueError, match="demo_test_inventory_submission_hold"):
            broker.submit("unexpected")
        position["position_fp"] = "-2.00"
        with pytest.raises(ValueError, match="external_or_unreconciled_position"):
            broker.reconcile_positions()
    finally:
        journal.close()


def test_wrong_fill_fails_before_inventory_is_admitted(inventory):
    receipt, client, fill, _ = inventory
    fill["yes_price_dollars"] = "0.19"
    with pytest.raises(ValueError, match="test_inventory_fill_price_mismatch"):
        verify_test_inventory(receipt, client)
