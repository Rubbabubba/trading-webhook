"""Verify explicitly attributed Demo test inventory outside the maker ledger."""
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json

from .kalshi_binary_accounting import reconcile_positions
from .kalshi_demo_broker import DemoClient, decimal_value, identifier, quantity


@dataclass(frozen=True)
class DemoTestInventory:
    positions: dict
    receipt_sha256: str

    def reconcile(self, accounting, rows):
        if accounting["positions"]:
            raise ValueError("maker_inventory_not_flat_during_test_inventory_hold")
        reconcile_positions({"positions": self.positions}, rows)


def verify_test_inventory(path, client):
    """Bind the hold to exact original orders and fills on Demo subaccount zero."""
    if type(client) is not DemoClient or client.BASE_URL != DemoClient.BASE_URL:
        raise ValueError("test_inventory_demo_client_required")
    with path.open("rb") as receipt:
        raw = receipt.read(16_385)
    if len(raw) > 16_384:
        raise ValueError("test_inventory_receipt_too_large")
    data = json.loads(raw)
    if data.get("environment") != "demo" or data.get("subaccount") != 0:
        raise ValueError("test_inventory_demo_scope_required")
    entries = data["positions"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= 4:
        raise ValueError("test_inventory_count_mismatch")
    positions, order_ids, fill_ids = {}, set(), set()
    for entry in entries:
        ticker = identifier(entry["ticker"])
        order_id = identifier(entry["order_id"])
        fill_id = identifier(entry["fill_id"])
        client_id = identifier(entry["client_order_id"])
        side = entry["outcome"]
        if (ticker in positions or order_id in order_ids or fill_id in fill_ids
                or side not in ("yes", "no") or not client_id.startswith("leaderboard-demo-20261106-")):
            raise ValueError("test_inventory_identity_mismatch")
        book_side = "bid" if side == "yes" else "ask"
        expected = {"ticker": ticker, "order_id": order_id, "subaccount_number": 0,
                    "book_side": book_side, "outcome_side": side}
        order = client.request("GET", "/portfolio/orders/" + order_id)["order"]
        if (any(order.get(k) != v for k, v in expected.items())
                or order.get("client_order_id") != client_id
                or order.get("status") != "executed"
                or quantity(order["initial_count_fp"]) != 1
                or quantity(order["fill_count_fp"]) != 1
                or quantity(order["remaining_count_fp"]) != 0):
            raise ValueError("test_inventory_order_mismatch")
        fills = client.pages("/portfolio/fills", "fills", ticker=ticker, subaccount=0)
        if len(fills) != 1:
            raise ValueError("test_inventory_fill_count_mismatch")
        fill = fills[0]
        if (any(fill.get(k) != v for k, v in expected.items())
                or fill.get("fill_id") != fill_id or quantity(fill["count_fp"]) != 1
                or decimal_value(fill["fee_cost"]) != decimal_value(entry["fee_cost"])):
            raise ValueError("test_inventory_fill_mismatch")
        yes_price = decimal_value(fill["yes_price_dollars"])
        price = yes_price if side == "yes" else Decimal(1) - yes_price
        if not 0 <= price <= 1 or price != decimal_value(entry["price_dollars"]):
            raise ValueError("test_inventory_fill_price_mismatch")
        positions[ticker] = 1 if side == "yes" else -1
        order_ids.add(order_id)
        fill_ids.add(fill_id)
    return DemoTestInventory(positions, hashlib.sha256(raw).hexdigest())
