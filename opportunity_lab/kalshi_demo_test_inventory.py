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
    opening_sha256 = hashlib.sha256(raw).hexdigest()
    close_path = path.with_name("separate-demo-test-inventory-close-v1.json")
    closes = {}
    receipt_sha256 = opening_sha256
    if close_path.exists():
        with close_path.open("rb") as receipt:
            closed_raw = receipt.read(16_385)
        if len(closed_raw) > 16_384:
            raise ValueError("test_inventory_close_receipt_too_large")
        closed = json.loads(closed_raw)
        if (closed.get("environment") != "demo" or closed.get("subaccount") != 0
                or closed.get("opening_sha256") != opening_sha256
                or len(closed["positions"]) != len(entries)):
            raise ValueError("test_inventory_close_scope_mismatch")
        closes = {entry["ticker"]: entry for entry in closed["positions"]}
        if set(closes) != {entry["ticker"] for entry in entries}:
            raise ValueError("test_inventory_close_scope_mismatch")
        receipt_sha256 = hashlib.sha256(raw + closed_raw).hexdigest()
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
        if len(fills) != (2 if closes else 1):
            raise ValueError("test_inventory_fill_count_mismatch")
        opening = [fill for fill in fills if fill.get("fill_id") == fill_id]
        if len(opening) != 1:
            raise ValueError("test_inventory_fill_mismatch")
        fill = opening[0]
        if (any(fill.get(k) != v for k, v in expected.items())
                or fill.get("fill_id") != fill_id or quantity(fill["count_fp"]) != 1
                or decimal_value(fill["fee_cost"]) != decimal_value(entry["fee_cost"])):
            raise ValueError("test_inventory_fill_mismatch")
        yes_price = decimal_value(fill["yes_price_dollars"])
        price = yes_price if side == "yes" else Decimal(1) - yes_price
        if not 0 <= price <= 1 or price != decimal_value(entry["price_dollars"]):
            raise ValueError("test_inventory_fill_price_mismatch")
        if closes:
            close = closes[ticker]
            closing_order_id = identifier(close["order_id"])
            closing_fill_id = identifier(close["fill_id"])
            closing_client_id = identifier(close["client_order_id"])
            if (closing_order_id in order_ids | {order_id} or closing_fill_id in fill_ids | {fill_id}
                    or not closing_client_id.startswith("wallet-close-20261109-")):
                raise ValueError("test_inventory_close_identity_mismatch")
            closing_order = client.request("GET", "/portfolio/orders/" + closing_order_id)["order"]
            opposite = "no" if side == "yes" else "yes"
            closing_expected = {"ticker": ticker, "order_id": closing_order_id, "subaccount_number": 0,
                                "book_side": "ask" if side == "yes" else "bid", "outcome_side": opposite}
            closing_fill = next((row for row in fills if row.get("fill_id") == closing_fill_id), {})
            if (any(row.get(k) != v for row in (closing_order, closing_fill)
                    for k, v in closing_expected.items())
                    or closing_order.get("client_order_id") != closing_client_id
                    or closing_order.get("status") != "executed"
                    or quantity(closing_order["initial_count_fp"]) != 1
                    or quantity(closing_order["fill_count_fp"]) != 1
                    or quantity(closing_order["remaining_count_fp"]) != 0
                    or quantity(closing_fill["count_fp"]) != 1
                    or decimal_value(closing_fill["yes_price_dollars"]) != decimal_value(close["yes_price_dollars"])
                    or decimal_value(closing_fill["fee_cost"]) != decimal_value(close["fee_cost"])):
                raise ValueError("test_inventory_close_mismatch")
            order_ids.add(closing_order_id)
            fill_ids.add(closing_fill_id)
        else:
            positions[ticker] = 1 if side == "yes" else -1
        order_ids.add(order_id)
        fill_ids.add(fill_id)
    return DemoTestInventory(positions, receipt_sha256)
