"""Demo-only Kalshi V2 order adapter. No production host or activation switch.

The HTTP transport never retries a mutation. The journal records uncertainty
before a write; full order/fill reconciliation is required before clearing it.
Amounts remain Decimal; only whole-contract YES positions are supported.
"""
from __future__ import annotations
from .kalshi_order_direction import outcome_for_book

import base64
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from .kalshi_account_monitor import NoRedirect

DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2"
CREATE = "/portfolio/events/orders"
READS = {"/portfolio/orders", "/portfolio/fills", "/portfolio/positions", "/portfolio/balance",
         "/historical/orders", "/historical/fills", "/portfolio/settlements", "/exchange/status"}
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


class BrokerError(RuntimeError):
    """Sanitized transport failure; response bodies and credentials are omitted."""

    def __init__(self, status=None, diagnostics=None):
        self.status = status
        self.diagnostics = diagnostics or {}
        super().__init__(f"kalshi_demo_http_{status}" if status else "kalshi_demo_transport_failure")


def safe_error_details(error):
    """Only known enum codes and exact health booleans; never free-form messages."""
    try:
        data = json.loads(error.read(8192))
        if not isinstance(data, dict):
            return {}
        result = {k:data[k] for k in ("exchange_active", "trading_active") if type(data.get(k)) is bool}
        nested = data.get("error", data)
        code = nested.get("code") if isinstance(nested, dict) else None
        if code in ("missing_parameters", "invalid_parameters", "invalid_signature", "market_closed", "market_inactive",
                    "exchange_unavailable", "exchange_inactive", "trading_paused", "service_unavailable",
                    "internal_error", "insufficient_balance", "duplicate_order", "deprecated_v1_order_endpoint"):
            result["code"] = code
        return result
    except Exception:
        return {}


def check_exchange(client):
    data = client.request("GET", "/exchange/status")
    if data.get("exchange_active") is not True or data.get("trading_active") is not True:
        raise ValueError("demo_exchange_unavailable")
    if "exchange_index_statuses" in data:
        rows = data["exchange_index_statuses"]
        if not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
            raise ValueError("invalid_exchange_status")
        primary = [r for r in rows if type(r.get("exchange_index")) is int and r["exchange_index"] == 0]
        if len(primary) != 1 or primary[0].get("exchange_active") is not True or primary[0].get("trading_active") is not True:
            raise ValueError("demo_exchange_unavailable")
    return {"exchange_active": True, "trading_active": True, "exchange_index": 0}


def decimal_value(value):
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError("invalid_fixed_point")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValueError("invalid_fixed_point") from None
    if not number.is_finite():
        raise ValueError("invalid_fixed_point")
    return number


def quantity(value, *, signed=False):
    number = decimal_value(value)
    if number != number.to_integral_value() or abs(number) > 1_000_000 or (not signed and number < 0):
        raise ValueError("unsupported_contract_quantity")
    return int(number)


def identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError("invalid_order_identifier")
    return value


class DemoClient:
    BASE_URL = DEMO_BASE
    def __init__(self, key_id, key_path, *, opener=None, clock=time.time, sleep=time.sleep):
        self.key_id = key_id
        self.key = serialization.load_pem_private_key(Path(key_path).read_bytes(), password=None)
        self.opener = opener or build_opener(NoRedirect())
        self.clock, self.sleep, self.next_request = clock, sleep, 0.0

    def request(self, method, path, *, params=None, body=None):
        order_read = re.fullmatch(r"/portfolio/orders/([A-Za-z0-9_-]{1,128})", path)
        queue_position_read = re.fullmatch(
            r"/portfolio/orders/([A-Za-z0-9_-]{1,128})/queue_position", path
        )
        order_cancel = path.startswith(CREATE + "/") and IDENTIFIER.fullmatch(path.rsplit("/", 1)[-1])
        allowed = ((method == "GET" and (path in READS or order_read or queue_position_read))
                   or (method == "POST" and path == CREATE)
                   or (method == "DELETE" and order_cancel))
        # Exact path shape prevents arbitrary paths, query injection and redirects.
        if not allowed or (order_cancel and path.count("/") != 4):
            raise ValueError("endpoint_not_allowed")
        if method != "POST" and body is not None:
            raise ValueError("unexpected_request_body")
        attempts = 3 if method == "GET" and path != "/exchange/status" else 1
        for attempt in range(attempts):
            self.sleep(max(0, self.next_request - self.clock()))
            stamp = str(int(self.clock() * 1000))
            signature = self.key.sign((stamp + method + "/trade-api/v2" + path).encode(),
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256())
            url = self.BASE_URL + path + ("?" + urlencode(params) if params else "")
            request = Request(url, method=method,
                data=json.dumps(body).encode() if body is not None else None,
                headers={"KALSHI-ACCESS-KEY": self.key_id, "KALSHI-ACCESS-TIMESTAMP": stamp,
                         "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
                         "Accept": "application/json", "Content-Type": "application/json"})
            self.next_request = self.clock() + 2
            try:
                with self.opener.open(request, timeout=20) as response:
                    result = json.load(response)
                if not isinstance(result, dict):
                    raise ValueError("invalid_response")
                return result
            except HTTPError as error:
                if error.code in (429, 500, 502, 503, 504) and attempt + 1 < attempts:
                    self.next_request = self.clock() + 30 * 2 ** attempt
                    continue
                raise BrokerError(error.code, safe_error_details(error)) from None
            except Exception:
                raise BrokerError() from None

    def pages(self, path, field, **params):
        result, seen, cursor = [], set(), ""
        for _ in range(100):
            page = self.request("GET", path, params={**params, "limit": 100, "cursor": cursor})
            if not isinstance(page.get(field), list) or any(not isinstance(row, dict) for row in page[field]):
                raise ValueError("invalid_page")
            result.extend(page[field])
            cursor = page.get("cursor")
            if cursor == "":
                return result
            if not isinstance(cursor, str) or cursor in seen:
                raise ValueError("incomplete_pagination")
            seen.add(cursor)
        raise ValueError("incomplete_pagination")


class DemoBroker:
    def __init__(self, journal, client):
        journal.bind_environment("demo")
        self.journal, self.client = journal, client

    def submit(self, client_id):
        if self.journal.get(client_id)["state"] != "reserved":
            raise ValueError("submission_not_allowed")
        # Serial demo execution: reconcile actual inventory and external orders
        # immediately before a new mutation. Pending submissions block this check.
        self.reconcile_positions(allow_reserved=True)
        check_exchange(self.client)
        payload = self.journal.mark_submission_started(client_id)
        # Any exception, including a rejection, leaves a durable uncertain intent.
        # No error response is interpreted as proof that an order never existed.
        ack = self.client.request("POST", CREATE, body=payload)
        if ack.get("client_order_id") != client_id:
            raise ValueError("create_identity_mismatch")
        filled, remaining = quantity(ack.get("fill_count")), quantity(ack.get("remaining_count"))
        if filled + remaining > quantity(payload["count"]):
            raise ValueError("invalid_create_counts")
        self.journal.acknowledge(client_id, identifier(ack.get("order_id")), filled)
        return self.refresh(client_id)

    def refresh(self, client_id):
        self.journal.mark_reconciliation_started(client_id)
        try:
            return self._refresh(client_id)
        except Exception:
            if self.journal.get(client_id)["state"] == "terminal":
                self.journal.stop()
            raise

    def _refresh(self, client_id):
        record = self.journal.get(client_id)
        if record["state"] == "reserved":
            raise ValueError("intent_not_submitted")
        payload = record["payload"]
        broker_id = record["broker_id"]
        archived_order = None
        if broker_id is None:
            # This endpoint does not document a client-ID filter: scan all pages
            # for the ticker and primary account, then require one exact match.
            rows = self.client.pages("/portfolio/orders", "orders", ticker=payload["ticker"], subaccount=0)
            matches = [row for row in rows if row.get("client_order_id") == client_id]
            if not matches:
                rows = self.client.pages("/historical/orders", "orders", ticker=payload["ticker"])
                matches = [row for row in rows if row.get("client_order_id") == client_id]
                if len(matches) == 1:
                    archived_order = matches[0]
            if len(matches) != 1:
                raise ValueError("submission_unresolved")
            broker_id = identifier(matches[0].get("order_id"))
        if archived_order is None:
            try:
                response = self.client.request("GET", "/portfolio/orders/" + identifier(broker_id))
                order = response.get("order")
            except BrokerError as error:
                if error.status != 404:
                    raise
                rows = self.client.pages("/historical/orders", "orders", ticker=payload["ticker"])
                matches = [row for row in rows if row.get("order_id") == broker_id]
                if len(matches) != 1:
                    raise ValueError("historical_order_unresolved") from None
                order = matches[0]
        else:
            order = archived_order
        if not isinstance(order, dict):
            raise ValueError("invalid_order_response")
        expected_action = "buy" if payload["side"] == "bid" else "sell"
        expected = {"order_id": broker_id, "client_order_id": client_id, "ticker": payload["ticker"],
                    "outcome_side": outcome_for_book(payload["side"]), "book_side": payload["side"], "type": "limit",
                    "subaccount_number": 0}
        if any(order.get(key) != value for key, value in expected.items()):
            raise ValueError("order_identity_mismatch")
        if type(order["subaccount_number"]) is not int:
            raise ValueError("order_identity_mismatch")
        self.validate_legacy_direction(order, payload)
        count = quantity(payload["count"])
        if quantity(order.get("initial_count_fp")) != count or decimal_value(order.get("yes_price_dollars")) != decimal_value(payload["price"]):
            raise ValueError("order_terms_changed")
        filled, remaining = quantity(order.get("fill_count_fp")), quantity(order.get("remaining_count_fp"))
        status = order.get("status")
        if status not in ("resting", "canceled", "executed"):
            raise ValueError("unknown_order_status")
        terminal = status != "resting"
        if ((terminal and remaining != 0) or (status == "executed" and filled != count)
                or (status == "resting" and (filled + remaining != count or remaining == 0))):
            raise ValueError("inconsistent_order_status")
        # Preserve the acknowledged lower bound even if the fill endpoint lags.
        fills = self.client.pages("/portfolio/fills", "fills", order_id=broker_id, subaccount=0)
        # Historical endpoints document ticker, not order/subaccount filters.
        # Read every page and filter order identity locally; validate account below.
        historical = self.client.pages("/historical/fills", "fills", ticker=payload["ticker"])
        fills.extend(row for row in historical if row.get("order_id") == broker_id)
        unique = {}
        for fill in fills:
            fill_id = identifier(fill.get("fill_id"))
            if fill_id in unique and unique[fill_id] != fill:
                raise ValueError("fill_identity_changed")
            unique[fill_id] = fill
        # Kalshi may split a whole-contract order into fractional execution
        # records even though the order-level filled quantity is whole.  Keep
        # the journal whole-contract-only, but sum the validated fill pieces as
        # Decimal before reconciling them to the integral order total.
        total, fees, gross = Decimal(0), Decimal(0), Decimal(0)
        for fill in unique.values():
            if (fill.get("order_id") != broker_id or fill.get("ticker") != payload["ticker"]
                    or fill.get("outcome_side") != outcome_for_book(payload["side"]) or fill.get("book_side") != payload["side"]
                    or type(fill.get("subaccount_number")) is not int or fill["subaccount_number"] != 0):
                raise ValueError("fill_identity_mismatch")
            q = decimal_value(fill.get("count_fp"))
            price = decimal_value(fill.get("yes_price_dollars"))
            fee = decimal_value(fill.get("fee_cost"))
            limit = decimal_value(payload["price"])
            if (q <= 0 or q > count or not 0 <= price <= 1 or fee < 0
                    or (price > limit if payload["side"] == "bid" else price < limit)):
                raise ValueError("invalid_fill_amount")
            total += q
            fees += fee
            gross += q * price
        order_fees = sum((decimal_value(order.get(k)) for k in ("taker_fees_dollars", "maker_fees_dollars")), Decimal(0))
        order_gross = sum((decimal_value(order.get(k)) for k in ("taker_fill_cost_dollars", "maker_fill_cost_dollars")), Decimal(0))
        if total != filled or fees != order_fees or self.order_cost_basis(record, order, gross, total) != order_gross:
            raise ValueError("fills_not_reconciled")
        # Accounting evidence and lifecycle state commit together, or neither does.
        self.journal.reconcile(client_id, broker_id=broker_id, filled=filled, remaining=remaining,
                               terminal=terminal, evidence={"order": order, "fills": list(unique.values()),
                                   "fees_dollars": str(fees), "gross_dollars": str(gross)})
        actual_reserved_cost = self.reserved_cost(record, gross, fees, total)
        if actual_reserved_cost > Decimal(record["reserve"]) / 100:
            self.journal.stop()
            raise ValueError("actual_cost_exceeds_reservation")
        return self.journal.get(client_id)

    def validate_legacy_direction(self, order, payload):
        expected_action = 'buy' if payload['side']=='bid' else 'sell'
        if order.get('side','yes')!='yes' or order.get('action',expected_action)!=expected_action:
            raise ValueError('order_identity_mismatch')

    def order_cost_basis(self, record, order, gross, total):
        return gross

    def reserved_cost(self, record, gross, fees, total):
        return gross+fees if record['payload']['side']=='bid' else fees

    def cancel(self, client_id):
        record = self.journal.get(client_id)
        self.journal.mark_cancel_started(client_id)
        # A DELETE timeout stays uncertain. A cancellation ACK never implies no fills.
        try:
            self.client.request("DELETE", CREATE + "/" + identifier(record["broker_id"]),
                                params={"market_ticker": record["payload"]["ticker"]})
        except BrokerError as error:
            # The order may have become terminal between the last read and DELETE.
            # A 404 proves nothing by itself; the normal refresh path must locate
            # and validate the active or archived order before resolving state.
            if error.status != 404:
                raise
        return self.refresh(client_id)

    def reconcile_positions(self, *, allow_reserved=False):
        records = self.journal.records()
        permitted = {"terminal", "reserved"} if allow_reserved else {"terminal"}
        if any(row["state"] not in permitted for row in records):
            raise ValueError("orders_not_terminal")
        expected = {}
        for row in records:
            p = row["payload"]
            expected[p["ticker"]] = expected.get(p["ticker"], 0) + row["filled"] * (1 if p["side"] == "bid" else -1)
        if self.journal.db.execute("SELECT 1 FROM settlements LIMIT 1").fetchone():
            expected = self.journal.accounting()["positions"]
        observed = {}
        for row in self.client.pages("/portfolio/positions", "market_positions", subaccount=0, count_filter="position"):
            ticker = row.get("ticker")
            if not isinstance(ticker, str) or not ticker or ticker in observed:
                raise ValueError("invalid_position_identity")
            observed[ticker] = quantity(row.get("position_fp"), signed=True)
        expected = {k: v for k, v in expected.items() if v}
        observed = {k: v for k, v in observed.items() if v}
        if expected != observed:
            self.journal.stop()
            raise ValueError("external_or_unreconciled_position")
        if self.client.pages("/portfolio/orders", "orders", subaccount=0, status="resting"):
            self.journal.stop()
            raise ValueError("external_resting_orders")
        return {"positions_match": True, "position_count": len(observed)}

    def reconcile_settlements(self):
        tickers = {r["payload"]["ticker"] for r in self.journal.records()}
        rows = self.client.pages("/portfolio/settlements", "settlements", subaccount=0)
        seen = set()
        try:
            for row in rows:
                ticker = row.get("ticker")
                if ticker not in tickers or ticker in seen:
                    raise ValueError("external_or_duplicate_settlement")
                seen.add(ticker)
                self.journal.record_settlement(row)
        except Exception:
            self.journal.stop()
            raise
        return {"reconciled_settlements": len(rows)}


def main(argv=None):
    """Credential preflight only: CLI has no submit or cancel command."""
    import argparse
    import os
    parser = argparse.ArgumentParser(description="Check separate Kalshi demo credentials; no orders sent.")
    parser.add_argument("--check-account", action="store_true", required=True)
    parser.parse_args(argv)
    try:
        client = DemoClient(os.environ["KALSHI_DEMO_API_KEY_ID"], os.environ["KALSHI_DEMO_PRIVATE_KEY_PATH"])
        balance = client.request("GET", "/portfolio/balance", params={"subaccount": 0})
        if type(balance.get("balance")) is not int or balance["balance"] < 0:
            raise ValueError("invalid_balance")
        positions = client.pages("/portfolio/positions", "market_positions", subaccount=0, count_filter="position")
        orders = client.pages("/portfolio/orders", "orders", subaccount=0, status="resting")
        print(json.dumps({"environment": "demo", "account_connected": True,
            "demo_cash_cents": balance["balance"], "position_rows": len(positions),
            "resting_orders": len(orders), "orders_submitted": 0}))
        return 0
    except Exception as error:
        # Never echo key parsing errors, environment values, paths or account bodies.
        print(json.dumps({"environment": "demo", "account_connected": False,
            "orders_submitted": 0, "error": str(error) if isinstance(error, BrokerError) else type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
