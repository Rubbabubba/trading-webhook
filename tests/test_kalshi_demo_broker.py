import base64
from copy import deepcopy
from decimal import Decimal
import io
import json
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from opportunity_lab.kalshi_demo_broker import BrokerError, CREATE, DemoBroker, DemoClient, quantity
from opportunity_lab.kalshi_order_journal import Journal


def order(*, client_id="one", broker_id="broker", status="executed", filled=2, remaining=0, side="bid"):
    return dict(order_id=broker_id, client_order_id=client_id, ticker="TEST", outcome_side="yes" if side == "bid" else "no",
                book_side=side, type="limit", subaccount_number=0, initial_count_fp="2.00",
                yes_price_dollars="0.4000", fill_count_fp=f"{filled}.00", remaining_count_fp=f"{remaining}.00",
                status=status, taker_fees_dollars=f"{filled / 100:.4f}", maker_fees_dollars="0.0000",
                taker_fill_cost_dollars=f"{filled * .4:.4f}", maker_fill_cost_dollars="0.0000")


def fill(*, client_order="broker", count=2, side="bid"):
    return dict(fill_id="fill-" + client_order, order_id=client_order, ticker="TEST", outcome_side="yes" if side == "bid" else "no",
                book_side=side, subaccount_number=0, count_fp=f"{count}.00", yes_price_dollars="0.4000",
                fee_cost=f"{count / 100:.4f}")


class Exchange:
    def __init__(self):
        self.order = order()
        self.fills = [fill()]
        self.calls = []
        self.create_error = None
        self.cancel_error = None
        self.positions = []
        self.lookup = None
        self.resting = []
        self.historical_orders = []
        self.historical_fills = []

    def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        if path == "/exchange/status":
            return {"exchange_active": True, "trading_active": True}
        if method == "POST":
            if self.create_error:
                raise self.create_error
            self.positions = ([{"ticker": "TEST", "position_fp": self.order["fill_count_fp"]}]
                              if self.order["book_side"] == "bid" else [])
            return dict(order_id=self.order["order_id"], client_order_id=self.order["client_order_id"],
                        fill_count=self.order["fill_count_fp"], remaining_count=self.order["remaining_count_fp"])
        if method == "DELETE":
            if self.cancel_error:
                raise self.cancel_error
            # Fill wins the race before cancellation returns.
            self.order = order()
            self.fills = [fill()]
            return {"reduced_by": "0.00"}
        return {"order": deepcopy(self.order)}

    def pages(self, path, field, **kwargs):
        self.calls.append(("PAGES", path, kwargs))
        if path == "/historical/orders":
            return deepcopy(self.historical_orders)
        if path == "/historical/fills":
            return deepcopy(self.historical_fills)
        if field == "fills":
            return deepcopy(self.fills)
        if field == "market_positions":
            return deepcopy(self.positions)
        if kwargs.get("status") == "resting":
            return deepcopy(self.resting)
        return deepcopy(self.lookup if self.lookup is not None else [self.order])


@pytest.fixture
def scenario(tmp_path):
    j = Journal(tmp_path / "demo.sqlite3")
    j.reserve("one", "TEST", 2, 40, 4, cash_cents=50000)
    exchange = Exchange()
    yield j, exchange, DemoBroker(j, exchange)
    j.close()


def test_complete_fill_and_fee_evidence_survives_restart(scenario, tmp_path):
    j, exchange, broker = scenario
    assert broker.submit("one")["state"] == "terminal"
    second = Journal(tmp_path / "demo.sqlite3")
    detail = json.loads(second.db.execute("SELECT detail FROM broker_evidence").fetchone()[0])
    assert Decimal(detail["fees_dollars"]) == Decimal("0.02")
    assert second.get("one")["filled"] == 2
    with pytest.raises(ValueError):
        DemoBroker(second, exchange).submit("one")
    assert sum(call[0] == "POST" for call in exchange.calls) == 1
    second.close()


@pytest.mark.parametrize("error", [TimeoutError(), BrokerError(400), BrokerError(409), BrokerError(429), BrokerError(503)])
def test_failed_submission_never_resends_and_can_recover(scenario, tmp_path, error):
    j, exchange, broker = scenario
    exchange.create_error = error
    with pytest.raises(type(error)):
        broker.submit("one")
    assert j.get("one")["state"] == "uncertain"
    second = Journal(tmp_path / "demo.sqlite3")
    recovered = DemoBroker(second, exchange)
    with pytest.raises(ValueError):
        recovered.submit("one")
    assert recovered.refresh("one")["filled"] == 2
    assert sum(call[0] == "POST" for call in exchange.calls) == 1
    second.close()


@pytest.mark.parametrize("matches", [[], [order(), order(broker_id="another")]])
def test_missing_or_ambiguous_lookup_keeps_block(scenario, matches):
    j, exchange, broker = scenario
    j.mark_submission_started("one")
    exchange.lookup = matches
    with pytest.raises(ValueError, match="submission_unresolved"):
        broker.refresh("one")
    with pytest.raises(ValueError, match="unreconciled"):
        j.reserve("two", "TEST", 1, 40, 2, cash_cents=50000)


@pytest.mark.parametrize("field,value", [("ticker", "OTHER"), ("subaccount_number", 1),
    ("subaccount_number", False), ("outcome_side", "no"), ("book_side", "ask"),
    ("yes_price_dollars", "0.5000"), ("initial_count_fp", "3.00"),
    ("status", "mystery"), ("fill_count_fp", "NaN"), ("remaining_count_fp", "0.50")])
def test_malformed_order_does_not_resolve_submission(scenario, field, value):
    j, exchange, broker = scenario
    j.mark_submission_started("one")
    exchange.order[field] = value
    with pytest.raises(ValueError):
        broker.refresh("one")
    assert j.get("one")["state"] == "uncertain"


def test_fill_endpoint_lag_keeps_acknowledged_count_and_blocks(scenario):
    j, exchange, broker = scenario
    exchange.fills = []
    with pytest.raises(ValueError, match="fills_not_reconciled"):
        broker.submit("one")
    assert (j.get("one")["filled"], j.get("one")["state"]) == (2, "uncertain")
    exchange.fills = [fill()]
    assert broker.refresh("one")["state"] == "terminal"


def test_partial_fill_cancel_race_and_stop(scenario):
    j, exchange, broker = scenario
    exchange.order = order(status="resting", filled=1, remaining=1)
    exchange.fills = [fill(count=1)]
    assert broker.submit("one")["state"] == "working"
    # Preserve the previous fill and add a second rather than mutating history.
    def cancel_request(method, path, **kwargs):
        if method == "DELETE":
            exchange.order = order()
            extra = fill(count=1)
            extra["fill_id"] = "second-fill"
            exchange.fills.append(extra)
            return {"reduced_by": "0.00"}
        return {"order": deepcopy(exchange.order)}
    exchange.request = cancel_request
    j.stop()
    assert broker.cancel("one")["filled"] == 2
    assert j.get("one")["reserve"] == 84


def test_cancel_timeout_and_stale_read_block_new_entries(scenario):
    j, exchange, broker = scenario
    exchange.order = order(status="resting", filled=1, remaining=1)
    exchange.fills = [fill(count=1)]
    broker.submit("one")
    exchange.cancel_error = TimeoutError()
    with pytest.raises(TimeoutError):
        broker.cancel("one")
    assert j.get("one")["state"] == "uncertain"
    exchange.order["fill_count_fp"] = "0.00"
    with pytest.raises(ValueError):
        broker.refresh("one")
    assert j.get("one")["state"] == "uncertain"


def test_cancel_404_reconciles_terminal_order_instead_of_crashing(scenario):
    j, exchange, broker = scenario
    exchange.order = order(status="resting", filled=0, remaining=2)
    exchange.fills = []
    assert broker.submit("one")["state"] == "working"
    exchange.order = order(status="canceled", filled=0, remaining=0)
    exchange.cancel_error = BrokerError(404)
    assert broker.cancel("one")["state"] == "terminal"


def test_duplicate_fills_deduplicate_and_changed_history_stops(scenario):
    j, exchange, broker = scenario
    exchange.fills *= 2
    broker.submit("one")
    exchange.fills = [fill()]
    exchange.fills[0]["fee_cost"] = "0.03"
    exchange.order["taker_fees_dollars"] = "0.03"
    with pytest.raises(ValueError, match="historical_fill_changed"):
        broker.refresh("one")
    assert j.db.execute("SELECT stopped FROM controls").fetchone()[0] == 1


def test_exit_reservation_cannot_oversell(scenario):
    j, exchange, broker = scenario
    broker.submit("one")
    assert broker.reconcile_positions()["positions_match"]
    j.reserve("exit", "TEST", 2, 40, 4, cash_cents=50000, side="ask")
    assert j.get("exit")["payload"]["reduce_only"] is True
    with pytest.raises(ValueError, match="exit_exceeds"):
        j.reserve("second-exit", "TEST", 1, 40, 2, cash_cents=50000, side="ask")
    exchange.order = order(client_id="exit", broker_id="exit-broker", side="ask")
    exchange.fills = [fill(client_order="exit-broker", side="ask")]
    assert broker.submit("exit")["state"] == "terminal"
    exchange.positions = []
    assert broker.reconcile_positions()["position_count"] == 0


@pytest.mark.parametrize("positions,resting", [([{"ticker": "MANUAL", "position_fp": "1.00"}], []),
    ([{"ticker": "TEST", "position_fp": "2.00"}], [{"order_id": "manual"}])])
def test_external_activity_stops_journal(scenario, positions, resting):
    j, exchange, broker = scenario
    broker.submit("one")
    exchange.positions, exchange.resting = positions, resting
    with pytest.raises(ValueError, match="external"):
        broker.reconcile_positions()
    assert j.db.execute("SELECT stopped FROM controls").fetchone()[0] == 1


def test_external_position_prevents_any_submission(scenario):
    j, exchange, broker = scenario
    exchange.positions = [{"ticker": "MANUAL", "position_fp": "1.00"}]
    with pytest.raises(ValueError, match="external"):
        broker.submit("one")
    assert not any(call[0] == "POST" for call in exchange.calls)
    assert j.get("one")["state"] == "reserved"


def test_excess_actual_fees_persist_evidence_and_stop(scenario):
    j, exchange, broker = scenario
    exchange.fills[0]["fee_cost"] = "0.10"
    exchange.order["taker_fees_dollars"] = "0.10"
    with pytest.raises(ValueError, match="exceeds_reservation"):
        broker.submit("one")
    assert j.get("one")["filled"] == 2
    assert j.db.execute("SELECT stopped FROM controls").fetchone()[0] == 1
    detail = json.loads(j.db.execute("SELECT detail FROM broker_evidence").fetchone()[0])
    assert Decimal(detail["fees_dollars"]) == Decimal("0.10")


def test_ioc_no_fill_cancellation_does_not_invent_position(scenario):
    j, exchange, broker = scenario
    exchange.order = order(status="canceled", filled=0)
    exchange.fills = []
    assert broker.submit("one")["state"] == "terminal"
    with pytest.raises(ValueError, match="exit_exceeds"):
        j.reserve("exit", "TEST", 1, 40, 2, cash_cents=50000, side="ask")


def test_preflight_missing_credentials_has_no_sensitive_error(monkeypatch, capsys):
    from opportunity_lab.kalshi_demo_broker import main
    monkeypatch.delenv("KALSHI_DEMO_API_KEY_ID", raising=False)
    assert main(["--check-account"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result == {"environment": "demo", "account_connected": False, "orders_submitted": 0, "error": "KeyError"}


@pytest.fixture
def transport(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    path = tmp_path / "demo-test.pem"
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                      serialization.NoEncryption()))
    return DemoClient("test-id", path, clock=lambda: 1000, sleep=lambda _: None)


@pytest.mark.parametrize("method,path", [("POST", CREATE), ("GET", "/portfolio/orders/broker"),
                                        ("DELETE", CREATE + "/broker")])
def test_demo_only_host_and_signature_excludes_query(transport, method, path):
    def opened(request, **kwargs):
        assert request.full_url.startswith("https://demo-api.kalshi.co/trade-api/v2/")
        headers = {k.lower(): v for k, v in request.header_items()}
        transport.key.public_key().verify(base64.b64decode(headers["kalshi-access-signature"]),
            ("1000000" + method + "/trade-api/v2" + path).encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        return io.BytesIO(b'{}')
    transport.opener = SimpleNamespace(open=opened)
    transport.request(method, path, params={"subaccount": 0})


@pytest.mark.parametrize("method,expected", [("POST", 1), ("DELETE", 1), ("GET", 3)])
def test_only_reads_retry_and_errors_are_sanitized(transport, method, expected):
    calls = []
    def opened(request, **kwargs):
        calls.append(request)
        raise HTTPError(request.full_url, 503, "SECRET", {}, io.BytesIO(b'SECRET'))
    transport.opener = SimpleNamespace(open=opened)
    path = CREATE if method == "POST" else CREATE + "/broker" if method == "DELETE" else "/portfolio/orders"
    with pytest.raises(BrokerError) as error:
        transport.request(method, path)
    assert "SECRET" not in str(error.value)
    assert len(calls) == expected


@pytest.mark.parametrize("path", ["https://external-api.kalshi.com", "/portfolio/orders/../balance",
    "/portfolio/orders/id?secret=value", "/portfolio/orders/id/extra"])
def test_unapproved_paths_rejected(transport, path):
    with pytest.raises(ValueError, match="endpoint_not_allowed"):
        transport.request("GET", path)


@pytest.mark.parametrize("pages", [[{"orders": []}], [{"orders": [], "cursor": "same"}] * 2])
def test_incomplete_pagination_fails_closed(transport, pages):
    iterator = iter(pages)
    transport.request = lambda *a, **kw: next(iterator)
    with pytest.raises(ValueError, match="incomplete"):
        transport.pages("/portfolio/orders", "orders")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "0.5", True, "-1", "1e100"])
def test_invalid_quantities(value):
    with pytest.raises(ValueError):
        quantity(value)


def test_archived_submission_recovers_without_resending(scenario, tmp_path):
    j, exchange, broker = scenario
    j.mark_submission_started("one")
    exchange.lookup = []
    exchange.historical_orders = [order()]
    exchange.fills = []
    exchange.historical_fills = [fill(), fill(client_order="unrelated")]
    assert broker.refresh("one")["state"] == "terminal"
    assert not any(c[0] == "POST" for c in exchange.calls)
    second = Journal(tmp_path / "demo.sqlite3")
    assert second.get("one")["filled"] == 2
    second.close()


def test_known_archived_order_404_falls_back(scenario):
    j, exchange, broker = scenario
    j.mark_submission_started("one")
    j.acknowledge("one", "broker", 2)
    exchange.historical_orders = [order()]
    exchange.request = lambda *a, **k: (_ for _ in ()).throw(BrokerError(404))
    assert broker.refresh("one")["filled"] == 2


def test_history_overlap_deduplicates_exact_fills(scenario):
    j, exchange, broker = scenario
    exchange.historical_fills = [fill()]
    assert broker.submit("one")["filled"] == 2
    evidence = json.loads(j.db.execute("SELECT detail FROM broker_evidence").fetchone()[0])
    assert len(evidence["fills"]) == 1


def test_history_conflicting_fill_keeps_uncertainty(scenario):
    j, exchange, broker = scenario
    exchange.historical_fills = [fill()]
    exchange.historical_fills[0]["fee_cost"] = "0.03"
    with pytest.raises(ValueError, match="fill_identity_changed"):
        broker.submit("one")
    assert j.get("one")["state"] == "uncertain"


@pytest.mark.parametrize("status", [401, 429, 503])
def test_order_read_failure_does_not_fall_back_to_stale_history(scenario, status):
    j, exchange, broker = scenario
    j.mark_submission_started("one")
    j.acknowledge("one", "broker", 0)
    exchange.request = lambda *a, **k: (_ for _ in ()).throw(BrokerError(status))
    with pytest.raises(BrokerError):
        broker.refresh("one")
    assert not any(c[1].startswith("/historical/") for c in exchange.calls)


def test_incomplete_history_cannot_finalize_order(scenario):
    j, exchange, broker = scenario
    original = exchange.pages
    def pages(path, field, **params):
        if path.startswith("/historical/"):
            raise ValueError("incomplete_pagination")
        return original(path, field, **params)
    exchange.pages = pages
    with pytest.raises(ValueError, match="incomplete_pagination"):
        broker.submit("one")
    assert j.get("one")["state"] == "uncertain"


@pytest.mark.parametrize("path", ["/historical/orders", "/historical/fills"])
def test_history_transport_is_read_only(transport, path):
    transport.opener = SimpleNamespace(open=lambda *a, **k: io.BytesIO(b'{}'))
    assert transport.request("GET", path) == {}
    for method in ("POST", "DELETE"):
        with pytest.raises(ValueError, match="endpoint_not_allowed"):
            transport.request(method, path)


def test_broker_settlement_reconciles_zero_position_after_restart(tmp_path):
    from datetime import datetime, timezone
    path = tmp_path / "settlement.sqlite3"
    j = Journal(path, clock=lambda: datetime(2026, 9, 14, 20, tzinfo=timezone.utc))
    j.reserve("one", "TEST", 2, 40, 4, cash_cents=50000)
    exchange = Exchange()
    exchange.fills[0]["created_time"] = "2026-09-14T12:00:00Z"
    broker = DemoBroker(j, exchange)
    broker.submit("one")
    row = dict(ticker="TEST", exchange_index=0, market_result="yes", yes_count_fp="2.00",
               no_count_fp="0.00", yes_total_cost_dollars="0.80", no_total_cost_dollars="0.00",
               fee_cost="0.02", revenue=200, value=100, settled_time="2026-09-14T14:00:00Z")
    original = exchange.pages
    exchange.pages = lambda path, field, **params: [row] if field == "settlements" else original(path,field,**params)
    assert broker.reconcile_settlements() == {"reconciled_settlements": 1}
    j.close()
    j = Journal(path)
    exchange.positions = []
    assert DemoBroker(j, exchange).reconcile_positions()["position_count"] == 0
    j.close()


def test_external_settlement_stops_broker(scenario):
    j, exchange, broker = scenario
    exchange.pages = lambda *a, **k: [{"ticker": "MANUAL"}]
    with pytest.raises(ValueError, match="external_or_duplicate_settlement"):
        broker.reconcile_settlements()
    assert j.db.execute("SELECT stopped FROM controls").fetchone()[0] == 1


@pytest.mark.parametrize('health',[{}, {'exchange_active':True,'trading_active':False},
    {'exchange_active':1,'trading_active':True},
    {'exchange_active':True,'trading_active':True,'exchange_index_statuses':[]},
    {'exchange_active':True,'trading_active':True,'exchange_index_statuses':[
        {'exchange_index':0,'exchange_active':False,'trading_active':False}]}])
def test_unavailable_exchange_blocks_before_submission(scenario,health):
    j,exchange,broker=scenario
    original=exchange.request
    exchange.request=lambda method,path,**kw: health if path=='/exchange/status' else original(method,path,**kw)
    with pytest.raises(ValueError):broker.submit('one')
    assert j.get('one')['state']=='reserved'
    assert not any(c[0]=='POST' for c in exchange.calls)


def test_health_503_is_not_retried_and_details_are_safe(transport):
    from opportunity_lab.kalshi_demo_broker import check_exchange
    calls=[]
    def opened(request,**kwargs):
        calls.append(request)
        raise HTTPError(request.full_url,503,'secret',{},io.BytesIO(
            b'{"exchange_active":false,"trading_active":false,"error":{"code":"service_unavailable","message":"PRIVATE KEY SECRET"}}'))
    transport.opener=SimpleNamespace(open=opened)
    with pytest.raises(BrokerError) as caught:check_exchange(transport)
    assert len(calls)==1
    assert caught.value.diagnostics=={'exchange_active':False,'trading_active':False,'code':'service_unavailable'}
    assert 'SECRET' not in str(caught.value)


def test_unknown_error_code_never_echoed(transport):
    def opened(request,**kwargs):
        raise HTTPError(request.full_url,400,'secret',{},io.BytesIO(b'{"code":"SECRET_KEY_MATERIAL"}'))
    transport.opener=SimpleNamespace(open=opened)
    with pytest.raises(BrokerError) as caught:transport.request('POST',CREATE,body={})
    assert caught.value.diagnostics=={}


def test_market_inactive_lifecycle_rejection_is_sanitized():
    from opportunity_lab.kalshi_demo_broker import safe_error_details
    error=HTTPError('https://demo.invalid',400,'secret',{},io.BytesIO(
        b'{"error":{"code":"market_inactive","message":"private exchange detail"}}'))
    assert safe_error_details(error)=={'code':'market_inactive'}
