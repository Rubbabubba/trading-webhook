import base64
import io
import json
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from opportunity_lab.kalshi_account_monitor import AccountClient, collect, summarize


@pytest.fixture
def client(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    path = tmp_path / "test.pem"
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    return AccountClient("test-id", path, clock=lambda: 1000, sleep=lambda _: None)


def test_signature_excludes_query_and_get_only(client):
    def opened(request, **kwargs):
        assert request.method == "GET"
        assert request.full_url.endswith("/portfolio/orders?limit=100")
        headers = {k.lower(): v for k, v in request.header_items()}
        client.key.public_key().verify(base64.b64decode(headers["kalshi-access-signature"]),
            b"1000000GET/trade-api/v2/portfolio/orders",
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256())
        return io.BytesIO(b'{"orders": []}')
    client.opener = SimpleNamespace(open=opened)
    assert client.get("/portfolio/orders", {"limit": 100}) == {"orders": []}
    with pytest.raises(ValueError):
        client.get("https://example.com")


def test_pagination_complete(client):
    replies = iter([{"orders": [1], "cursor": "next"}, {"orders": [2], "cursor": ""}])
    client.get = lambda *args: next(replies)
    assert client.pages("/portfolio/orders", "orders") == [1, 2]


def test_repeated_cursor_fails_closed(client):
    client.get = lambda *args: {"orders": [], "cursor": "same"}
    with pytest.raises(ValueError, match="repeated_account_cursor"):
        client.pages("/portfolio/orders", "orders")


def test_missing_page_not_empty_account(client):
    client.get = lambda *args: {}
    with pytest.raises(ValueError, match="invalid_account_response"):
        client.pages("/portfolio/orders", "orders")


@pytest.mark.parametrize("cash", [0, 50000, 80000])
def test_ceiling_and_no_false_live_readiness(cash):
    report = summarize({"balance": {"balance": cash, "portfolio_value": 0},
                        "positions": [], "resting_orders": [], "observed_at": 1})
    assert report["cash_within_ceiling_cents"] == min(cash, 50000)
    assert report["execution_enabled"] is False
    assert report["live_ready"] is False


def test_account_scope_and_bad_balance():
    def get(path, params):
        assert params["subaccount"] == 0
        return {"balance": "50000", "portfolio_value": 0}
    def pages(path, field, **params):
        assert params["subaccount"] == 0
        return []
    with pytest.raises(ValueError, match="invalid_balance_response"):
        collect(SimpleNamespace(get=get, pages=pages))
