import os
import json
import base64

import pytest

from opportunity_lab import coinbase_market_data as coinbase


def test_credentials_are_isolated(monkeypatch):
    monkeypatch.delenv("OPPORTUNITY_COINBASE_API_KEY_NAME", raising=False)
    monkeypatch.delenv("OPPORTUNITY_COINBASE_API_KEY_SECRET", raising=False)
    monkeypatch.setenv("COINBASE_API_KEY", "wrong-scope")
    assert coinbase.credentials_configured() is False


def test_product_response_is_sanitized(monkeypatch):
    monkeypatch.setattr(coinbase, "_get", lambda *args, **kwargs: ({"products": [{
        "product_id": "BIPZ30", "product_venue": "FCM", "price": "100", "account_uuid": "must-not-leak",
        "future_product_details": {"contract_size": "0.01", "funding_rate": "0.0001", "funding_interval": "ONE_HOUR", "contract_expiry_type": "PERPETUAL"},
    }]}, {"authenticated": True, "status_code": 200}))
    products, transport = coinbase.list_perpetual_products()
    assert transport["product_count"] == 1
    assert products[0]["funding_rate"] == pytest.approx(.0001)
    assert products[0]["contract_size"] == pytest.approx(.01)
    assert "account_uuid" not in products[0]


def test_intx_products_are_excluded_from_us_research(monkeypatch):
    monkeypatch.setattr(coinbase, "_get", lambda *args, **kwargs: ({"products": [
        {"product_id": "BTC-PERP-INTX", "product_venue": "neptune", "future_product_details": {"contract_expiry_type": "PERPETUAL"}},
        {"product_id": "BIPZ30", "product_venue": "FCM", "future_product_details": {"contract_expiry_type": "PERPETUAL", "venue": "CDE"}},
    ], "pagination": {"has_next": False}}, {"authenticated": True, "status_code": 200}))
    products, transport = coinbase.list_perpetual_products()
    assert [row["product_id"] for row in products] == ["BIPZ30"]
    assert transport["excluded_intx_count"] == 1


def test_cfm_access_discards_balance_payload(monkeypatch):
    monkeypatch.setattr(coinbase, "_get", lambda *args, **kwargs: ({"balance_summary": {"total_usd_balance": {"value": "secret"}}}, {"authenticated": True, "status_code": 200}))
    result = coinbase.check_cfm_read_access()
    assert result["cfm_read_access"] is True
    assert result["account_data_returned"] is False
    assert "balance_summary" not in result


def test_missing_credentials_fail_closed(monkeypatch):
    monkeypatch.delenv("OPPORTUNITY_COINBASE_API_KEY_NAME", raising=False)
    monkeypatch.delenv("OPPORTUNITY_COINBASE_API_KEY_SECRET", raising=False)
    payload, transport = coinbase._get("/api/v3/brokerage/products")
    assert payload == {}
    assert transport["authenticated"] is False
    assert transport["error"] == "opportunity_coinbase_read_credentials_missing"


def test_normalizes_coinbase_json_download_and_escaped_newlines():
    raw = json.dumps({"name": "organizations/a/apiKeys/b", "privateKey": "-----BEGIN EC PRIVATE KEY-----\\nabc\\n-----END EC PRIVATE KEY-----\\n"})
    assert coinbase._normalize_private_key(raw) == "-----BEGIN EC PRIVATE KEY-----\nabc\n-----END EC PRIVATE KEY-----"


def test_malformed_key_error_is_sanitized(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_COINBASE_API_KEY_NAME", "organizations/a/apiKeys/b")
    monkeypatch.setenv("OPPORTUNITY_COINBASE_API_KEY_SECRET", "not-a-private-key")
    payload, transport = coinbase._get("/api/v3/brokerage/products")
    assert payload == {}
    assert transport["error"] == "opportunity_coinbase_private_key_format_invalid"
    assert "cryptography" not in transport["error"]


def test_raw_base64_ed25519_secret_builds_jwt(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_COINBASE_API_KEY_NAME", "organizations/a/apiKeys/b")
    monkeypatch.setenv("OPPORTUNITY_COINBASE_API_KEY_SECRET", base64.b64encode(bytes(range(32))).decode())
    token = coinbase._token("GET", "/api/v3/brokerage/products")
    assert token.count(".") == 2
