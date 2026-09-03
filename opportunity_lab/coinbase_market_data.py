"""Read-only Coinbase Advanced Trade market-data adapter."""

from __future__ import annotations

import json
import os
import secrets
import time
import base64
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import jwt
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from cryptography.hazmat.primitives.serialization import load_pem_private_key


HOST = "api.coinbase.com"
BASE_URL = f"https://{HOST}"


def credentials_configured() -> bool:
    return bool(_credentials()[0] and _credentials()[1])


def list_perpetual_products() -> tuple[list[dict], dict]:
    """Return sanitized perpetual product economics without account or order data."""
    params = {"product_type": "FUTURE", "contract_expiry_type": "PERPETUAL", "limit": "100"}
    payload, transport = _get("/api/v3/brokerage/products", params)
    if transport.get("error"):
        return [], transport
    rows = []
    for product in payload.get("products") or []:
        details = product.get("future_product_details") or {}
        perpetual = details.get("perpetual_details") or product.get("perpetual_details") or {}
        rows.append({
            "product_id": product.get("product_id"),
            "display_name": product.get("display_name") or product.get("product_id"),
            "price": _number(product.get("price")),
            "best_bid": _number(product.get("price")),
            "best_ask": _number(product.get("price")),
            "index_price": _number(details.get("index_price") or product.get("index_price")),
            "funding_rate": _number(perpetual.get("funding_rate") or details.get("funding_rate") or product.get("funding_rate")),
            "funding_time": perpetual.get("funding_time") or details.get("funding_time") or product.get("funding_time"),
            "funding_interval": details.get("funding_interval") or product.get("funding_interval"),
            "contract_size": _number(details.get("contract_size")),
            "contract_expiry_type": details.get("contract_expiry_type") or product.get("contract_expiry_type"),
            "venue": details.get("venue") or product.get("product_venue"),
            "trading_disabled": bool(product.get("trading_disabled")),
        })
    return rows, {**transport, "product_count": len(rows)}


def _credentials() -> tuple[str, str]:
    name = (os.getenv("OPPORTUNITY_COINBASE_API_KEY_NAME") or "").strip()
    private_key = _normalize_private_key(os.getenv("OPPORTUNITY_COINBASE_API_KEY_SECRET") or "")
    return name, private_key


def _normalize_private_key(raw: str) -> str:
    """Accept Coinbase's copied PEM, JSON key download, or quoted PEM value."""
    value = str(raw or "").strip()
    try:
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            value = str(decoded.get("privateKey") or decoded.get("private_key") or decoded.get("secret") or "").strip()
        elif isinstance(decoded, str):
            value = decoded.strip()
    except (json.JSONDecodeError, TypeError):
        pass
    value = value.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\r\n", "\n")
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1].strip()
    return value.strip()


def _token(method: str, path: str) -> str:
    key_name, private_key = _credentials()
    if not key_name or not private_key:
        raise ValueError("opportunity_coinbase_read_credentials_missing")
    now = int(time.time())
    payload = {
        "sub": key_name,
        "iss": "cdp",
        "nbf": now,
        "exp": now + 120,
        "uri": f"{method.upper()} {HOST}{path}",
    }
    try:
        if "-----BEGIN" in private_key:
            loaded_key = load_pem_private_key(private_key.encode("utf-8"), password=None)
        else:
            raw_key = base64.b64decode(private_key, validate=True)
            if len(raw_key) not in {32, 64}:
                raise ValueError("unexpected Ed25519 key length")
            loaded_key = ed25519.Ed25519PrivateKey.from_private_bytes(raw_key[:32])
    except (TypeError, ValueError) as exc:
        raise ValueError("opportunity_coinbase_private_key_format_invalid") from exc
    if isinstance(loaded_key, ed25519.Ed25519PrivateKey):
        algorithm = "EdDSA"
    elif isinstance(loaded_key, ec.EllipticCurvePrivateKey):
        algorithm = "ES256"
    else:
        raise ValueError("opportunity_coinbase_private_key_type_unsupported")
    return jwt.encode(payload, loaded_key, algorithm=algorithm, headers={"kid": key_name, "nonce": secrets.token_hex()})


def _get(path: str, params: dict | None = None) -> tuple[dict, dict]:
    debug = {"method": "coinbase_advanced_trade_rest", "path": path, "authenticated": False}
    try:
        token = _token("GET", path)
        url = f"{BASE_URL}{path}"
        if params:
            url += f"?{urlencode(params)}"
        request = Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": "trading-webhook/opportunity-lab-v2"})
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload, {**debug, "authenticated": True, "status_code": 200}
    except HTTPError as exc:
        return {}, {**debug, "status_code": exc.code, "error": "coinbase_api_request_rejected"}
    except ValueError as exc:
        return {}, {**debug, "error": str(exc)}
    except Exception as exc:
        return {}, {**debug, "error": f"coinbase_transport_error:{type(exc).__name__}"}


def _number(value) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None
