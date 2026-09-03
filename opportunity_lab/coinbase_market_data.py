"""Read-only Coinbase Advanced Trade market-data adapter."""

from __future__ import annotations

import json
import os
import secrets
import time
import base64
from datetime import datetime, timezone
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
    """Return sanitized U.S. CFM funding-bearing futures, excluding INTX rows.

    Coinbase's U.S. perpetual-style products are long-dated CFM futures and may be
    classified as EXPIRING rather than PERPETUAL, so discovery cannot filter on
    contract_expiry_type alone.
    """
    raw_products: list[dict] = []
    page_count = 0
    for offset in range(0, 1000, 100):
        params = {"product_type": "FUTURE", "expiring_contract_status": "STATUS_UNEXPIRED", "limit": "100", "offset": str(offset)}
        payload, transport = _get("/api/v3/brokerage/products", params)
        if transport.get("error"):
            return [], {**transport, "pages": page_count}
        page = payload.get("products") or []
        raw_products.extend(page)
        page_count += 1
        pagination = payload.get("pagination") or {}
        if len(page) < 100 or not pagination.get("has_next"):
            break
    rows = []
    excluded_intx = 0
    for product in raw_products:
        details = product.get("future_product_details") or {}
        product_id = str(product.get("product_id") or "")
        venue = str(details.get("venue") or product.get("product_venue") or "")
        if product_id.endswith("-INTX") or venue.lower() in {"neptune", "intx"}:
            excluded_intx += 1
            continue
        perpetual = details.get("perpetual_details") or product.get("perpetual_details") or {}
        funding_rate = perpetual.get("funding_rate") or details.get("funding_rate") or product.get("funding_rate")
        funding_interval = details.get("funding_interval") or product.get("funding_interval")
        if funding_rate in (None, "") and funding_interval in (None, ""):
            continue
        rows.append({
            "product_id": product_id,
            "display_name": product.get("display_name") or product.get("product_id"),
            "price": _number(product.get("price")),
            "best_bid": _number(product.get("best_bid_price")),
            "best_ask": _number(product.get("best_ask_price")),
            "index_price": _number(details.get("index_price") or product.get("index_price")),
            "funding_rate": _number(funding_rate),
            "funding_time": perpetual.get("funding_time") or details.get("funding_time") or product.get("funding_time"),
            "funding_interval": funding_interval,
            "contract_size": _number(details.get("contract_size")),
            "contract_expiry_type": details.get("contract_expiry_type") or product.get("contract_expiry_type"),
            "contract_expiry": details.get("contract_expiry"),
            "contract_code": details.get("contract_code"),
            "venue": venue or None,
            "trading_disabled": bool(product.get("trading_disabled")),
        })
    return rows, {**transport, "discovery": "all_unexpired_futures_then_funding_filter", "pages": page_count, "catalog_product_count": len(raw_products), "excluded_intx_count": excluded_intx, "product_count": len(rows)}


def check_cfm_read_access() -> dict:
    """Check CFM entitlement while discarding all returned financial values."""
    _, transport = _get("/api/v3/brokerage/cfm/balance_summary")
    return {
        "authenticated": bool(transport.get("authenticated")),
        "cfm_read_access": not bool(transport.get("error")),
        "status_code": transport.get("status_code"),
        "error": transport.get("error"),
        "account_data_returned": False,
    }


def get_fee_schedule() -> tuple[dict, dict]:
    """Return only current rate fields; discard balances, volumes, and identifiers."""
    schedules = {}
    transports = {}
    queries = {
        "spot": {"product_type": "SPOT"},
        "us_derivatives": {"product_type": "FUTURE", "contract_expiry_type": "EXPIRING", "product_venue": "FCM"},
    }
    for label, params in queries.items():
        payload, transport = _get("/api/v3/brokerage/transaction_summary", params)
        transports[label] = transport
        if transport.get("error"):
            return {}, transports
        tier = payload.get("fee_tier") or {}
        schedules[label] = {
            "pricing_tier": tier.get("pricing_tier"),
            "maker_fee_rate": _number(tier.get("maker_fee_rate")),
            "taker_fee_rate": _number(tier.get("taker_fee_rate")),
            "margin_rate": _number(payload.get("margin_rate")),
        }
    return schedules, transports


def fetch_product_candles(product_id: str, *, start: datetime, end: datetime, granularity: str = "ONE_HOUR", max_pages: int = 100) -> tuple[list[dict], dict]:
    """Fetch complete Coinbase candles in bounded 350-bucket requests."""
    seconds = {"ONE_MINUTE": 60, "FIVE_MINUTE": 300, "ONE_HOUR": 3600, "ONE_DAY": 86400}.get(granularity)
    if not seconds:
        return [], {"error": "unsupported_granularity", "granularity": granularity}
    start_ts, end_ts = int(start.timestamp()), int(end.timestamp())
    cursor = start_ts
    rows: dict[int, dict] = {}
    pages = 0
    while cursor < end_ts and pages < max(1, min(500, int(max_pages))):
        chunk_end = min(end_ts, cursor + seconds * 349)
        payload, transport = _get(
            f"/api/v3/brokerage/products/{product_id}/candles",
            {"start": str(cursor), "end": str(chunk_end), "granularity": granularity, "limit": "350"},
        )
        if transport.get("error"):
            return [], {**transport, "product_id": product_id, "pages": pages}
        for candle in payload.get("candles") or []:
            timestamp = int(candle.get("start") or 0)
            if timestamp:
                rows[timestamp] = {
                    "timestamp": timestamp,
                    "close": _number(candle.get("close")),
                    "volume": _number(candle.get("volume")),
                }
        pages += 1
        cursor = chunk_end + seconds
    ordered = [rows[key] for key in sorted(rows)]
    truncated = cursor < end_ts
    return ordered, {
        "method": "coinbase_advanced_trade_candles",
        "product_id": product_id,
        "granularity": granularity,
        "requested_start": datetime.fromtimestamp(start_ts, timezone.utc).isoformat(),
        "requested_end": datetime.fromtimestamp(end_ts, timezone.utc).isoformat(),
        "pages": pages,
        "count": len(ordered),
        "truncated": truncated,
    }


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
