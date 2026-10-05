"""Production readiness acquisition with GET-only account transport, no grant."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
import hashlib

from .kalshi_account_monitor import AccountClient, collect
from .kalshi_factory_fee_probe import fee_basis
from .kalshi_shadow import price_book


def check(client, markets):
    snapshot = collect(client)
    quote_verified = fee_verified = False
    page, _, _ = markets.get(params={"status": "open", "limit": 200})
    rows = page.get("markets")
    if not isinstance(rows, list) or len(rows) > 200:
        raise ValueError("invalid_production_catalog")
    for market in rows[:200]:
        if market.get("market_type") != "binary" or market.get("exchange_index", 0) != 0:
            continue
        quote = markets.quote({"ticker": market["ticker"]})
        _, ask, _, depth = price_book(quote, "yes")
        age = datetime.now(timezone.utc).timestamp() - quote["observed_at"]
        if depth < 1 or not 0 <= age <= 5:
            break
        price = int((Decimal(ask.numerator) / Decimal(ask.denominator) * 100).to_integral_value(rounding=ROUND_CEILING))
        event, _, event_at = markets.get_event(market["event_ticker"])
        series, _, series_at = markets.get_series(event["event"]["series_ticker"])
        fee_basis({"observed_at": datetime.fromtimestamp(quote["observed_at"], timezone.utc).isoformat(),
                   "event_id": market["event_ticker"], "price_cents": price,
                   "market_type": "binary", "exchange_index": 0},
                  event["event"], series["series"], fetched_at=max(event_at, series_at))
        quote_verified = fee_verified = True
        break
    return {"schema": "kalshi_live_preflight_v1", "environment": "production",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "account_key_sha256": hashlib.sha256(client.key_id.encode()).hexdigest(),
            "available_cash_cents": snapshot["balance"]["balance"],
            "position_rows": len(snapshot["positions"]), "resting_orders": len(snapshot["resting_orders"]),
            "account_verified": True, "quote_verified": quote_verified, "fee_verified": fee_verified,
            "execution_enabled": False}


def run(key_id, key_path, markets):
    return check(AccountClient(key_id, key_path), markets)
