"""Production readiness acquisition with GET-only account transport, no grant."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
import hashlib
import json
from pathlib import Path

from .kalshi_account_monitor import AccountClient, collect
from .kalshi_factory_fee_probe import fee_basis
from .kalshi_shadow import price_book


def check(client, markets, *, progress=None):
    snapshot = collect(client)
    quote_verified = fee_verified = False
    progress = progress if progress is not None else {'cursor':'','offset':0}
    if (set(progress) != {'cursor','offset'} or not isinstance(progress['cursor'],str)
            or len(progress['cursor'])>4096 or type(progress['offset']) is not int or not 0<=progress['offset']<=200):
        raise ValueError('invalid_preflight_catalog_progress')
    # Readiness supports ordinary binary contracts. Combo markets dominate the
    # default catalog and can have empty books; they cannot establish this check.
    params={"status":"open","limit":200,"mve_filter":"exclude"}
    if progress['cursor']: params['cursor']=progress['cursor']
    page, _, _ = markets.get(params=params)
    rows = page.get("markets")
    if not isinstance(rows, list) or len(rows) > 200 or not isinstance(page.get('cursor',''),str):
        raise ValueError("invalid_production_catalog")
    checked = 0
    for index in range(progress['offset'],len(rows)):
        if checked >= 3:
            break
        progress['offset']=index+1
        market=rows[index]
        if market.get("market_type") != "binary" or market.get("exchange_index", 0) != 0:
            continue
        checked += 1
        quote = markets.quote({"ticker": market["ticker"]})
        try:
            _, ask, _, depth = price_book(quote, "yes")
        except ValueError as error:
            if str(error) == 'missing_book':
                continue
            raise
        age = datetime.now(timezone.utc).timestamp() - quote["observed_at"]
        if depth < 1 or not 0 <= age <= 5:
            continue
        price = int((Decimal(ask.numerator) / Decimal(ask.denominator) * 100).to_integral_value(rounding=ROUND_CEILING))
        event, _, event_at = markets.get_event(market["event_ticker"])
        series, _, series_at = markets.get_series(event["event"]["series_ticker"])
        fee_basis({"observed_at": datetime.fromtimestamp(quote["observed_at"], timezone.utc).isoformat(),
                   "event_id": market["event_ticker"], "price_cents": price,
                   "market_type": "binary", "exchange_index": 0},
                  event["event"], series["series"], fetched_at=max(event_at, series_at))
        quote_verified = fee_verified = True
        break
    if progress['offset']>=len(rows):
        progress.update(cursor=page.get('cursor',''),offset=0)
    return {"schema": "kalshi_live_preflight_v1", "environment": "production",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "account_key_sha256": hashlib.sha256(client.key_id.encode()).hexdigest(),
            "available_cash_cents": snapshot["balance"]["balance"],
            "position_rows": len(snapshot["positions"]), "resting_orders": len(snapshot["resting_orders"]),
            "account_verified": True, "quote_verified": quote_verified, "fee_verified": fee_verified,
            "execution_enabled": False}


def run(key_id, key_path, markets, *, progress_file=None):
    path=Path(progress_file) if progress_file else None
    progress=json.loads(path.read_text()) if path and path.exists() else {'cursor':'','offset':0}
    try:
        return check(AccountClient(key_id, key_path), markets, progress=progress)
    finally:
        if path:
            temporary=path.with_suffix('.tmp')
            temporary.write_text(json.dumps(progress),encoding='utf-8'); temporary.replace(path)
