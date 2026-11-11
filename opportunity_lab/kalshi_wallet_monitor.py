"""Bounded continuous public wallet observation. This module has no order transport."""
from decimal import Decimal, ROUND_CEILING
from fractions import Fraction
import hashlib
import json
import re
import time
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from .kalshi_account_monitor import NoRedirect
from .kalshi_demo_stream_source import levels
from .kalshi_factory_fee_probe import fee_basis

ACTIVITY = "https://data-api.polymarket.com/v2/activity"
DEMO = "https://external-api.demo.kalshi.co/trade-api/v2"
WALLET = re.compile(r"0x[0-9a-f]{40}")
DIGEST = re.compile(r"[0-9a-f]{64}")
TICKER = re.compile(r"[A-Z0-9._-]{1,120}")
FIELDS = ("proxy_wallet", "transaction_hash", "condition_id", "token_id", "timestamp",
          "type", "side", "price", "size", "outcome", "title", "slug", "event_slug")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def public_get(url, params=None):
    sent = time.time()
    request = Request(url + ("?" + urlencode(params) if params else ""),
                      headers={"Cache-Control": "no-cache", "Accept": "application/json",
                               "User-Agent": "KalshiWalletResearch/1.0"})
    with build_opener(NoRedirect()).open(request, timeout=3) as response:
        raw = response.read(2_000_001)
        received = time.time()
        if len(raw) > 2_000_000:
            raise ValueError("public_response_too_large")
        return json.loads(raw), sent, received, response.headers.get("Age"), response.headers.get("Date")


def read_trades(wallet, since, read=public_get, *, condition=None):
    params = {"user": wallet, "type": "TRADE", "limit": 100, "start": int(since)}
    if condition is not None:
        params["condition"] = condition
    trades, cursors = [], set()
    for _ in range(3):
        page, _, received, _, _ = read(ACTIVITY, params)
        for row in page["data"]:
            if row.get("proxy_wallet", "").lower() != wallet:
                raise ValueError("wallet_identity_mismatch")
            if row.get("type") != "TRADE" or row.get("side") not in ("BUY", "SELL"):
                continue
            trade = {key: row.get(key) for key in FIELDS}
            trade["proxy_wallet"] = wallet
            if condition is not None and trade["condition_id"] != condition:
                raise ValueError("wallet_activity_condition_mismatch")
            if (type(trade["timestamp"]) is not int or not DIGEST.fullmatch(str(trade["transaction_hash"])[2:])
                    or not str(trade["transaction_hash"]).startswith("0x")
                    or not WALLET.fullmatch(str(trade["proxy_wallet"]))
                    or not re.fullmatch(r"0x[0-9a-f]{64}", str(trade["condition_id"]))
                    or not re.fullmatch(r"[0-9]{1,78}", str(trade["token_id"]))):
                raise ValueError("invalid_wallet_trade_identity")
            trade["observed_at"] = received
            trades.append(trade)
        cursor = page["pagination"]["next_cursor"]
        if not cursor:
            return trades
        if cursor in cursors:
            break
        cursors.add(cursor)
        params["cursor"] = cursor
    raise ValueError("wallet_activity_pagination_incomplete")


def trade_key(trade):
    return digest([trade[k] for k in FIELDS[:6]] + [trade["price"], trade["size"]])


def cents(price):
    return int((Decimal(price.numerator) / Decimal(price.denominator) * 100)
               .to_integral_value(rounding=ROUND_CEILING))


def price_book(frame, side):
    """Use public depth limits rather than the private accounting amount limit."""
    if side not in ("yes", "no"):
        raise ValueError("wallet_book_side_invalid")
    ladders = []
    for outcome in (side, "no" if side == "yes" else "yes"):
        rows = frame["orderbook_fp"][outcome + "_dollars"]
        ladder = levels(rows)
        if not ladder or len(ladder) != len(rows):
            raise ValueError("wallet_book_unavailable")
        price = max(ladder)
        ladders.append((Fraction(price), Fraction(ladder[price])))
    (bid, bid_depth), (opposite_bid, ask_depth) = ladders
    ask = 1 - opposite_bid
    if bid >= ask:
        raise ValueError("crossed_book")
    return bid, ask, bid_depth, ask_depth


def read_quote(mapping, base, read=public_get):
    if base != DEMO:
        raise ValueError("wallet_demo_quote_required")
    ticker = mapping["ticker"]
    if not TICKER.fullmatch(ticker):
        raise ValueError("invalid_wallet_mapping")
    book, sent, received, age, date = read(base + "/markets/" + ticker + "/orderbook", {"depth": 20})
    market = read(base + "/markets/" + ticker)[0]["market"]
    if (market["ticker"] != ticker or market["status"] != "active" or market["market_type"] != "binary"
            or market.get("exchange_index", 0) != mapping.get("exchange_index", 0)
            or market["event_ticker"] != mapping["event_ticker"]
            or any(market.get(k) != v for k, v in mapping["kalshi_rules"].items())):
        raise ValueError("wallet_market_changed")
    if not 0 <= received - sent <= 2 or (age is not None and not 0 <= float(age) <= 2):
        raise ValueError("wallet_book_transport_stale")
    bid, ask, _, depth = price_book(book, mapping["side"])
    return {**book, "ticker": ticker, "side": mapping["side"], "market": market, "started_at": sent, "observed_at": received,
            "http_age": age, "http_date": date, "ask": str(Decimal(ask.numerator) / ask.denominator),
            "depth": str(Decimal(depth.numerator) / depth.denominator),
            "bid": str(Decimal(bid.numerator) / bid.denominator),
            "environment": "demo"}


def read_fee(quote, base, read=public_get):
    from datetime import datetime, timezone
    exchange_index = quote["market"].get("exchange_index", 0)
    if exchange_index != 0:
        raise ValueError("wallet_fee_exchange_unverified")
    event_id = quote["market"]["event_ticker"]
    if not TICKER.fullmatch(event_id):
        raise ValueError("invalid_wallet_event")
    event = read(base + "/events/" + event_id)[0]["event"]
    series_id = event["series_ticker"]
    if not TICKER.fullmatch(series_id):
        raise ValueError("invalid_wallet_series")
    data, _, received, _, _ = read(base + "/series/" + series_id)
    return fee_basis({"event_id": event_id, "price_cents": cents(price_book(quote, quote["side"])[1]),
                      "market_type": "binary", "exchange_index": exchange_index,
                      "observed_at": datetime.fromtimestamp(quote["observed_at"], timezone.utc).isoformat()},
                     event, data["series"], fetched_at=received)
