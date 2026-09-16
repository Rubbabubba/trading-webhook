"""Validate a non-crossing demo-only post-only GTC acceptance order."""
import math
from fractions import Fraction

from .kalshi_risk import amount, display


def validate(payload, quote, *, now, environment="demo"):
    if environment != "demo" or not isinstance(quote, dict) or quote.get("environment") != "demo":
        raise ValueError("demo_quote_required")
    if quote.get("ticker") != payload.get("ticker"):
        raise ValueError("quote_scope_mismatch")
    if payload.get("time_in_force") != "good_till_canceled" or payload.get("post_only") is not True \
            or payload.get("cancel_order_on_pause") is not True:
        raise ValueError("post_only_gtc_required")
    start, end = quote.get("started_at"), quote.get("observed_at")
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in (start, end)) \
            or not 0 <= end - start <= 2 or not 0 <= now.timestamp() - end <= 5:
        raise ValueError("quote_stale_or_inconsistent")
    books = []
    for name in ("yes_dollars", "no_dollars"):
        levels = quote.get("orderbook_fp", {}).get(name)
        if not isinstance(levels, list) or not levels:
            raise ValueError("two_sided_book_required")
        parsed = []
        for level in levels:
            if not isinstance(level, list) or len(level) != 2:
                raise ValueError("invalid_book_level")
            price, depth = amount(level[0]), amount(level[1])
            if not 0 < price < 1 or depth <= 0:
                raise ValueError("invalid_book_level")
            parsed.append((price, depth))
        books.append(parsed)
    best_bid = max(price for price, _depth in books[0])
    best_ask = 1 - max(price for price, _depth in books[1])
    if best_bid >= best_ask:
        raise ValueError("crossed_or_locked_book")
    limit = amount(payload.get("price"))
    count = amount(payload.get("count"))
    if count != 1 or not 0 < limit < 1:
        raise ValueError("acceptance_order_must_be_one_contract")
    side = payload.get("side")
    if side == "bid" and limit >= best_ask:
        raise ValueError("post_only_order_would_cross")
    if side == "ask" and limit <= best_bid:
        raise ValueError("post_only_order_would_cross")
    if side not in ("bid", "ask"):
        raise ValueError("unsupported_quote_side")
    return {
        "best_yes_bid": display(best_bid), "best_yes_ask": display(best_ask),
        "limit": display(limit), "contracts": int(Fraction(count)), "observed_at": end,
        "post_only": True,
    }
