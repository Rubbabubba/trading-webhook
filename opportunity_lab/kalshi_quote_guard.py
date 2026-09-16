"""Validate a timestamped production REST book before an IOC journal transition."""
import math
from fractions import Fraction

from .kalshi_risk import amount, display


def validate(payload, quote, *, now, environment='production'):
    if not isinstance(quote, dict):
        raise ValueError("production_quote_required")
    if environment not in ('production','demo') or quote.get("environment") != environment or quote.get("ticker") != payload["ticker"]:
        raise ValueError("quote_scope_mismatch")
    start, end = quote.get("started_at"), quote.get("observed_at")
    if (any(type(v) not in (int,float) or not math.isfinite(v) for v in (start,end))
            or not 0 <= end-start <= 2 or not 0 <= now.timestamp()-end <= 5):
        raise ValueError("quote_stale_or_inconsistent")
    books = []
    for side in ("yes_dollars", "no_dollars"):
        levels = quote.get("orderbook_fp", {}).get(side)
        if not isinstance(levels, list) or not levels:
            raise ValueError("two_sided_book_required")
        seen, parsed = set(), []
        for level in levels:
            if not isinstance(level, list) or len(level) != 2:
                raise ValueError("invalid_book_level")
            price = amount(level[0])
            from decimal import Decimal, InvalidOperation
            if not isinstance(level[1],str) or len(level[1])>40:
                raise ValueError('invalid_book_level')
            try: depth=Decimal(level[1])
            except InvalidOperation: raise ValueError('invalid_book_level') from None
            if not depth.is_finite() or depth<=0:
                raise ValueError('invalid_book_level')
            # Only need enough depth for this order; large public queue sizes
            # are not account balances and must not inherit their amount ceiling.
            count = Fraction(min(depth,Decimal(payload['count'])))
            if not 0 < price < 1 or count <= 0 or price in seen:
                raise ValueError("invalid_book_level")
            seen.add(price);parsed.append((price,count))
        books.append(parsed)
    bid = max(p for p,q in books[0])
    ask = 1-max(p for p,q in books[1])
    if bid >= ask:
        raise ValueError("crossed_or_locked_book")
    limit, count = amount(payload["price"]), amount(payload["count"])
    if count <= 0 or count.denominator != 1 or not 0 < limit < 1:
        raise ValueError("unsupported_quote_order")
    if payload["side"] == "bid":
        executable = [(1-p,q) for p,q in books[1] if 1-p <= limit]
    elif payload["side"] == "ask":
        executable = [(p,q) for p,q in books[0] if p >= limit]
    else:
        raise ValueError("unsupported_quote_side")
    if sum((q for p,q in executable),Fraction(0)) < count:
        raise ValueError("insufficient_depth_within_limit")
    return {"best_yes_bid":display(bid),"best_yes_ask":display(ask),
            "limit":display(limit),"contracts":int(count),"observed_at":end}
