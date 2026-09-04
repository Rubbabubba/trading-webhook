"""Unauthenticated, read-only Kalshi market discovery for Opportunity Lab."""

from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://external-api.kalshi.com/trade-api/v2"


def fetch_series_markets(series_ticker: str, *, limit: int = 1000) -> tuple[list[dict], dict, dict]:
    """Fetch active markets plus series metadata for one recurring Kalshi series."""
    series_payload, series_transport = _get(f"/series/{series_ticker}", {})
    if series_transport.get("error"):
        return [], {}, {**series_transport, "path": f"/trade-api/v2/series/{series_ticker}"}
    market_payload, market_transport = _get("/markets", {
        "series_ticker": series_ticker, "status": "open", "limit": max(1, min(1000, int(limit))),
    })
    transport = {**market_transport, "method": "kalshi_public_rest", "path": "/trade-api/v2/markets",
                 "series_ticker": series_ticker, "market_count": len(market_payload.get("markets") or []),
                 "authenticated": False}
    return market_payload.get("markets") or [], series_payload.get("series") or {}, transport


def fetch_settled_series_markets(series_ticker: str, *, min_settled_ts: int, limit: int = 1000) -> tuple[list[dict], dict]:
    payload, transport = _get("/markets", {"series_ticker": series_ticker, "status": "settled",
                                           "min_settled_ts": int(min_settled_ts),
                                           "limit": max(1, min(1000, int(limit)))})
    markets = payload.get("markets") or []
    return markets, {**transport, "method": "kalshi_public_rest", "path": "/trade-api/v2/markets",
                     "series_ticker": series_ticker, "status_filter": "settled", "market_count": len(markets),
                     "authenticated": False}


def fetch_event_candlesticks(series_ticker: str, event_ticker: str, *, start_ts: int, end_ts: int,
                             period_interval: int = 60) -> tuple[dict[str, list[dict]], dict]:
    """Fetch public bid/ask candles for all markets in one event."""
    path = f"/series/{series_ticker}/events/{event_ticker}/candlesticks"
    payload, transport = _get(path, {"start_ts": int(start_ts), "end_ts": int(end_ts),
                                     "period_interval": int(period_interval)})
    tickers = payload.get("market_tickers") or []
    series = payload.get("market_candlesticks") or []
    candles = {str(ticker): rows for ticker, rows in zip(tickers, series) if isinstance(rows, list)}
    return candles, {**transport, "method": "kalshi_public_rest", "path": f"/trade-api/v2{path}",
                     "series_ticker": series_ticker, "event_ticker": event_ticker,
                     "market_count": len(candles), "authenticated": False}


def fetch_open_events(*, limit: int = 200, pages: int = 1) -> tuple[list[dict], dict]:
    """Fetch open events and nested quotes without credentials or account data."""
    limit = max(1, min(200, int(limit)))
    pages = max(1, min(10, int(pages)))
    events: list[dict] = []
    cursor = ""
    page_count = 0
    for _ in range(pages):
        params = {"status": "open", "limit": limit, "with_nested_markets": "true"}
        if cursor:
            params["cursor"] = cursor
        payload, transport = _get("/events", params)
        if transport.get("error"):
            return events, {**transport, "pages": page_count, "event_count": len(events)}
        events.extend(payload.get("events") or [])
        page_count += 1
        cursor = str(payload.get("cursor") or "")
        if not cursor:
            break
    return events, {
        "method": "kalshi_public_rest",
        "path": "/trade-api/v2/events",
        "authenticated": False,
        "pages": page_count,
        "event_count": len(events),
        "more_available": bool(cursor),
    }


def fetch_recent_trades(*, min_ts: int, limit: int = 1000, pages: int = 3) -> tuple[list[dict], dict]:
    """Fetch public trade prints after a Unix timestamp."""
    limit = max(1, min(1000, int(limit)))
    pages = max(1, min(10, int(pages)))
    trades, cursor, page_count = [], "", 0
    for _ in range(pages):
        params = {"min_ts": int(min_ts), "limit": limit}
        if cursor:
            params["cursor"] = cursor
        payload, transport = _get("/markets/trades", params)
        if transport.get("error"):
            return trades, {**transport, "pages": page_count, "trade_count": len(trades)}
        trades.extend(payload.get("trades") or [])
        page_count += 1
        cursor = str(payload.get("cursor") or "")
        if not cursor:
            break
    return trades, {"method": "kalshi_public_rest", "path": "/trade-api/v2/markets/trades",
                    "authenticated": False, "pages": page_count, "trade_count": len(trades),
                    "more_available": bool(cursor)}


def rank_event_dislocations(events: list[dict], *, category: str = "", minimum_market_count: int = 2, now: datetime | None = None) -> dict:
    """Rank complete-set candidates using displayed YES asks; fees remain unmodeled."""
    wanted = category.strip().casefold()
    candidates = []
    no_pair_candidates = []
    closest_no_pairs = []
    now = now or datetime.now(timezone.utc)
    skipped = {"category": 0, "not_mutually_exclusive": 0, "too_few_quoted_markets": 0}
    for event in events:
        event_category = str(event.get("category") or "")
        if wanted and event_category.casefold() != wanted:
            skipped["category"] += 1
            continue
        if event.get("mutually_exclusive") is not True:
            skipped["not_mutually_exclusive"] += 1
            continue
        legs = []
        for market in event.get("markets") or []:
            ask = _positive_number(market.get("yes_ask_dollars"))
            size = _positive_number(market.get("yes_ask_size_fp"))
            if ask is None or ask >= 1 or size is None:
                continue
            legs.append({
                "ticker": market.get("ticker"),
                "outcome": market.get("yes_sub_title") or market.get("title") or market.get("ticker"),
                "yes_ask_dollars": ask,
                "yes_ask_size_contracts": size,
                "close_time": market.get("close_time"),
                "rules_primary": market.get("rules_primary"),
                "rules_secondary": market.get("rules_secondary"),
            })
        pair_legs = []
        for market in event.get("markets") or []:
            no_ask = _positive_number(market.get("no_ask_dollars"))
            if no_ask is None:
                yes_bid = _positive_number(market.get("yes_bid_dollars"))
                no_ask = 1.0 - yes_bid if yes_bid is not None else None
            size = _positive_number(market.get("yes_bid_size_fp"))
            if no_ask is None or no_ask >= 1 or size is None:
                continue
            pair_legs.append({
                "ticker": market.get("ticker"),
                "outcome": market.get("yes_sub_title") or market.get("title") or market.get("ticker"),
                "no_ask_dollars": no_ask,
                "no_ask_size_contracts": size,
                "close_time": market.get("close_time"),
            })
        event_best_pair = None
        for left_index, left in enumerate(pair_legs):
            for right in pair_legs[left_index + 1:]:
                contracts = math.floor(min(left["no_ask_size_contracts"], right["no_ask_size_contracts"]) * 100) / 100
                fees = _taker_fee(left["no_ask_dollars"], contracts) + _taker_fee(right["no_ask_dollars"], contracts)
                cost = contracts * (left["no_ask_dollars"] + right["no_ask_dollars"])
                net_profit = contracts - cost - fees
                settlement = _latest_time(left.get("close_time"), right.get("close_time"))
                days = max((settlement - now).total_seconds() / 86400, 1 / 24) if settlement else None
                roi = net_profit / (cost + fees) * 100 if cost + fees else 0
                annualized = ((1 + roi / 100) ** (365 / days) - 1) * 100 if days and roi > -100 else None
                pair = {
                    "event_ticker": event.get("event_ticker"), "title": event.get("title"), "category": event_category,
                    "strategy": "buy_no_on_two_mutually_exclusive_outcomes", "contracts": contracts,
                    "cost_before_fees": round(cost, 4), "estimated_taker_fees": round(fees, 4),
                    "minimum_settlement_payout": round(contracts, 4), "estimated_net_profit": round(net_profit, 4),
                    "estimated_net_roi_pct": round(roi, 6), "days_to_latest_close": round(days, 3) if days else None,
                    "annualized_return_pct": round(annualized, 6) if annualized is not None else None,
                    "shortfall_to_break_even": round(max(0, -net_profit), 4),
                    "profitable_after_estimated_fees": net_profit > 0,
                    "legs": [left, right], "eligible": False,
                    "blockers": (["series_specific_fee_not_verified", "account_and_jurisdiction_not_verified"]
                                 if net_profit > 0 else ["not_profitable_after_estimated_fees"]),
                }
                if event_best_pair is None or pair["estimated_net_roi_pct"] > event_best_pair["estimated_net_roi_pct"]:
                    event_best_pair = pair
                if net_profit > 0:
                    no_pair_candidates.append(pair)
        if event_best_pair is not None:
            closest_no_pairs.append(event_best_pair)
        if len(legs) < minimum_market_count:
            skipped["too_few_quoted_markets"] += 1
            continue
        ask_sum = sum(leg["yes_ask_dollars"] for leg in legs)
        gross_profit_per_set = 1.0 - ask_sum
        max_sets = min(leg["yes_ask_size_contracts"] for leg in legs)
        candidates.append({
            "event_ticker": event.get("event_ticker"),
            "title": event.get("title"),
            "category": event_category,
            "market_count": len(legs),
            "displayed_yes_ask_sum": round(ask_sum, 6),
            "gross_profit_per_complete_set": round(gross_profit_per_set, 6),
            "gross_roi_pct_before_fees": round((gross_profit_per_set / ask_sum * 100) if ask_sum else 0, 6),
            "max_displayed_complete_sets": max_sets,
            "gross_profit_at_displayed_size": round(gross_profit_per_set * max_sets, 4),
            "price_dislocation": gross_profit_per_set > 0,
            "eligible": False,
            "blockers": ["event_exhaustiveness_not_verified", "fees_not_modeled", "account_and_jurisdiction_not_verified"],
            "collateral_return_type": event.get("collateral_return_type"),
            "legs": legs,
        })
    candidates.sort(key=lambda row: (row["gross_profit_at_displayed_size"], row["gross_roi_pct_before_fees"]), reverse=True)
    no_pair_candidates.sort(key=lambda row: (row["annualized_return_pct"] or -1, row["estimated_net_profit"]), reverse=True)
    closest_no_pairs.sort(key=lambda row: (row["estimated_net_roi_pct"], row["estimated_net_profit"]), reverse=True)
    return {
        "source": "kalshi_public_market_data",
        "events_received": len(events),
        "category_filter": category or None,
        "candidate_count": len(candidates),
        "price_dislocation_count": sum(row["price_dislocation"] for row in candidates),
        "candidates": candidates,
        "mutually_exclusive_no_pair_count": len(no_pair_candidates),
        "mutually_exclusive_no_pairs": no_pair_candidates[:50],
        "closest_no_pair_count": len(closest_no_pairs),
        "closest_no_pairs": closest_no_pairs,
        "fee_model": "conservative_general_taker_estimate: ceil_to_cent(0.07*C*P*(1-P)) per leg",
        "skipped": skipped,
        "research_only": True,
        "execution_enabled": False,
    }


def _taker_fee(price: float, contracts: float) -> float:
    return math.ceil((0.07 * contracts * price * (1 - price)) * 100 - 1e-12) / 100


def _latest_time(*values: str | None) -> datetime | None:
    parsed = []
    for value in values:
        if not value:
            continue
        try:
            parsed.append(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            continue
    return max(parsed) if parsed else None


def _positive_number(value) -> float | None:
    try:
        number = float(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None


def _get(path: str, params: dict, *, _retry: bool = True) -> tuple[dict, dict]:
    url = f"{BASE_URL}{path}?{urlencode(params)}"
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "OpportunityLab/1.0"})
    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return payload, {"status_code": response.status, "authenticated": False}
    except HTTPError as exc:
        if exc.code == 429 and _retry:
            try:
                delay = min(2.0, max(.25, float(exc.headers.get("Retry-After") or 1)))
            except (TypeError, ValueError):
                delay = 1.0
            time.sleep(delay)
            return _get(path, params, _retry=False)
        return {}, {"status_code": exc.code, "authenticated": False, "error": f"kalshi_http_{exc.code}"}
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {}, {"authenticated": False, "error": f"kalshi_transport_error:{type(exc).__name__}"}
