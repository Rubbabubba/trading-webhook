"""Unauthenticated, read-only Kalshi market discovery for Opportunity Lab."""

from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://external-api.kalshi.com/trade-api/v2"


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


def rank_event_dislocations(events: list[dict], *, category: str = "", minimum_market_count: int = 2) -> dict:
    """Rank complete-set candidates using displayed YES asks; fees remain unmodeled."""
    wanted = category.strip().casefold()
    candidates = []
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
    return {
        "source": "kalshi_public_market_data",
        "events_received": len(events),
        "category_filter": category or None,
        "candidate_count": len(candidates),
        "price_dislocation_count": sum(row["price_dislocation"] for row in candidates),
        "candidates": candidates,
        "skipped": skipped,
        "research_only": True,
        "execution_enabled": False,
    }


def _positive_number(value) -> float | None:
    try:
        number = float(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None


def _get(path: str, params: dict) -> tuple[dict, dict]:
    url = f"{BASE_URL}{path}?{urlencode(params)}"
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "OpportunityLab/1.0"})
    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return payload, {"status_code": response.status, "authenticated": False}
    except HTTPError as exc:
        return {}, {"status_code": exc.code, "authenticated": False, "error": f"kalshi_http_{exc.code}"}
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {}, {"authenticated": False, "error": f"kalshi_transport_error:{type(exc).__name__}"}
