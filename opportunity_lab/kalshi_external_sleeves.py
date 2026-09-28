"""Pure, fail-closed signal logic for external-research Kalshi sleeves."""
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re


SPORTS_PREFIXES = (
    "KXNCAAF", "KXNFL", "KXMLB", "KXNBA", "KXEPL", "KXMLS",
    "KXATP", "KXWTA", "KXNHL", "KXNCAAB", "KXWNBA",
)
PRICE_BINS = ((2, 5), (5, 10), (90, 95), (95, 98))


def _decimal(value, name):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("invalid_" + name) from None
    if not result.is_finite():
        raise ValueError("invalid_" + name)
    return result


def _iso(value, name):
    if not isinstance(value, str) or not value:
        raise ValueError("missing_" + name)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid_" + name) from None
    if parsed.tzinfo is None:
        raise ValueError("timezone_required")
    return value


def research_relevance(market):
    """Return storage tags for records relevant to registered sleeves."""
    tags = []
    if (market.get("status") == "active" and market.get("market_type") == "binary"
            and market.get("exchange_index", 0) == 0):
        try:
            prices = (_decimal(market.get("yes_ask_dollars"), "yes_ask"),
                      _decimal(market.get("no_ask_dollars"), "no_ask"))
            if any(Decimal(".02") <= price <= Decimal(".98") and
                   (price <= Decimal(".10") or price >= Decimal(".90"))
                   for price in prices):
                tags.append("favorite_longshot")
        except ValueError:
            pass
        if (market.get("strike_type") == "greater" and market.get("rules_primary")
                and market.get("rules_secondary") and market.get("event_ticker")):
            tags.append("nested_threshold")
    return tags


def threshold_identity(market):
    """Normalize only an explicit greater-than strike in otherwise identical rules."""
    if market.get("strike_type") != "greater":
        raise ValueError("unsupported_threshold")
    strike = _decimal(market.get("floor_strike"), "strike")
    primary = market.get("rules_primary")
    if not isinstance(primary, str):
        raise ValueError("missing_rules")
    patterns = list(re.finditer(
        r"(?i)\b(?:above|greater\s+than|exceed(?:s|ed)?)\s+(-?[0-9]+(?:\.[0-9]+)?)\b",
        primary,
    ))
    exact = [match for match in patterns if _decimal(match.group(1), "rule_strike") == strike]
    if len(exact) != 1:
        raise ValueError("unrecognized_rule_template")
    match = exact[0]
    template = primary[:match.start(1)] + "<STRIKE>" + primary[match.end(1):]
    identity = {
        "event_ticker": market.get("event_ticker"),
        "template": template,
        "rules_secondary": market.get("rules_secondary"),
        "close_time": _iso(market.get("close_time"), "close_time"),
        "expiration_time": _iso(market.get("expiration_time"), "expiration_time"),
        "occurrence_datetime": _iso(market.get("occurrence_datetime"), "occurrence_datetime"),
        "exchange_index": market.get("exchange_index", 0),
    }
    if not identity["event_ticker"] or not identity["rules_secondary"]:
        raise ValueError("missing_identity")
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest(), strike


def structural_candidates(markets, *, fee_cents_per_leg=2, unfinished_leg_stress_cents=2):
    """Find indicative nested-threshold violations; fresh books remain mandatory."""
    groups = defaultdict(list)
    for market in markets:
        try:
            identity, strike = threshold_identity(market)
            if market.get("status") != "active":
                continue
            groups[identity].append((strike, market))
        except ValueError:
            continue
    result = []
    for identity, rows in groups.items():
        rows.sort(key=lambda item: item[0])
        for index, (low_strike, low) in enumerate(rows):
            for high_strike, high in rows[index + 1:]:
                try:
                    yes_low = round(_decimal(low["yes_ask_dollars"], "yes_ask") * 100)
                    no_high = round(_decimal(high["no_ask_dollars"], "no_ask") * 100)
                    raw = 100 - int(yes_low) - int(no_high)
                    stressed = raw - 2 * fee_cents_per_leg - unfinished_leg_stress_cents
                except (KeyError, ValueError):
                    continue
                if stressed >= 1:
                    result.append({
                        "relationship_id": identity,
                        "event_id": low["event_ticker"],
                        "low_ticker": low["ticker"],
                        "high_ticker": high["ticker"],
                        "low_strike": str(low_strike),
                        "high_strike": str(high_strike),
                        "indicative_surplus_cents": raw,
                        "indicative_stressed_surplus_cents": stressed,
                        "execution_enabled": False,
                        "fresh_book_required": True,
                    })
    return sorted(result, key=lambda row: (-row["indicative_stressed_surplus_cents"],
                                            row["low_ticker"], row["high_ticker"]))


def _fresh_ask(quote, side):
    book = quote.get("orderbook_fp") or {}
    opposite = "no_dollars" if side == "yes" else "yes_dollars"
    levels = book.get(opposite) or []
    parsed = [(_decimal(row[0], "book_price"), _decimal(row[1], "book_depth"))
              for row in levels if isinstance(row, (list, tuple)) and len(row) >= 2]
    if not parsed:
        raise ValueError("missing_executable_depth")
    bid, depth = max(parsed, key=lambda row: row[0])
    ask_cents = int(round((Decimal(1) - bid) * 100))
    if not 0 < ask_cents < 100 or depth < 1:
        raise ValueError("invalid_executable_depth")
    return ask_cents, depth


def confirm_structural_candidate(candidate, low_quote, high_quote, *, now,
                                 fee_cents_per_leg=2, unfinished_leg_stress_cents=2):
    """Require current matching metadata and near-synchronous executable depth."""
    low, high = low_quote.get("market") or {}, high_quote.get("market") or {}
    low_identity, low_strike = threshold_identity(low)
    high_identity, high_strike = threshold_identity(high)
    if (low_identity != candidate.get("relationship_id") or low_identity != high_identity
            or low_strike >= high_strike or low.get("ticker") != candidate.get("low_ticker")
            or high.get("ticker") != candidate.get("high_ticker")):
        raise ValueError("relationship_changed")
    times = [float(low_quote["observed_at"]), float(high_quote["observed_at"])]
    if any(not 0 <= now - observed <= 10 for observed in times) or abs(times[0] - times[1]) > 5:
        raise ValueError("stale_or_asynchronous_books")
    yes_low, low_depth = _fresh_ask(low_quote, "yes")
    no_high, high_depth = _fresh_ask(high_quote, "no")
    raw = 100 - yes_low - no_high
    stressed = raw - 2 * fee_cents_per_leg - unfinished_leg_stress_cents
    if stressed < 1:
        raise ValueError("fresh_surplus_below_gate")
    return {
        **candidate,
        "yes_low_ask_cents": yes_low,
        "no_high_ask_cents": no_high,
        "executable_contracts": str(min(low_depth, high_depth)),
        "fresh_surplus_cents": raw,
        "fresh_stressed_surplus_cents": stressed,
        "observed_at": max(times),
        "decision_bucket": int(max(times)) - int(max(times)) % 3600,
        "fully_executable_snapshot": True,
        "execution_enabled": False,
        "fill_assumed": False,
    }


def favorite_longshot_observations(market, observed_at, *, bucket_seconds=3600):
    """Create symmetric side observations without assuming a fill or an edge."""
    _iso(observed_at, "observed_at")
    event_id = market.get("event_ticker")
    ticker = market.get("ticker")
    if not event_id or not ticker or market.get("status") != "active":
        return []
    timestamp = int(datetime.fromisoformat(observed_at.replace("Z", "+00:00")).timestamp())
    bucket = timestamp - timestamp % bucket_seconds
    family = market.get("category") or event_id.split("-")[0]
    stratum = "sports" if event_id.upper().startswith(SPORTS_PREFIXES) else "non_sports"
    result = []
    for side in ("yes", "no"):
        try:
            cents = int(round(_decimal(market.get(side + "_ask_dollars"), side + "_ask") * 100))
        except ValueError:
            continue
        bin_name = next((f"{low}-{high}" for low, high in PRICE_BINS if low <= cents <= high), None)
        if bin_name is None:
            continue
        result.append({
            "observation_id": f"{ticker}:{side}:{bucket}",
            "event_id": event_id,
            "ticker": ticker,
            "side": side,
            "price_cents": cents,
            "price_bin": bin_name,
            "classification": "favorite" if cents >= 90 else "longshot",
            "family": family,
            "stratum": stratum,
            "observed_at": observed_at,
            "expiration_time": market.get("expiration_time"),
            "decision_bucket": bucket,
            "execution_enabled": False,
            "fill_assumed": False,
        })
    return result
