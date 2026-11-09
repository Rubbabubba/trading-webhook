"""Find full-game NBA/NFL counterparts for public quote comparisons."""
from datetime import datetime
from decimal import Decimal
import json
import re
from zoneinfo import ZoneInfo

SOURCE = "https://gamma-api.polymarket.com/events"
GAME = re.compile(r"(nba|nfl)-([a-z]{2,3})-([a-z]{2,3})-(\d{4}-\d{2}-\d{2})")
SUFFIX = {"moneyline": "GAME", "spreads": "SPREAD", "totals": "TOTAL"}


def discover(trade, base, read):
    slug = trade.get("event_slug")
    match = GAME.fullmatch(slug or "")
    if match is None:
        raise ValueError("unsupported_sports_event")
    league, away, home, slug_date = match.groups()
    events = read(SOURCE, {"slug": slug})[0]
    event, = [event for event in events if event["slug"] == slug]
    source, = [market for market in event["markets"]
               if market["conditionId"] == trade["condition_id"]]
    names = event["title"].split(" vs. ")
    if len(names) != 2 or away == home:
        raise ValueError("sports_teams_unverified")
    teams = dict(zip(names, (away.upper(), home.upper()), strict=True))
    start = datetime.fromisoformat(source["gameStartTime"])
    if start.utcoffset() is None:
        raise ValueError("sports_game_time_unverified")
    game_date = start.astimezone(ZoneInfo("America/New_York")).date()
    if slug_date not in (game_date.isoformat(), start.date().isoformat()):
        raise ValueError("sports_game_date_mismatch")
    outcomes = json.loads(source["outcomes"])
    tokens = json.loads(source["clobTokenIds"])
    if len(tokens) != 2 or len(set(tokens)) != 2 or len(outcomes) != 2 or len(set(outcomes)) != 2:
        raise ValueError("sports_outcomes_unverified")
    outcome = outcomes[tokens.index(trade["token_id"])]
    if outcome.casefold() != str(trade["outcome"]).casefold():
        raise ValueError("sports_token_side_mismatch")
    kind = source["sportsMarketType"]
    if kind not in SUFFIX:
        raise ValueError("unsupported_sports_market_type")
    description = source["description"]
    if re.search(r"regulation only|exclud\w* overtime|overtime (?:does|will) not count",
                 description, re.I):
        raise ValueError("sports_period_mismatch")
    line = None
    side = "yes"
    if kind == "moneyline":
        if source["question"] != event["title"] or set(outcomes) != set(teams):
            raise ValueError("sports_moneyline_unverified")
        team = teams[outcome]
    else:
        line = Decimal(str(source["line"]))
        if not line.is_finite() or abs(line) % 1 != Decimal(".5"):
            raise ValueError("sports_half_point_required")
        if kind == "spreads":
            question = re.fullmatch(r"Spread: (.+) \((-\d+\.5)\)", source["question"])
            if (not question or Decimal(question[2]) != line or question[1] not in teams
                    or set(outcomes) != set(teams)):
                raise ValueError("sports_spread_unverified")
            favorite = question[1]
            team, line = teams[favorite], abs(line)
            side = "yes" if outcome == favorite else "no"
        else:
            if (line <= 0 or source["question"] != f"{event['title']}: O/U {line}"
                    or set(outcomes) != {"Over", "Under"}):
                raise ValueError("sports_total_unverified")
            team = None
            side = "yes" if outcome == "Over" else "no"
    date_code = game_date.strftime("%y%b%d").upper()
    event_id = f"KX{league.upper()}{SUFFIX[kind]}-{date_code}{away.upper()}{home.upper()}"
    page = read(base + "/markets", {"event_ticker": event_id, "limit": 100})[0]
    if page.get("cursor"):
        raise ValueError("sports_market_page_incomplete")
    candidates = []
    for market in page["markets"]:
        ticker = market["ticker"]
        if (market["event_ticker"] != event_id or not ticker.startswith(event_id + "-")
                or market["market_type"] != "binary"):
            continue
        strike = ticker.removeprefix(event_id + "-")
        if kind == "moneyline":
            if strike != team or market.get("strike_type") != "structured":
                continue
        else:
            if (market.get("strike_type") != "greater"
                    or Decimal(str(market.get("floor_strike", "NaN"))) != line
                    or (team is not None and not re.fullmatch(team + r"\d+", strike))):
                continue
        candidates.append(market)
    if len(candidates) != 1:
        raise ValueError("sports_market_missing_or_ambiguous")
    market = candidates[0]
    return {
        "ticker": market["ticker"], "event_ticker": event_id, "side": side,
        "exchange_index": market.get("exchange_index", 0),
        "kalshi_rules": {key: market[key] for key in ("rules_primary", "rules_secondary")},
        "sports_match": {
            "scope": "completed_game_quote_diagnostic", "league": league, "game_date_et": str(game_date),
            "kind": kind, "line": str(line) if line is not None else None,
            "source_description": description, "source_outcome": outcome,
            "settlement_exception_status": "requires_separate_review",
        },
    }
