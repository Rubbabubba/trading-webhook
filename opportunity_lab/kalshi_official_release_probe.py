"""Prospective, read-only BEA release and Kalshi Demo quote capture.

This is data collection for a proposed strategy, not a rule parser, signal,
backtest, or order path. A scheduled date and a matching ticker are insufficient
to establish the contract's settlement value or an executable edge.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
from decimal import Decimal, InvalidOperation
from urllib.request import Request, urlopen


BEA_SCHEDULE_URL = "https://apps.bea.gov/API/signup/release_dates.json"
RELEASE_SERIES = {
    "Gross Domestic Product": ("KXGDP-",),
    "Personal Income and Outlays": ("KXPCE-", "KXPCECORE-"),
}
REFRESH_HOURS = 6
WINDOW_BEFORE = timedelta(minutes=10)
WINDOW_AFTER = timedelta(minutes=10)
MAX_QUOTES_PER_CYCLE = 2
GDP_THRESHOLD = re.compile(r"(?i)\b(?:more than|greater than|exceed(?:s)?)\s+(-?[0-9]+(?:\.[0-9]+)?)\s*%?")


def _utc(value):
    if value.tzinfo is None:
        raise ValueError("timezone_required")
    return value.astimezone(timezone.utc)


def init(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS official_release_schedule(
            release_name TEXT NOT NULL,scheduled_at TEXT NOT NULL,
            fetched_at TEXT NOT NULL,source_url TEXT NOT NULL,
            PRIMARY KEY(release_name,scheduled_at));
        CREATE TABLE IF NOT EXISTS official_release_watchlist(
            release_name TEXT NOT NULL,scheduled_at TEXT NOT NULL,
            ticker TEXT NOT NULL,event_ticker TEXT NOT NULL,
            close_at TEXT NOT NULL,rules_sha256 TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            PRIMARY KEY(release_name,scheduled_at,ticker));
        CREATE TABLE IF NOT EXISTS official_release_contract_terms(
            release_name TEXT NOT NULL,scheduled_at TEXT NOT NULL,
            ticker TEXT NOT NULL,rules_sha256 TEXT NOT NULL,
            terms_json TEXT NOT NULL,first_seen_at TEXT NOT NULL,
            PRIMARY KEY(release_name,scheduled_at,ticker,rules_sha256));
        CREATE TABLE IF NOT EXISTS official_release_quotes(
            release_name TEXT NOT NULL,scheduled_at TEXT NOT NULL,
            ticker TEXT NOT NULL,observed_at TEXT NOT NULL,
            request_started_at TEXT NOT NULL,rules_sha256 TEXT NOT NULL,
            book_json TEXT NOT NULL,
            PRIMARY KEY(release_name,scheduled_at,ticker,observed_at));
        CREATE INDEX IF NOT EXISTS official_release_quote_event_idx
            ON official_release_quotes(release_name,scheduled_at,ticker);
    """)


def fetch_bea_schedule():
    request = Request(BEA_SCHEDULE_URL, headers={"User-Agent": "KalshiDemoResearch/1.0"})
    with urlopen(request, timeout=8) as response:
        if response.url != BEA_SCHEDULE_URL:
            raise ValueError("bea_schedule_redirected")
        body = response.read(131073)
    if len(body) > 131072:
        raise ValueError("bea_schedule_too_large")
    return json.loads(body)


def parse_schedule(payload, now):
    if not isinstance(payload, dict):
        raise ValueError("invalid_bea_schedule")
    at = _utc(now)
    result = []
    for name in RELEASE_SERIES:
        item = payload.get(name)
        if not isinstance(item, dict) or not isinstance(item.get("release_dates"), list):
            raise ValueError("missing_bea_release_series")
        if len(item["release_dates"]) > 100:
            raise ValueError("bea_release_dates_unbounded")
        for raw in item["release_dates"]:
            if not isinstance(raw, str):
                raise ValueError("invalid_bea_release_date")
            release = _utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
            if at - timedelta(days=1) <= release <= at + timedelta(days=180):
                result.append((name, release.isoformat()))
    return sorted(set(result), key=lambda row: (row[1], row[0]))


def refresh_schedule(db, now, fetcher=fetch_bea_schedule):
    at = _utc(now)
    row = db.execute("SELECT max(fetched_at) FROM official_release_schedule").fetchone()
    if row and row[0] and at - datetime.fromisoformat(row[0]) < timedelta(hours=REFRESH_HOURS):
        return False
    dates = parse_schedule(fetcher(), at)
    if not dates:
        raise ValueError("bea_schedule_has_no_upcoming_releases")
    for name, scheduled in dates:
        db.execute("INSERT INTO official_release_schedule VALUES(?,?,?,?) "
                   "ON CONFLICT(release_name,scheduled_at) DO UPDATE SET fetched_at=excluded.fetched_at",
                   (name, scheduled, at.isoformat(), BEA_SCHEDULE_URL))
    return True


def _market_match(name, release_at, market):
    ticker = market.get("ticker")
    if not isinstance(ticker, str) or not ticker.startswith(RELEASE_SERIES[name]):
        return None
    if market.get("status") != "active" or market.get("market_type") != "binary":
        return None
    rules = market.get("rules_primary")
    close = market.get("close_time")
    if not isinstance(rules, str) or "bea" not in rules.lower() or not isinstance(close, str):
        return None
    try:
        close_at = _utc(datetime.fromisoformat(close.replace("Z", "+00:00")))
    except ValueError:
        return None
    # A contract that closes before publication cannot support this hypothesis.
    if not release_at <= close_at <= release_at + timedelta(days=2):
        return None
    event = market.get("event_ticker")
    if not isinstance(event, str) or len(event) > 100:
        return None
    terms = [market.get("title"), market.get("rules_primary"), market.get("rules_secondary")]
    if any(value is not None and (not isinstance(value, str) or len(value) > 8000)
           for value in terms):
        return None
    raw_terms = {"title": terms[0], "rules_primary": terms[1],
                 "rules_secondary": terms[2], "strike_type": market.get("strike_type"),
                 "floor_strike": market.get("floor_strike")}
    digest = hashlib.sha256(json.dumps(raw_terms, sort_keys=True).encode()).hexdigest()
    mapping = _rule_mapping(name, market)
    record = json.dumps({**raw_terms, "mapping": mapping}, sort_keys=True)
    return ticker, event, close_at.isoformat(), digest, record


def _rule_mapping(name, market):
    """Recognize one exact GDP threshold template; never infer an outcome."""
    if name != "Gross Domestic Product" or market.get("strike_type") != "greater":
        return {"state": "unverified"}
    rules = market.get("rules_primary") or ""
    lowered = rules.lower()
    if (("real gdp" not in lowered and "real gross domestic product" not in lowered)
            or "advance estimate" not in lowered or "bea" not in lowered):
        return {"state": "unverified"}
    matches = GDP_THRESHOLD.findall(rules)
    if len(matches) != 1:
        return {"state": "unverified"}
    try:
        strike = Decimal(str(market.get("floor_strike")))
        threshold = Decimal(matches[0])
    except (InvalidOperation, TypeError, ValueError):
        return {"state": "unverified"}
    if not strike.is_finite() or strike != threshold or abs(strike) > 100:
        return {"state": "unverified"}
    return {"state": "template_parsed_outcome_unverified",
            "comparison": "greater_than", "threshold_percent": str(threshold)}


def register_watchlist(db, markets, now):
    at = _utc(now)
    releases = [(name, raw, datetime.fromisoformat(raw)) for name, raw in db.execute(
        "SELECT release_name,scheduled_at FROM official_release_schedule WHERE scheduled_at>=? "
        "AND scheduled_at<=? ORDER BY scheduled_at LIMIT 8",
        ((at - timedelta(hours=12)).isoformat(), (at + timedelta(days=45)).isoformat()),
    )]
    added = 0
    for name, scheduled, release_at in releases:
        # Reuse the maker's existing catalog. Limit persisted contracts per
        # release so the separate research collector cannot grow unchecked.
        matches = []
        for market in markets:
            match = _market_match(name, release_at, market)
            if match:
                matches.append(match)
        for ticker, event, close_at, digest, terms_json in sorted(matches)[:24]:
            db.execute("INSERT OR IGNORE INTO official_release_contract_terms VALUES(?,?,?,?,?,?)",
                       (name, scheduled, ticker, digest, terms_json, at.isoformat()))
            cursor = db.execute("INSERT OR IGNORE INTO official_release_watchlist VALUES(?,?,?,?,?,?,?)",
                                (name, scheduled, ticker, event, close_at, digest, at.isoformat()))
            added += cursor.rowcount
            if cursor.rowcount == 0:
                db.execute("UPDATE official_release_watchlist SET event_ticker=?,close_at=?,rules_sha256=? "
                           "WHERE release_name=? AND scheduled_at=? AND ticker=? AND rules_sha256<>?",
                           (event, close_at, digest, name, scheduled, ticker, digest))
    return added


def _book_levels(raw):
    if not isinstance(raw, dict):
        raise ValueError("invalid_demo_orderbook")
    result = {}
    for side in ("yes_dollars", "no_dollars"):
        levels = raw.get(side)
        if not isinstance(levels, list):
            raise ValueError("invalid_demo_orderbook")
        clean = []
        for level in levels[:5]:
            if not isinstance(level, list) or len(level) < 2:
                raise ValueError("invalid_demo_orderbook")
            price, size = float(level[0]), float(level[1])
            if not math.isfinite(price) or not math.isfinite(size) or not 0 <= price <= 1 or size < 0:
                raise ValueError("invalid_demo_orderbook")
            clean.append([str(level[0]), str(level[1])])
        result[side] = clean
    return result


def capture_quotes(db, client, now):
    at = _utc(now)
    rows = db.execute(
        "SELECT release_name,scheduled_at,ticker,rules_sha256 FROM official_release_watchlist "
        "WHERE scheduled_at>=? AND scheduled_at<=? ORDER BY scheduled_at,ticker",
        ((at - WINDOW_AFTER).isoformat(), (at + WINDOW_BEFORE).isoformat()),
    ).fetchall()
    captured = 0
    for name, scheduled, ticker, digest in rows:
        if captured >= MAX_QUOTES_PER_CYCLE:
            break
        # One per ticker per minute, without holding up the maker order loop.
        previous = db.execute(
            "SELECT max(observed_at) FROM official_release_quotes "
            "WHERE release_name=? AND scheduled_at=? AND ticker=?",
            (name, scheduled, ticker),
        ).fetchone()[0]
        if previous and at - datetime.fromisoformat(previous) < timedelta(seconds=55):
            continue
        quote = client.quote({"ticker": ticker})
        if quote.get("environment") != "demo" or quote.get("ticker") != ticker:
            raise ValueError("non_demo_quote")
        observed = datetime.fromtimestamp(float(quote["observed_at"]), timezone.utc)
        started = datetime.fromtimestamp(float(quote["started_at"]), timezone.utc)
        if not started <= observed <= _utc(datetime.now(timezone.utc)) + timedelta(seconds=5):
            raise ValueError("invalid_quote_clock")
        book = _book_levels(quote.get("orderbook_fp"))
        cursor = db.execute("INSERT OR IGNORE INTO official_release_quotes VALUES(?,?,?,?,?,?,?)",
                            (name, scheduled, ticker, observed.isoformat(), started.isoformat(),
                             digest, json.dumps(book, sort_keys=True)))
        captured += cursor.rowcount
    return captured


def cycle(db, client, markets, now, fetcher=fetch_bea_schedule):
    at = _utc(now)
    refreshed = refresh_schedule(db, at, fetcher)
    added = register_watchlist(db, markets, at)
    quotes = capture_quotes(db, client, at)
    return {"schedule_refreshed": refreshed, "watchlist_added": added,
            "quotes_captured": quotes}


def status(db, now):
    at = _utc(now)
    next_rows = db.execute(
        "SELECT s.release_name,s.scheduled_at,count(w.ticker),"
        "sum(CASE WHEN (julianday(w.close_at)-julianday(s.scheduled_at))*24>2 "
        "THEN 1 ELSE 0 END) "
        "FROM official_release_schedule s LEFT JOIN official_release_watchlist w "
        "ON w.release_name=s.release_name AND w.scheduled_at=s.scheduled_at "
        "WHERE s.scheduled_at>=? GROUP BY s.release_name,s.scheduled_at "
        "ORDER BY s.scheduled_at LIMIT 3", (at.isoformat(),),
    ).fetchall()
    return {
        "schema": "kalshi_official_release_probe_v1", "execution_enabled": False,
        "source": "BEA official release calendar", "source_url": BEA_SCHEDULE_URL,
        "latest_schedule_at": db.execute("SELECT max(fetched_at) FROM official_release_schedule").fetchone()[0],
        "watchlist_contracts": db.execute("SELECT count(*) FROM official_release_watchlist").fetchone()[0],
        "contract_rule_versions": db.execute("SELECT count(*) FROM official_release_contract_terms").fetchone()[0],
        "contracts_with_rule_changes": db.execute(
            "SELECT count(*) FROM (SELECT 1 FROM official_release_contract_terms "
            "GROUP BY release_name,scheduled_at,ticker HAVING count(*)>1)").fetchone()[0],
        "parsed_gdp_templates": db.execute(
            "SELECT count(*) FROM official_release_watchlist w "
            "JOIN official_release_contract_terms t ON t.release_name=w.release_name "
            "AND t.scheduled_at=w.scheduled_at AND t.ticker=w.ticker "
            "AND t.rules_sha256=w.rules_sha256 "
            "WHERE json_extract(t.terms_json,'$.mapping.state')="
            "'template_parsed_outcome_unverified'").fetchone()[0],
        "demo_quote_snapshots": db.execute("SELECT count(*) FROM official_release_quotes").fetchone()[0],
        "next_releases": [{"name": name, "scheduled_at": scheduled,
                           "watchlist_contracts": count, "close_timing_review": review or 0}
                          for name, scheduled, count, review in next_rows],
        "research_state": "capture_only_rule_mapping_unverified",
        "profitability_evidence": False,
    }
