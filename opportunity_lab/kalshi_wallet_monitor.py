"""Bounded continuous public wallet observation. This module has no order transport."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, ROUND_CEILING
from email.utils import parsedate_to_datetime
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading
import time
from urllib.parse import urlencode
from urllib.request import Request, build_opener

from .kalshi_account_monitor import NoRedirect
from .kalshi_demo_stream_source import levels
from .kalshi_factory_fee_probe import fee_basis
from .kalshi_process_lock import acquire

ACTIVITY = "https://data-api.polymarket.com/v2/activity"
POLY_CLOCK = "https://clob.polymarket.com/time"
PRODUCTION = "https://external-api.kalshi.com/trade-api/v2"
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


def clock_bounds(read=public_get):
    offsets = []
    for url in (POLY_CLOCK, PRODUCTION + "/exchange/status"):
        body, sent, received, age, date = read(url)
        if not 0 <= received - sent <= .75 or age not in (None, "0") or not date:
            raise ValueError("wallet_clock_unverified")
        edge = parsedate_to_datetime(date).timestamp()
        if url == POLY_CLOCK and (type(body) is not int or body != edge):
            raise ValueError("wallet_clock_disagreement")
        offsets.append((sent - edge - 1, received - edge))
    low, high = max(v[0] for v in offsets), min(v[1] for v in offsets)
    if not -10 <= low <= high <= 10 or high - low > 1.5:
        raise ValueError("wallet_clock_disagreement")
    return low - .1, high + .1


def read_buys(wallet, since, read=public_get):
    params = {"user": wallet, "type": "TRADE", "limit": 100, "start": int(since)}
    buys, cursors = [], set()
    for _ in range(3):
        page, _, received, _, _ = read(ACTIVITY, params)
        for row in page["data"]:
            if row.get("proxy_wallet", "").lower() != wallet:
                raise ValueError("wallet_identity_mismatch")
            if row.get("type") != "TRADE" or row.get("side") != "BUY":
                continue
            trade = {key: row.get(key) for key in FIELDS}
            trade["proxy_wallet"] = wallet
            if (type(trade["timestamp"]) is not int or not DIGEST.fullmatch(str(trade["transaction_hash"])[2:])
                    or not str(trade["transaction_hash"]).startswith("0x")
                    or not WALLET.fullmatch(str(trade["proxy_wallet"]))
                    or not re.fullmatch(r"0x[0-9a-f]{64}", str(trade["condition_id"]))
                    or not re.fullmatch(r"[0-9]{1,78}", str(trade["token_id"]))):
                raise ValueError("invalid_wallet_trade_identity")
            trade["observed_at"] = received
            buys.append(trade)
        cursor = page["pagination"]["next_cursor"]
        if not cursor:
            return buys
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


def read_quote(mapping, base, read=public_get, *, deadline=None):
    ticker = mapping["ticker"]
    if not TICKER.fullmatch(ticker):
        raise ValueError("invalid_wallet_mapping")
    if deadline is not None and time.time() > deadline:
        raise ValueError("wallet_quote_dispatch_late")
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
            "environment": "production" if base == PRODUCTION else "demo"}


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


def load_policy(path):
    policy = json.loads(Path(path).read_bytes())
    wallets = policy["wallets"]
    if (policy["execution_enabled"] is not False or len(wallets) != 4 or len(set(wallets)) != 4
            or any(not WALLET.fullmatch(wallet) for wallet in wallets)):
        raise ValueError("read_only_screened_wallet_policy_required")
    sports_wallets = policy.get("sports_discovery_wallets", [])
    if not isinstance(sports_wallets, list) or not set(sports_wallets) <= set(wallets):
        raise ValueError("invalid_sports_discovery_wallets")
    mappings = {(m["condition_id"], m["token_id"]): m for m in policy["mappings"]}
    if len(mappings) != len(policy["mappings"]):
        raise ValueError("duplicate_wallet_mapping")
    return policy, mappings


def _read_trade_quote(trade, mapping, base, read):
    deadline = trade["observed_at"] + 2
    if mapping is None:
        from .kalshi_wallet_sports import discover
        if time.time() > deadline:
            raise ValueError("wallet_quote_dispatch_late")
        mapping = discover(trade, base, read)
    if "sports_match" not in mapping:
        return read_quote(mapping, base, read, deadline=deadline)
    try:
        quote = read_quote(mapping, base, read, deadline=deadline)
    except Exception as exc:
        quote = {key: mapping[key] for key in ("ticker", "event_ticker", "side", "exchange_index")}
        quote["error"] = str(exc) if isinstance(exc, ValueError) else getattr(exc, "code", type(exc).__name__)
    return {**quote, "sports_match": mapping["sports_match"]}


def monitor_tick(db, wallets, mappings, since, read, *, sports_wallets=()):
    errors, scheduled = {}, {}
    with ThreadPoolExecutor(max_workers=4) as source_pool, ThreadPoolExecutor(max_workers=8) as quote_pool:
        sources = {source_pool.submit(read_buys, wallet, since, read): wallet for wallet in wallets}
        for source in as_completed(sources):
            try:
                trades = source.result()
            except Exception as exc:
                errors[sources[source]] = getattr(exc, "code", type(exc).__name__)
                continue
            for trade in sorted(trades, key=lambda row: (row["observed_at"], trade_key(row))):
                key = trade_key(trade)
                if not db.execute("INSERT OR IGNORE INTO buys VALUES(?,?,'pending',NULL)",
                                  (key, json.dumps(trade))).rowcount:
                    continue
                mapping = mappings.get((trade["condition_id"], trade["token_id"]))
                discover_sports = (trade["proxy_wallet"] in sports_wallets
                                   and str(trade.get("event_slug")).startswith(("nba-", "nfl-")))
                age = trade["observed_at"] - trade["timestamp"]
                state = ("unmapped" if mapping is None and not discover_sports else
                         "stale_or_quote_budget" if not -10 <= age <= 30 or len(scheduled) >= 4 else None)
                if state:
                    db.execute("UPDATE buys SET state=? WHERE id=?", (state, key))
                    continue
                scheduled[key] = {venue: (base, quote_pool.submit(_read_trade_quote, trade, mapping, base, read))
                                  for venue, base in (("production", PRODUCTION), ("demo", DEMO))}
        for key, jobs in scheduled.items():
            detail = {}
            for venue, (base, job) in jobs.items():
                try:
                    quote = detail[venue] = job.result()
                except Exception as exc:
                    detail[venue] = {"error": str(exc) if isinstance(exc, ValueError)
                                     else getattr(exc, "code", type(exc).__name__)}
                    continue
                if "ask" not in quote:
                    continue
                try:
                    quote["fee_basis"] = read_fee(quote, base, read)
                except Exception as exc:
                    quote["fee_error"] = str(exc) if isinstance(exc, ValueError) else getattr(exc, "code", type(exc).__name__)
            db.execute("UPDATE buys SET state='quote_attempted',evidence=? WHERE id=?", (json.dumps(detail), key))
    return errors


def capture(root, policy_path, *, stop=None, duration=86400, read=public_get):
    """Use a durable first-seen journal, with explicit unmatched and interrupted rows."""
    if type(duration) is not int or not 1 <= duration <= 86400:
        raise ValueError("invalid_wallet_capture_duration")
    stop = stop or threading.Event()
    policy, mappings = load_policy(policy_path)
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with acquire(root / "wallet-monitor.lock"):
        db = sqlite3.connect(root / "wallet-monitor.sqlite3", isolation_level=None)
        try:
            db.executescript("""
                PRAGMA journal_mode=DELETE;
                PRAGMA max_page_count=4096;
                CREATE TABLE IF NOT EXISTS registration(id INTEGER PRIMARY KEY, digest TEXT, started REAL, expires REAL);
                CREATE TABLE IF NOT EXISTS buys(id TEXT PRIMARY KEY, trade TEXT NOT NULL, state TEXT NOT NULL, evidence TEXT);
                CREATE TABLE IF NOT EXISTS health(id INTEGER PRIMARY KEY, at REAL, detail TEXT);
                CREATE TABLE IF NOT EXISTS health_changes(at REAL PRIMARY KEY, detail TEXT NOT NULL);
            """)
            now = time.time()
            db.execute("INSERT OR IGNORE INTO registration VALUES(1,?,?,?)", (digest(policy), now, now + duration))
            registered, started, expires = db.execute("SELECT digest,started,expires FROM registration").fetchone()
            if registered != digest(policy):
                raise ValueError("wallet_monitor_policy_changed")
            db.execute("UPDATE buys SET state='interrupted' WHERE state='pending'")
            previous = db.execute("SELECT at,detail FROM health WHERE id=1").fetchone()
            if previous:
                db.execute("INSERT OR IGNORE INTO health_changes VALUES(?,?)", previous)
            while not stop.is_set() and time.time() < expires:
                tick = time.monotonic()
                since = max(0, int(max(started - 10, time.time() - 40)))
                errors = monitor_tick(db, policy["wallets"], mappings, since, read,
                                      sports_wallets=policy.get("sports_discovery_wallets", ()))
                status = (time.time(), json.dumps({"poll_errors": errors, "wallets": len(policy["wallets"])},
                                                  sort_keys=True))
                if previous is None or status[1] != previous[1] or status[0] - previous[0] > 10:
                    db.execute("INSERT INTO health_changes VALUES(?,?)", status)
                db.execute("INSERT OR REPLACE INTO health VALUES(1,?,?)", status)
                previous = status
                stop.wait(max(0, 5 - (time.monotonic() - tick)))
        finally:
            db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--duration", type=int, default=86400)
    args = parser.parse_args()
    capture(args.root, args.policy, duration=args.duration)
