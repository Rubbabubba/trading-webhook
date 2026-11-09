"""One-contract Demo wallet order with a durable, single-use broker journal."""

from datetime import datetime
from decimal import Decimal, ROUND_CEILING
import hashlib
import json
import math
import re

from .kalshi_binary_broker import BinaryDemoBroker
from .kalshi_demo_broker import DemoClient
from .kalshi_process_lock import acquire
from .kalshi_shadow import price_book


CLIENT_ID = "wallet-demo-one-contract-v1"
PRICE_TOLERANCE_CENTS = 4
CHECKS = {
    "outcome_and_payoff", "event_and_period", "cutoff", "resolution_source",
    "threshold_boundary", "tie_cancellation_delay_fallback_and_revision",
}
HEX64 = r"[0-9a-f]{64}"
RULE_FIELDS = {"rules_primary", "rules_secondary", "close_time"}


def evaluate(registration, trade, quote, *, now, offset_low, offset_high):
    """Require a pinned exact match, verified clocks and an actionable Demo ask."""
    mapping = registration["mapping"]
    wallets = registration["wallets"]
    rules = mapping["demo_rules"]
    if (registration["environment"] != "demo" or len(wallets) != 4
            or len(set(wallets)) != 4
            or any(not re.fullmatch(r"0x[0-9a-f]{40}", wallet) for wallet in wallets)
            or mapping["decision"] != "exact" or set(mapping["checks"]) != CHECKS
            or any(value is not True for value in mapping["checks"].values())
            or mapping["side"] not in ("yes", "no")
            or set(rules) != RULE_FIELDS or not rules["rules_primary"]
            or not re.fullmatch(HEX64, mapping["rules_receipt_sha256"])
            or not re.fullmatch(HEX64, mapping["fee_receipt_sha256"])):
        raise ValueError("wallet_demo_exact_admission_required")
    start, end = (datetime.fromisoformat(registration[k]) for k in ("start_utc", "end_utc"))
    if (start.utcoffset() is None or end.utcoffset() is None
            or not 0 < (end - start).total_seconds() <= 86400
            or not start.timestamp() <= now < end.timestamp()):
        raise ValueError("wallet_demo_registration_inactive")

    observed, timestamp = trade["observed_at"], trade["timestamp"]
    if (trade["proxy_wallet"] not in wallets or trade["type"] != "TRADE"
            or trade["side"] != "BUY" or not re.fullmatch("0x" + HEX64, trade["transaction_hash"])
            or not re.fullmatch("0x" + HEX64, trade["condition_id"])
            or not re.fullmatch(r"[0-9]{1,78}", trade["token_id"])
            or (trade["condition_id"], trade["token_id"]) != (mapping["condition_id"], mapping["token_id"])
            or not all(type(x) in (int, float) and math.isfinite(x)
                       for x in (now, observed, timestamp, offset_low, offset_high))
            or not -10 <= offset_low <= offset_high <= 10 or offset_high - offset_low > 1.5
            or not start.timestamp() <= timestamp + offset_low
            or not timestamp + offset_high <= observed <= now
            or now - timestamp - offset_low > 28):
        raise ValueError("wallet_demo_signal_stale_or_unmatched")

    market = quote["market"]
    if (quote["environment"] != "demo" or quote["ticker"] != mapping["ticker"]
            or market["ticker"] != mapping["ticker"] or market["event_ticker"] != mapping["event_ticker"]
            or market["market_type"] != "binary" or market["status"] != "active"
            or market.get("exchange_index", 0) != 0
            or any(market.get(key) != value for key, value in rules.items())
            or now >= datetime.fromisoformat(rules["close_time"]).timestamp()
            or not observed <= quote["started_at"]
            or not 0 <= quote["observed_at"] - quote["started_at"] <= 2
            or not 0 <= now - quote["observed_at"] <= 2):
        raise ValueError("wallet_demo_quote_or_rules_unverified")
    _, ask, _, depth = price_book(quote, mapping["side"])
    cents = int((Decimal(ask.numerator) * 100 / ask.denominator).to_integral_value(rounding=ROUND_CEILING))
    fee = mapping["fee_ceiling_cents"]
    wallet_price, size = Decimal(str(trade["price"])), Decimal(str(trade["size"]))
    if (type(fee) is not int or not 0 <= fee <= 5 or not 1 <= cents <= 99 or depth < 1
            or not wallet_price.is_finite() or not size.is_finite()
            or not 0 < wallet_price < 1 or size <= 0
            or abs(cents + fee - wallet_price * 100) > PRICE_TOLERANCE_CENTS):
        raise ValueError("wallet_demo_price_or_depth_guard")
    return cents, fee


def run_once(broker, lock_path, registration, trade, *, quote_provider, offsets):
    """Submit at most once. A later call may only reconcile an existing intent."""
    if (not isinstance(broker, BinaryDemoBroker) or type(broker.client) is not DemoClient
            or broker.client.BASE_URL != DemoClient.BASE_URL or broker.journal.environment != "demo"):
        raise ValueError("wallet_demo_broker_required")
    journal = broker.journal
    with acquire(lock_path):
        orders = journal.records()
        if orders:
            if len(orders) != 1 or orders[0]["payload"]["client_order_id"] != CLIENT_ID:
                raise ValueError("wallet_demo_isolated_journal_required")
            record = orders[0]
            if record["state"] == "reserved":
                return {"state": "reserved_unsent", "fill_verified": False}
            record = broker.refresh(CLIENT_ID)
        else:
            mapping = registration["mapping"]
            payload = {"ticker": mapping["ticker"]}
            snapshot = broker.snapshot()
            quote = quote_provider(payload)

            def check(frame):
                return evaluate(registration, trade, frame, now=journal.clock().timestamp(),
                                offset_low=offsets[0], offset_high=offsets[1])

            price, fee = check(quote)
            journal.reserve(CLIENT_ID, payload["ticker"], 1, price, fee, outcome=mapping["side"],
                            action="buy", account_snapshot=snapshot)

            def fresh_quote(order):
                fresh = quote_provider(order)
                if check(fresh) != (price, fee):
                    raise ValueError("wallet_demo_quote_changed")
                encoded = json.dumps(registration, sort_keys=True, separators=(",", ":")).encode()
                return {**fresh, "wallet_registration_sha256": hashlib.sha256(encoded).hexdigest(),
                        "wallet_signal": trade, "wallet_clock_offsets": offsets,
                        "wallet_exact_mapping": mapping}

            record = broker.submit(CLIENT_ID, quote_provider=fresh_quote)
        if record["state"] != "terminal":
            return {"state": record["state"], "fill_verified": False}
        broker.reconcile_positions()
        journal.validate_reconciliation()
        raw = journal.db.execute("SELECT detail FROM broker_evidence WHERE client_id=?", (CLIENT_ID,)).fetchone()
        evidence = json.loads(raw[0])
        return {"state": "terminal", "fill_verified": record["filled"] == 1,
                "filled": record["filled"], "order_id": record["broker_id"],
                "fees_dollars": evidence["fees_dollars"]}
