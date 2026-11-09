"""Mirror a verified full wallet exit from the one-contract Demo trial."""

from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import importlib.util
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time

from .kalshi_binary_broker import BinaryDemoBroker
from .kalshi_binary_journal import BinaryJournal
from .kalshi_demo_broker import BrokerError, DemoClient
from .kalshi_process_lock import acquire


EXIT_ID = "wallet-demo-one-contract-exit-v1"
POSITION_URL = "https://data-api.polymarket.com/v2/positions"


def matching_sale(entry, sale, *, filled_at, now, offsets):
    low, high = offsets
    return (
        sale["type"] == "TRADE" and sale["side"] == "SELL"
        and all(sale[k] == entry[k] for k in ("proxy_wallet", "condition_id", "token_id"))
        and all(type(x) in (int, float) and math.isfinite(x)
                for x in (low, high, filled_at, now, sale["timestamp"], sale["observed_at"]))
        and -10 <= low <= high <= 10 and high - low <= 1.5
        and filled_at < sale["timestamp"] + low
        and sale["timestamp"] + high <= sale["observed_at"] <= now
        and now - sale["timestamp"] - low <= 28
        and Decimal(0) < Decimal(str(sale["price"])) < Decimal(1)
        and Decimal(0) < Decimal(str(sale["size"])) < Decimal("Infinity")
    )


def close_evidence(sale, read):
    params = {"user": sale["proxy_wallet"], "condition": sale["condition_id"], "limit": 20}

    def positions(status):
        body, sent, received, age, _ = read(
            POSITION_URL, {**params, "status": status, "filter_type": "TOKENS", "filter_amount": 0}
        )
        if (not 0 <= received - sent <= 2 or age not in (None, "0")
                or body["pagination"]["next_cursor"] or body["pagination"]["has_more"]
                or not isinstance(body["data"], list)
                or any(row["proxy_wallet"].lower() != sale["proxy_wallet"]
                       or row["condition_id"] != sale["condition_id"] for row in body["data"])):
            raise ValueError("wallet_exit_positions_unverified")
        return [row for row in body["data"] if row["token_id"] == sale["token_id"]], received

    opened, observed = positions("OPEN")
    if opened:
        if len(opened) != 1 or opened[0]["last_event_at"] < sale["timestamp"]:
            raise ValueError("wallet_exit_position_feed_lag")
        return None
    closed, _ = positions("CLOSED")
    if (len(closed) != 1 or closed[0]["status"] != "CLOSED"
            or closed[0]["last_event_at"] != sale["timestamp"]
            or closed[0]["current_size"] is None
            or Decimal(str(closed[0]["current_size"])) != 0):
        raise ValueError("wallet_exit_full_close_unverified")
    return {"closed_position": closed[0], "open_positions_checked_at": observed}


def exit_limit(sale, quote, *, now, fee, monitor):
    if (quote["environment"] != "demo" or quote["market"]["status"] != "active"
            or not sale["observed_at"] <= quote["started_at"] <= quote["observed_at"] <= now
            or now - quote["observed_at"] > 2):
        raise ValueError("wallet_exit_quote_unverified")
    bid, _, depth, _ = monitor.price_book(quote, quote["side"])
    bid_cents = int(bid * 100)
    if depth < 1 or not 1 <= bid_cents <= 99 or abs(bid_cents - fee - Decimal(str(sale["price"])) * 100) > 4:
        raise ValueError("wallet_exit_price_or_depth_guard")
    return bid_cents


def main(root, expected_source_hash, expected_registration_hash):
    trial = root / "wallet-demo-one-contract-v1"
    observer = root / "wallet-monitor-sports-v2"
    source = observer / "monitor-sells-v4.py"
    registration_bytes = (trial / "registration.json").read_bytes()
    if (sha256(source.read_bytes()).hexdigest() != expected_source_hash
            or sha256(registration_bytes).hexdigest() != expected_registration_hash):
        raise ValueError("wallet_exit_source_or_registration_changed")
    spec = importlib.util.spec_from_file_location("opportunity_lab.wallet_monitor_with_sells", source)
    monitor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(monitor)
    registration = json.loads(registration_bytes)
    mapping = registration["mapping"]
    quote_mapping = {**mapping, "kalshi_rules": mapping["demo_rules"]}
    ticker, side, fee = (mapping[k] for k in ("ticker", "side", "fee_ceiling_cents"))
    cutoff = datetime.fromisoformat(registration["end_utc"]).timestamp()
    exit_cutoff = cutoff + 5 * 3600
    client = DemoClient(os.environ["KALSHI_DEMO_API_KEY_ID"], os.environ["KALSHI_DEMO_PRIVATE_KEY_PATH"])
    journal = BinaryJournal(trial / "journal.sqlite3", order_limit_cents=105,
                            capital_limit_cents=105, daily_loss_cents=105)
    broker = BinaryDemoBroker(journal, client, unresolved_journals=(root / "journal.sqlite3",))

    def report(state, **detail):
        print(json.dumps({"at": datetime.now(timezone.utc).isoformat(), "state": state, **detail}), flush=True)

    def evidence(table, client_id):
        return json.loads(journal.db.execute(f"SELECT detail FROM {table} WHERE client_id=?", (client_id,)).fetchone()[0])

    def trigger(entry, filled_at, sale, offsets):
        with acquire(trial / "order.lock"):
            if journal.accounting()["positions"].get(ticker) != (1 if side == "yes" else -1):
                raise ValueError("wallet_exit_owned_position_unverified")
            snapshot = broker.snapshot()

            def validated_quote():
                remaining = close_evidence(sale, monitor.public_get)
                if remaining is None:
                    raise ValueError("wallet_exit_partial_trim")
                quote = monitor.read_quote(quote_mapping, monitor.DEMO)
                fee_basis = monitor.read_fee(quote, monitor.DEMO)
                if fee != 2 or fee_basis["fee_multiplier"] != "1":
                    raise ValueError("wallet_exit_fee_unverified")
                now = time.time()
                if not matching_sale(entry, sale, filled_at=filled_at, now=now, offsets=offsets):
                    raise ValueError("wallet_exit_signal_stale")
                price = exit_limit(sale, quote, now=now, fee=fee, monitor=monitor)
                return price, {**quote, "wallet_exit_signal": sale, "wallet_exit_evidence": remaining,
                               "fee_basis": fee_basis, "wallet_clock_offsets": offsets}

            price, _ = validated_quote()
            n = journal.db.execute("SELECT count(*) FROM abandoned_intents WHERE id LIKE ?", (EXIT_ID + "%",)).fetchone()[0]
            exit_id = f"{EXIT_ID}-{n}" if n else EXIT_ID
            journal.reserve(exit_id, ticker, 1, price, fee, outcome=side, action="sell", account_snapshot=snapshot)

            def fresh_quote(_):
                current, quote = validated_quote()
                if current < price:
                    raise ValueError("wallet_exit_bid_dropped")
                return quote

            try:
                return broker.submit(exit_id, quote_provider=fresh_quote)
            except Exception:
                if journal.get(exit_id)["state"] == "reserved":
                    journal.abandon_reserved(exit_id)
                raise

    with acquire(trial / "exit-host.lock"), sqlite3.connect(
        (observer / "wallet-monitor.sqlite3").as_uri() + "?mode=ro", uri=True
    ) as db:
        collector_end = db.execute("SELECT expires FROM registration").fetchone()[0]
        report("waiting_for_demo_entry", ticker=ticker)
        seen = {}
        while time.time() < exit_cutoff:
            records = journal.records()
            if len(records) > 2:
                raise ValueError("wallet_exit_journal_unexpected")
            if len(records) == 2:
                exit_record = next(row for row in records if row["intent"]["action"] == "sell")
                exit_id = exit_record["payload"]["client_order_id"]
                if exit_record["state"] == "reserved":
                    report("reserved_exit_requires_review")
                    return
                try:
                    exit_record = broker.refresh(exit_id)
                    if exit_record["state"] == "terminal":
                        broker.reconcile_positions()
                        journal.validate_reconciliation()
                        report("exit_reconciled", filled=exit_record["filled"],
                               order_id=exit_record["broker_id"],
                               fees_dollars=evidence("broker_evidence", exit_id)["fees_dollars"])
                        return
                except (BrokerError, ValueError) as error:
                    report("exit_reconciliation_pending", reason=type(error).__name__)
                time.sleep(5)
                continue
            if not records or records[0]["state"] != "terminal":
                if time.time() > cutoff + 60:
                    report("no_reconciled_entry")
                    return
                time.sleep(1)
                continue
            entry_record = records[0]
            if entry_record["filled"] == 0:
                report("entry_unfilled")
                return
            entry_id = entry_record["payload"]["client_order_id"]
            if (entry_record["filled"] != 1 or entry_record["intent"]["action"] != "buy"
                    or entry_record["payload"]["ticker"] != ticker or entry_record["intent"]["outcome"] != side
                    or not entry_id.startswith("wallet-demo-one-contract-v1")):
                raise ValueError("wallet_exit_entry_identity_mismatch")
            quote = evidence("submission_quotes", entry_id)
            if quote["wallet_registration_sha256"] != expected_registration_hash:
                raise ValueError("wallet_exit_entry_registration_mismatch")
            entry = quote["wallet_signal"]
            filled_at = max(datetime.fromisoformat(row["created_time"]).timestamp()
                            for row in evidence("broker_evidence", entry_id)["fills"])
            now = time.time()
            if now < collector_end:
                rows = db.execute(
                    """SELECT id,trade FROM sells WHERE json_extract(trade,'$.proxy_wallet')=?
                       AND json_extract(trade,'$.condition_id')=? AND json_extract(trade,'$.token_id')=?
                       AND json_extract(trade,'$.observed_at')>=? ORDER BY rowid LIMIT 100""",
                    (entry["proxy_wallet"], entry["condition_id"], entry["token_id"], now - 28),
                ).fetchall()
                sells = [(key, json.loads(raw)) for key, raw in rows]
            else:
                try:
                    trades = monitor.read_trades(entry["proxy_wallet"], now - 28, condition=entry["condition_id"])
                except (OSError, ValueError):
                    time.sleep(5)
                    continue
                sells = [(monitor.trade_key(s), s) for s in trades if s["side"] == "SELL"]
            for key, sale in sells:
                attempts = seen.get(key, 0)
                if attempts >= 3:
                    continue
                seen[key] = attempts + 1
                try:
                    offsets = monitor.clock_bounds()
                    if matching_sale(entry, sale, filled_at=filled_at, now=time.time(), offsets=offsets):
                        trigger(entry, filled_at, sale, offsets)
                        break
                    seen[key] = 3
                except (BrokerError, KeyError, OSError, ValueError) as error:
                    reason = str(error) if str(error).startswith("wallet_exit_") else type(error).__name__
                    if reason not in ("wallet_exit_position_feed_lag", "wallet_exit_full_close_unverified"):
                        seen[key] = 3
                    report("exit_signal_held", reason=reason, trade_key=key)
                    if len(journal.records()) == 2:
                        break
            time.sleep(1 if now < collector_end else 5)
        report("exit_window_expired")


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve(), sys.argv[2], sys.argv[3])
