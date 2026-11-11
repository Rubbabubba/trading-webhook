"""Continuously mirror ranked Polymarket wallet trades on Kalshi Demo."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from fractions import Fraction
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from .kalshi_binary_accounting import reconcile_positions
from .kalshi_binary_broker import BinaryDemoBroker
from .kalshi_binary_journal import BinaryJournal
from .kalshi_demo_broker import BrokerError, DemoClient
from .kalshi_process_lock import acquire
from .kalshi_wallet_monitor import DEMO, WALLET, price_book, public_get, read_fee, read_quote, read_trades, trade_key
from .kalshi_wallet_sports import discover


DIRECTORY = "ranked-wallet-copy-v1"
LEADERBOARD = "https://data-api.polymarket.com/v2/leaderboard"
SIGNAL_SECONDS = 90
MAX_OPEN = 5
MAX_DAILY_BUYS = 10


def open_journal(root):
    return BinaryJournal(root / DIRECTORY / "journal.sqlite3", order_limit_cents=105,
                         capital_limit_cents=500, daily_loss_cents=500)


class SharedDemoInventory:
    """Keep the maker's submission hold while accounting for owned wallet fills."""

    def __init__(self, baseline, root):
        self.baseline = baseline
        self.root = root
        self.receipt_sha256 = baseline.receipt_sha256

    @property
    def positions(self):
        if not (self.root / DIRECTORY / "journal.sqlite3").is_file():
            return self.baseline.positions
        journal = open_journal(self.root)
        try:
            wallet_positions = journal.accounting()["positions"]
        finally:
            journal.close()
        if self.baseline.positions.keys() & wallet_positions.keys():
            raise ValueError("demo_inventory_overlap")
        return self.baseline.positions | wallet_positions

    def reconcile(self, accounting, rows):
        if accounting["positions"]:
            raise ValueError("maker_inventory_not_flat_during_test_inventory_hold")
        reconcile_positions({"positions": self.positions}, rows)


def day(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat()


def ranked_wallets(read):
    wallets = []
    for category in ("OVERALL", "SPORTS"):
        page = read(LEADERBOARD, {"category": category, "time_period": "WEEK",
                                  "order_by": "PNL", "limit": 4})[0]
        rows = page["data"]
        cohort = [row["user_id"].lower() for row in rows]
        if ([int(row["rank"]) for row in rows] != [1, 2, 3, 4]
                or len(set(cohort)) != 4 or any(not WALLET.fullmatch(w) for w in cohort)):
            raise ValueError("invalid_wallet_leaderboard")
        wallets.extend(cohort)
    return list(dict.fromkeys(wallets))


def executable(trade, mapping, quote):
    bid, ask, bid_depth, ask_depth = price_book(quote, mapping["side"])
    buying = trade["side"] == "BUY"
    price, depth = (ask, ask_depth) if buying else (bid, bid_depth)
    source = Decimal(str(trade["price"]))
    if not source.is_finite() or not 0 < source < 1:
        raise ValueError("invalid_source_price")
    exact_cents = Decimal(price.numerator) * 100 / price.denominator
    cents = int(exact_cents.to_integral_value(rounding=ROUND_CEILING if buying else ROUND_FLOOR))
    if (not 1 <= cents <= 99 or abs(price - Fraction(source)) > Fraction(5, 100)
            or abs(Decimal(cents) / 100 - source) > Decimal(".05")):
        raise ValueError("outside_five_cent_band")
    if depth < 1:
        raise ValueError("insufficient_wallet_copy_depth")
    return cents


def source_identity(trade):
    return tuple(trade[key] for key in ("proxy_wallet", "condition_id", "token_id"))


def error_code(exc):
    if isinstance(exc, ValueError) and re.fullmatch(r"[a-z0-9_]{1,80}", str(exc)):
        return str(exc)
    status = getattr(exc, "status", None) or getattr(exc, "code", None)
    return f"http_{status}" if type(status) is int else type(exc).__name__


class WalletCopy:
    def __init__(self, root, db, broker, *, read=public_get, clock=time.time):
        self.root, self.db, self.broker, self.read, self.clock = root, db, broker, read, clock
        self.journal = broker.journal
        self.next_settlement = 0
        self.startup_recovered = False
        db.executescript("""
            CREATE TABLE IF NOT EXISTS cohort(day TEXT PRIMARY KEY, wallets TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS start(id INTEGER PRIMARY KEY, at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS signals(id TEXT PRIMARY KEY, at INTEGER NOT NULL,
              side TEXT NOT NULL, trade TEXT NOT NULL, state TEXT NOT NULL, mapping TEXT);
            CREATE INDEX IF NOT EXISTS signals_by_day ON signals(side, at);
            CREATE TABLE IF NOT EXISTS uncertain_checks(client_id TEXT PRIMARY KEY, evidence TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS submission_failures(
              signal_id TEXT PRIMARY KEY, observed_at INTEGER NOT NULL, detail TEXT NOT NULL);
        """)
        db.execute("INSERT OR IGNORE INTO start VALUES(1, ?)", (int(clock()) + 1,))
        self.started = db.execute("SELECT at FROM start WHERE id=1").fetchone()[0]

    def active_owners(self, accounting):
        owners = {}
        for record in self.journal.records():
            ticker = record["payload"]["ticker"]
            if ticker not in accounting["positions"] or record["intent"]["action"] != "buy" or not record["filled"]:
                continue
            row = self.db.execute("SELECT trade,mapping FROM signals WHERE id=?",
                                  (record["payload"]["client_order_id"].removeprefix("wallet-copy-"),)).fetchone()
            if row is None or row[1] is None:
                raise ValueError("wallet_fill_owner_missing")
            owners[ticker] = tuple(json.loads(item) for item in row)
        if set(owners) != accounting["positions"].keys():
            raise ValueError("wallet_fill_owner_missing")
        return owners

    def recover(self):
        if not self.startup_recovered:
            for (client_id,) in self.journal.db.execute('SELECT id FROM uncertain_quarantine'):
                self.record_quarantined_signal(client_id)
        for record in self.journal.records():
            client_id = record["payload"]["client_order_id"]
            if record["state"] == "terminal" and self.startup_recovered:
                continue
            if record["state"] == "reserved":
                self.journal.abandon_reserved(client_id)
                state = "interrupted_before_submission"
            elif record["state"] != "terminal":
                try:
                    record = self.broker.refresh(client_id)
                except ValueError as error:
                    if str(error) != 'submission_unresolved' or not self.quarantine_unmatched_entry(record):
                        raise
                    self.record_quarantined_signal(client_id)
                    continue
                if record["state"] != "terminal":
                    raise ValueError("wallet_ioc_unresolved")
                state = "filled" if record["filled"] else "ioc_no_fill"
            else:
                state = "filled" if record["filled"] else "ioc_no_fill"
            self.db.execute("UPDATE signals SET state=? WHERE id=? AND state IN ('submitting','submission_unresolved')",
                            (state, client_id.removeprefix("wallet-copy-")))
        self.db.execute("UPDATE signals SET state='interrupted_before_submission' WHERE state='submitting'")
        self.startup_recovered = True
        if self.journal.accounting()["positions"] and self.clock() >= self.next_settlement:
            self.broker.reconcile_settlements()
            self.next_settlement = self.clock() + 60

    def record_quarantined_signal(self, client_id):
        self.db.execute("UPDATE signals SET state='quarantined_zero_fill' WHERE id=? "
                        "AND state IN ('submitting','submission_unresolved')", (client_id.removeprefix('wallet-copy-'),))

    def quarantine_unmatched_entry(self, record):
        payload, intent = record['payload'], record['intent']
        client_id, ticker = payload['client_order_id'], payload['ticker']
        if (record['state'] != 'uncertain' or record['broker_id'] is not None or record['filled']
                or not client_id.startswith('wallet-copy-') or intent.get('order_mode') != 'ioc'
                or intent.get('action') != 'buy' or intent.get('count') != 1):
            return False
        signal = self.db.execute('SELECT side,state,mapping FROM signals WHERE id=?',
                                 (client_id.removeprefix('wallet-copy-'),)).fetchone()
        quote = self.journal.db.execute('SELECT detail FROM submission_quotes WHERE client_id=?', (client_id,)).fetchone()
        submitted = json.loads(quote[0]).get('observed_at') if quote else None
        if (not signal or signal[0] != 'BUY' or signal[1] not in ('submitting','submission_unresolved')
                or json.loads(signal[2] or '{}').get('ticker') != ticker
                or type(submitted) not in (int,float) or self.clock() - submitted < 300):
            return False
        prior = self.db.execute('SELECT evidence FROM uncertain_checks WHERE client_id=?', (client_id,)).fetchone()
        previous = json.loads(prior[0]) if prior else None
        if previous and self.clock() - previous['observed_at'] < 60:
            return False
        client = self.broker.client
        current = client.pages('/portfolio/orders','orders',ticker=ticker,subaccount=0)
        archived = client.pages('/historical/orders','orders',ticker=ticker)
        fills = client.pages('/portfolio/fills','fills',ticker=ticker,subaccount=0)
        archived_fills = client.pages('/historical/fills','fills',ticker=ticker)
        positions = client.pages('/portfolio/positions','market_positions',subaccount=0,count_filter='position')
        resting = client.pages('/portfolio/orders','orders',subaccount=0,status='resting')
        known = {row['broker_id'] for row in self.journal.records() if row['broker_id']}
        counts = dict(current_exact_orders=sum(row.get('client_order_id') == client_id for row in current),
                      historical_exact_orders=sum(row.get('client_order_id') == client_id for row in archived),
                      ticker_unattributed_current_fills=sum(row.get('order_id') not in known for row in fills),
                      ticker_unattributed_historical_fills=sum(row.get('order_id') not in known for row in archived_fills),
                      ticker_positions=sum(row.get('ticker') == ticker for row in positions),
                      all_positions=len(positions), all_resting_orders=len(resting))
        if any(counts.values()):
            self.db.execute('DELETE FROM uncertain_checks WHERE client_id=?', (client_id,))
            return False
        proof = dict(counts, environment='demo', observed_at=self.clock())
        if not previous or proof['observed_at'] - previous['observed_at'] > 180:
            self.db.execute('INSERT INTO uncertain_checks VALUES(?,?) ON CONFLICT(client_id) DO UPDATE SET '
                            'evidence=excluded.evidence', (client_id, json.dumps(proof)))
            return False
        self.journal.quarantine_uncertain_zero_fill(client_id, dict(proof, negative_observations=[previous, proof]))
        self.db.execute('DELETE FROM uncertain_checks WHERE client_id=?', (client_id,))
        return True

    def require_demo_hold(self):
        status = json.loads((self.root / "status.json").read_bytes())
        if (status.get("environment") != "demo" or status.get("production_execution_enabled") is not False
                or status.get("new_submissions_enabled") is not False or status.get("phase") != "running"
                or status.get("wallet_copy_inventory_ledger") is not True
                or not 0 <= self.clock() - datetime.fromisoformat(status["at"]).timestamp() <= 180):
            raise ValueError("shared_demo_hold_unverified")

    def execute(self, key, trade, cohort, owners, accounting):
        buying = trade["side"] == "BUY"
        if buying:
            if trade["proxy_wallet"] not in cohort:
                return "wallet_outside_current_cohort"
            since = datetime.fromisoformat(day(self.clock())).replace(tzinfo=timezone.utc).timestamp()
            buys = self.db.execute(
                "SELECT count(*) FROM signals WHERE side='BUY' AND at>=? AND state IN "
                "('filled','ioc_no_fill','submitting','submission_unresolved','quarantined_zero_fill')",
                (since,)).fetchone()[0]
            if len(owners) >= MAX_OPEN or buys >= MAX_DAILY_BUYS:
                return "demo_entry_limit"
            mapping = discover(trade, DEMO, self.read)
            receipt = json.loads((self.root / "separate-demo-test-inventory-v1.json").read_bytes())
            if any(entry["ticker"] == mapping["ticker"] for entry in receipt["positions"]):
                return "legacy_demo_inventory_market"
            if (mapping["ticker"] in owners
                    or any(source_identity(entry) == source_identity(trade) for entry, _ in owners.values())):
                return "wallet_market_already_held"
            previous = accounting["outcomes"].get(mapping["ticker"])
            if previous is not None and previous != mapping["side"]:
                return "opposite_outcome_previously_tested"
        else:
            match = [(entry, mapping) for entry, mapping in owners.values()
                     if source_identity(entry) == source_identity(trade) and entry["timestamp"] <= trade["timestamp"]]
            if not match:
                return "no_owned_contract_for_wallet_sell"
            (_, mapping), = match
        self.require_demo_hold()
        quote = read_quote(mapping, DEMO, self.read)
        price = executable(trade, mapping, quote)
        fee = read_fee(quote, DEMO, self.read)
        # Reserve the modeled fee at its maximum across any improvement to the limit.
        reserve = int((Decimal("1.75") * Decimal(fee["fee_multiplier"])).to_integral_value(rounding=ROUND_CEILING))
        client_id = "wallet-copy-" + key
        snapshot = self.broker.snapshot()
        self.db.execute("UPDATE signals SET state='submitting',mapping=? WHERE id=?",
                        (json.dumps(mapping), key))
        self.journal.reserve(client_id, mapping["ticker"], 1, price, reserve, outcome=mapping["side"],
                             action=trade["side"].lower(), account_snapshot=snapshot)

        def fresh_quote(_):
            self.require_demo_hold()
            frame = read_quote(mapping, DEMO, self.read)
            fresh = executable(trade, mapping, frame)
            if (fresh > price if buying else fresh < price):
                raise ValueError("wallet_quote_moved")
            if self.clock() - trade["timestamp"] > SIGNAL_SECONDS:
                raise ValueError("wallet_signal_expired")
            return dict(frame, wallet_signal_id=key, fee_basis=fee)

        try:
            result = self.broker.submit(client_id, quote_provider=fresh_quote)
        except Exception as error:
            if self.journal.get(client_id)["state"] != "reserved":
                detail = {'reason':error_code(error)}
                if isinstance(error, BrokerError):
                    detail['broker_code'] = error.diagnostics.get('code')
                self.db.execute('INSERT OR IGNORE INTO submission_failures VALUES(?,?,?)',
                                (key, int(self.clock()), json.dumps(detail)))
                return "submission_unresolved"
            self.journal.abandon_reserved(client_id)
            raise
        if result["state"] != "terminal":
            return "submission_unresolved"
        return "filled" if result["filled"] else "ioc_no_fill"

    def tick(self):
        now = self.clock()
        today = day(now)
        saved = self.db.execute("SELECT wallets FROM cohort WHERE day=?", (today,)).fetchone()
        if saved:
            cohort = json.loads(saved[0])
        else:
            cohort = ranked_wallets(self.read)
            self.db.execute("INSERT INTO cohort VALUES(?,?)", (today, json.dumps(cohort)))
        execution_error = None
        try:
            self.recover()
            self.require_demo_hold()
        except Exception as exc:
            execution_error = error_code(exc)
        try:
            accounting = self.journal.accounting()
            owners = self.active_owners(accounting)
        except Exception as exc:
            execution_error = execution_error or error_code(exc)
            owners = {}
        wallets = sorted(set(cohort) | {entry["proxy_wallet"] for entry, _ in owners.values()})
        source_errors = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            tasks = {wallet: pool.submit(read_trades, wallet, max(self.started, now - SIGNAL_SECONDS), self.read)
                     for wallet in wallets}
            for wallet, task in tasks.items():
                try:
                    trades = task.result()
                except Exception as exc:
                    source_errors[wallet] = error_code(exc)
                    continue
                for trade in trades:
                    if self.started <= trade["timestamp"] <= self.clock() + 5:
                        self.db.execute("INSERT OR IGNORE INTO signals VALUES(?,?,?,?,'observed',NULL)",
                                        (trade_key(trade), trade["timestamp"], trade["side"], json.dumps(trade)))
        self.db.execute("UPDATE signals SET state='stale' WHERE state='observed' AND at<?",
                        (self.clock() - SIGNAL_SECONDS,))
        if not execution_error:
            rows = self.db.execute(
                "SELECT id,trade FROM signals WHERE state='observed' AND at<=? ORDER BY at,side,rowid",
                (self.clock(),)).fetchall()
            for key, detail in rows:
                trade = json.loads(detail)
                if self.clock() - trade["timestamp"] > SIGNAL_SECONDS:
                    state = "stale"
                else:
                    try:
                        state = self.execute(key, trade, cohort, owners, accounting)
                    except Exception as exc:
                        state = error_code(exc)
                if state == "shared_demo_hold_unverified" or state == "FileNotFoundError":
                    execution_error = state
                    break
                self.db.execute("UPDATE signals SET state=? WHERE id=?", (state, key))
                if state == "submission_unresolved":
                    execution_error = state
                    break
                if state == "filled":
                    accounting = self.journal.accounting()
                    owners = self.active_owners(accounting)
        failure = self.db.execute('SELECT observed_at,detail FROM submission_failures '
                                  'ORDER BY observed_at DESC, rowid DESC LIMIT 1').fetchone()
        return {
            "ranked_wallets": len(cohort), "wallets_polled": len(wallets),
            "source_errors": list(source_errors.values()), "execution_error": execution_error,
            "last_submission_failure": dict(json.loads(failure[1]), observed_at=failure[0]) if failure else None,
            "open_positions": None if execution_error == "missing_accounting_evidence" else len(owners),
            "signals": {f"{side.lower()}:{state}": count for side, state, count in self.db.execute(
                "SELECT side,state,count(*) FROM signals GROUP BY side,state")},
        }


def write_health(path, *, log=True, **data):
    health = dict(at=datetime.now(timezone.utc).isoformat(), environment="demo", **data)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(health) + "\n")
    temp.replace(path)
    if log:
        print(json.dumps(dict(event="ranked_wallet_copy", **health)), flush=True)


def run(data_root):
    root = Path(data_root)
    directory = root / DIRECTORY
    directory.mkdir(parents=True, exist_ok=True)
    with acquire(directory / "worker.lock"):
        db = sqlite3.connect(directory / "signals.sqlite3", isolation_level=None)
        journal = open_journal(root)
        try:
            client = DemoClient(os.environ["KALSHI_DEMO_API_KEY_ID"], os.environ["KALSHI_DEMO_PRIVATE_KEY_PATH"])
            earlier = [root / "journal.sqlite3"]
            legacy = root / "wallet-demo-one-contract-v1" / "journal.sqlite3"
            if legacy.is_file():
                earlier.append(legacy)
            worker = WalletCopy(root, db, BinaryDemoBroker(journal, client, unresolved_journals=earlier))
            last_logged = 0
            while True:
                try:
                    health = worker.tick()
                except Exception as exc:
                    health = {"error": error_code(exc)}
                log = time.monotonic() - last_logged >= 60
                write_health(directory / "status.json", log=log, **health)
                if log:
                    last_logged = time.monotonic()
                time.sleep(8)
        finally:
            journal.close()
            db.close()


def supervise(data_root):
    while True:
        try:
            run(data_root)
        except Exception as exc:
            print(json.dumps({"event": "ranked_wallet_copy_restart", "error": error_code(exc)}), flush=True)
            time.sleep(30)
