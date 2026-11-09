"""Use fresh records from the existing wallet collector for a bounded Demo trial."""

from datetime import datetime, timezone
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

from .kalshi_binary_broker import BinaryDemoBroker
from .kalshi_binary_journal import BinaryJournal
from .kalshi_demo_broker import BrokerError, DemoClient
from .kalshi_wallet_demo_trial import evaluate, run_once


def collector(path):
    source = path / "monitor-depth-v3.py"
    if sha256(source.read_bytes()).hexdigest() != "6198fc104bfe88aeafcf38343d53c62dab6afcd04a6a8230e41bdebd261e4a36":
        raise ValueError("wallet_collector_source_changed")
    spec = importlib.util.spec_from_file_location("opportunity_lab.kalshi_wallet_monitor", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main(root):
    trial = root / "wallet-demo-one-contract-v1"
    observer = root / "wallet-monitor-sports-v2"
    monitor = collector(observer)
    registration = json.loads((trial / "registration.json").read_bytes())
    mapping = registration["mapping"]
    rules_receipt = (trial / "rules.json").read_bytes()
    fee_receipt = (trial / "fee.json").read_bytes()
    if (sha256(rules_receipt).hexdigest() != mapping["rules_receipt_sha256"]
            or sha256(fee_receipt).hexdigest() != mapping["fee_receipt_sha256"]
            or json.loads(fee_receipt)["model_fee_upper_bound_cents"] != mapping["fee_ceiling_cents"]
            or json.loads(rules_receipt)["kalshi_demo_rules"] != mapping["demo_rules"]):
        raise ValueError("wallet_demo_receipt_mismatch")
    deadline = datetime.fromisoformat(registration["end_utc"]).timestamp()
    window_start = datetime.fromisoformat(registration["start_utc"]).timestamp()
    quote_mapping = {**mapping, "kalshi_rules": mapping["demo_rules"]}

    def quote(payload):
        if payload["ticker"] != mapping["ticker"]:
            raise ValueError("wallet_demo_ticker_changed")
        frame = monitor.read_quote(quote_mapping, monitor.DEMO)
        fee = monitor.read_fee(frame, monitor.DEMO)
        if fee["model_fee_upper_bound_cents"] != mapping["fee_ceiling_cents"]:
            raise ValueError("wallet_demo_fee_changed")
        return {**frame, "fee_basis": fee}

    def report(state, **extra):
        print(json.dumps({"at": datetime.now(timezone.utc).isoformat(),
                          "state": state, **extra}), flush=True)

    client = DemoClient(os.environ["KALSHI_DEMO_API_KEY_ID"], os.environ["KALSHI_DEMO_PRIVATE_KEY_PATH"])
    journal = BinaryJournal(trial / "journal.sqlite3", order_limit_cents=105,
                            capital_limit_cents=105, daily_loss_cents=105)
    broker = BinaryDemoBroker(journal, client, unresolved_journals=(root / "journal.sqlite3",))
    source_db = observer / "wallet-monitor.sqlite3"
    with sqlite3.connect(source_db.as_uri() + "?mode=ro", uri=True) as db:
        if json.loads((observer / "registration.json").read_bytes())["wallets"] != registration["wallets"]:
            raise ValueError("wallet_demo_cohort_changed")
        if db.execute("SELECT expires FROM registration").fetchone()[0] < deadline:
            raise ValueError("wallet_demo_collector_window_too_short")
        # A restarted process reuses the same journal and only reconciles submitted intent.
        if not journal.records():
            broker.reconcile_positions()
        report("ready", ticker=mapping["ticker"], closes_to_entries=registration["end_utc"])
        last_row = 0
        while time.time() < deadline or journal.records():
            if journal.records():
                try:
                    result = run_once(broker, trial / "order.lock", registration, {}, quote_provider=quote, offsets=())
                except Exception as error:
                    report("reconciliation_required", error=type(error).__name__)
                    return
                report("receipt", result=result)
                return
            status = json.loads((root / "status.json").read_bytes())
            health = db.execute("SELECT at FROM health WHERE id=1").fetchone()
            if (status.get("new_submissions_enabled") is not False
                    or status.get("production_execution_enabled") is not False):
                raise ValueError("wallet_demo_maker_hold_unverified")
            if health is None or not 0 <= time.time() - health[0] <= 20:
                time.sleep(1)
                continue
            rows = db.execute(
                """SELECT rowid, trade, evidence FROM buys
                   WHERE rowid>? AND state='quote_attempted'
                     AND json_extract(trade,'$.condition_id')=?
                     AND json_extract(trade,'$.token_id')=?
                     AND json_extract(trade,'$.observed_at')>=?
                   ORDER BY rowid LIMIT 20""",
                (last_row, mapping["condition_id"], mapping["token_id"],
                 max(window_start, time.time() - 28)),
            ).fetchall()
            for rowid, raw, evidence in rows:
                last_row = rowid
                trade = json.loads(raw)
                detected = json.loads(evidence).get("demo", {})
                try:
                    offsets = monitor.clock_bounds()
                    evaluate(registration, trade, detected, now=detected["observed_at"],
                             offset_low=offsets[0], offset_high=offsets[1])
                    result = run_once(broker, trial / "order.lock", registration, trade,
                                      quote_provider=quote, offsets=offsets)
                except (BrokerError, ValueError, KeyError) as error:
                    code = error.args[0] if error.args and str(error.args[0]).startswith("wallet_") else type(error).__name__
                    report("signal_rejected", reason=code, source_at=trade["timestamp"])
                    if journal.records():
                        break
                else:
                    report("receipt", result=result)
                    return
            time.sleep(0.5)
        report("expired_without_submission")


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve())
