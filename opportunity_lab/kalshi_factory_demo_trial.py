"""A bounded, fail-closed Demo execution lane for a passed factory hypothesis."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import hashlib
import sqlite3
import time

from .kalshi_external_sleeves import SPORTS_PREFIXES
from .kalshi_strategy_factory import evaluate, fingerprint
from .kalshi_shadow import price_book
from .kalshi_demo_v4_worker import limit_price_cents, one_contract_frame
from .kalshi_binary_accounting import replay_binary


CLIENT_ID_PREFIX = "factory-demo-"
MAX_ATTEMPTS = 100
MAX_FILLS = 40
MAX_ATTEMPTS_PER_DAY = 3
MAX_FLAT_LOSS_CENTS = 100
FEE_RESERVE_CENTS = 5
MAX_EXPIRY_SECONDS = 24 * 3600
MIN_EXPIRY_SECONDS = 300
SCAN_INTERVAL_SECONDS = 60


def attest_attempt(state, journal, client_id, strategy_id):
    """Persist forward risk proof after reservation and before any Demo write."""
    protocol = state.load("factory_trial_protocol") or {}
    counts = trial_counts(state, journal, strategy_id=strategy_id)
    prior = journal.accounting(exclude=client_id)
    record = journal.get(client_id)
    if (protocol.get("strategy_id") != strategy_id or protocol.get("environment") != "demo"
            or record["state"] != "reserved" or record["intent"]["action"] != "buy"
            or record["intent"]["count"] != 1 or not 0 < record["reserve"] <= MAX_FLAT_LOSS_CENTS
            or counts["attempts"] > MAX_ATTEMPTS or counts["attempts_today"] > MAX_ATTEMPTS_PER_DAY
            or counts["fills"] >= MAX_FILLS or prior["positions"]
            or any(r["state"] != "terminal" for r in journal.records() if r["payload"]["client_order_id"] != client_id)
            or trial_counts(state, journal)["realized_net_cents"] - record["reserve"] < -MAX_FLAT_LOSS_CENTS):
        raise ValueError("factory_forward_risk_audit_failed")
    value = {"schema": "factory_demo_risk_attestation_v1", "client_id": client_id, "strategy_id": strategy_id,
             "protocol": protocol, "counts": counts, "reserve_cents": record["reserve"],
             "prior_positions": {}, "prior_unresolved_orders": 0, "created_at": datetime.now(timezone.utc).isoformat()}
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    state.db.execute("CREATE TABLE IF NOT EXISTS factory_risk_attestations(client_id TEXT PRIMARY KEY,detail TEXT NOT NULL,sha256 TEXT NOT NULL)")
    state.db.execute("INSERT INTO factory_risk_attestations VALUES(?,?,?)", (client_id, raw, hashlib.sha256(raw.encode()).hexdigest()))


def eligible_candidate(root: str | Path, *, excluded=()):
    """Read only a version that passed its immutable prospective shadow gate."""
    path = Path(root) / "research_sleeves.sqlite3"
    if not path.exists():
        return None
    db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        rows = db.execute(
            "SELECT strategy_id,spec_hash,spec_json,registered_at,state FROM strategy_factory_candidates "
            "WHERE state='demo_trial_candidate' ORDER BY registered_at,strategy_id"
        ).fetchall()
        for strategy_id, digest, raw, registered_at, state in rows:
            if strategy_id in excluded:
                continue
            spec = json.loads(raw)
            if fingerprint(spec) != digest or spec.get("execution_enabled") is not False:
                raise ValueError("factory_candidate_not_frozen")
            proof = evaluate(db, strategy_id, update_state=False)
            if proof["holdout_state"] == "rejected":
                continue
            if (proof["state"] != "demo_trial_candidate"
                    or proof["reason"] != "prospective_shadow_gate_passed"
                    or proof["complete_independent_events"] < 30
                    or proof["elapsed_days"] < 14
                    or (proof["cost_stressed_net_cents"] or 0) <= 0
                    or (proof["event_cluster_lower_bound_cents"] or 0) <= 0
                    or not proof["holdout_started_at"]
                    or proof["holdout_state"] != "collecting"):
                raise ValueError("factory_shadow_gate_not_reproducible")
            return {"strategy_id": strategy_id, "spec_hash": digest,
                    "spec": spec, "registered_at": registered_at,
                    "holdout_started_at": proof["holdout_started_at"]}
        return None
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return None
        raise
    finally:
        db.close()


def filled_fees_reconciled(fills, evidence):
    """Require broker fee and fill details for every filled trial intent."""
    return all(
        cid in evidence
        and isinstance(evidence[cid].get("fees_dollars"), str)
        and isinstance(evidence[cid].get("fills"), list)
        and len(evidence[cid]["fills"]) > 0
        for cid, _, _ in fills
    )


def trial_counts(state, journal, *, strategy_id=None):
    rows = state.db.execute(
        "SELECT m.client_id,m.event_id,m.created_at FROM intent_meta m "
        "JOIN factory_trial_assignments a ON a.client_id=m.client_id "
        "WHERE m.kind='factory_trial_entry' AND (? IS NULL OR a.strategy_id=?)",
        (strategy_id, strategy_id),
    ).fetchall()
    records = {r["payload"]["client_order_id"]: r for r in journal.records()}
    fills = [(cid, event, at) for cid, event, at in rows
             if cid in records and records[cid]["filled"] == 1]
    tickers = {records[cid]["payload"]["ticker"] for cid, _, _ in rows if cid in records}
    trial_records = [r for r in records.values() if r["payload"]["ticker"] in tickers]
    entries = [records[cid] for cid, _, _ in rows if cid in records]
    trial_ids = {r["payload"]["client_order_id"] for r in trial_records}
    evidence = {key: json.loads(value) for key, value in journal.db.execute(
        "SELECT client_id,detail FROM broker_evidence"
    ) if key in trial_ids}
    # The accounting replay tolerates a missing broker record for an unfilled
    # order.  A filled order, however, is not fee-reconciled until the broker's
    # actual fill and fee detail is present for that exact client ID.
    fees_reconciled = filled_fees_reconciled(fills, evidence)
    settlements = [json.loads(value) for (value,) in journal.db.execute("SELECT detail FROM settlements")
                   if json.loads(value).get("ticker") in tickers]
    ledger = replay_binary(trial_records, evidence, as_of=journal.clock(), settlements=settlements)
    today = datetime.now(timezone.utc).date()
    return {"attempts": len(rows), "fills": len(fills),
            "attempts_today": sum(datetime.fromtimestamp(at, timezone.utc).date() == today
                                  for _, _, at in rows),
            "independent_events": len({event for _, event, _ in fills}),
            "independent_days": len({datetime.fromtimestamp(at, timezone.utc).date()
                                     for _, _, at in fills}),
            "terminal_orders": sum(r["state"] == "terminal" for r in entries),
            "unresolved_orders": sum(r["state"] != "terminal" for r in trial_records),
            "realized_net_cents": round(float(ledger["realized"] * 100), 2),
            "fees_cents": round(float(ledger["fees"] * 100), 2),
            "flat_at_review": not bool(ledger["positions"]),
            "fees_reconciled": fees_reconciled}


def trial_allowed(state, journal, candidate, *, flat_balance_cents, now=None):
    if candidate is None:
        return False, "no_shadow_pass"
    saved = state.load("factory_trial_protocol")
    protocol = {"strategy_id": candidate["strategy_id"], "spec_hash": candidate["spec_hash"],
                "client_id_prefix": CLIENT_ID_PREFIX, "max_attempts": MAX_ATTEMPTS,
                "max_fills": MAX_FILLS, "max_flat_loss_cents": MAX_FLAT_LOSS_CENTS,
                "max_attempts_per_day": MAX_ATTEMPTS_PER_DAY,
                "max_expiry_seconds": MAX_EXPIRY_SECONDS, "environment": "demo"}
    if saved is None or saved != protocol:
        if saved is None and trial_counts(state, journal)["attempts"]:
            raise ValueError("factory_trial_protocol_missing")
        if saved is not None and (journal.accounting()["positions"] or
                                  any(r["state"] != "terminal" for r in journal.records())):
            return False, "prior_trial_not_flat"
        state.save("factory_trial_protocol", protocol)
        if state.load("factory_trial_start_balance_cents") is None:
            state.save("factory_trial_start_balance_cents", flat_balance_cents)
        state.record(None, {"action": "factory_trial_activated", **protocol})
    start = state.load("factory_trial_start_balance_cents")
    if type(start) is not int or type(flat_balance_cents) is not int:
        return False, "balance_unavailable"
    if trial_counts(state, journal)["realized_net_cents"] <= -MAX_FLAT_LOSS_CENTS:
        return False, "realized_loss_stop"
    counts = trial_counts(state, journal, strategy_id=candidate["strategy_id"])
    if counts["attempts"] >= MAX_ATTEMPTS:
        return False, "attempt_cap"
    if counts["fills"] >= MAX_FILLS:
        return False, "fill_cap"
    if counts["attempts_today"] >= MAX_ATTEMPTS_PER_DAY:
        return False, "daily_attempt_cap"
    if counts["fills"] >= 5 and counts["flat_at_review"] and counts["realized_net_cents"] <= -20:
        return False, "negative_demo_net"
    if journal.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0]:
        return False, "journal_stopped"
    if any(r["state"] != "terminal" for r in journal.records()):
        return False, "unresolved_order"
    if journal.accounting()["positions"]:
        return False, "position_open"
    return True, "bounded_demo_trial"


def recent_signal(root, candidate, state, markets, *, now=None,
                  max_total_risk_cents=MAX_FLAT_LOSS_CENTS):
    """Requote a recent research signal and verify the same stratum and ask bin."""
    clock = time.time() if now is None else now
    if not candidate.get("holdout_started_at"):
        return None
    path = Path(root) / "research_sleeves.sqlite3"
    db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        rows = db.execute(
            "SELECT o.detail,o.observed_at FROM calibration_parent_observations o "
            "WHERE o.observed_at>? AND NOT EXISTS ("
            "SELECT 1 FROM strategy_factory_events e WHERE e.strategy_id=? "
            "AND e.event_id=o.event_id) ORDER BY o.observed_at DESC LIMIT 500",
            (candidate["holdout_started_at"], candidate["strategy_id"]),
        ).fetchall()
    finally:
        db.close()
    low, high = map(int, candidate["spec"]["price_bin"].split("-"))
    checked_tickers = set()
    for raw, observed_at in rows:
        try:
            row = json.loads(raw)
            seen = datetime.fromisoformat(observed_at.replace("Z", "+00:00")).timestamp()
            if not 0 <= clock - seen <= 3600:
                continue
            if (row.get("stratum") != candidate["spec"]["stratum"]
                    or row.get("price_bin") != candidate["spec"]["price_bin"]
                    or (candidate["spec"].get("side", "either") != "either"
                        and row.get("side") != candidate["spec"]["side"])
                    or (candidate["spec"].get("family", "*") != "*"
                        and row.get("family") != candidate["spec"]["family"])):
                continue
            event = row["event_id"]
            if state.db.execute("SELECT 1 FROM entered_events WHERE event_id=?", (event,)).fetchone():
                continue
            if state.db.execute(
                "SELECT 1 FROM intent_meta WHERE event_id=? AND kind='factory_trial_entry'", (event,)
            ).fetchone():
                continue
            if state.db.execute("SELECT 1 FROM intent_meta WHERE ticker=?", (row["ticker"],)).fetchone():
                continue
            if row["ticker"] in checked_tickers:
                continue
            if len(checked_tickers) >= 3:
                break
            checked_tickers.add(row["ticker"])
            quote = markets.quote({"ticker": row["ticker"]})
            market = quote["market"]
            expiry = datetime.fromisoformat(market["expiration_time"].replace("Z", "+00:00")).timestamp()
            if not MIN_EXPIRY_SECONDS <= expiry - clock <= MAX_EXPIRY_SECONDS:
                continue
            stratum = "sports" if event.upper().startswith(SPORTS_PREFIXES) else "non_sports"
            if market.get("event_ticker") != event or stratum != candidate["spec"]["stratum"]:
                continue
            family = market.get("category") or event.split("-")[0]
            if candidate["spec"].get("family", "*") != "*" and family != candidate["spec"]["family"]:
                continue
            frame = one_contract_frame(quote, book_id=str(quote["observed_at"]))
            _bid, ask, _bid_size, ask_size = price_book(frame, row["side"])
            cents = int((Decimal(ask.numerator) / Decimal(ask.denominator) * 100).to_integral_value())
            if ask_size < 1 or not low <= cents <= high:
                continue
            price = limit_price_cents(frame, row["side"], "buy")
            if price > high or price + FEE_RESERVE_CENTS > max_total_risk_cents:
                continue
            return {"market": market, "frame": frame, "outcome": row["side"],
                    "price_cents": price, "event_id": event,
                    "strategy_id": candidate["strategy_id"]}
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    return None
