"""Export auditable event-level Demo evidence; missing proof never becomes a pass."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
import hashlib
import json
from pathlib import Path
from collections import Counter
import sqlite3

from .kalshi_strategy_factory import evaluate, _event_values, fingerprint
from .kalshi_binary_journal import BinaryJournal
from .kalshi_factory_demo_trial import (trial_counts, CLIENT_ID_PREFIX, MAX_FLAT_LOSS_CENTS,
                                      MAX_ATTEMPTS, MAX_FILLS, MAX_ATTEMPTS_PER_DAY)
from .kalshi_factory_fee_probe import SCHEDULE_ID


def _db(path):
    db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    db.execute("BEGIN")
    return db


def _fee_audit(db, strategy, table):
    for detail, resolution, basis_raw, digest in db.execute(
            f"SELECT o.detail,o.resolution,f.detail,f.sha256 FROM {table} e "
            "JOIN calibration_parent_observations o USING(observation_id) "
            "LEFT JOIN factory_fee_observations f ON f.strategy_id=e.strategy_id AND f.observation_id=e.observation_id "
            "WHERE e.strategy_id=? AND o.resolution IS NOT NULL", (strategy,)):
        if basis_raw is None or hashlib.sha256(basis_raw.encode()).hexdigest() != digest:
            raise ValueError("contemporaneous_fee_evidence_missing")
        row = json.loads(detail); basis = json.loads(basis_raw); outcome = json.loads(resolution)
        multiplier = Decimal(basis["fee_multiplier"]); price = Decimal(row["price_cents"]) / 100
        fee = int((Decimal(7) * multiplier * price * (1 - price)).to_integral_value(rounding=ROUND_CEILING))
        observed = datetime.fromisoformat(row["observed_at"])
        fetched = datetime.fromisoformat(basis["metadata_fetched_at"])
        if (basis["schedule_id"] != SCHEDULE_ID or basis["fee_type"] != "quadratic"
                or not multiplier.is_finite() or not 0 <= multiplier <= 10
                or not 0 <= (fetched - observed).total_seconds() <= 120
                or basis["event_ticker"] != row["event_id"] or basis["price_cents"] != row["price_cents"]
                or basis["modeled_entry_cost_cents"] != row["price_cents"] + fee
                or outcome.get("hypothetical_only") is not True or outcome.get("payout_cents") not in (0, 100)
                or outcome["cost_stressed_net_cents"] != outcome["payout_cents"] - row["price_cents"] - 2):
            raise ValueError("fee_or_resolution_audit_failed")
        if fee > 2:
            # Do not silently revise the registered two-cent fee model.
            raise ValueError("fee_exceeds_registered_model_new_protocol_required")


def build(root, packet):
    root = Path(root); databases = []
    report = {"schema": "kalshi_dossier_export_v1", "ready": False, "blockers": [], "evidence": None}
    try:
        trial = packet.get("factory_demo_trial") or {}; protocol = trial.get("protocol") or {}
        strategy = protocol.get("strategy_id")
        if not strategy:
            raise ValueError("no_shadow_passed_version")
        research = _db(root / "research_sleeves.sqlite3"); databases.append(research)
        candidate = evaluate(research, strategy, update_state=False)
        if candidate["state"] != "demo_trial_candidate" or candidate["spec_hash"] != protocol.get("spec_hash"):
            raise ValueError("candidate_not_frozen_shadow_pass")
        if candidate["evaluation_protocol"] not in ("tournament_v1", "tournament_fee_v2"):
            raise ValueError("selection_protocol_not_registered")
        if not candidate["holdout_started_at"] or candidate["holdout_state"] == "rejected":
            raise ValueError("untouched_holdout_missing_or_rejected")
        checkpoint_sets = {}
        for split, table in (("prospective", "strategy_factory_events"), ("holdout", "strategy_factory_holdout_events")):
            _fee_audit(research, strategy, table)
            current = _event_values(research, table, strategy, candidate["spec"]["extra_fee_stress_cents"])
            checkpoint = None
            for raw, digest in research.execute("SELECT snapshot_json,sha256 FROM strategy_tournament_checkpoints "
                                                "WHERE strategy_id=? AND split=? ORDER BY look", (strategy, split)):
                if hashlib.sha256(raw.encode()).hexdigest() != digest:
                    raise ValueError("checkpoint_artifact_changed")
                check = json.loads(raw)
                if check["adjusted_lower_bound_cents"] > 0:
                    checkpoint = check; break
            if not checkpoint or len(checkpoint["event_values"]) > 1000:
                raise ValueError("positive_bounded_checkpoint_missing")
            if any(current.get(event) != value for event, value in checkpoint["event_values"]):
                raise ValueError("frozen_event_outcomes_changed")
            checkpoint_sets[split] = checkpoint
        state_db = _db(root / "worker.sqlite3"); databases.append(state_db)
        journal_db = _db(root / "journal.sqlite3"); databases.append(journal_db)
        # Reuse the verified replay against read-only snapshots, without opening
        # a writer, changing a protocol or touching an external account.
        state = type("StateSnapshot", (), {"db": state_db})()
        journal = BinaryJournal.__new__(BinaryJournal)
        journal.db = journal_db; journal.clock = lambda: datetime.now(timezone.utc)
        journal.validate_reconciliation()
        counts = trial_counts(state, journal, strategy_id=strategy)
        if (counts["unresolved_orders"] or not counts["flat_at_review"] or not counts["fees_reconciled"]
                or counts["attempts"] != counts["terminal_orders"]):
            raise ValueError("demo_broker_reconciliation_incomplete")
        assigned = {cid for (cid,) in state_db.execute("SELECT client_id FROM factory_trial_assignments WHERE strategy_id=?", (strategy,))}
        entries = [r for r in journal.records() if r["payload"]["client_order_id"] in assigned]
        if len(entries) != counts["attempts"] or any(r["intent"]["count"] != 1 or r["reserve"] > MAX_FLAT_LOSS_CENTS for r in entries):
            raise ValueError("demo_risk_protocol_audit_failed")
        if (protocol.get("environment") != "demo" or protocol.get("client_id_prefix") != CLIENT_ID_PREFIX
                or protocol.get("max_attempts") != MAX_ATTEMPTS or protocol.get("max_fills") != MAX_FILLS
                or protocol.get("max_attempts_per_day") != MAX_ATTEMPTS_PER_DAY
                or protocol.get("max_flat_loss_cents") != MAX_FLAT_LOSS_CENTS
                or counts["attempts"] > MAX_ATTEMPTS or counts["fills"] > MAX_FILLS):
            raise ValueError("demo_risk_protocol_audit_failed")
        days = Counter(datetime.fromtimestamp(at, timezone.utc).date() for (at,) in state_db.execute(
            "SELECT m.created_at FROM intent_meta m JOIN factory_trial_assignments a USING(client_id) WHERE a.strategy_id=?",
            (strategy,)))
        if any(count > MAX_ATTEMPTS_PER_DAY for count in days.values()):
            raise ValueError("demo_daily_attempt_cap_breached")
        # Any persisted risk latch needs explicit resolution before promotion.
        if journal_db.execute("SELECT count(*) FROM risk_stops").fetchone()[0]:
            raise ValueError("demo_risk_stop_requires_review")
        for record in entries:
            cid = record["payload"]["client_order_id"]
            proof = state_db.execute("SELECT detail,sha256 FROM factory_risk_attestations WHERE client_id=?", (cid,)).fetchone()
            if not proof or hashlib.sha256(proof[0].encode()).hexdigest() != proof[1]:
                raise ValueError("forward_demo_risk_evidence_missing")
            audit = json.loads(proof[0]); checked = audit["counts"]
            if (audit["schema"] != "factory_demo_risk_attestation_v1" or audit["client_id"] != cid
                    or audit["strategy_id"] != strategy or audit["protocol"] != protocol
                    or audit["reserve_cents"] != record["reserve"] or audit["prior_positions"] != {}
                    or audit["prior_unresolved_orders"] != 0 or checked["attempts"] > MAX_ATTEMPTS
                    or checked["attempts_today"] > MAX_ATTEMPTS_PER_DAY or checked["fills"] >= MAX_FILLS):
                raise ValueError("forward_demo_risk_evidence_invalid")
        restart = trial.get("restart_reconciliation") or {}
        if restart.get("environment") != "demo" or restart.get("positions_verified") is not True or not restart.get("reconciled_at"):
            raise ValueError("restart_reconciliation_evidence_missing")
        prospective_start = research.execute("SELECT min(observed_at) FROM strategy_factory_events WHERE strategy_id=?", (strategy,)).fetchone()[0]
        evidence = {"schema": "kalshi_promotion_evidence_v1", "strategy_id": strategy,
                    "version": candidate["evaluation_protocol"], "registration_sha256": candidate["spec_hash"],
                    "strategy_spec": candidate["spec"],
                    "registered_at": candidate["registered_at"], "prospective_started_at": prospective_start,
                    "holdout_started_at": candidate["holdout_started_at"],
                    "tournament_audit": {key: {field: value for field, value in check.items()
                                               if field in {"look", "candidate_index", "alpha"}}
                                         for key, check in checkpoint_sets.items()},
                    "demo": {"attempts": counts["attempts"], "fills": counts["fills"],
                             "terminal_orders": counts["terminal_orders"], "unresolved_orders": 0,
                             "risk_breaches": 0, "independent_days": counts["independent_days"],
                             "net_after_fees_cents": counts["realized_net_cents"],
                             "restart_reconciled": True, "flat_at_review": True, "fees_reconciled": True}}
        for split, check in checkpoint_sets.items():
            evidence[split + "_events"] = [{"event_id": event, "net_after_fees_cents": value}
                                           for event, value in check["event_values"]]
        evidence["evidence_sha256"] = hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        report.update(ready=True, evidence=evidence)
    except (ValueError, KeyError, TypeError, sqlite3.Error, OSError, ArithmeticError) as error:
        code = str(error) if isinstance(error, ValueError) and str(error).replace("_", "").isalnum() else "dossier_evidence_unavailable"
        report["blockers"] = [code[:100]]
    finally:
        for db in databases:
            db.close()
    return report
