"""Versioned, allowlisted research experiments. Registration grants no order authority."""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing
import hashlib
import json
import re
import sqlite3

from .kalshi_strategy_factory import EXTRA_FEE_STRESS_CENTS, _valid_generated_spec


CAPABILITIES = {
    "depth_replenishment_quote_v1": {"version": 1, "runner": "maker_depth_recorder",
                                   "evidence": "prospective displayed depth; no trade/cancel attribution or fills", "orders": False},
    "ask_to_settlement_v1": {"version": 1, "runner": "strategy_factory",
                             "evidence": "prospective quotes and settlements", "orders": False},
    "bea_gdp_release_quote_v1": {"version": 1, "runner": "official_release_probe",
                                 "evidence": "official advance GDP release and later Demo quotes",
                                 "orders": False},
    "nested_threshold_spread_quote_v1": {"version":1,"runner":"structural_signals",
        "evidence":"matching nested rules and near-synchronous two-leg Demo depth; indicative only","orders":False},
}


def init(db):
    db.execute("""CREATE TABLE IF NOT EXISTS research_experiments(
        idea_id TEXT PRIMARY KEY,capability_id TEXT NOT NULL,version INTEGER NOT NULL,
        spec_hash TEXT NOT NULL,registered_at TEXT NOT NULL)""")


def register(db, ideas, *, now=None):
    init(db)
    if not isinstance(ideas, list) or len(ideas) > 32:
        return 0
    at = now or datetime.now(timezone.utc)
    if at.tzinfo is None:
        raise ValueError("timezone_required")
    added = 0
    for idea in ideas:
        if not isinstance(idea, dict) or set(idea) != {"id", "capability_id", "version", "spec_hash", "spec"}:
            continue
        if (not isinstance(idea["id"], str) or not re.fullmatch(r"[0-9a-f-]{36}", idea["id"])
                or idea["capability_id"] not in CAPABILITIES
                or type(idea["version"]) is not int or idea["version"] != 1):
            continue
        capability = idea["capability_id"]
        if capability in {"bea_gdp_release_quote_v1","nested_threshold_spread_quote_v1","depth_replenishment_quote_v1"}:
            if idea["spec"] != {}:
                continue
            frozen = {"capability_id": capability, "version": 1, "spec": {}}
            digest = hashlib.sha256(json.dumps(frozen, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            if digest != idea["spec_hash"]:
                continue
        else:
            # Factory validates this strategy grammar and hash independently.
            if not isinstance(idea["spec"], dict):
                continue
            generated = {"primitive": "buy_at_observed_ask_to_settlement", **idea["spec"],
                         "origin_idea_id": idea["id"],
                         "extra_fee_stress_cents": EXTRA_FEE_STRESS_CENTS,
                         "execution_enabled": False}
            if not _valid_generated_spec(generated):
                continue
            digest = hashlib.sha256(json.dumps(idea["spec"], sort_keys=True,
                                               separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
            if digest != idea["spec_hash"]:
                continue
        cursor = db.execute("INSERT OR IGNORE INTO research_experiments VALUES(?,?,?,?,?)",
                            (idea["id"], capability, 1, digest, at.astimezone(timezone.utc).isoformat()))
        added += cursor.rowcount
    return added


def status(db, factory, release, *, depth_path=None):
    init(db)
    candidates = {(row.get("spec") or {}).get("origin_idea_id", row.get("origin_idea_id")): row
                  for row in factory.get("candidates", [])}
    rows = []
    for idea_id, capability, version, digest, registered_at in db.execute(
        "SELECT idea_id,capability_id,version,spec_hash,registered_at FROM research_experiments "
        "ORDER BY registered_at DESC,idea_id LIMIT 24"
    ):
        if capability == "ask_to_settlement_v1":
            candidate = candidates.get(idea_id)
            state = candidate.get("state", "awaiting_runner") if candidate else "awaiting_runner"
            evidence_count = candidate.get("complete_independent_events", 0) if candidate else 0
            evidence_ref = candidate.get("strategy_id") if candidate else None
        elif capability == 'depth_replenishment_quote_v1':
            evidence_count = 0; state = 'awaiting_runner'
            if depth_path is not None:
                try:
                    with closing(sqlite3.connect(depth_path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as depth_db:
                        exists = depth_db.execute("SELECT 1 FROM sqlite_master WHERE name='depth_snapshots'").fetchone()
                        if exists:
                            start = datetime.fromisoformat(registered_at).timestamp()
                            evidence_count = depth_db.execute('SELECT count(DISTINCT event_id) FROM depth_snapshots WHERE observed_at>=?', (start,)).fetchone()[0]
                            state = 'shadow'
                except sqlite3.Error:
                    pass
            evidence_ref = 'depth_snapshots:prospective_displayed_depth_not_fills'
        elif capability=='nested_threshold_spread_quote_v1':
            # Only new captures count; parent events, not contract pairs, are independent.
            exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='structural_signals'").fetchone()
            observations=list(db.execute('SELECT event_id,detail FROM structural_signals WHERE observed_at>? ORDER BY observed_at LIMIT 20000',
                                         (registered_at,))) if exists else []
            verified=[]
            for event_id,raw in observations:
                value=json.loads(raw)
                if value.get('fill_assumed') is False and value.get('execution_enabled') is False and value.get('fully_executable_snapshot') is True:
                    verified.append((event_id,value))
            evidence_count=len({event for event,_ in verified})
            # An installed collector with no eligible prospective captures is not
            # a missing runner. Zero events still confer no promotion evidence.
            state='shadow' if exists else 'awaiting_runner'
            evidence_ref='structural_signals:prospective_depth_only_not_filled_profit'
        else:
            published_at = release.get("first_publication_observed_at")
            after_registration = bool(published_at and published_at > registered_at)
            screen = release.get("shadow_screen") or {}
            evidence_count = screen.get("release_events_observed", 0) if after_registration else 0
            state = "shadow" if after_registration else "awaiting_future_release"
            evidence_ref = release.get("first_publication_source_url") if after_registration else None
        rows.append({"idea_id": idea_id, "capability_id": capability, "version": version,
                     "spec_hash": digest, "registered_at": registered_at,
                     "state": state, "independent_events": evidence_count,
                     "evidence_ref": evidence_ref, "orders_enabled": False})
    return {"schema": "kalshi_experiment_registry_v1", "execution_enabled": False,
            "capabilities": CAPABILITIES, "experiments": rows}
