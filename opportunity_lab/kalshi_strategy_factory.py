"""Bounded, prospective Kalshi hypothesis factory using approved market primitives.

The factory discovers a new price-bin/market-group hypothesis after rejection.
It never submits an order, treats historical outcomes only as a ranking screen,
and evaluates each version solely on observations created after registration.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
import sqlite3
import statistics

from .kalshi_external_sleeves import PRICE_BINS


MIN_COMPLETE_EVENTS = 30
MIN_ELAPSED_DAYS = 14
EARLY_REJECT_EVENTS = 15
PRODUCTIVITY_DAYS = 45
EXTRA_FEE_STRESS_CENTS = 3  # Existing resolution already subtracts 2 cents.
MAX_CANDIDATES = 8


def specs():
    for stratum in ("sports", "non_sports"):
        for low, high in PRICE_BINS:
            yield {"primitive": "buy_at_observed_ask_to_settlement",
                   "stratum": stratum, "price_bin": f"{low}-{high}",
                   "extra_fee_stress_cents": EXTRA_FEE_STRESS_CENTS,
                   "execution_enabled": False}


def fingerprint(spec):
    if spec not in tuple(specs()):
        raise ValueError("unapproved_strategy_spec")
    return hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def init(db: sqlite3.Connection):
    db.executescript("""
      CREATE TABLE IF NOT EXISTS strategy_factory_candidates(
        strategy_id TEXT PRIMARY KEY,spec_hash TEXT NOT NULL UNIQUE,
        spec_json TEXT NOT NULL,registered_at TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('shadow','rejected','demo_trial_candidate')),
        reason TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS strategy_factory_events(
        strategy_id TEXT NOT NULL,observation_id TEXT NOT NULL,
        event_id TEXT NOT NULL,observed_at TEXT NOT NULL,
        PRIMARY KEY(strategy_id,observation_id));
      CREATE INDEX IF NOT EXISTS strategy_factory_event_idx
        ON strategy_factory_events(strategy_id,event_id);
      CREATE INDEX IF NOT EXISTS calibration_parent_observed_idx
        ON calibration_parent_observations(observed_at);
    """)


def _rows(db):
    for observation_id, event_id, observed_at, detail, resolution in db.execute(
        "SELECT observation_id,event_id,observed_at,detail,resolution "
        "FROM calibration_parent_observations"
    ):
        try:
            row = json.loads(detail)
            outcome = json.loads(resolution) if resolution is not None else None
        except (TypeError, ValueError):
            continue
        yield observation_id, event_id, observed_at, row, outcome


def _matches(spec, row):
    return (row.get("stratum") == spec["stratum"]
            and row.get("price_bin") == spec["price_bin"]
            and row.get("execution_enabled") is False
            and row.get("fill_assumed") is False)


def _rank_untried(db):
    tried = {row[0] for row in db.execute("SELECT spec_hash FROM strategy_factory_candidates")}
    rows = list(_rows(db))
    ranked = []
    for spec in specs():
        digest = fingerprint(spec)
        if digest in tried:
            continue
        event_outcomes = {}
        for _id, event_id, _at, row, outcome in rows:
            if (_matches(spec, row) and outcome is not None
                    and isinstance(outcome.get("cost_stressed_net_cents"), (int, float))):
                event_outcomes.setdefault(event_id, float(outcome["cost_stressed_net_cents"]) - EXTRA_FEE_STRESS_CENTS)
        values = list(event_outcomes.values())
        # The old sample ranks what to test next; it never contributes to the
        # prospective release gate. A small-sample penalty prevents a lone win
        # from dominating a better-supported group.
        score = statistics.mean(values) - 10 / math.sqrt(len(values)) if values else -1000.0
        ranked.append((-score, -len(values), digest, spec))
    return min(ranked)[-1] if ranked else None


def _now(now):
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        raise ValueError("timezone_required")
    return value.astimezone(timezone.utc)


def register_next(db, *, now=None):
    at = _now(now)
    if db.execute("SELECT 1 FROM strategy_factory_candidates WHERE state='shadow'").fetchone():
        return None
    if db.execute("SELECT count(*) FROM strategy_factory_candidates").fetchone()[0] >= MAX_CANDIDATES:
        return None
    spec = _rank_untried(db)
    if spec is None:
        return None
    digest = fingerprint(spec)
    strategy_id = "kalshi_factory_" + digest[:12]
    db.execute("INSERT INTO strategy_factory_candidates VALUES(?,?,?,?,?,?)",
               (strategy_id, digest, json.dumps(spec, sort_keys=True),
                at.isoformat(), "shadow", "prospective_registration"))
    return strategy_id


def capture_future(db):
    active = db.execute("SELECT strategy_id,spec_json,registered_at FROM strategy_factory_candidates "
                        "WHERE state='shadow'").fetchall()
    if not active:
        return 0
    added = 0
    for strategy_id, raw_spec, registered_at in active:
        spec = json.loads(raw_spec)
        future = db.execute(
            "SELECT o.observation_id,o.event_id,o.observed_at,o.detail "
            "FROM calibration_parent_observations o "
            "WHERE o.observed_at>? AND NOT EXISTS ("
            "SELECT 1 FROM strategy_factory_events e WHERE e.strategy_id=? "
            "AND e.observation_id=o.observation_id)",
            (registered_at, strategy_id),
        )
        for observation_id, event_id, observed_at, detail in future:
            try:
                row = json.loads(detail)
            except (TypeError, ValueError):
                continue
            if not _matches(spec, row):
                continue
            cursor = db.execute("INSERT OR IGNORE INTO strategy_factory_events VALUES(?,?,?,?)",
                                (strategy_id, observation_id, event_id, observed_at))
            added += cursor.rowcount
    return added


def evaluate(db, strategy_id, *, now=None):
    at = _now(now)
    candidate = db.execute("SELECT spec_json,registered_at,state,reason FROM strategy_factory_candidates "
                           "WHERE strategy_id=?", (strategy_id,)).fetchone()
    if candidate is None:
        raise ValueError("unknown_candidate")
    spec = json.loads(candidate[0]); registered = datetime.fromisoformat(candidate[1])
    elapsed_days = max(0, (at - registered).total_seconds() / 86400)
    events = defaultdict(list); signals = 0
    for event_id, resolution in db.execute(
        "SELECT e.event_id,o.resolution FROM strategy_factory_events e "
        "JOIN calibration_parent_observations o ON o.observation_id=e.observation_id "
        "WHERE e.strategy_id=?", (strategy_id,)
    ):
        signals += 1
        try:
            outcome = json.loads(resolution) if resolution else None
            raw = outcome.get("cost_stressed_net_cents") if outcome else None
            if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw):
                continue
        except (TypeError, ValueError):
            continue
        events[event_id].append(float(raw) - spec["extra_fee_stress_cents"])
    # Average once per parent event to avoid correlated contracts inflating n.
    values = [statistics.mean(rows) for rows in events.values()]
    total = sum(values)
    lower = (statistics.mean(values) - 1.96 * statistics.stdev(values) / math.sqrt(len(values))
             if len(values) >= 2 else None)
    state, reason = candidate[2], candidate[3]
    if state == "shadow":
        if len(values) >= EARLY_REJECT_EVENTS and statistics.mean(values) <= -1:
            state, reason = "rejected", "negative_prospective_mean"
        elif elapsed_days >= PRODUCTIVITY_DAYS and len(values) < MIN_COMPLETE_EVENTS:
            state, reason = "rejected", "insufficient_resolved_events"
        elif (elapsed_days >= MIN_ELAPSED_DAYS and len(values) >= MIN_COMPLETE_EVENTS
              and total > 0 and lower is not None and lower > 0):
            state, reason = "demo_trial_candidate", "prospective_shadow_gate_passed"
        if state != candidate[2]:
            db.execute("UPDATE strategy_factory_candidates SET state=?,reason=? WHERE strategy_id=?",
                       (state, reason, strategy_id))
    return {"strategy_id": strategy_id, "spec_hash": fingerprint(spec), "spec": spec,
            "registered_at": registered.isoformat(), "state": state, "reason": reason,
            "prospective_signals": signals, "complete_independent_events": len(values),
            "elapsed_days": round(elapsed_days, 2), "extra_fee_stress_cents": EXTRA_FEE_STRESS_CENTS,
            "cost_stressed_net_cents": round(total, 2) if values else None,
            "event_cluster_lower_bound_cents": round(lower, 3) if lower is not None else None,
            "execution_enabled": False, "fill_assumed": False,
            "live_promotion_eligible": False}


def cycle(db, *, now=None):
    at = _now(now)
    init(db)
    capture_future(db)
    for (strategy_id,) in db.execute("SELECT strategy_id FROM strategy_factory_candidates WHERE state='shadow'").fetchall():
        evaluate(db, strategy_id, now=at)
    register_next(db, now=at)
    return status(db, now=at)


def status(db, *, now=None):
    at = _now(now)
    rows = [evaluate(db, row[0], now=at) for row in db.execute(
        "SELECT strategy_id FROM strategy_factory_candidates ORDER BY registered_at,strategy_id")]
    active = next((row["strategy_id"] for row in rows if row["state"] == "shadow"), None)
    exhausted = active is None and len(rows) >= MAX_CANDIDATES
    return {"schema": "kalshi_strategy_factory_v1", "generated_at": at.isoformat(),
            "execution_enabled": False, "candidate_limit": MAX_CANDIDATES,
            "candidates": rows,
            "active_strategy_id": active, "grammar_exhausted": exhausted,
            "next_action": "new_approved_primitive_required" if exhausted else "continue_prospective_search"}
