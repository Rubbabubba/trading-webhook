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
import re
import sqlite3
import statistics

from .kalshi_external_sleeves import PRICE_BINS


MIN_COMPLETE_EVENTS = 30
MIN_ELAPSED_DAYS = 14
EARLY_REJECT_EVENTS = 15
PRODUCTIVITY_DAYS = 45
EXTRA_FEE_STRESS_CENTS = 3  # Existing resolution already subtracts 2 cents.
MAX_CANDIDATES = 8
MAX_REPORTED_CANDIDATES = 24


def specs():
    for stratum in ("sports", "non_sports"):
        for low, high in PRICE_BINS:
            yield {"primitive": "buy_at_observed_ask_to_settlement",
                   "stratum": stratum, "price_bin": f"{low}-{high}",
                   "extra_fee_stress_cents": EXTRA_FEE_STRESS_CENTS,
                   "execution_enabled": False}


def fingerprint(spec):
    if spec not in tuple(specs()) and not _valid_generated_spec(spec):
        raise ValueError("unapproved_strategy_spec")
    return hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _valid_generated_spec(spec):
    if not isinstance(spec, dict) or set(spec) != {
        "primitive", "stratum", "price_bin", "side", "family", "origin_idea_id",
        "extra_fee_stress_cents", "execution_enabled",
    }:
        return False
    return (spec["primitive"] == "buy_at_observed_ask_to_settlement"
            and spec["stratum"] in {"sports", "non_sports"}
            and spec["price_bin"] in {f"{low}-{high}" for low, high in PRICE_BINS}
            and spec["side"] in {"yes", "no", "either"}
            and isinstance(spec["family"], str)
            and bool(re.fullmatch(r"[A-Za-z0-9 _./:-]{1,80}|\*", spec["family"]))
            and isinstance(spec["origin_idea_id"], str)
            and bool(re.fullmatch(r"[0-9a-f-]{36}", spec["origin_idea_id"]))
            and type(spec["extra_fee_stress_cents"]) is int
            and spec["extra_fee_stress_cents"] == EXTRA_FEE_STRESS_CENTS
            and spec["execution_enabled"] is False)


def register_ideas(db, ideas, *, now=None):
    """Admit at most one validated, novel AI idea to prospective shadow tests."""
    if not isinstance(ideas, list) or len(ideas) > 32:
        return None
    if db.execute("SELECT 1 FROM strategy_factory_candidates WHERE state='shadow' "
                  "AND strategy_id LIKE 'kalshi_idea_%'").fetchone():
        return None
    for idea in ideas:
        if not isinstance(idea, dict) or set(idea) != {"id", "spec_hash", "spec"}:
            continue
        raw = idea["spec"]
        if not isinstance(raw, dict) or set(raw) != {"stratum", "price_bin", "side", "family"}:
            continue
        raw_hash = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":"),
                                             ensure_ascii=False).encode()).hexdigest()
        if raw_hash != idea["spec_hash"]:
            continue
        spec = {"primitive": "buy_at_observed_ask_to_settlement", **raw,
                "origin_idea_id": idea["id"],
                "extra_fee_stress_cents": EXTRA_FEE_STRESS_CENTS,
                "execution_enabled": False}
        if not _valid_generated_spec(spec):
            continue
        digest = fingerprint(spec)
        strategy_id = "kalshi_idea_" + digest[:12]
        db.execute("INSERT OR IGNORE INTO strategy_factory_candidates VALUES(?,?,?,?,?,?)",
                   (strategy_id, digest, json.dumps(spec, sort_keys=True),
                    _now(now).isoformat(), "shadow", "ai_idea_prospective_registration"))
        return strategy_id
    return None


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
      CREATE TABLE IF NOT EXISTS strategy_factory_holdouts(
        strategy_id TEXT PRIMARY KEY,started_at TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('collecting','rejected')),
        reason TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS strategy_factory_holdout_events(
        strategy_id TEXT NOT NULL,observation_id TEXT NOT NULL,
        event_id TEXT NOT NULL,observed_at TEXT NOT NULL,
        PRIMARY KEY(strategy_id,observation_id));
      CREATE INDEX IF NOT EXISTS strategy_factory_holdout_event_idx
        ON strategy_factory_holdout_events(strategy_id,event_id);
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
            and (spec.get("side", "either") == "either" or row.get("side") == spec["side"])
            and (spec.get("family", "*") == "*" or row.get("family") == spec["family"])
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
    if db.execute("SELECT 1 FROM strategy_factory_candidates WHERE state='shadow' "
                  "AND strategy_id LIKE 'kalshi_factory_%'").fetchone():
        return None
    if db.execute("SELECT count(*) FROM strategy_factory_candidates "
                  "WHERE strategy_id LIKE 'kalshi_factory_%'").fetchone()[0] >= MAX_CANDIDATES:
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


def capture_holdout(db):
    """Collect later events that never appeared in a candidate's first split."""
    added = 0
    for strategy_id, raw_spec, started_at in db.execute(
        "SELECT c.strategy_id,c.spec_json,h.started_at FROM strategy_factory_candidates c "
        "JOIN strategy_factory_holdouts h USING(strategy_id)"
    ).fetchall():
        spec = json.loads(raw_spec)
        cursor = db.execute(
            "SELECT o.observation_id,o.event_id,o.observed_at,o.detail "
            "FROM calibration_parent_observations o WHERE o.observed_at>? "
            "AND NOT EXISTS (SELECT 1 FROM strategy_factory_events e "
            "WHERE e.strategy_id=? AND e.event_id=o.event_id) "
            "AND NOT EXISTS (SELECT 1 FROM strategy_factory_holdout_events h "
            "WHERE h.strategy_id=? AND h.observation_id=o.observation_id)",
            (started_at, strategy_id, strategy_id),
        )
        for observation_id, event_id, observed_at, detail in cursor:
            try:
                row = json.loads(detail)
            except (TypeError, ValueError):
                continue
            if _matches(spec, row):
                inserted = db.execute(
                    "INSERT OR IGNORE INTO strategy_factory_holdout_events VALUES(?,?,?,?)",
                    (strategy_id, observation_id, event_id, observed_at),
                )
                added += inserted.rowcount
    return added


def _event_values(db, table, strategy_id, extra_fee_cents):
    groups = defaultdict(list)
    for event_id, resolution in db.execute(
        f"SELECT e.event_id,o.resolution FROM {table} e "
        "JOIN calibration_parent_observations o ON o.observation_id=e.observation_id "
        "WHERE e.strategy_id=?", (strategy_id,),
    ):
        try:
            outcome = json.loads(resolution) if resolution else None
            raw = outcome.get("cost_stressed_net_cents") if outcome else None
            if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw):
                continue
        except (TypeError, ValueError):
            continue
        groups[event_id].append(float(raw) - extra_fee_cents)
    return {event_id: statistics.mean(rows) for event_id, rows in groups.items()}


def evaluate(db, strategy_id, *, now=None, update_state=True):
    at = _now(now)
    candidate = db.execute("SELECT spec_json,registered_at,state,reason FROM strategy_factory_candidates "
                           "WHERE strategy_id=?", (strategy_id,)).fetchone()
    if candidate is None:
        raise ValueError("unknown_candidate")
    spec = json.loads(candidate[0]); registered = datetime.fromisoformat(candidate[1])
    elapsed_days = max(0, (at - registered).total_seconds() / 86400)
    signals = db.execute("SELECT count(*) FROM strategy_factory_events WHERE strategy_id=?",
                         (strategy_id,)).fetchone()[0]
    # Average once per parent event to avoid correlated contracts inflating n.
    values = list(_event_values(db, "strategy_factory_events", strategy_id,
                                spec["extra_fee_stress_cents"]).values())
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
        if state != candidate[2] and update_state:
            db.execute("UPDATE strategy_factory_candidates SET state=?,reason=? WHERE strategy_id=?",
                       (state, reason, strategy_id))
            if state == "demo_trial_candidate":
                db.execute("INSERT OR IGNORE INTO strategy_factory_holdouts VALUES(?,?,?,?)",
                           (strategy_id, at.isoformat(), "collecting", "future_holdout_started"))
    holdout_row = db.execute("SELECT started_at,state,reason FROM strategy_factory_holdouts WHERE strategy_id=?",
                             (strategy_id,)).fetchone()
    held = list(_event_values(db, "strategy_factory_holdout_events", strategy_id,
                              spec["extra_fee_stress_cents"]).values()) if holdout_row else []
    held_lower = (statistics.mean(held) - 1.96 * statistics.stdev(held) / math.sqrt(len(held))
                  if len(held) >= 2 else None)
    if holdout_row and holdout_row[1] == "collecting" and len(held) >= 10 and statistics.mean(held) <= -1:
        if update_state:
            db.execute("UPDATE strategy_factory_holdouts SET state='rejected',reason='negative_holdout_mean' "
                       "WHERE strategy_id=?", (strategy_id,))
        holdout_row = (holdout_row[0], "rejected", "negative_holdout_mean")
    return {"strategy_id": strategy_id, "spec_hash": fingerprint(spec), "spec": spec,
            "registered_at": registered.isoformat(), "state": state, "reason": reason,
            "prospective_signals": signals, "complete_independent_events": len(values),
            "elapsed_days": round(elapsed_days, 2), "extra_fee_stress_cents": EXTRA_FEE_STRESS_CENTS,
            "cost_stressed_net_cents": round(total, 2) if values else None,
            "event_cluster_lower_bound_cents": round(lower, 3) if lower is not None else None,
            "holdout_started_at": holdout_row[0] if holdout_row else None,
            "holdout_state": holdout_row[1] if holdout_row else None,
            "holdout_reason": holdout_row[2] if holdout_row else None,
            "holdout_complete_independent_events": len(held),
            "holdout_cost_stressed_net_cents": round(sum(held), 2) if held else None,
            "holdout_event_cluster_lower_bound_cents": round(held_lower, 3) if held_lower is not None else None,
            "execution_enabled": False, "fill_assumed": False,
            "live_promotion_eligible": False}


def cycle(db, *, now=None, ideas=None):
    at = _now(now)
    init(db)
    capture_future(db)
    for (strategy_id,) in db.execute("SELECT strategy_id FROM strategy_factory_candidates WHERE state='shadow'").fetchall():
        evaluate(db, strategy_id, now=at)
    capture_holdout(db)
    register_next(db, now=at)
    if ideas:
        register_ideas(db, ideas, now=at)
    return status(db, now=at)


def status(db, *, now=None):
    at = _now(now)
    all_rows = [evaluate(db, row[0], now=at) for row in db.execute(
        "SELECT strategy_id FROM strategy_factory_candidates ORDER BY registered_at,strategy_id")]
    # Keep active and Demo candidates visible while bounding the monitor packet.
    priority = [row for row in all_rows if row["state"] != "rejected"]
    space = max(0, MAX_REPORTED_CANDIDATES - len(priority))
    recent_rejected = ([row for row in all_rows if row["state"] == "rejected"][-space:]
                       if space else [])
    rows = sorted((priority + recent_rejected)[-MAX_REPORTED_CANDIDATES:],
                  key=lambda row: (row["registered_at"], row["strategy_id"]))
    active = next((row["strategy_id"] for row in all_rows if row["state"] == "shadow"), None)
    fixed = [row for row in all_rows if row["strategy_id"].startswith("kalshi_factory_")]
    exhausted = not any(row["state"] == "shadow" for row in fixed) and len(fixed) >= MAX_CANDIDATES
    return {"schema": "kalshi_strategy_factory_v1", "generated_at": at.isoformat(),
            "execution_enabled": False, "candidate_limit": MAX_CANDIDATES,
            "candidate_total": len(all_rows), "reported_candidate_limit": MAX_REPORTED_CANDIDATES,
            "candidates": rows,
            "active_strategy_id": active, "grammar_exhausted": exhausted,
            "next_action": "new_approved_primitive_required" if exhausted and active is None else "continue_prospective_search"}
