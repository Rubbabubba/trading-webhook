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
from . import kalshi_strategy_tournament as tournament


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
    if spec not in tuple(specs()) and not _valid_generated_spec(spec) and not _valid_tournament_spec(spec):
        raise ValueError("unapproved_strategy_spec")
    return hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _valid_generated_spec(spec):
    if isinstance(spec, dict) and spec.get("evaluation_protocol") in tournament.SUPPORTED_PROTOCOLS:
        spec = {key: value for key, value in spec.items() if key != "evaluation_protocol"}
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


def _valid_tournament_spec(spec):
    if not isinstance(spec, dict) or set(spec) != {
        "primitive", "stratum", "price_bin", "side", "family", "evaluation_protocol",
        "extra_fee_stress_cents", "execution_enabled",
    } or spec.get("evaluation_protocol") not in tournament.SUPPORTED_PROTOCOLS:
        return False
    return _valid_generated_spec({key: value for key, value in spec.items()
                                 if key != "evaluation_protocol"} | {
        "origin_idea_id": "00000000-0000-0000-0000-000000000000"})


def register_ideas(db, ideas, *, now=None, parallel=False):
    """Admit validated novel ideas within the shared shadow concurrency cap."""
    if not isinstance(ideas, list) or len(ideas) > 32:
        return None
    active = db.execute("SELECT count(*) FROM strategy_factory_candidates WHERE state='shadow' "
                        "AND strategy_id LIKE 'kalshi_idea_%'").fetchone()[0]
    slots = (tournament.MAX_AI_ACTIVE if parallel else 1) - active
    if slots <= 0:
        return None
    # An AI batch may assign a fresh idea ID to a previously tested scope.
    # Keep a retired scope retired within the same evaluation protocol; a new
    # forward-fee protocol is a distinct, explicitly registered experiment.
    tried_scopes = {
        (spec.get("stratum"), spec.get("price_bin"), spec.get("side"),
         spec.get("family"), spec.get("evaluation_protocol", "legacy_factory_v1"))
        for (raw,) in db.execute("SELECT spec_json FROM strategy_factory_candidates "
                                 "WHERE strategy_id LIKE 'kalshi_idea_%'")
        for spec in (json.loads(raw),)
    }
    admitted = []
    for idea in ideas:
        if parallel and db.execute("SELECT count(*) FROM strategy_tournament_protocols").fetchone()[0] >= tournament.MAX_REGISTERED:
            break
        if not isinstance(idea, dict) or set(idea) not in (
                {"id", "spec_hash", "spec"},
                {"id", "capability_id", "version", "spec_hash", "spec"}):
            continue
        if idea.get("capability_id", "ask_to_settlement_v1") != "ask_to_settlement_v1" or idea.get("version", 1) != 1:
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
        if parallel:
            spec["evaluation_protocol"] = tournament.PROTOCOL
        if not _valid_generated_spec(spec):
            continue
        scope = (spec["stratum"], spec["price_bin"], spec["side"],
                 spec["family"], spec.get("evaluation_protocol", "legacy_factory_v1"))
        if scope in tried_scopes:
            continue
        if db.execute("SELECT 1 FROM strategy_factory_candidates WHERE json_extract(spec_json,'$.origin_idea_id')=? "
                      "AND coalesce(json_extract(spec_json,'$.evaluation_protocol'),'legacy_factory_v1')=?",
                      (idea["id"], spec.get("evaluation_protocol", "legacy_factory_v1"))).fetchone():
            continue
        digest = fingerprint(spec)
        strategy_id = "kalshi_idea_" + digest[:12]
        cursor = db.execute("INSERT OR IGNORE INTO strategy_factory_candidates VALUES(?,?,?,?,?,?)",
                            (strategy_id, digest, json.dumps(spec, sort_keys=True),
                             _now(now).isoformat(), "shadow", "ai_idea_prospective_registration"))
        if cursor.rowcount:
            tried_scopes.add(scope)
            if parallel:
                tournament.register_protocol(db, strategy_id, digest, _now(now))
            admitted.append(strategy_id)
            if len(admitted) >= slots:
                break
    return admitted[0] if admitted else None


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
    tournament.init(db)
    from .kalshi_factory_fee_probe import init as fee_init
    fee_init(db)


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
    return _capture_shared(db, holdout=False)


def _capture_shared(db, *, holdout):
    """Dispatch one bounded append-only feed page to every active candidate."""
    db.execute("CREATE TABLE IF NOT EXISTS strategy_factory_cursors("
               "strategy_id TEXT,split TEXT,last_rowid INTEGER NOT NULL,PRIMARY KEY(strategy_id,split))")
    split = "holdout" if holdout else "prospective"
    table = "strategy_factory_holdout_events" if holdout else "strategy_factory_events"
    query = ("SELECT c.strategy_id,c.spec_json,h.started_at FROM strategy_factory_candidates c "
             "JOIN strategy_factory_holdouts h USING(strategy_id) WHERE h.state='collecting'" if holdout else
             "SELECT strategy_id,spec_json,registered_at FROM strategy_factory_candidates WHERE state='shadow'")
    active = []
    for strategy_id, raw_spec, started in db.execute(query).fetchall():
        cursor = db.execute("SELECT last_rowid FROM strategy_factory_cursors WHERE strategy_id=? AND split=?",
                            (strategy_id, split)).fetchone()
        # Existing rows after registration remain eligible during migration.
        if cursor:
            last = cursor[0]
        else:
            first = db.execute("SELECT min(rowid) FROM calibration_parent_observations WHERE observed_at>?",
                               (started,)).fetchone()[0]
            last = first - 1 if first is not None else db.execute(
                "SELECT coalesce(max(rowid),0) FROM calibration_parent_observations").fetchone()[0]
        active.append((strategy_id, json.loads(raw_spec), started, last))
    if not active:
        return 0
    rows = db.execute("SELECT rowid,observation_id,event_id,observed_at,detail FROM calibration_parent_observations "
                      "WHERE rowid>? ORDER BY rowid LIMIT 2000", (min(row[3] for row in active),)).fetchall()
    added = 0
    for strategy_id, spec, started, last in active:
        progress = last
        for rowid, observation_id, event_id, observed_at, detail in rows:
            if rowid <= last:
                continue
            progress = rowid
            if observed_at <= started:
                continue
            try:
                row = json.loads(detail)
            except (TypeError, ValueError):
                continue
            if not _matches(spec, row):
                continue
            if spec.get("evaluation_protocol") == tournament.PROTOCOL:
                # New registrations accept only quotes audited before settlement.
                # Missing metadata is acquisition failure, never a later outcome filter.
                proof = db.execute("SELECT detail,sha256 FROM factory_fee_observations "
                                   "WHERE strategy_id=? AND observation_id=?",
                                   (strategy_id, observation_id)).fetchone()
                if not proof or hashlib.sha256(proof[0].encode()).hexdigest() != proof[1]:
                    continue
                # Exactly one audited quote per parent event is preregistered
                # in v2. Later resolutions cannot change a frozen event mean.
                if db.execute(f"SELECT 1 FROM {table} WHERE strategy_id=? AND event_id=?",
                              (strategy_id, event_id)).fetchone():
                    continue
            if holdout and db.execute("SELECT 1 FROM strategy_factory_events WHERE strategy_id=? AND event_id=?",
                                      (strategy_id, event_id)).fetchone():
                continue
            inserted = db.execute(f"INSERT OR IGNORE INTO {table} VALUES(?,?,?,?)",
                                  (strategy_id, observation_id, event_id, observed_at))
            added += inserted.rowcount
        db.execute("INSERT INTO strategy_factory_cursors VALUES(?,?,?) ON CONFLICT(strategy_id,split) "
                   "DO UPDATE SET last_rowid=excluded.last_rowid", (strategy_id, split, progress))
    return added


def capture_holdout(db):
    return _capture_shared(db, holdout=True)


def _event_values(db, table, strategy_id, extra_fee_cents):
    groups = defaultdict(list)
    for event_id, resolution in db.execute(
        f"SELECT e.event_id,o.resolution FROM {table} e "
        "JOIN calibration_parent_observations o ON o.observation_id=e.observation_id "
        "WHERE e.strategy_id=? ORDER BY e.observed_at,e.observation_id", (strategy_id,),
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
    prospective_checkpoint = None
    if spec.get("evaluation_protocol") in tournament.SUPPORTED_PROTOCOLS:
        prospective_checkpoint = tournament.checkpoint(
            db, strategy_id, fingerprint(spec), "prospective",
            _event_values(db, "strategy_factory_events", strategy_id, spec["extra_fee_stress_cents"]),
            now=at, persist=update_state)
        lower = prospective_checkpoint["adjusted_lower_bound_cents"] if prospective_checkpoint else None
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
    holdout_checkpoint = None
    if spec.get("evaluation_protocol") in tournament.SUPPORTED_PROTOCOLS:
        holdout_checkpoint = tournament.checkpoint(
            db, strategy_id, fingerprint(spec), "holdout",
            _event_values(db, "strategy_factory_holdout_events", strategy_id, spec["extra_fee_stress_cents"]),
            now=at, persist=update_state)
        held_lower = holdout_checkpoint["adjusted_lower_bound_cents"] if holdout_checkpoint else None
    if holdout_row and holdout_row[1] == "collecting" and len(held) >= 10 and statistics.mean(held) <= -1:
        if update_state:
            db.execute("UPDATE strategy_factory_holdouts SET state='rejected',reason='negative_holdout_mean' "
                       "WHERE strategy_id=?", (strategy_id,))
        holdout_row = (holdout_row[0], "rejected", "negative_holdout_mean")
    historical = (db.execute("SELECT historical_events,historical_net_cents FROM strategy_tournament_screens "
                            "WHERE spec_hash=?", (fingerprint(spec),)).fetchone()
                  if spec.get("evaluation_protocol") in tournament.SUPPORTED_PROTOCOLS else None)
    return {"strategy_id": strategy_id, "spec_hash": fingerprint(spec), "spec": spec,
            "evaluation_protocol": spec.get("evaluation_protocol", "legacy_factory_v1"),
            "historical_events": historical[0] if historical else 0,
            "historical_net_cents": historical[1] if historical else None,
            "prospective_checkpoint_events": prospective_checkpoint["events"] if prospective_checkpoint else 0,
            "holdout_checkpoint_events": holdout_checkpoint["events"] if holdout_checkpoint else 0,
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


def cycle(db, *, now=None, ideas=None, parallel=False):
    at = _now(now)
    init(db)
    from .kalshi_factory_fee_probe import init as fee_init
    fee_init(db)
    # Retain every old result and checkpoint. Versions without complete forward
    # fee evidence cannot become live dossiers; retire them and register v2
    # independently, with a new hash, date and statistical candidate index.
    if parallel:
        for strategy_id, raw in db.execute("SELECT strategy_id,spec_json FROM strategy_factory_candidates "
                                          "WHERE state='shadow'").fetchall():
            if json.loads(raw).get("evaluation_protocol") == "tournament_v1":
                db.execute("UPDATE strategy_factory_candidates SET state='rejected',reason=? WHERE strategy_id=?",
                           ("superseded_by_forward_fee_protocol", strategy_id))
    capture_future(db)
    for (strategy_id,) in db.execute("SELECT strategy_id FROM strategy_factory_candidates WHERE state='shadow'").fetchall():
        evaluate(db, strategy_id, now=at)
    capture_holdout(db)
    if parallel:
        tournament.screen_and_admit(db, now=at, fingerprint=fingerprint)
    else:
        register_next(db, now=at)
    if ideas:
        register_ideas(db, ideas, now=at, parallel=parallel)
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
            "tournament": tournament.summary(db),
            "active_strategy_id": active, "grammar_exhausted": exhausted,
            "next_action": "new_approved_primitive_required" if exhausted and active is None else "continue_prospective_search"}
