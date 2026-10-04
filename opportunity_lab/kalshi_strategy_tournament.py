"""Bounded historical screening and frozen checkpoints for parallel shadow tests.

Historical results only rank admission. Checkpoints spend a decreasing alpha
budget across registered variants, both splits, and repeated looks. Bounds
use a normal approximation; they are research screens, not profit guarantees.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
import re
import statistics


PROTOCOL = "tournament_v1"
MAX_ACTIVE = 8
MAX_AI_ACTIVE = 2
MAX_FAMILIES = 6
MAX_REGISTERED = 256
CHECKPOINTS = 7
MAX_REPLAY_ROWS = 20000


def init(db):
    db.executescript("""
      CREATE TABLE IF NOT EXISTS strategy_tournament_protocols(
        candidate_index INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL UNIQUE,spec_hash TEXT NOT NULL,
        registered_at TEXT NOT NULL,protocol TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS strategy_tournament_screens(
        spec_hash TEXT PRIMARY KEY,spec_json TEXT NOT NULL,screened_at TEXT NOT NULL,
        historical_events INTEGER NOT NULL,historical_net_cents REAL,
        rank_score REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS strategy_tournament_checkpoints(
        strategy_id TEXT NOT NULL,split TEXT NOT NULL,look INTEGER NOT NULL,
        observed_at TEXT NOT NULL,snapshot_json TEXT NOT NULL,sha256 TEXT NOT NULL,
        PRIMARY KEY(strategy_id,split,look));
    """)


def register_protocol(db, strategy_id, spec_hash, at):
    db.execute("INSERT OR IGNORE INTO strategy_tournament_protocols"
               "(strategy_id,spec_hash,registered_at,protocol) VALUES(?,?,?,?)",
               (strategy_id, spec_hash, at.isoformat(), PROTOCOL))


def screen_and_admit(db, *, now, fingerprint):
    """Stream the shared local capture once; retain every bounded variant's screen."""
    if db.execute("SELECT count(*) FROM strategy_tournament_protocols").fetchone()[0] >= MAX_REGISTERED:
        return []
    active = db.execute("SELECT count(*) FROM strategy_factory_candidates "
                        "WHERE state='shadow' AND strategy_id LIKE 'kalshi_tournament_%'").fetchone()[0]
    slots = MAX_ACTIVE - active
    if slots <= 0:
        return []
    # At most six observed families plus broad scopes. Aggregate per parent
    # event so multiple contracts never inflate the historical screen sample.
    families = defaultdict(set)
    groups = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))
    for event_id, detail, resolution in db.execute(
        "SELECT event_id,detail,resolution FROM calibration_parent_observations "
        "WHERE resolution IS NOT NULL ORDER BY observed_at DESC,observation_id DESC LIMIT 20000"
    ):
        try:
            row = json.loads(detail); result = json.loads(resolution)
            scope = row["stratum"], row["price_bin"], row["side"]
            net = result["cost_stressed_net_cents"] - 3
            if (scope[0] not in ("sports", "non_sports") or scope[1] not in ("2-5", "5-10", "90-95", "95-98")
                    or scope[2] not in ("yes", "no") or type(net) not in (int, float)
                    or not math.isfinite(net) or row.get("fill_assumed") is not False
                    or row.get("execution_enabled") is not False):
                continue
            group = groups[(*scope, "*")][event_id]
            group[0] += net; group[1] += 1
            family = row.get("family")
            if isinstance(family, str) and re.fullmatch(r"[A-Za-z0-9 _./:-]{1,80}", family):
                families[family].add(event_id)
                group = groups[(*scope, family)][event_id]
                group[0] += net; group[1] += 1
        except (TypeError, ValueError, KeyError):
            continue
    selected_families = [name for name, _ in sorted(families.items(), key=lambda x: (-len(x[1]), x[0]))[:MAX_FAMILIES]]
    tried = {row[0] for row in db.execute("SELECT spec_hash FROM strategy_factory_candidates")}
    ranked = []
    for stratum in ("sports", "non_sports"):
        for price_bin in ("2-5", "5-10", "90-95", "95-98"):
            for side in ("yes", "no"):
                for family in ("*", *selected_families):
                    spec = {"primitive": "buy_at_observed_ask_to_settlement", "stratum": stratum,
                            "price_bin": price_bin, "side": side, "family": family,
                            "evaluation_protocol": PROTOCOL, "extra_fee_stress_cents": 3,
                            "execution_enabled": False}
                    digest = fingerprint(spec)
                    if digest in tried:
                        continue
                    values = [v[0] / v[1] for v in groups.get((stratum, price_bin, side, family), {}).values()]
                    score = statistics.mean(values) - 10 / math.sqrt(len(values)) if values else -1000.0
                    encoded = json.dumps(spec, sort_keys=True, separators=(",", ":"))
                    db.execute("INSERT INTO strategy_tournament_screens VALUES(?,?,?,?,?,?) "
                               "ON CONFLICT(spec_hash) DO UPDATE SET screened_at=excluded.screened_at,"
                               "historical_events=excluded.historical_events,historical_net_cents=excluded.historical_net_cents,"
                               "rank_score=excluded.rank_score",
                               (digest, encoded, now.isoformat(), len(values), sum(values) if values else None, score))
                    ranked.append((-score, -len(values), digest, spec))
    # Round robin across stratum/side prevents a single historical winner
    # family taking every slot. Negative historical results are retained too.
    buckets = defaultdict(list)
    for item in sorted(ranked):
        buckets[(item[3]["stratum"], item[3]["side"])].append(item)
    admitted = []
    remaining = MAX_REGISTERED - db.execute("SELECT count(*) FROM strategy_tournament_protocols").fetchone()[0]
    while buckets and len(admitted) < min(slots, remaining):
        for key in sorted(list(buckets)):
            if len(admitted) >= min(slots, remaining):
                break
            _score, _events, digest, spec = buckets[key].pop(0)
            if not buckets[key]:
                del buckets[key]
            strategy_id = "kalshi_tournament_" + digest[:12]
            db.execute("INSERT INTO strategy_factory_candidates VALUES(?,?,?,?,?,?)",
                       (strategy_id, digest, json.dumps(spec, sort_keys=True), now.isoformat(),
                        "shadow", "tournament_prospective_registration"))
            register_protocol(db, strategy_id, digest, now)
            admitted.append(strategy_id)
    return admitted


def checkpoint(db, strategy_id, spec_hash, split, events, *, now, persist=True):
    protocol = db.execute("SELECT candidate_index,spec_hash,protocol FROM strategy_tournament_protocols "
                          "WHERE strategy_id=?", (strategy_id,)).fetchone()
    if not protocol or protocol[1] != spec_hash or protocol[2] != PROTOCOL:
        raise ValueError("tournament_protocol_missing_or_changed")
    base = 30 if split == "prospective" else 20
    latest = None
    for look in range(1, CHECKPOINTS + 1):
        size = base * (2 ** (look - 1))
        if len(events) < size:
            break
        saved = db.execute("SELECT snapshot_json,sha256 FROM strategy_tournament_checkpoints "
                           "WHERE strategy_id=? AND split=? AND look=?", (strategy_id, split, look)).fetchone()
        if saved:
            if hashlib.sha256(saved[0].encode()).hexdigest() != saved[1]:
                raise ValueError("tournament_checkpoint_changed")
            result = json.loads(saved[0])
        else:
            frozen = [list(pair) for pair in list(events.items())[:size]]
            values = [v for _, v in frozen]
            index = protocol[0]
            alpha = 0.025 / (index * (index + 1) * look * (look + 1))
            z = statistics.NormalDist().inv_cdf(1 - alpha)
            lower = statistics.mean(values) - z * statistics.stdev(values) / math.sqrt(size)
            result = {"look": look, "events": size, "alpha": alpha,
                      "adjusted_lower_bound_cents": lower, "candidate_index": index,
                      "event_values": frozen, "normal_approximation": True}
            encoded = json.dumps(result, sort_keys=True, separators=(",", ":"))
            if persist:
                db.execute("INSERT INTO strategy_tournament_checkpoints VALUES(?,?,?,?,?,?)",
                           (strategy_id, split, look, now.isoformat(), encoded,
                            hashlib.sha256(encoded.encode()).hexdigest()))
        latest = result
        # First positive checkpoint is frozen. Subsequent minute-by-minute
        # observations cannot tune its test boundary or manufacture a pass.
        if result["adjusted_lower_bound_cents"] > 0:
            break
    return latest


def summary(db):
    return {"protocol": PROTOCOL, "active_limit": MAX_ACTIVE,
            "ai_active_limit": MAX_AI_ACTIVE, "registered_limit": MAX_REGISTERED,
            "variants_screened": db.execute("SELECT count(*) FROM strategy_tournament_screens").fetchone()[0],
            "registered": db.execute("SELECT count(*) FROM strategy_tournament_protocols").fetchone()[0],
            "active": db.execute("SELECT count(*) FROM strategy_factory_candidates WHERE state='shadow' "
                                 "AND json_extract(spec_json,'$.evaluation_protocol')=?", (PROTOCOL,)).fetchone()[0],
            "rejected": db.execute("SELECT count(*) FROM strategy_factory_candidates WHERE state='rejected' "
                                   "AND json_extract(spec_json,'$.evaluation_protocol')=?", (PROTOCOL,)).fetchone()[0],
            "screened_at": db.execute("SELECT max(screened_at) FROM strategy_tournament_screens").fetchone()[0],
            "replay_source": "local_captured_observations",
            "replay_row_limit": MAX_REPLAY_ROWS,
            "historical_results_are_validation": False,
            "multiple_test_method": "alpha_spending_across_variants_splits_and_frozen_checkpoints_normal_approximation"}
