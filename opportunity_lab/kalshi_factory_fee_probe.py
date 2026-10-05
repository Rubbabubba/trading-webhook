"""Forward-only, read-only fee metadata audit for factory observations.

This audit does not alter the frozen factory gate or claim actual exchange fees.
One fresh observation is enriched per cycle. A later promotion evaluator must
verify the applicable historical fee schedule and broker account precision.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
import math
import statistics


SCHEDULE_ID = "kalshi_binary_taker_2026-07-07_provisional"
SCHEDULE_URL = "https://kalshi.com/docs/kalshi-fee-schedule.pdf"
MAX_METADATA_LAG_SECONDS = 120


def _decimal(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("invalid_fee_multiplier") from None
    if not result.is_finite() or not 0 <= result <= 10:
        raise ValueError("invalid_fee_multiplier")
    return result


def fee_basis(row, event, series, *, fetched_at):
    """Calculate a conservative whole-cent model estimate, never an actual fee."""
    if (row.get("market_type") != "binary" or row.get("exchange_index") != 0
            or event.get("event_ticker") != row.get("event_id")
            or series.get("ticker") != event.get("series_ticker")):
        raise ValueError("fee_market_identity_mismatch")
    observed = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
    fetched = datetime.fromtimestamp(fetched_at, timezone.utc)
    if (observed.tzinfo is None or not 0 <= (fetched - observed).total_seconds()
            <= MAX_METADATA_LAG_SECONDS):
        raise ValueError("fee_metadata_not_contemporaneous")
    if event.get("fee_type_override") is not None:
        fee_type = event["fee_type_override"]
        multiplier = event.get("fee_multiplier_override")
        source = "event_override"
    else:
        fee_type = series.get("fee_type")
        multiplier = series.get("fee_multiplier")
        source = "series"
    if fee_type != "quadratic":
        raise ValueError("unsupported_fee_type")
    factor = _decimal(multiplier)
    price = Decimal(str(row["price_cents"])) / 100
    if not 0 < price < 1:
        raise ValueError("invalid_observed_price")
    # July 2026 binary taker schedule: 0.07 * M * C * P * (1-P), C=1.
    # Round *up* to a whole cent as a conservative model estimate for a
    # one-contract order. Actual fees may differ with fill and account rules.
    modeled_cents = Decimal(7) * factor * price * (1 - price)
    upper_cents = int(modeled_cents.to_integral_value(rounding=ROUND_CEILING))
    return {"schema": "kalshi_fee_basis_probe_v1", "schedule_id": SCHEDULE_ID,
            "schedule_url": SCHEDULE_URL, "fee_source": source,
            "series_ticker": series["ticker"], "event_ticker": row["event_id"],
            "fee_type": fee_type, "fee_multiplier": str(factor),
            "observed_at": observed.isoformat(), "metadata_fetched_at": fetched.isoformat(),
            "price_cents": row["price_cents"],
            "model_fee_upper_bound_cents": upper_cents,
            "modeled_entry_cost_cents": row["price_cents"] + upper_cents,
            "actual_fee_verified": False, "fill_assumed": False}


def init(db):
    db.executescript("""
      CREATE TABLE IF NOT EXISTS factory_fee_probe_protocols(
        strategy_id TEXT PRIMARY KEY,spec_hash TEXT NOT NULL,
        started_at TEXT NOT NULL,probe_version TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS factory_fee_observations(
        strategy_id TEXT NOT NULL,observation_id TEXT NOT NULL,
        event_id TEXT NOT NULL,detail TEXT NOT NULL,sha256 TEXT NOT NULL,
        PRIMARY KEY(strategy_id,observation_id));
      CREATE TABLE IF NOT EXISTS factory_fee_acquisition_attempts(
        strategy_id TEXT NOT NULL,event_id TEXT NOT NULL,attempted_at TEXT NOT NULL,
        PRIMARY KEY(strategy_id,event_id));
    """)


def probe_next(db, client, *, now=None):
    """Audit one new candidate observation; failures leave it unclaimed."""
    at = now or datetime.now(timezone.utc)
    if at.tzinfo is None:
        raise ValueError("timezone_required")
    init(db)
    candidates = db.execute(
        "SELECT c.strategy_id,c.spec_hash FROM strategy_factory_candidates c "
        "LEFT JOIN factory_fee_observations f ON f.strategy_id=c.strategy_id "
        "WHERE c.state IN ('shadow','demo_trial_candidate') "
        "GROUP BY c.strategy_id,c.spec_hash "
        "ORDER BY count(f.observation_id),c.registered_at,c.strategy_id LIMIT 12"
    ).fetchall()
    if not candidates:
        return {"probed": False, "reason": "no_active_candidate"}
    selected = None
    has_specs = "spec_json" in {r[1] for r in db.execute("PRAGMA table_info(strategy_factory_candidates)")}
    # Initialize all registrations before selecting a quote, so shared metadata
    # cannot starve hypotheses later in this bounded list.
    for strategy_id, digest in candidates:
        db.execute("INSERT OR IGNORE INTO factory_fee_probe_protocols VALUES(?,?,?,?)",
                   (strategy_id, digest, at.isoformat(), "fee_probe_v1"))
    for strategy_id, digest in candidates:
        protocol = db.execute(
            "SELECT spec_hash,started_at,probe_version FROM factory_fee_probe_protocols "
            "WHERE strategy_id=?", (strategy_id,)
        ).fetchone()
        if protocol[0] != digest or protocol[2] != "fee_probe_v1":
            raise ValueError("fee_probe_protocol_changed")
        spec_row = db.execute("SELECT spec_json FROM strategy_factory_candidates WHERE strategy_id=?", (strategy_id,)).fetchone() if has_specs else None
        spec = json.loads(spec_row[0]) if spec_row else {}
        forward_v2 = spec.get("evaluation_protocol") == "tournament_fee_v2"
        target = db.execute(
        "SELECT o.observation_id,o.event_id,o.detail FROM calibration_parent_observations o "
        "WHERE o.observed_at>? AND o.observed_at>=? AND ("
        "EXISTS(SELECT 1 FROM strategy_factory_events e WHERE e.strategy_id=? "
        "AND e.observation_id=o.observation_id) OR "
        "EXISTS(SELECT 1 FROM strategy_factory_holdout_events h WHERE h.strategy_id=? "
        "AND h.observation_id=o.observation_id)) "
        "AND NOT EXISTS(SELECT 1 FROM factory_fee_observations f WHERE f.strategy_id=? "
        "AND f.observation_id=o.observation_id) "
        "ORDER BY o.observed_at DESC,o.observation_id LIMIT 1",
            (protocol[1], (at - timedelta(seconds=MAX_METADATA_LAG_SECONDS - 20)).isoformat(),
             strategy_id, strategy_id, strategy_id),
        ).fetchone()
        if forward_v2:
            # Fee acquisition precedes membership and never inspects outcomes.
            # Quotes already resolved or older than the metadata window cannot
            # enter a v2 cohort. Search is bounded and independent of P&L.
            from .kalshi_strategy_factory import _matches
            target = next((item for item in db.execute(
                "SELECT observation_id,event_id,detail FROM calibration_parent_observations o "
                "WHERE observed_at>=? AND resolution IS NULL "
                "AND NOT EXISTS(SELECT 1 FROM factory_fee_observations f WHERE f.strategy_id=? "
                "AND f.event_id=o.event_id) "
                "AND NOT EXISTS(SELECT 1 FROM strategy_factory_events e WHERE e.strategy_id=? AND e.event_id=o.event_id) "
                "AND NOT EXISTS(SELECT 1 FROM strategy_factory_holdout_events h WHERE h.strategy_id=? AND h.event_id=o.event_id) "
                "AND NOT EXISTS(SELECT 1 FROM factory_fee_acquisition_attempts a WHERE a.strategy_id=? AND a.event_id=o.event_id AND a.attempted_at>?) "
                "ORDER BY observed_at DESC,observation_id LIMIT 2000",
                ((at - timedelta(days=45)).isoformat(), strategy_id, strategy_id, strategy_id, strategy_id,
                 (at - timedelta(minutes=5)).isoformat()))
                if _matches(spec, json.loads(item[2]))), None)
        if target is not None:
            selected = strategy_id, target
            break
    if selected is None:
        return {"probed": False, "reason": "no_new_observation"}
    strategy_id, target = selected
    observation_id, event_id, raw = target
    row = json.loads(raw)
    if forward_v2:
        # Old indicative rows locate a market only. Measurement is a new book
        # acquired after registration, never the old price or a known outcome.
        db.execute("INSERT INTO factory_fee_acquisition_attempts VALUES(?,?,?) "
                   "ON CONFLICT(strategy_id,event_id) DO UPDATE SET attempted_at=excluded.attempted_at",
                   (strategy_id, event_id, at.isoformat()))
        quote = client.quote({"ticker": row["ticker"]})
        market = quote.get("market") or {}
        if (quote.get("environment") != "demo" or market.get("event_ticker") != event_id
                or market.get("market_type") != "binary" or market.get("status") != "active"
                or market.get("exchange_index", 0) != 0):
            raise ValueError("fee_market_identity_mismatch")
        from .kalshi_shadow import price_book
        from .kalshi_external_sleeves import PRICE_BINS, SPORTS_PREFIXES
        _, ask, _, depth = price_book(quote, row["side"])
        price = int((Decimal(ask.numerator) / Decimal(ask.denominator) * 100).to_integral_value(rounding=ROUND_CEILING))
        observed = datetime.fromtimestamp(quote["observed_at"], timezone.utc)
        if (not 0 <= quote["observed_at"] - quote["started_at"] <= 2
                or not 0 <= (observed - at).total_seconds() <= 30
                or observed <= datetime.fromisoformat(protocol[1]) or depth < 1):
            raise ValueError("fee_quote_not_fresh")
        row = {**row, "price_cents": price, "observed_at": observed.isoformat(),
               "price_bin": next((f"{a}-{b}" for a, b in PRICE_BINS if a <= price <= b), None),
               "stratum": "sports" if event_id.upper().startswith(SPORTS_PREFIXES) else "non_sports",
               "family": market.get("category") or event_id.split("-")[0],
               "expiration_time": market.get("expiration_time"), "decision_bucket": int(quote["observed_at"]),
               "classification": "favorite" if price >= 90 else "longshot",
               "fill_assumed": False, "execution_enabled": False}
        if not _matches(spec, row):
            return {"probed": False, "reason": "fresh_quote_outside_strategy"}
        observation_id = "fee-v2:" + hashlib.sha256((row["ticker"] + ":" + row["side"] + ":" + row["observed_at"]).encode()).hexdigest()
        row["observation_id"] = observation_id
    # Get Event identifies its parent series and any event-level fee override.
    event_payload, _, event_at = client.get_event(event_id)
    event = event_payload["event"]
    series_payload, _, series_at = client.get_series(event["series_ticker"])
    market_payload, _, market_at = client.get(row["ticker"])
    market = market_payload["market"]
    if market.get("ticker") != row["ticker"] or market.get("event_ticker") != event_id:
        raise ValueError("fee_market_identity_mismatch")
    basis = fee_basis({**row, "market_type": market.get("market_type"),
                       "exchange_index": market.get("exchange_index", 0)},
                      event, series_payload["series"],
                      fetched_at=max(event_at, series_at, market_at))
    if forward_v2 and basis["model_fee_upper_bound_cents"] > 2:
        return {"probed": False, "reason": "fee_exceeds_registered_model"}
    encoded = json.dumps(basis, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    if forward_v2:
        db.execute("INSERT OR IGNORE INTO calibration_parent_observations "
                   "(observation_id,event_id,decision_bucket,observed_at,detail,resolution) VALUES(?,?,?,?,?,NULL)",
                   (observation_id, event_id, row["decision_bucket"], row["observed_at"], json.dumps(row, sort_keys=True)))
    db.execute("INSERT OR IGNORE INTO factory_fee_observations VALUES(?,?,?,?,?)",
               (strategy_id, observation_id, event_id, encoded, digest))
    # One quote can belong to several independent hypotheses. Reuse the exact
    # contemporaneous metadata, not a later refetch or a new assumed fee.
    for other_id, other_digest in candidates:
        if other_id == strategy_id:
            continue
        protocol = db.execute("SELECT started_at FROM factory_fee_probe_protocols WHERE strategy_id=?", (other_id,)).fetchone()
        if not protocol or row["observed_at"] <= protocol[0]:
            continue
        other_spec_row = db.execute("SELECT spec_json FROM strategy_factory_candidates WHERE strategy_id=?", (other_id,)).fetchone() if spec_row else None
        other_spec = json.loads(other_spec_row[0]) if other_spec_row else {}
        member = db.execute("SELECT 1 FROM strategy_factory_events WHERE strategy_id=? AND observation_id=? "
                            "UNION SELECT 1 FROM strategy_factory_holdout_events WHERE strategy_id=? AND observation_id=?",
                            (other_id, observation_id, other_id, observation_id)).fetchone()
        if other_spec.get("evaluation_protocol") == "tournament_fee_v2":
            from .kalshi_strategy_factory import _matches
            member = (_matches(other_spec, row) and basis["model_fee_upper_bound_cents"] <= 2
                      and not db.execute("SELECT 1 FROM factory_fee_observations WHERE strategy_id=? AND event_id=?", (other_id, event_id)).fetchone()
                      and db.execute("SELECT resolution FROM calibration_parent_observations WHERE observation_id=?", (observation_id,)).fetchone()[0] is None)
        if member:
            db.execute("INSERT OR IGNORE INTO factory_fee_observations VALUES(?,?,?,?,?)",
                       (other_id, observation_id, event_id, encoded, digest))
    return {"probed": True, "strategy_id": strategy_id,
            "observation_id": observation_id, "sha256": digest}


def status(db):
    init(db)
    def split_coverage(strategy_id, table):
        groups = {}
        for event_id, resolution, detail, digest in db.execute(
                f"SELECT o.event_id,o.resolution,f.detail,f.sha256 FROM {table} e "
                "JOIN calibration_parent_observations o ON o.observation_id=e.observation_id "
                "LEFT JOIN factory_fee_observations f ON f.strategy_id=e.strategy_id "
                "AND f.observation_id=e.observation_id WHERE e.strategy_id=?", (strategy_id,)):
            if resolution is None:
                continue
            valid = False
            try:
                outcome = json.loads(resolution)
                basis = json.loads(detail) if detail is not None else None
                valid = (outcome.get("hypothetical_only") is True
                         and type(outcome.get("payout_cents")) is int
                         and outcome["payout_cents"] in (0, 100)
                         and basis.get("actual_fee_verified") is False
                         and type(basis.get("modeled_entry_cost_cents")) is int
                         and digest == hashlib.sha256(detail.encode()).hexdigest())
            except (AttributeError, TypeError, ValueError):
                pass
            groups[event_id] = groups.get(event_id, True) and valid
        covered = sum(groups.values())
        return {"resolved_events": len(groups), "fully_modeled_fee_events": covered,
                "missing_modeled_fee_events": len(groups) - covered}

    def summary(strategy_id):
        grouped = {}
        for event_id, detail, digest, resolution in db.execute(
                "SELECT f.event_id,f.detail,f.sha256,o.resolution FROM factory_fee_observations f "
                "JOIN calibration_parent_observations o ON o.observation_id=f.observation_id "
                "WHERE f.strategy_id=? AND o.resolution IS NOT NULL", (strategy_id,)):
            try:
                basis = json.loads(detail)
                result = json.loads(resolution)
                payout = result.get("payout_cents")
                if (hashlib.sha256(detail.encode()).hexdigest() != digest
                        or result.get("hypothetical_only") is not True
                        or type(payout) is not int or payout not in (0, 100)):
                    continue
                net = payout - basis["modeled_entry_cost_cents"]
                if not math.isfinite(net):
                    continue
                grouped.setdefault(event_id, []).append(net)
            except (KeyError, TypeError, ValueError):
                continue
        values = [statistics.mean(rows) for rows in grouped.values()]
        lower = (statistics.mean(values) - 1.96 * statistics.stdev(values) / math.sqrt(len(values))
                 if len(values) >= 2 else None)
        return {"resolved_independent_events": len(values),
                "modeled_net_cents": round(sum(values), 2) if values else None,
                "event_cluster_lower_bound_cents": round(lower, 3) if lower is not None else None}
    return {"schema": "kalshi_factory_fee_probe_v1", "schedule_id": SCHEDULE_ID,
            "actual_fees_verified": False,
            "versions": [{"strategy_id": strategy_id, "started_at": started,
                          "observations": db.execute(
                              "SELECT count(*) FROM factory_fee_observations WHERE strategy_id=?",
                              (strategy_id,)).fetchone()[0],
                          "prospective_fee_coverage": split_coverage(
                              strategy_id, "strategy_factory_events"),
                          "holdout_fee_coverage": split_coverage(
                              strategy_id, "strategy_factory_holdout_events"),
                          **summary(strategy_id)}
                         for strategy_id, started in db.execute(
                             "SELECT p.strategy_id,p.started_at FROM factory_fee_probe_protocols p "
                             "JOIN strategy_factory_candidates c USING(strategy_id) "
                             "ORDER BY c.state='rejected',p.started_at DESC,p.strategy_id")][:8]}
