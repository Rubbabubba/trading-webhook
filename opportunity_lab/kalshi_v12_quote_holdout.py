"""Frozen, event-isolated future quote-markout holdout for V12.

This is read-only research. Quote markouts never become executed returns.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics


ROOT = Path(__file__).resolve().parent.parent
REGISTRATION = ROOT / "configs" / "kalshi_v12_holdout_20261004" / "registration.json"
STRATEGY_MODULE = Path(__file__).with_name("kalshi_maker_v12.py")


def _lower(values):
    if len(values) < 2:
        return None
    return statistics.mean(values) - 1.96 * statistics.stdev(values) / math.sqrt(len(values))


def evaluate(db, *, now=None, registration_path=REGISTRATION, strategy_path=STRATEGY_MODULE):
    """Evaluate only complete future signals from events absent in earlier V12 data."""
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        raise ValueError("holdout_now_needs_timezone")
    moment = moment.astimezone(timezone.utc)
    source = Path(registration_path).read_bytes()
    registration = json.loads(source)
    if (registration.get("schema") != "kalshi_v12_quote_holdout_v1"
            or registration.get("environment") != "demo"
            or registration.get("execution_enabled") is not False
            or registration.get("production_execution_enabled") is not False):
        raise ValueError("invalid_holdout_registration")
    registered = datetime.fromisoformat(registration["registered_at"].replace("Z", "+00:00"))
    start = datetime.fromisoformat(registration["holdout_start_at"].replace("Z", "+00:00"))
    if registered.tzinfo is None or start.tzinfo is None or not registered < start:
        raise ValueError("invalid_holdout_chronology")
    start = start.astimezone(timezone.utc)
    horizons = tuple(int(value) for value in registration["required_markout_seconds"])
    if horizons != (5, 30, 300):
        raise ValueError("holdout_horizons_changed")
    code_hash = hashlib.sha256(Path(strategy_path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    code_unchanged = code_hash == registration["strategy_module_sha256"]
    earlier = {row[0] for row in db.execute(
        "SELECT DISTINCT event_id FROM v12_shadow_signals WHERE observed_at<?", (start.timestamp(),)
    )}
    signals = {}
    excluded_event_signals = 0
    for signal_id, event_id, observed_at, horizon, stressed in db.execute(
        "SELECT s.id,s.event_id,s.observed_at,m.horizon_seconds,CAST(m.stressed_cents AS REAL) "
        "FROM v12_shadow_signals s JOIN v12_shadow_markouts m ON m.signal_id=s.id "
        "WHERE s.observed_at>=? ORDER BY s.id,m.horizon_seconds", (start.timestamp(),)
    ):
        if event_id in earlier:
            excluded_event_signals += int(horizon == horizons[0])
            continue
        record = signals.setdefault(signal_id, {"event_id": event_id, "observed_at": observed_at, "markouts": {}})
        record["markouts"][horizon] = stressed
    complete = [row for row in signals.values() if all(h in row["markouts"] for h in horizons)]
    events = {row["event_id"] for row in complete}
    days = {datetime.fromtimestamp(row["observed_at"], timezone.utc).date() for row in complete}
    stressed_sum = {}
    lower_bounds = {}
    for horizon in horizons:
        groups = defaultdict(list)
        for row in complete:
            groups[row["event_id"]].append(row["markouts"][horizon])
        stressed_sum[str(horizon)] = sum(value for values in groups.values() for value in values)
        lower_bounds[str(horizon)] = _lower([statistics.mean(values) for values in groups.values()])
    gates = {
        "registered_before_start": registered < start,
        "holdout_started": moment >= start,
        "strategy_code_frozen": code_unchanged,
        "minimum_observation_days": (moment - start).days >= registration["minimum_observation_days"],
        "minimum_active_days": len(days) >= registration["minimum_active_days"],
        "minimum_completed_signals": len(complete) >= registration["minimum_completed_signals"],
        "minimum_independent_events": len(events) >= registration["minimum_independent_events"],
        "positive_stressed_net_every_horizon": all(stressed_sum[str(h)] > 0 for h in horizons),
        "positive_event_cluster_lower_every_horizon": all(
            lower_bounds[str(h)] is not None and lower_bounds[str(h)] > 0 for h in horizons
        ),
    }
    return {
        "schema": "kalshi_v12_quote_holdout_v1", "strategy_id": registration["strategy_id"],
        "version": registration["version"],
        "registration_sha256": hashlib.sha256(source).hexdigest(),
        "holdout_start_at": start.isoformat(), "evaluated_at": moment.isoformat(),
        "complete_signals": len(complete), "independent_events": len(events),
        "active_days": len(days), "prior_event_signals_excluded": excluded_event_signals,
        "stressed_markout_sum_cents": stressed_sum,
        "event_cluster_lower_bound_cents": lower_bounds,
        "gates": gates, "passed": all(gates.values()),
        "interpretation": "Future quote markouts only; actual fills and fee-adjusted trading returns remain separate.",
        "production_execution_enabled": False,
    }
