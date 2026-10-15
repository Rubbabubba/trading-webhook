"""Read-only retrospective audit of frozen maker evidence; no trading or downloads."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import sqlite3

from .kalshi_strategy_evaluation import cluster_lower_bound

HORIZONS = (5, 30, 300)
STRESSES = (0, 1, 2, 5)
LIMIT = 50000
REGISTRATION = Path(__file__).resolve().parents[1] / "configs/kalshi_historical_execution_audit_v1_20261015/registration.json"


def number(value):
    if isinstance(value, bool):
        raise ValueError("boolean_number")
    try:
        if "/" in str(value):
            fraction = Fraction(str(value))
            result = Decimal(fraction.numerator) / Decimal(fraction.denominator)
        else:
            result = Decimal(str(value))
    except (InvalidOperation, ValueError, ZeroDivisionError):
        raise ValueError("invalid_number") from None
    if not result.is_finite():
        raise ValueError("nonfinite_number")
    return result


def summarize(rows):
    output = {"signals": len(rows), "independent_events": len({r["event"] for r in rows}),
              "stress_sensitivity": {}}
    for stress in STRESSES:
        horizons = {}
        for horizon in HORIZONS:
            groups = defaultdict(list)
            for row in rows:
                groups[row["event"]].append(float(row["values"][horizon]) - stress)
            horizons[str(horizon)] = {
                "net_quote_markout_cents": sum(sum(v) for v in groups.values()),
                "event_clustered_95pct_lower_bound_cents": (
                    cluster_lower_bound(groups) if len(groups) >= 2 else None)}
        output["stress_sensitivity"][str(stress)] = horizons
    return output


def audit_strategy(db, strategy, *, now):
    if strategy not in ("v10", "v11", "v12"):
        raise ValueError("unsupported_strategy")
    table = strategy + "_shadow_signals"
    names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if table not in names or strategy + "_shadow_markouts" not in names:
        return {"state": "missing_tables", "complete": False}
    total = db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    if total > LIMIT:
        return {"state": "bounded_audit_limit_exceeded", "signals": total,
                "limit": LIMIT, "complete": False}
    marks = defaultdict(dict); fingerprint = hashlib.sha256()
    for sid, horizon, at, mid, gross, stressed in db.execute(
            f"SELECT signal_id,horizon_seconds,observed_at,yes_mid,gross_cents,stressed_cents "
            f"FROM {strategy}_shadow_markouts ORDER BY signal_id,horizon_seconds"):
        marks[sid][horizon] = (at, mid, gross, stressed)
        fingerprint.update(json.dumps([sid, horizon, at, mid, gross, stressed]).encode())
    reasons = Counter(); valid = []; raw_complete = 0; raw_net = Counter()
    maximum_lateness = None
    for sid, ticker, at, outcome, price, event in db.execute(
            f"SELECT id,ticker,observed_at,outcome,price_cents,event_id FROM {table} ORDER BY observed_at,id"):
        fingerprint.update(json.dumps([sid, ticker, at, outcome, price, event]).encode())
        errors = set(); values = {}
        if not event or not ticker or outcome not in ("yes", "no"):
            errors.add("invalid_identity")
        try:
            entry_at = number(at); entry = number(price)
            if not 0 <= entry_at <= number(now) or not 0 < entry < 100:
                errors.add("invalid_entry")
        except ValueError:
            errors.add("invalid_entry"); entry_at = entry = None
        if all(h in marks[sid] for h in HORIZONS):
            raw_complete += 1
        for horizon in HORIZONS:
            if horizon not in marks[sid]:
                errors.add("missing_horizon"); continue
            observed, mid, gross, stressed = marks[sid][horizon]
            try:
                observed, mid, gross, stressed = map(number, (observed, mid, gross, stressed))
                raw_net[str(horizon)] += float(stressed)
                if entry_at is None or entry is None:
                    continue
                lateness = observed - entry_at - horizon
                maximum_lateness = max(maximum_lateness or 0, float(lateness))
                if lateness < 0:
                    errors.add("early_markout")
                if lateness > 5:
                    errors.add("late_markout")
                if observed > number(now):
                    errors.add("future_markout")
                if not 0 <= mid <= 1:
                    errors.add("invalid_midpoint")
                expected = (mid if outcome == "yes" else 1 - mid) * 100 - entry
                if abs(gross - expected) > Decimal("0.000001") or stressed != gross - 2:
                    errors.add("markout_arithmetic_mismatch")
                values[horizon] = stressed
            except ValueError:
                errors.add("invalid_markout_number")
        if errors:
            reasons.update(errors)
        else:
            valid.append({"signal_id": sid, "event": event, "at": float(entry_at), "values": values})
    event_first = {}
    for row in valid:
        event_first[row["event"]] = min(event_first.get(row["event"], row["at"]), row["at"])
    events = sorted(event_first, key=lambda e: (event_first[e], e))
    split = len(events) * 2 // 3
    old = set(events[:split])
    return {"state": "audited", "complete": True, "signals": total,
            "input_rows_sha256": fingerprint.hexdigest(),
            "raw_complete_signals": raw_complete, "raw_stressed_quote_totals_cents": dict(raw_net),
            "punctual_complete": summarize(valid), "excluded_signals": total - len(valid),
            "exclusion_reasons_nonexclusive": dict(reasons),
            "maximum_markout_lateness_seconds": maximum_lateness,
            "chronological_diagnostics": {
                "scope": "retrospective, already inspected data; not untouched validation",
                "oldest_event_group": summarize([r for r in valid if r["event"] in old]),
                "newest_event_group": summarize([r for r in valid if r["event"] not in old])}}


def audit(database, *, now=None):
    now = datetime.now(timezone.utc).timestamp() if now is None else now
    registration = REGISTRATION.read_bytes()
    with sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        results = {strategy: audit_strategy(db, strategy, now=now) for strategy in ("v10", "v11", "v12")}
    return {"schema": "kalshi_historical_execution_audit_v1", "generated_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "registration_sha256": hashlib.sha256(registration).hexdigest(), "results": results,
            "actual_fill_evidence": "not supplied by shadow tables", "fill_assumed": False,
            "execution_enabled": False, "profitability_proven": False, "live_promotion_ready": False,
            "limitations": ["Retained selected observations, not a complete historical market tape.",
                            "Prices are quotes, not independently reconciled executions.",
                            "No strategy parameters were fitted or selected by this audit."]}


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()
    raw = json.dumps(audit(args.database), sort_keys=True, indent=2, allow_nan=False) + "\n"
    if args.output:
        with Path(args.output).open("x", encoding="utf-8") as destination:
            destination.write(raw)
    print(raw)


if __name__ == "__main__":
    main()
