"""Deterministic release dossier for one frozen candidate; no order interface."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics

from .kalshi_high_edge_shadow import PROTOCOL
from .kalshi_historical_execution_audit import number

REGISTRATION = Path(__file__).resolve().parents[1] / "configs/kalshi_v15_release_review_20261015/registration.json"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def review(db, *, now):
    stored = db.execute("SELECT detail,started_at FROM high_edge_v15_protocol WHERE id=1").fetchone()
    if stored is None or json.loads(stored[0]) != PROTOCOL:
        raise ValueError("frozen_v15_protocol_mismatch")
    start = number(stored[1]); now = number(now)
    if start > now:
        raise ValueError("future_protocol_start")
    signals = db.execute("SELECT id,ticker,event,at,detail FROM high_edge_v15_signals ORDER BY at,id").fetchall()
    if len(signals) > PROTOCOL["maximum_signals"]:
        raise ValueError("v15_signal_capacity_exceeded")
    marks = defaultdict(dict); digest = hashlib.sha256(canonical(json.loads(stored[0])).encode())
    for sid, horizon, at, raw in db.execute("SELECT signal_id,horizon,at,detail FROM high_edge_v15_marks ORDER BY signal_id,horizon"):
        marks[sid][horizon] = (at, raw)
        digest.update(canonical([sid, horizon, at, raw]).encode())
    exclusions = Counter(); complete = []; total_events = set(); mark_count = 0
    for sid, ticker, event, at, raw in signals:
        digest.update(canonical([sid, ticker, event, at, raw]).encode())
        total_events.add(event); errors = set(); values = {}
        try:
            at = number(at)
            if not start <= at <= now or not ticker or not event:
                errors.add("invalid_signal_identity_or_time")
            detail = json.loads(raw)
            candidate = detail["challenger"]; source = detail["frozen_v12"]
            if (candidate["outcome"] != source["outcome"] or candidate["outcome"] not in ("yes", "no")
                    or number(candidate["price_cents"]) != number(source["price_cents"]) + 2
                    or number(source["gross_edge_cents"]) < 5
                    or candidate["strategy_id"] != PROTOCOL["schema"]):
                errors.add("source_decision_mismatch")
            source_price = number(source["price_cents"])
            source_fair = number(source["fair_value_cents"])
            if (not 0 < source_price < source_fair < 100
                    or abs(source_fair - source_price - number(source["gross_edge_cents"])) > Decimal("0.000001")):
                errors.add("source_value_arithmetic_mismatch")
            for horizon in PROTOCOL["horizons_seconds"]:
                if horizon not in marks[sid]:
                    errors.add("missing_horizon"); continue
                mark_count += 1
                observed, mark_raw = marks[sid][horizon]
                observed = number(observed)
                if not horizon <= observed - at <= horizon + 5 or observed > now:
                    errors.add("invalid_markout_time")
                outcomes = json.loads(mark_raw); implied = []
                for name in ("challenger", "frozen_v12", "frozen_v10"):
                    value = outcomes[name]; gross = number(value["gross_cents"])
                    stressed = number(value["stressed_cents"])
                    entry = detail.get(name)
                    if entry is None:
                        if not value.get("no_trade") or gross != 0 or stressed != 0:
                            errors.add("invalid_no_trade_benchmark")
                        continue
                    if stressed != gross - 2:
                        errors.add("invalid_cost_arithmetic")
                    price = number(entry["price_cents"])
                    if not 0 < price < 100 or entry["outcome"] not in ("yes", "no"):
                        errors.add("invalid_entry_price_or_side")
                    mid = price + gross
                    implied.append(mid if entry["outcome"] == "yes" else 100 - mid)
                if not implied or any(not 0 <= mid <= 100 for mid in implied) or max(implied) - min(implied) > Decimal("0.000001"):
                    errors.add("same_observation_price_mismatch")
                values[horizon] = {name: float(number(outcomes[name]["stressed_cents"]))
                                   for name in ("challenger", "frozen_v12", "frozen_v10")}
        except (ValueError, TypeError, KeyError):
            errors.add("malformed_or_nonfinite_evidence")
        if errors:
            exclusions.update(errors)
        else:
            complete.append({"id": sid, "event": event, "values": values})
    independent = len({r["event"] for r in complete}); horizons = {}
    for horizon in PROTOCOL["horizons_seconds"]:
        groups = defaultdict(list)
        for row in complete:
            groups[row["event"]].append(row["values"][horizon]["challenger"])
        event_means = [statistics.mean(v) for v in groups.values()]
        mean = statistics.mean(event_means) if event_means else None
        lower = (mean - 1.96 * statistics.stdev(event_means) / len(event_means) ** .5
                 if len(event_means) > 1 else None)
        horizons[str(horizon)] = {"quote_net_cents": sum(sum(v) for v in groups.values()),
            "event_mean_cents": mean, "event_cluster_lcb_cents": lower,
            "same_observation_benchmarks": {name: sum(r["values"][horizon][name] for r in complete)
                                            for name in ("challenger", "frozen_v12", "frozen_v10")},
            "cost_sensitivity": {str(cost): {"quote_net_cents": sum(sum(v) for v in groups.values()) - cost * len(complete),
                "event_cluster_lcb_cents": lower - cost if lower is not None else None} for cost in (0, 2, 5)}}
    gates = {"minimum_complete_signals": len(complete) >= 100,
             "minimum_independent_events": independent >= 40,
             "positive_stressed_quote_net_every_horizon": all(h["quote_net_cents"] > 0 for h in horizons.values()),
             "positive_cluster_bound_every_horizon": all(h["event_cluster_lcb_cents"] is not None and h["event_cluster_lcb_cents"] > 0 for h in horizons.values())}
    passed = all(gates.values())
    return {"schema": "kalshi_v15_release_review_v1", "candidate": PROTOCOL["schema"],
            "review_implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "generated_at": datetime.fromtimestamp(float(now), timezone.utc).isoformat(),
            "original_registered_at_unix": float(start), "input_rows_sha256": digest.hexdigest(),
            "review_registration_sha256": hashlib.sha256(canonical(json.loads(REGISTRATION.read_text())).encode()).hexdigest(),
            "signals": len(signals), "all_signal_events": len(total_events), "mark_records_examined": mark_count,
            "complete_signals": len(complete), "independent_complete_events": independent,
            "incomplete_or_invalid_signals": len(signals) - len(complete),
            "exclusion_reasons_nonexclusive": dict(exclusions), "horizons": horizons, "original_quote_gates": gates,
            "original_quote_screen_passed": passed,
            "unused_new_holdout_supplied": False,
            "execution_evidence": {"actual_v15_fills_verified": False, "actual_v15_fees_verified": False,
                                   "executable_exit_path_verified": False, "queue_fill_model_verified": False},
            "release_decision": "prepare_separate_demo_trial" if passed else "blocked_insufficient_or_failed_evidence",
            "blockers": [k for k, v in gates.items() if not v] + ["separate_demo_trial_registration_required",
                         "actual_fills_and_fees_missing", "operational_execution_verification_missing"],
            "execution_enabled": False, "fill_assumed": False, "profitability_proven": False,
            "live_promotion_ready": False}


def audit(database, *, now=None):
    now = datetime.now(timezone.utc).timestamp() if now is None else now
    with sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.execute("PRAGMA query_only=ON"); db.execute("BEGIN")
        return review(db, now=now)


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True); parser.add_argument("--output", required=True)
    args = parser.parse_args(); result = audit(args.database)
    with Path(args.output).open("x", encoding="utf-8") as destination:
        destination.write(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")
    print(json.dumps(result, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
