"""Comparable metrics and honest pairing across independent strategy sleeves."""
from collections import defaultdict


COMMON_METRICS = (
    "independent_events", "complete_observations", "cost_stressed_net_cents",
    "event_clustered_95pct_lower_bound_cents", "capital_days",
    "maximum_drawdown_cents",
)


def normalize(summary):
    if not isinstance(summary, dict) or not summary.get("strategy_id"):
        raise ValueError("invalid_sleeve_summary")
    return {
        "strategy_id": summary["strategy_id"],
        "execution_enabled": bool(summary.get("execution_enabled", False)),
        **{name: summary.get(name) for name in COMMON_METRICS},
    }


def paired_counts(records_by_strategy):
    """Count only same-event, same-decision-bucket observations."""
    indexes = {}
    for strategy_id, records in records_by_strategy.items():
        index = defaultdict(int)
        for row in records:
            event_id = row.get("event_id")
            bucket = row.get("decision_bucket")
            if event_id and isinstance(bucket, int):
                index[(event_id, bucket)] += 1
        indexes[strategy_id] = index
    result = []
    names = sorted(indexes)
    for i, left in enumerate(names):
        for right in names[i + 1:]:
            keys = set(indexes[left]) & set(indexes[right])
            result.append({
                "left": left,
                "right": right,
                "paired_event_time_buckets": len(keys),
                "paired": bool(keys),
                "interpretation": ("same-event paired comparison available" if keys else
                                   "portfolio metrics only; no same-event time overlap"),
            })
    return result


def comparison_packet(summaries, records_by_strategy=None):
    sleeves = [normalize(summary) for summary in summaries]
    if len({row["strategy_id"] for row in sleeves}) != len(sleeves):
        raise ValueError("duplicate_strategy_id")
    return {
        "schema": "kalshi_sleeve_comparison_v1",
        "common_metrics": list(COMMON_METRICS),
        "sleeves": sorted(sleeves, key=lambda row: row["strategy_id"]),
        "paired_comparisons": paired_counts(records_by_strategy or {}),
        "rules": {
            "paired_comparison": "same event_id and decision_bucket only",
            "unrelated_universes": "report separately; never pool into a synthetic win rate",
            "missing_values": "insufficient completed evidence, not zero",
        },
    }
