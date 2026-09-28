import pytest

from opportunity_lab.kalshi_sleeve_comparison import comparison_packet


def summary(name):
    return {"strategy_id": name, "execution_enabled": False,
            "independent_events": 2, "complete_observations": 3,
            "cost_stressed_net_cents": None}


def test_comparison_preserves_missing_metrics_and_pairs_only_same_event_time():
    packet = comparison_packet([summary("sports"), summary("structural"), summary("flb")], {
        "sports": [{"event_id": "E", "decision_bucket": 10}],
        "structural": [{"event_id": "E", "decision_bucket": 10}],
        "flb": [{"event_id": "OTHER", "decision_bucket": 10}],
    })
    pairs = {(row["left"], row["right"]): row for row in packet["paired_comparisons"]}
    assert pairs[("sports", "structural")]["paired"]
    assert not pairs[("flb", "sports")]["paired"]
    assert packet["sleeves"][0]["cost_stressed_net_cents"] is None


def test_duplicate_strategy_is_rejected():
    with pytest.raises(ValueError, match="duplicate_strategy_id"):
        comparison_packet([summary("same"), summary("same")])
