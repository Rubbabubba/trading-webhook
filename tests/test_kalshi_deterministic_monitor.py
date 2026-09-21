import json

from opportunity_lab.kalshi_deterministic_monitor import check, gate_state, run_check


REGISTRATION = {
    "shadow_gate": {
        "minimum_independent_events": 30,
        "minimum_complete_signals": 100,
        "required_markout_seconds": [5, 30, 300],
        "positive_stressed_net_at_every_horizon": True,
        "positive_event_cluster_95_percent_lower_bound": True,
    }
}


def healthy(at="2026-09-21T18:00:00+00:00"):
    return {
        "at": at, "environment": "demo", "phase": "running", "errors": [],
        "production_execution_enabled": False, "strategy_id": "stable_balanced_maker_v9",
        "evidence": {
            "post_only_attempts": 194, "maker_fills": 1, "terminal_orders": 194,
            "unresolved_orders": 0, "ending_position_contracts": 0,
            "v10_shadow": {
                "strategy_id": "queue_toxicity_maker_v10_shadow",
                "execution_enabled": False, "signals": 0, "complete_signals": 0,
                "independent_events": 0, "markout_records": {"5": 0, "30": 0, "300": 0},
                "stressed_markout_pnl_cents": {"5": 0, "30": 0, "300": 0},
                "event_cluster_lcb_cents": {"5": None, "30": None, "300": None},
            },
        },
    }


def test_missing_evidence_cannot_pass_gate():
    result = gate_state(healthy(), REGISTRATION)
    assert result["state"] == "collecting"
    assert not result["requirements"]["minimum_complete_signals"]
    assert not result["requirements"]["complete_horizons"]


def test_complete_positive_gate_passes():
    status = healthy()
    shadow = status["evidence"]["v10_shadow"]
    shadow.update({
        "signals": 100, "complete_signals": 100, "independent_events": 30,
        "markout_records": {"5": 100, "30": 100, "300": 100},
        "stressed_markout_pnl_cents": {"5": 10, "30": 4, "300": 1},
        "event_cluster_lcb_cents": {"5": .1, "30": .01, "300": .001},
    })
    assert gate_state(status, REGISTRATION)["state"] == "passed"


def test_healthy_unchanged_check_requests_no_investigation():
    status = healthy()
    first, checkpoint, _ = check(status, {}, REGISTRATION, now=1790013601)
    assert first["trigger_categories"] == ["daily_review"]
    second, _, _ = check(status, checkpoint, REGISTRATION, now=1790013661)
    assert second["investigation_needed"] is False
    assert second["trigger_categories"] == []


def test_fault_dedup_and_recovery():
    status = healthy()
    status["errors"] = ["BrokerError"]
    status["phase"] = "blocked"
    first, checkpoint, duplicate = check(status, {}, REGISTRATION, now=1790013601)
    assert not duplicate and first["trigger_categories"] == ["new_fault", "daily_review"]
    second, checkpoint, duplicate = check(status, checkpoint, REGISTRATION, now=1790013661)
    assert duplicate
    third, checkpoint, _ = check(status, checkpoint, REGISTRATION, now=1790013721)
    assert "persistent_fault" in third["trigger_categories"]
    recovered, _, _ = check(healthy("2026-09-21T18:03:01+00:00"), checkpoint,
                            REGISTRATION, now=1790013781)
    assert recovered["trigger_categories"] == ["recovery"]


def test_restart_preserves_checkpoint_and_metrics(tmp_path):
    status = healthy()
    status["at"] = "2026-09-21T18:00:01+00:00"
    first = run_check(tmp_path, status, now=1790013601, force=True)
    second = run_check(tmp_path, status, now=1790013661, force=True)
    metrics = json.loads((tmp_path / "monitor" / "metrics.json").read_text())
    assert first["investigation_needed"] is True
    assert second["investigation_needed"] is False
    assert metrics["checks"] == 2
    assert metrics["checks_without_ai"] == 2


def test_safeguard_change_triggers_once():
    status = healthy()
    status["production_execution_enabled"] = True
    packet, checkpoint, _ = check(status, {}, REGISTRATION, now=1790013601)
    assert "production_execution_changed" in packet["health"]["faults"]
    packet, _, duplicate = check(status, checkpoint, REGISTRATION, now=1790013661)
    assert duplicate and not packet["investigation_needed"]


def test_stopped_or_stale_worker_is_detected_without_ai():
    packet, _, _ = check(healthy("2026-09-21T17:55:00+00:00"), {}, REGISTRATION,
                         now=1790013601)
    assert "status_stale" in packet["health"]["faults"]
    assert packet["investigation_needed"]


def test_gate_transition_triggers_once():
    status = healthy()
    _, checkpoint, _ = check(status, {}, REGISTRATION, now=1790013601)
    shadow = status["evidence"]["v10_shadow"]
    shadow.update({
        "signals": 100, "complete_signals": 100, "independent_events": 30,
        "markout_records": {"5": 100, "30": 100, "300": 100},
        "stressed_markout_pnl_cents": {"5": 10, "30": 4, "300": 1},
        "event_cluster_lcb_cents": {"5": .1, "30": .01, "300": .001},
    })
    packet, checkpoint, _ = check(status, checkpoint, REGISTRATION, now=1790013661)
    assert packet["v10_gate"]["state"] == "passed"
    assert packet["trigger_categories"] == ["evidence_gate_transition"]
    packet, _, duplicate = check(status, checkpoint, REGISTRATION, now=1790013721)
    assert not packet["investigation_needed"]
    assert not duplicate
