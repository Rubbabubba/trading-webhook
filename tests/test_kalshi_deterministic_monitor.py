import json

from opportunity_lab.kalshi_deterministic_monitor import (
    check, factory_promotion_preflight, gate_state, run_check,
)


def test_factory_promotion_preflight_exposes_missing_fee_and_dossier_proof():
    packet = {
        "generated_at": "2026-11-01T00:00:00+00:00",
        "evidence": {"market_discovery_complete": True},
        "research_sleeves": {"strategy_factory": {"candidates": [{
            "strategy_id": "factory-v1", "state": "demo_trial_candidate",
            "complete_independent_events": 35, "cost_stressed_net_cents": 100,
            "event_cluster_lower_bound_cents": 1,
            "holdout_started_at": "2026-10-10T00:00:00+00:00",
            "holdout_state": "collecting", "holdout_complete_independent_events": 25,
            "holdout_cost_stressed_net_cents": 70,
            "holdout_event_cluster_lower_bound_cents": 1,
        }]}},
        "factory_demo_trial": {"protocol": {"strategy_id": "factory-v1"},
                               "attempts": 25, "terminal_orders": 25,
                               "unresolved_orders": 0, "fills": 20,
                               "independent_days": 15, "realized_net_cents": 50,
                               "flat_at_review": True, "fees_reconciled": True},
    }
    result = factory_promotion_preflight(packet)
    assert result["ready_for_dossier"] is False
    assert result["strategy_id"] == "factory-v1"
    assert result["blockers"] == ["prospective_modeled_fee_coverage_incomplete",
                                  "holdout_modeled_fee_coverage_incomplete",
                                  "actual_fee_basis_unverified",
                                  "event_level_dossier_not_exported",
                                  "restart_and_risk_attestation_missing"]
    packet["factory_demo_trial"]["fees_reconciled"] = False
    assert "demo_reconciliation_incomplete" in factory_promotion_preflight(packet)["blockers"]


REGISTRATION = {
    "shadow_gate": {
        "minimum_independent_events": 30,
        "minimum_complete_signals": 100,
        "required_markout_seconds": [5, 30, 300],
        "positive_stressed_net_at_every_horizon": True,
        "positive_event_cluster_95_percent_lower_bound": True,
    }
}

V12_REGISTRATION = {
    "fixed_parameters": {"required_markout_seconds": [5, 30, 300]},
    "shadow_gate": {
        "minimum_independent_events": 30,
        "minimum_complete_signals": 100,
        "positive_stressed_net_at_every_horizon": True,
        "positive_event_cluster_95_percent_lower_bound": True,
    },
}


def healthy(at="2026-09-21T18:00:00+00:00"):
    return {
        "at": at, "environment": "demo", "phase": "running", "errors": [],
        "production_execution_enabled": False, "strategy_id": "stable_balanced_maker_v9",
        "strategy_execution_enabled": False,
        "execution_policy_id": "v9_retired_after_8_losses_20260924",
        "v12_demo_trial_enabled": True,
        "v12_demo_trial_policy_id": "v12_one_contract_demo_trial_20261001",
        "v12_demo_trial": {
            "strategy_id": "microprice_value_maker_v12_demo_trial",
            "attempts": 0, "fills": 0, "max_order_attempts": 50,
            "max_fills": 10, "loss_stop_cents": 25,
            "shadow_gate_passed": True, "reason": "authorized",
        },
        "v12_fillability_trial_enabled": True,
        "v12_fillability_trial_policy_id": "v12_one_tick_fillability_trial_20261002",
        "v12_fillability_trial": {
            "strategy_id": "microprice_value_maker_v12_fillability_trial",
            "attempts": 0, "fills": 0, "terminal_orders": 0,
            "attempted_independent_events": 0, "attempted_market_families": 0,
            "max_order_attempts": 200, "max_fills": 20,
            "loss_stop_cents": 100, "price_improvement_cents": 1,
            "shadow_gate_passed": True, "reason": "authorized",
            "flat_pnl_cents": 0,
        },
        "evidence": {
            "post_only_attempts": 194, "maker_fills": 1, "terminal_orders": 194,
            "unresolved_orders": 0, "ending_position_contracts": 0,
            "v10_shadow": {
                "strategy_id": "queue_toxicity_maker_v10_shadow",
                "execution_enabled": False, "signals": 0, "evaluations": 10,
                "last_evaluation_at": 1790013600,
                "rejection_reasons": {"directional_disagreement": 10},
                "complete_signals": 0,
                "independent_events": 0, "markout_records": {"5": 0, "30": 0, "300": 0},
                "stressed_markout_pnl_cents": {"5": 0, "30": 0, "300": 0},
                "event_cluster_lcb_cents": {"5": None, "30": None, "300": None},
            },
            "v11_shadow": {
                "strategy_id": "strong_imbalance_maker_v11_shadow",
                "execution_enabled": False, "signals": 0, "evaluations": 10,
                "last_evaluation_at": 1790013600,
                "rejection_reasons": {"weak_imbalance": 1},
                "complete_signals": 0, "independent_events": 0,
                "markout_records": {"5": 0, "30": 0, "300": 0},
                "stressed_markout_pnl_cents": {"5": 0, "30": 0, "300": 0},
                "event_cluster_lcb_cents": {"5": None, "30": None, "300": None},
                "automatic_rejection_triggered": False,
            },
            "v12_shadow": {
                "strategy_id": "microprice_value_maker_v12_shadow",
                "execution_enabled": False, "signals": 0, "evaluations": 10,
                "last_evaluation_at": 1790013600,
                "rejection_reasons": {"balanced_book": 1},
                "complete_signals": 0, "independent_events": 0,
                "markout_records": {"5": 0, "30": 0, "300": 0},
                "stressed_markout_pnl_cents": {"5": 0, "30": 0, "300": 0},
                "event_cluster_lcb_cents": {"5": None, "30": None, "300": None},
                "automatic_rejection_triggered": False,
            },
        },
    }


def test_missing_evidence_cannot_pass_gate():
    result = gate_state(healthy(), REGISTRATION)
    assert result["state"] == "collecting"
    assert not result["requirements"]["minimum_complete_signals"]
    assert not result["requirements"]["complete_horizons"]


def test_compact_packet_reports_completed_market_discovery():
    status = healthy()
    status["evidence"]["market_discovery"] = {
        "in_progress": False, "coverage_accounting_complete": True,
        "completed_at": 1790013600,
        "markets_scanned": 1200, "eligible_markets": 80,
        "market_families": {"sports": 900, "economics": 300},
    }
    packet, _, _ = check(status, {}, REGISTRATION, now=1790013601,
                         v12_registration=V12_REGISTRATION)
    assert packet["evidence"]["market_discovery_complete"] is True
    assert packet["evidence"]["market_discovery_scanned"] == 1200
    assert packet["evidence"]["market_discovery_eligible"] == 80
    assert packet["evidence"]["market_discovery_families"] == 2


def test_compact_packet_reports_crossing_feasibility():
    status = healthy()
    status["evidence"]["v12_crossing_feasibility"] = {
        "gross_crossing_edge_cents": -1.0,
        "reason": "nonpositive_pre_fee_crossing_edge",
        "observed_at": 1790013600,
    }
    packet, _, _ = check(status, {}, REGISTRATION, now=1790013601,
                         v12_registration=V12_REGISTRATION)
    assert packet["evidence"]["v12_crossing_gross_edge_cents"] == -1.0
    assert packet["evidence"]["v12_crossing_reason"] == "nonpositive_pre_fee_crossing_edge"


def test_research_snapshot_carries_bounded_factory_candidate(tmp_path):
    from opportunity_lab.kalshi_deterministic_monitor import _research_snapshot
    source = {
        "schema": "kalshi_sleeve_comparison_v1",
        "generated_at": "2026-10-03T20:00:00+00:00", "execution_enabled": False,
        "sleeves": [], "coverage": {},
        "official_release_probe": {
            "schema": "kalshi_official_release_probe_v1",
            "execution_enabled": False, "profitability_evidence": False,
            "research_state": "capture_only_rule_mapping_unverified",
            "latest_schedule_at": "2026-10-03T19:00:00+00:00",
            "watchlist_contracts": 2, "demo_quote_snapshots": 0,
            "next_releases": [{"name": "Gross Domestic Product",
                               "scheduled_at": "2026-10-29T12:30:00+00:00",
                               "watchlist_contracts": 2, "close_timing_review": 2}],
        },
        "strategy_factory": {
            "schema": "kalshi_strategy_factory_v1", "execution_enabled": False,
            "candidates": [{"strategy_id": "kalshi_factory_example", "state": "shadow",
                            "execution_enabled": False, "spec": {
                                "primitive": "buy_at_observed_ask_to_settlement",
                                "stratum": "sports", "price_bin": "2-5"},
                            "registered_at": "2026-10-03T20:00:00+00:00",
                            "complete_independent_events": 0,
                            "cost_stressed_net_cents": None,
                            "event_cluster_lower_bound_cents": None}]},
    }
    path = tmp_path / "research.json"
    path.write_text(json.dumps(source))
    result = _research_snapshot(path, now=1791057600)
    assert result["strategy_factory"]["candidates"][0]["strategy_id"] == "kalshi_factory_example"
    assert result["official_release_probe"]["watchlist_contracts"] == 2
    source["official_release_probe"]["profitability_evidence"] = True
    path.write_text(json.dumps(source))
    assert _research_snapshot(path, now=1791057600) is None


def test_completed_sweep_remains_verifiable_during_next_scan():
    status = healthy()
    status["evidence"]["market_discovery"] = {
        "in_progress": True, "coverage_accounting_complete": True,
        "markets_scanned": 300, "eligible_markets": 20,
        "market_families": {"sports": 300},
        "last_completed": {
            "completed_at": 1790010000, "coverage_accounting_complete": True,
            "markets_scanned": 1200, "eligible_markets": 80,
            "market_families": 2,
        },
    }
    packet, _, _ = check(status, {}, REGISTRATION, now=1790013601,
                         v12_registration=V12_REGISTRATION)
    assert packet["evidence"]["market_discovery_complete"] is True
    assert packet["evidence"]["market_discovery_scanned"] == 1200
    assert packet["evidence"]["market_discovery_families"] == 2


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


def test_v12_gate_uses_registered_fixed_horizons():
    status = healthy()
    shadow = status["evidence"]["v12_shadow"]
    shadow.update({
        "signals": 100, "complete_signals": 100, "independent_events": 30,
        "markout_records": {"5": 100, "30": 100, "300": 100},
        "stressed_markout_pnl_cents": {"5": 10, "30": 4, "300": 1},
        "event_cluster_lcb_cents": {"5": .1, "30": .01, "300": .001},
    })
    assert gate_state(status, V12_REGISTRATION, "v12_shadow")["state"] == "passed"


def test_v12_gate_uses_complete_sample_while_new_signals_mature():
    status = healthy()
    shadow = status["evidence"]["v12_shadow"]
    shadow.update({
        "signals": 105, "complete_signals": 100, "independent_events": 30,
        "markout_records": {"5": 105, "30": 104, "300": 100},
        "stressed_markout_pnl_cents": {"5": 10, "30": 4, "300": 1},
        "event_cluster_lcb_cents": {"5": .1, "30": .01, "300": .001},
    })
    assert gate_state(status, V12_REGISTRATION, "v12_shadow")["state"] == "passed"


def test_v12_automatic_rejection_triggers_review_once():
    status = healthy()
    _, checkpoint, _ = check(
        status, {}, REGISTRATION, now=1790013601, v12_registration=V12_REGISTRATION
    )
    status["evidence"]["v12_shadow"]["automatic_rejection_triggered"] = True
    packet, checkpoint, _ = check(
        status, checkpoint, REGISTRATION, now=1790013661,
        v12_registration=V12_REGISTRATION,
    )
    assert packet["trigger_categories"] == ["v12_automatic_rejection"]
    packet, _, _ = check(
        status, checkpoint, REGISTRATION, now=1790013721,
        v12_registration=V12_REGISTRATION,
    )
    assert packet["investigation_needed"] is False


def test_future_holdout_code_change_faults_and_gate_pass_escalates_once():
    status = healthy()
    status["evidence"]["v12_quote_holdout"] = {
        "holdout_start_at": "2026-10-04T00:00:00+00:00",
        "gates": {"strategy_code_frozen": False, "evaluator_code_frozen": True}, "passed": False,
    }
    packet, checkpoint, _ = check(status, {}, REGISTRATION, now=1790013601,
                                  v12_registration=V12_REGISTRATION)
    assert "v12_holdout_code_changed" in packet["health"]["faults"]
    status["evidence"]["v12_quote_holdout"]["gates"]["strategy_code_frozen"] = True
    status["evidence"]["v12_quote_holdout"]["passed"] = True
    packet, checkpoint, _ = check(status, checkpoint, REGISTRATION,
                                  now=1790013661, v12_registration=V12_REGISTRATION)
    assert "v12_holdout_gate_passed" in packet["trigger_categories"]
    packet, _, _ = check(status, checkpoint, REGISTRATION,
                         now=1790013721, v12_registration=V12_REGISTRATION)
    assert "v12_holdout_gate_passed" not in packet["trigger_categories"]


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


def test_compact_monitor_includes_independent_research_sleeves(tmp_path):
    now = 1790013601
    (tmp_path / "sleeve_comparison.json").write_text(json.dumps({
        "schema": "kalshi_sleeve_comparison_v1",
        "generated_at": "2026-09-21T18:00:01+00:00", "execution_enabled": False,
        "coverage": {"coverage_complete": True,
                     "multivariate_coverage": {"markets_scanned": 340}},
        "sleeves": [{"strategy_id": "kalshi_favorite_maker_v13_shadow",
                     "execution_enabled": False, "independent_events": 12,
                     "complete_observations": 12, "cost_stressed_net_cents": -4.0,
                     "event_clustered_95pct_lower_bound_cents": -1.5,
                     "maximum_drawdown_cents": 10.0}],
    }))
    status = healthy("2026-09-21T18:00:01+00:00")
    status["factory_demo_trial"] = {
        "protocol": None, "attempts": 0, "fills": 0, "attempts_today": 0,
        "independent_events": 0, "independent_days": 0,
        "terminal_orders": 0, "unresolved_orders": 0,
        "realized_net_cents": 0, "fees_cents": 0,
        "flat_at_review": True, "fees_reconciled": True,
        "execution_environment": "demo", "live_execution_enabled": False,
    }
    run_check(tmp_path, status, now=now, force=True)
    packet = json.loads((tmp_path / "monitor" / "review_packet.json").read_text())
    assert packet["research_sleeves"]["execution_enabled"] is False
    assert packet["research_sleeves"]["catalog_complete"] is True
    assert packet["research_sleeves"]["multivariate_scanned"] == 340
    assert packet["research_sleeves"]["sleeves"][0]["cost_stressed_net_cents"] == -4.0
    assert packet["factory_demo_trial"]["attempts"] == 0
    assert packet["factory_demo_trial"]["live_execution_enabled"] is False


def test_stale_research_sleeves_are_not_reported_as_current(tmp_path):
    (tmp_path / "sleeve_comparison.json").write_text(json.dumps({
        "schema": "kalshi_sleeve_comparison_v1",
        "generated_at": "2026-09-21T17:00:00+00:00", "execution_enabled": False,
        "sleeves": [],
    }))
    run_check(tmp_path, healthy("2026-09-21T18:00:01+00:00"), now=1790013601, force=True)
    packet = json.loads((tmp_path / "monitor" / "review_packet.json").read_text())
    assert packet["research_sleeves"] is None


def test_safeguard_change_triggers_once():
    status = healthy()
    status["production_execution_enabled"] = True
    packet, checkpoint, _ = check(status, {}, REGISTRATION, now=1790013601)
    assert "production_execution_changed" in packet["health"]["faults"]
    packet, _, duplicate = check(status, checkpoint, REGISTRATION, now=1790013661)
    assert duplicate and not packet["investigation_needed"]


def test_retired_strategy_cannot_be_reenabled_silently():
    status = healthy()
    status["strategy_execution_enabled"] = True
    packet, _, _ = check(status, {}, REGISTRATION, now=1790013601)
    assert "retired_strategy_execution_enabled" in packet["health"]["faults"]


def test_challenger_cannot_be_replaced_or_enabled_silently():
    status = healthy()
    status["evidence"]["v11_shadow"]["execution_enabled"] = True
    packet, _, _ = check(status, {}, REGISTRATION, now=1790013601)
    assert "v11_execution_enabled" in packet["health"]["faults"]
    status["evidence"]["v11_shadow"]["execution_enabled"] = False
    status["evidence"]["v11_shadow"]["strategy_id"] = "changed"
    packet, _, _ = check(status, {}, REGISTRATION, now=1790013601)
    assert "v11_strategy_changed" in packet["health"]["faults"]

    status = healthy()
    status["evidence"]["v12_shadow"]["execution_enabled"] = True
    packet, _, _ = check(status, {}, REGISTRATION, now=1790013601)
    assert "v12_execution_enabled" in packet["health"]["faults"]
    status["evidence"]["v12_shadow"]["execution_enabled"] = False
    status["evidence"]["v12_shadow"]["strategy_id"] = "changed"
    packet, _, _ = check(status, {}, REGISTRATION, now=1790013601)
    assert "v12_strategy_changed" in packet["health"]["faults"]


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


def test_v10_evidence_stall_and_recovery_are_detected():
    first_status = healthy("2026-09-21T18:00:01+00:00")
    _, checkpoint, _ = check(first_status, {}, REGISTRATION, now=1790013601)

    still_fresh = healthy("2026-09-21T18:14:59+00:00")
    packet, checkpoint, _ = check(still_fresh, checkpoint, REGISTRATION,
                                  now=1790014499)
    assert "v10_evidence_stalled" not in packet["health"]["faults"]

    stalled = healthy("2026-09-21T18:15:01+00:00")
    packet, checkpoint, _ = check(stalled, checkpoint, REGISTRATION,
                                  now=1790014501)
    assert "v10_evidence_stalled" in packet["health"]["faults"]
    assert "new_fault" in packet["trigger_categories"]

    recovered = healthy("2026-09-21T18:16:01+00:00")
    recovered["evidence"]["v10_shadow"]["evaluations"] += 1
    packet, _, _ = check(recovered, checkpoint, REGISTRATION, now=1790014561)
    assert "v10_evidence_stalled" not in packet["health"]["faults"]
    assert "recovery" in packet["trigger_categories"]
