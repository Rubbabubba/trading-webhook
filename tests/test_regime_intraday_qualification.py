from regime_intraday_qualification import qualification_report, run_mechanical_matrix, run_randomized_execution_invariants


def test_production_scenario_matrix_covers_expected_failures_and_passes():
    result = run_mechanical_matrix()
    assert result["passed"] is True
    assert result["case_count"] >= 12
    names = {row["name"] for row in result["cases"]}
    assert {"entry_target_close", "confirmed_option_stop", "immediate_underlying_stop",
            "entry_never_marketable", "partial_entry", "broker_rejection",
            "wide_spread_rejection", "missing_exit_quote", "end_of_day_liquidation"} <= names


def test_randomized_execution_invariants_are_deterministic_and_clean():
    first = run_randomized_execution_invariants(trials=250, seed=7)
    second = run_randomized_execution_invariants(trials=250, seed=7)
    assert first == second
    assert first["passed"] is True
    assert first["violation_count"] == 0
    assert first["trial_count"] == 250


def test_qualification_is_fail_closed_for_live_capital():
    result = qualification_report({"orders": {}, "pending_candidates": {}}, trials=100)
    assert result["paper_production_qualified"] is True
    assert result["live_capital_qualified"] is False
    assert "live_order_transport_hard_closed" in result["live_blockers"]
    assert "minimum_30_independent_broker_roundtrips_not_met" in result["live_blockers"]
    assert result["live_submission"] is False
