from regime_intraday_validation import cost_stress, daily_goal_feasibility, entry_execution_analysis, latency_stress, monte_carlo_daily, paper_fill_reconciliation, parameter_stability, update_canceled_entry_outcomes, validation_lab


def _report(values):
    return {
        "accepted_session_count": len(values),
        "trade_count": len(values),
        "trades": [{"session": f"2026-08-{index + 1:02d}", "realized_r": value} for index, value in enumerate(values)],
    }


def test_cost_and_latency_stress_are_monotonic():
    report = _report([1.0, -0.5, 2.0])
    costs = cost_stress(report)
    latency = latency_stress(report)
    assert [row["round_trip_cost_r"] for row in costs] == [0.12, 0.2, 0.3, 0.5]
    assert all(left["net_average_r"] > right["net_average_r"] for left, right in zip(costs, costs[1:]))
    assert all(left["net_average_r"] > right["net_average_r"] for left, right in zip(latency, latency[1:]))


def test_block_monte_carlo_is_deterministic_and_reports_tail_risk():
    report = _report([1.0, -1.0, 0.5, -0.5, 2.0, -1.0])
    first = monte_carlo_daily(report, trials=200, seed=7)
    second = monte_carlo_daily(report, trials=200, seed=7)
    assert first == second
    assert first["ready"] is True
    assert first["max_drawdown_r_p95"] >= first["max_drawdown_r_p50"]


def test_validation_lab_never_auto_promotes_settings():
    report = _report([1.0] * 24)
    candidates = [
        {"eligible": True, "selection_score": 0.4, "cost_adjusted": {"net_average_r": 0.4}}
        for _ in range(8)
    ]
    walk = {"out_of_sample_positive": True, "test": {"trade_count": 24}, "candidates": candidates}
    lab = validation_lab(baseline=report, walk_forward=walk, instrument_reports={"spy_only": report, "qqq_only": report}, candidate_reports={"trend_pullback": report})
    assert parameter_stability(walk)["stable"] is True
    assert lab["gate"]["paper_validation_pass"] is True
    assert lab["gate"]["promotion_locked"] is True
    assert lab["historical_option_fill_model"] is False
    assert lab["candidate_sleeves"]["trend_pullback"]["research_pass"] is True
    assert lab["candidate_sleeves"]["trend_pullback"]["execution_enabled"] is False


def test_paper_fill_reconciliation_measures_actual_slippage_and_stays_locked():
    ledger = {
        "pending_candidates": {"sig-1": {"created_at": "2026-09-03T14:00:00+00:00"}},
        "orders": {"sig-1": {
            "status": "filled_closed",
            "recorded_at": "2026-09-03T14:00:30+00:00",
            "plan": {"underlying": "SPY", "limit_debit": 0.40},
            "broker": {"submitted_at": "2026-09-03T14:00:31+00:00", "filled_at": "2026-09-03T14:00:40+00:00", "filled_avg_price": "0.42"},
            "valuation": {"liquidation_credit": 0.70},
            "close_order": {"broker": {"filled_avg_price": "-0.68"}},
        }},
    }
    result = paper_fill_reconciliation(ledger, minimum_roundtrips=20)
    assert result["roundtrip_count"] == 1
    assert result["forward_validation_ready"] is False
    assert result["average_adverse_slippage_dollars"] == 4.0
    assert result["rows"][0]["actual_realized_dollars"] == 26.0
    assert result["rows"][0]["signal_to_submit_seconds"] == 30.0


def test_daily_goal_feasibility_exposes_required_risk_instead_of_promising_income():
    report = _report([0.5] * 10)
    report["max_drawdown_r"] = 2.0
    result = daily_goal_feasibility(report, risk_dollars=100)
    assert result["modeled_average_daily_dollars"] == 38.0
    assert result["goals"][0]["required_risk_per_trade_dollars"] == 263.16
    assert result["goals"][0]["fits_current_100_dollar_trade_cap"] is False


def test_canceled_entry_counterfactual_is_stop_first_and_not_broker_pnl():
    ledger = {"orders": {"sig": {"status": "canceled", "broker": {"filled_qty": "0"}, "signal": {
        "symbol": "SPY", "underlying_side": "buy", "entry_price": 100, "stop_price": 99, "target_price": 102,
    }}}}
    scan = {"features": {"SPY": {"ready": True, "last_ts": "2026-09-04T10:30:00-04:00", "last_high": 103, "last_low": 98, "price": 101}}}
    update_canceled_entry_outcomes(ledger, scan)
    outcome = ledger["orders"]["sig"]["counterfactual_underlying_outcome"]
    assert outcome["status"] == "stop"
    assert outcome["realized_r"] == -1.0
    assert "not broker P/L" in outcome["assumption"]


def test_entry_execution_analysis_reports_quote_gap_without_claiming_fill():
    ledger = {"orders": {"sig": {"status": "canceled", "plan": {"underlying": "SPY", "limit_debit": .52,
        "selection_quotes": {"entry_debit_from_quotes": .52}}, "terminal_quotes": {"entry_debit_from_quotes": .54},
        "broker": {"submitted_at": "2026-09-04T14:15:00+00:00", "canceled_at": "2026-09-04T14:18:00+00:00"},
        "entry_quote_path": [{}, {}]}}}
    result = entry_execution_analysis(ledger)
    row = result["rows"][0]
    assert row["required_limit_increase_at_terminal"] == .02
    assert row["terminal_quote_was_within_one_cent"] is False
    assert row["quote_path_points"] == 2
    assert result["policy"].startswith("Observational only")
