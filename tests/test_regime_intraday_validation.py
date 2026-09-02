from regime_intraday_validation import cost_stress, latency_stress, monte_carlo_daily, paper_fill_reconciliation, parameter_stability, validation_lab


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
    lab = validation_lab(baseline=report, walk_forward=walk, instrument_reports={"spy_only": report, "qqq_only": report})
    assert parameter_stability(walk)["stable"] is True
    assert lab["gate"]["paper_validation_pass"] is True
    assert lab["gate"]["promotion_locked"] is True
    assert lab["historical_option_fill_model"] is False


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
