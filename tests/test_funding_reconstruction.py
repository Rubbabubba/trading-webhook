from opportunity_lab.funding_reconstruction import conditional_carry_walk_forward, reconstruct_hourly_funding


def _candles(price, count=48):
    return [{"timestamp": 1_700_000_000 + index * 3600, "close": price, "volume": 1} for index in range(count)]


def test_positive_premium_pays_short_and_covers_basis_convergence():
    result = reconstruct_hourly_funding(_candles(101), _candles(100), total_cost_bps=10)
    assert result["valid"] is True
    assert result["funding_pnl_bps"] > 0
    assert result["basis_capture_bps"] == 0
    assert result["profitable"] is True
    assert result["official_history"] is False
    assert result["max_carry_curve_drawdown_bps"] >= 0


def test_requires_aligned_history():
    assert reconstruct_hourly_funding(_candles(101, 5), _candles(100, 5))["reason"] == "insufficient_aligned_hourly_candles"


def test_basis_convergence_is_included():
    futures = _candles(101)
    futures[-1]["close"] = 100
    result = reconstruct_hourly_funding(futures, _candles(100), total_cost_bps=0)
    assert result["basis_capture_bps"] == 100


def test_annualization_uses_elapsed_time_not_observation_count():
    futures = _candles(101)
    spots = _candles(100)
    del futures[10:20]
    del spots[10:20]
    result = reconstruct_hourly_funding(futures, spots, total_cost_bps=0)
    assert result["elapsed_hours"] == 47
    assert result["observation_density"] < 1
    expected = result["return_on_fully_collateralized_capital_pct"] * 8760 / 47
    assert abs(result["annualized_return_on_fully_collateralized_capital_pct"] - expected) < .001


def test_rolling_holds_charge_cost_on_each_entry():
    result = reconstruct_hourly_funding(_candles(101, 24 * 40), _candles(100, 24 * 40), total_cost_bps=1000)
    seven_day = result["rolling_holding_periods"]["7"]
    assert seven_day["sample_count"] > 0
    assert seven_day["profitable_fraction"] == 0
    assert seven_day["maximum_net_pnl_bps"] < 0


def test_conditional_carry_is_chronological_and_never_executable():
    futures = _candles(101, 24 * 90)
    spots = _candles(100, 24 * 90)
    result = conditional_carry_walk_forward(futures, spots, total_cost_bps=10)
    assert result["valid"] is True
    assert result["grid_size"] == 240
    assert result["split_timestamp"] == futures[int(len(futures) * 2 / 3)]["timestamp"]
    assert result["calibration"]["trade_count"] > 0
    assert result["eligible"] is False
    assert result["execution_enabled"] is False
