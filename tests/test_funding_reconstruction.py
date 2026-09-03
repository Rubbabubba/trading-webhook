from opportunity_lab.funding_reconstruction import reconstruct_hourly_funding


def _candles(price, count=48):
    return [{"timestamp": 1_700_000_000 + index * 3600, "close": price, "volume": 1} for index in range(count)]


def test_positive_premium_pays_short_and_covers_basis_convergence():
    result = reconstruct_hourly_funding(_candles(101), _candles(100), total_cost_bps=10)
    assert result["valid"] is True
    assert result["funding_pnl_bps"] > 0
    assert result["basis_capture_bps"] == 0
    assert result["profitable"] is True
    assert result["official_history"] is False


def test_requires_aligned_history():
    assert reconstruct_hourly_funding(_candles(101, 5), _candles(100, 5))["reason"] == "insufficient_aligned_hourly_candles"


def test_basis_convergence_is_included():
    futures = _candles(101)
    futures[-1]["close"] = 100
    result = reconstruct_hourly_funding(futures, _candles(100), total_cost_bps=0)
    assert result["basis_capture_bps"] == 100
