from datetime import datetime, timedelta, timezone

from opportunity_lab.crypto_hypotheses import RelativeStrengthConfig, crypto_hypothesis_walk_forward, replay_relative_strength


def _bars(multiplier: float, count: int = 900):
    start, price, rows = datetime(2025, 1, 1, tzinfo=timezone.utc), 100.0, []
    for index in range(count):
        price *= multiplier if (index // 200) % 2 == 0 else 2 - multiplier
        rows.append({"ts_utc": start + timedelta(hours=index), "open": price, "high": price * 1.002,
                     "low": price * .998, "close": price, "volume": 10, "vwap": price})
    return rows


def test_relative_strength_replay_is_cost_aware_and_closed_trade_only():
    result = replay_relative_strength(_bars(1.002), _bars(1.0005), RelativeStrengthConfig(24, 6, 0, .001))
    assert result["bar_count"] == 900
    assert result["trade_count"] > 0
    assert all("exit_ts" in row for row in result["trades"])


def test_new_hypothesis_suite_is_research_only():
    result = crypto_hypothesis_walk_forward(_bars(1.002), _bars(1.0005))
    assert result["breakout"]["BTC/USD"]["grid_size"] == 48
    assert result["relative_strength"]["grid_size"] == 54
    assert result["execution_enabled"] is False
