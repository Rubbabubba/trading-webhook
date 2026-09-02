import regime_intraday_candidates as candidates
from regime_intraday import RegimeIntradayConfig


def _base(regime, spy, qqq=None):
    return {"regime": regime, "features": {"SPY": spy, "QQQ": qqq or spy}, "signals": [], "signal_count": 0}


def _feature(**overrides):
    row = {
        "ready": True,
        "price": 100.2,
        "last_open": 100.0,
        "last_low": 99.9,
        "last_high": 100.3,
        "ema_fast": 100.0,
        "atr_1m": 0.5,
        "vwap_distance_atr": 0.4,
        "opening_high": 101.0,
        "opening_low": 99.0,
        "recent_closes": [100.0, 100.2],
        "last_ts": "2026-09-03T10:15:00-04:00",
    }
    row.update(overrides)
    return row


def test_trend_pullback_candidate_requires_aligned_reversal(monkeypatch):
    monkeypatch.setattr(candidates, "evaluate_regime_intraday", lambda *_args: _base({"name": "trend", "direction": "bullish"}, _feature()))
    result = candidates.trend_pullback_candidate({}, RegimeIntradayConfig(trade_symbols=("SPY",)))
    assert result["signals"][0]["strategy"] == "trend_pullback"
    assert result["signals"][0]["underlying_side"] == "buy"


def test_failed_breakout_fade_enters_back_inside_opening_range(monkeypatch):
    spy = _feature(price=100.8, last_open=100.9, opening_high=101.0, recent_closes=[101.2, 100.8])
    monkeypatch.setattr(candidates, "evaluate_regime_intraday", lambda *_args: _base({"name": "range"}, spy))
    result = candidates.failed_breakout_fade_candidate({}, RegimeIntradayConfig(trade_symbols=("SPY",)))
    assert result["signals"][0]["strategy"] == "failed_breakout_fade"
    assert result["signals"][0]["underlying_side"] == "sell"


def test_relative_strength_divergence_fades_spy_outperformance(monkeypatch):
    spy = _feature(vwap_distance_atr=1.2)
    qqq = _feature(vwap_distance_atr=-0.1)
    monkeypatch.setattr(candidates, "evaluate_regime_intraday", lambda *_args: _base({"name": "transition"}, spy, qqq))
    result = candidates.relative_strength_divergence_candidate({}, RegimeIntradayConfig(trade_symbols=("SPY",)))
    assert result["signals"][0]["strategy"] == "relative_strength_divergence"
    assert result["signals"][0]["underlying_side"] == "sell"
