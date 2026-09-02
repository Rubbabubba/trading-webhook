from datetime import datetime, timedelta

from regime_intraday import RegimeIntradayConfig, evaluate_regime_intraday


def _bars(*, rising: bool, final_break: bool = False):
    start = datetime.fromisoformat("2026-09-02T09:30:00-04:00")
    rows = []
    for i in range(60):
        base = 100.0 + ((i * 0.001) if rising else -(i * 0.001))
        if final_break and i == 59:
            base += 1.0 if rising else -1.0
        rows.append({
            "ts_ny": (start + timedelta(minutes=i)).isoformat(),
            "open": base - (0.02 if rising else -0.02),
            "high": base + 0.04,
            "low": base - 0.04,
            "close": base,
            "volume": 5000 if i < 57 else 15000,
        })
    return rows


def test_router_waits_for_both_symbols():
    out = evaluate_regime_intraday({"SPY": _bars(rising=True)})
    assert out["regime"]["name"] == "not_ready"
    assert out["signals"] == []


def test_trend_router_emits_only_momentum_strategy():
    cfg = RegimeIntradayConfig(momentum_max_vwap_extension_pct=0.10)
    out = evaluate_regime_intraday({"SPY": _bars(rising=True, final_break=True), "QQQ": _bars(rising=True, final_break=True)}, cfg)
    assert out["regime"]["name"] == "trend"
    assert out["signal_count"] == 2
    assert all(row["strategy"] == "opening_range_momentum" for row in out["signals"])
    assert all(row["option_intent"]["live_submission"] is False for row in out["signals"])
    assert all(row["signal_id"] for row in out["signals"])


def test_transition_regime_never_forces_trade():
    up = _bars(rising=True)
    down = _bars(rising=False)
    out = evaluate_regime_intraday({"SPY": up, "QQQ": down})
    assert out["regime"]["name"] in {"range", "transition"}
    if out["regime"]["name"] == "transition":
        assert out["signals"] == []
