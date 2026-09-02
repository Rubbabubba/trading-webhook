from datetime import datetime, timedelta, timezone

import regime_intraday_replay as replay_module
from regime_intraday_replay import _outcome, chronological_holdout, cost_adjusted_report, mean_reversion_walk_forward, replay_sessions, rolling_mean_reversion_walk_forward, split_sessions, walk_forward
from regime_intraday import RegimeIntradayConfig


def test_cost_adjusted_report_tracks_daily_goal_rates():
    report = {"accepted_session_count": 4, "trades": [{"session": "2026-01-02", "realized_r": 2.0}, {"session": "2026-01-03", "realized_r": -1.0}]}
    result = cost_adjusted_report(report, risk_dollars=100, round_trip_cost_r=0.1)
    assert result["net_total_dollars"] == 80.0
    assert result["days_at_or_above_100"] == 1
    assert result["daily_goal_100_rate"] == 0.25
    assert result["average_daily_dollars"] == 20.0


def test_paper_defaults_match_validated_mean_reversion_thresholds():
    config = RegimeIntradayConfig()
    assert config.range_efficiency_max == 0.24
    assert config.mean_reversion_min_vwap_atr == 1.0


def _row(ts, o, h, low, close, volume=1000):
    return {"ts_ny": ts, "open": o, "high": h, "low": low, "close": close, "volume": volume}


def test_outcome_is_stop_first_when_same_bar_touches_both():
    ts = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
    signal = {"underlying_side": "buy", "entry_price": 100, "stop_price": 99, "target_price": 102}
    result = _outcome(signal, [_row(ts, 100, 103, 98, 101)])
    assert result["exit_reason"] == "stop"
    assert result["realized_r"] == -1.0


def test_sessions_are_separated_without_leaking_bars():
    first = datetime(2026, 8, 3, 14, 30, tzinfo=timezone.utc)
    bars = {"SPY": [_row(first, 1, 1, 1, 1), _row(first + timedelta(days=1), 2, 2, 2, 2)]}
    sessions = split_sessions(bars, ("SPY",))
    assert list(sessions) == ["2026-08-03", "2026-08-04"]
    assert len(sessions["2026-08-03"]["SPY"]) == 1


def test_replay_and_walk_forward_return_auditable_empty_results():
    first = datetime(2026, 8, 3, 14, 30, tzinfo=timezone.utc)
    bars = {}
    for symbol in ("SPY", "QQQ"):
        rows = []
        for day in range(2):
            for minute in range(45):
                ts = first + timedelta(days=day, minutes=minute)
                rows.append(_row(ts, 100, 100.02, 99.98, 100, 1000))
        bars[symbol] = rows
    replay = replay_sessions(bars)
    assert replay["accepted_session_count"] == 2
    assert replay["trade_count"] == 0
    result = walk_forward(bars)
    assert len(result["train_sessions"]) == 1
    assert len(result["test_sessions"]) == 1


def test_mean_reversion_walk_forward_selects_on_train_and_freezes_for_test(monkeypatch):
    first = datetime(2026, 8, 3, 14, 30, tzinfo=timezone.utc)
    bars = {
        symbol: [_row(first + timedelta(days=day), 100, 101, 99, 100) for day in range(10)]
        for symbol in ("SPY", "QQQ")
    }
    calls = []

    def fake_replay(candidate_bars, config, **_kwargs):
        session_count = len({_row["ts_ny"].date() for _row in candidate_bars["SPY"]})
        parameters = (config.range_efficiency_max, config.mean_reversion_min_vwap_atr)
        calls.append((session_count, parameters))
        realized_r = 0.6 if parameters == (0.24, 1.25) else 0.2
        trade_count = 10 if session_count == 7 else 3
        trades = [
            {"session": f"session-{index}", "realized_r": realized_r}
            for index in range(trade_count)
        ]
        return {
            "session_count": session_count,
            "accepted_session_count": session_count,
            "trade_count": trade_count,
            "average_r": realized_r,
            "max_drawdown_r": 0.5,
            "trades": trades,
        }

    monkeypatch.setattr(replay_module, "replay_sessions", fake_replay)
    result = mean_reversion_walk_forward(bars, train_fraction=0.7)

    assert result["ready"] is True
    assert result["train_sessions"] == 7
    assert result["test_sessions"] == 3
    assert result["selected_parameters"] == {
        "range_efficiency_max": 0.24,
        "mean_reversion_min_vwap_atr": 1.25,
    }
    assert result["test"]["trade_count"] == 3
    assert result["test"]["cost_adjusted"]["net_average_r"] == 0.48
    assert result["out_of_sample_positive"] is True
    assert calls[-1] == (3, (0.24, 1.25))


def test_rolling_walk_forward_uses_non_overlapping_test_windows(monkeypatch):
    first = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    bars = {symbol: [_row(first + timedelta(days=day), 100, 101, 99, 100) for day in range(16)] for symbol in ("SPY", "QQQ")}
    observed = []

    def fake_walk(window, _cfg, **_kwargs):
        dates = sorted({_row["ts_ny"].date().isoformat() for _row in window["SPY"]})
        observed.append(dates)
        return {"ready": True, "out_of_sample_positive": True, "test": {"trade_count": 3}}

    monkeypatch.setattr(replay_module, "mean_reversion_walk_forward", fake_walk)
    result = rolling_mean_reversion_walk_forward(bars, train_sessions=10, test_sessions=3)

    assert result["fold_count"] == 2
    assert result["positive_fold_fraction"] == 1.0
    assert observed[0][-3:] != observed[1][-3:]


def test_chronological_holdout_freezes_parameters_and_stresses_test_costs(monkeypatch):
    first = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    bars = {symbol: [_row(first + timedelta(days=day), 100, 101, 99, 100) for day in range(10)] for symbol in ("SPY", "DIA")}

    def fake_replay(candidate_bars, config, **_kwargs):
        count = len(candidate_bars["SPY"])
        return {"accepted_session_count": count, "trade_count": count, "max_drawdown_r": 1.0, "trades": [{"session": str(index), "realized_r": 0.5} for index in range(count)]}

    monkeypatch.setattr(replay_module, "replay_sessions", fake_replay)
    config = RegimeIntradayConfig(symbols=("SPY", "DIA"), trade_symbols=("DIA",))
    result = chronological_holdout(bars, config)
    assert result["train_sessions"] == 7
    assert result["test_sessions"] == 3
    assert result["parameters_frozen"] is True
    assert result["test"]["cost_012"]["net_average_r"] == 0.38
    assert result["test"]["cost_030"]["net_average_r"] == 0.2
