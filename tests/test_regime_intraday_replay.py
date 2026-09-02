from datetime import datetime, timedelta, timezone

from regime_intraday_replay import _outcome, cost_adjusted_report, replay_sessions, split_sessions, walk_forward


def test_cost_adjusted_report_tracks_daily_goal_rates():
    report = {"trades": [{"session": "2026-01-02", "realized_r": 2.0}, {"session": "2026-01-03", "realized_r": -1.0}]}
    result = cost_adjusted_report(report, risk_dollars=100, round_trip_cost_r=0.1)
    assert result["net_total_dollars"] == 80.0
    assert result["days_at_or_above_100"] == 1
    assert result["daily_goal_100_rate"] == 0.5


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
