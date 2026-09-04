from datetime import date

from opportunity_lab.weather_value import _temperatures_for_date, _ticker_date, calibrate_snapshot, score_event
from opportunity_lab.weather_backtest import _observed_temperature, build_historical_snapshot


def test_weather_event_scores_brackets_and_never_becomes_eligible():
    markets = [
        {"ticker": "LOW", "strike_type": "less", "cap_strike": 71, "yes_sub_title": "70 or below",
         "yes_ask_dollars": ".10", "yes_ask_size_fp": "4", "no_ask_dollars": ".92", "yes_bid_size_fp": "3"},
        {"ticker": "MID", "strike_type": "between", "floor_strike": 75, "cap_strike": 76,
         "yes_sub_title": "75 to 76", "yes_ask_dollars": ".20", "yes_ask_size_fp": "5",
         "no_ask_dollars": ".82", "yes_bid_size_fp": "2"},
    ]
    result = score_event("KXLOWTDAL-26SEP05", markets, forecast_mean_f=75.5, sigma_f=2.5,
                         extreme="low", target_date=date(2026, 9, 5),
                         series={"settlement_sources": [{"name": "The Weather Company"}]})
    assert result["candidate_count"] == 4
    assert result["best_model_edge_after_fee"] > 0
    assert result["eligible"] is False
    assert result["execution_enabled"] is False


def test_event_ticker_date_parser():
    assert _ticker_date("KXHIGHTDAL-26SEP05-T100") == date(2026, 9, 5)


def test_partial_local_day_is_rejected():
    partial = [{"startTime": f"2026-09-04T{hour:02d}:00:00-05:00", "temperature": 80, "temperatureUnit": "F"}
               for hour in range(12, 24)]
    assert _temperatures_for_date(partial, date(2026, 9, 4)) == []


def test_settled_snapshot_calculates_brier_and_paper_pnl():
    snapshot = score_event("E", [
        {"ticker": "LOW", "strike_type": "less", "cap_strike": 71, "yes_sub_title": "low",
         "yes_ask_dollars": ".10", "yes_ask_size_fp": "4", "no_ask_dollars": ".92", "yes_bid_size_fp": "3"},
        {"ticker": "MID", "strike_type": "between", "floor_strike": 75, "cap_strike": 76, "yes_sub_title": "mid",
         "yes_ask_dollars": ".20", "yes_ask_size_fp": "5", "no_ask_dollars": ".82", "yes_bid_size_fp": "2"},
    ], forecast_mean_f=75.5, sigma_f=2.5, extreme="low", target_date=date(2026, 9, 5))
    result = calibrate_snapshot(snapshot, {"LOW": {"ticker": "LOW", "result": "no", "strike_type": "less", "cap_strike": 71},
                                           "MID": {"ticker": "MID", "result": "yes", "strike_type": "between", "floor_strike": 75, "cap_strike": 76}})
    assert result is not None
    assert result["market_count"] == 2
    assert result["forecast_error_lower_bound_f"] == 0
    assert result["paper_trade"]["realized_pnl_per_contract"] > 0


def test_historical_snapshot_uses_only_candles_at_or_before_local_midnight():
    markets = [{"ticker": "MID", "strike_type": "between", "floor_strike": 75, "cap_strike": 76,
                "yes_sub_title": "75 to 76"}]
    candles = {"MID": [
        {"end_period_ts": 1788584400, "yes_ask": {"close_dollars": ".20"},
         "yes_bid": {"close_dollars": ".18"}},
        {"end_period_ts": 1788588000, "yes_ask": {"close_dollars": ".90"},
         "yes_bid": {"close_dollars": ".88"}},
    ]}
    snapshot = build_historical_snapshot("KXLOWTDAL-26SEP05", markets, candles, forecast_mean_f=75.5,
                                         sigma_f=2.5, extreme="low", target_date=date(2026, 9, 5))
    assert snapshot is not None
    assert snapshot["candidates"][0]["ask"] != .9
    assert snapshot["latest_quote_timestamp"] <= snapshot["decision_cutoff"]


def test_actual_weather_settlement_value_is_read_from_winning_bracket():
    assert _observed_temperature([
        {"ticker": "A", "result": "no", "expiration_value": "94.00"},
        {"ticker": "B", "result": "yes", "expiration_value": "94.00"},
    ]) == 94.0
