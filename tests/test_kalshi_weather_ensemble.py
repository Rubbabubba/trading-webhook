from datetime import date, datetime, timezone
import json

from opportunity_lab.kalshi_external_sleeves import research_relevance
from opportunity_lab.kalshi_external_sleeves_worker import (
    WEATHER_ENSEMBLE_ID, _weather_summary, collect_weather_one_station, open_db,
)
from opportunity_lab.kalshi_weather_ensemble import (
    WEATHER_SERIES, coherent_event, fetch_ensemble, score_resolution,
    weather_event_observation, weather_market_spec,
)


def weather_market(ticker, subtitle, *, yes_bid=".18", yes_ask=".20"):
    return {
        "ticker": ticker, "event_ticker": "KXHIGHTDAL-26OCT01",
        "status": "active", "market_type": "binary", "exchange_index": 0,
        "title": "Highest temperature in Dallas-Fort Worth on October 1?",
        "yes_sub_title": subtitle, "rules_primary": "Dallas-Fort Worth temperature",
        "rules_secondary": "Settles from the official Dallas-Fort Worth climate report",
        "yes_bid_dollars": yes_bid, "yes_ask_dollars": yes_ask,
        "no_bid_dollars": str(round(1 - float(yes_ask), 2)),
        "no_ask_dollars": str(round(1 - float(yes_bid), 2)),
        "yes_bid_size_fp": "5", "yes_ask_size_fp": "5",
        "no_bid_size_fp": "5", "no_ask_size_fp": "5",
        "expiration_time": "2026-10-02T12:00:00Z",
        "close_time": "2026-10-02T12:00:00Z",
    }


def ladder():
    return [
        weather_market("DAL-LOW", "77 or below", yes_bid=".08", yes_ask=".10"),
        weather_market("DAL-MID", "78 to 79", yes_bid=".18", yes_ask=".20"),
        weather_market("DAL-HIGH", "80 or above", yes_bid=".18", yes_ask=".20"),
    ]


def forecasts():
    return {
        "gfs": {"high": [80.0] * 31, "low": [60.0] * 31, "forecast_hash": "g"},
        "ecmwf_ifs": {"high": [81.0] * 51, "low": [61.0] * 51, "forecast_hash": "e"},
    }


def test_weather_series_is_exact_and_routes_research_market():
    row = ladder()[0]
    assert weather_market_spec(row).station == "KDFW"
    assert "weather_ensemble" in research_relevance(row)
    wrong = dict(row, title="Lowe's credit card spend", rules_primary="Lowe's credit card spend",
                 rules_secondary="Carbon Arc")
    assert weather_market_spec(wrong) is None
    assert "weather_ensemble" not in research_relevance(wrong)


def test_weather_ladder_signal_requires_two_model_agreement_and_complete_buckets():
    rows, error = coherent_event(ladder())
    assert error is None and len(rows) == 3
    observation = weather_event_observation(
        "KXHIGHTDAL-26OCT01", ladder(), forecasts(),
        "2026-09-30T18:00:00+00:00")
    assert observation["classification"] == "signal"
    assert observation["signal"]["ticker"] == "DAL-HIGH"
    assert observation["signal"]["side"] == "yes"
    assert observation["signal"]["stressed_edge"] > .7
    disagree = forecasts()
    disagree["ecmwf_ifs"]["high"] = [75.0] * 51
    assert weather_event_observation(
        "KXHIGHTDAL-26OCT01", ladder(), disagree,
        "2026-09-30T18:00:00+00:00")["signal"] is None


def test_weather_resolution_scores_model_against_normalized_market():
    observation = weather_event_observation(
        "KXHIGHTDAL-26OCT01", ladder(), forecasts(),
        "2026-09-30T18:00:00+00:00")
    result = score_resolution(observation, 80.0)
    assert result["model_brier"] == 0
    assert result["brier_delta_vs_market"] < 0
    assert result["cost_stressed_net_dollars"] > 0


class Response:
    status = 200
    def __init__(self, payload): self.payload = payload
    def __enter__(self): return self
    def __exit__(self, *_args): return None
    def read(self): return json.dumps(self.payload).encode()


def test_open_meteo_parser_keeps_model_families_separate():
    payload = {"daily": {"time": ["2026-10-01"],
        "temperature_2m_max_ncep_gefs_seamless": [80],
        "temperature_2m_max_member01_ncep_gefs_seamless": [81],
        "temperature_2m_min_ncep_gefs_seamless": [60],
        "temperature_2m_min_member01_ncep_gefs_seamless": [61],
        "temperature_2m_max_ecmwf_ifs025_ensemble": [79],
        "temperature_2m_max_member01_ecmwf_ifs025_ensemble": [82],
        "temperature_2m_min_ecmwf_ifs025_ensemble": [59],
        "temperature_2m_min_member01_ecmwf_ifs025_ensemble": [62]}}
    rows, transport = fetch_ensemble(
        WEATHER_SERIES["KXHIGHTDAL"], date(2026, 10, 1), date(2026, 10, 1),
        opener=lambda *_args, **_kwargs: Response(payload))
    assert not transport.get("error")
    assert rows["2026-10-01"]["gfs"]["high"] == [80.0, 81.0]
    assert rows["2026-10-01"]["ecmwf_ifs"]["low"] == [59.0, 62.0]


def test_worker_collects_vintages_observations_and_gate_is_shadow_only(tmp_path):
    db = open_db(tmp_path / "research.sqlite3")
    def fake_fetch(spec, start, end):
        assert spec.station == "KDFW" and start == end == date(2026, 10, 1)
        return {"2026-10-01": forecasts()}, {"status_code": 200}
    status = collect_weather_one_station(
        db, ladder(), datetime(2026, 9, 30, 18, tzinfo=timezone.utc),
        fetcher=fake_fetch)
    assert status["new_forecast_vintages"] == 4
    assert status["new_observations"] == status["new_signals"] == 1
    summary = _weather_summary(db)
    assert summary["strategy_id"] == WEATHER_ENSEMBLE_ID
    assert summary["candidate_observations"] == 1
    assert summary["execution_enabled"] is False
    assert summary["state"] == "collecting"
    db.close()
