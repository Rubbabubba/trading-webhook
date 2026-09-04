"""Lookahead-safe historical research for Dallas Kalshi temperature markets."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .kalshi_market_data import fetch_event_candlesticks, fetch_settled_series_markets
from .weather_value import DFW_COORDS, SERIES, _ticker_date, calibrate_snapshot, score_event


LOCAL = ZoneInfo("America/Chicago")
OPEN_METEO_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"


def fetch_archived_gfs(start: date, end: date, *, lead_days: int = 1) -> tuple[dict[date, list[float]], dict]:
    variable = f"temperature_2m_previous_day{max(1, min(7, int(lead_days)))}"
    params = {"latitude": DFW_COORDS[0], "longitude": DFW_COORDS[1], "start_date": start.isoformat(),
              "end_date": end.isoformat(), "hourly": variable, "temperature_unit": "fahrenheit",
              "timezone": "America/Chicago", "models": "gfs_seamless"}
    request = Request(f"{OPEN_METEO_URL}?{urlencode(params)}",
                      headers={"Accept": "application/json", "User-Agent": "OpportunityLab/1.0"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return {}, {"error": f"open_meteo_http_{exc.code}", "status_code": exc.code}
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {}, {"error": f"open_meteo_transport_error:{type(exc).__name__}"}
    hourly = payload.get("hourly") or {}
    grouped: dict[date, list[float]] = defaultdict(list)
    for stamp, value in zip(hourly.get("time") or [], hourly.get(variable) or []):
        if value is not None:
            grouped[datetime.fromisoformat(stamp).date()].append(float(value))
    complete = {day: values for day, values in grouped.items() if len(values) >= 23}
    return complete, {"method": "open_meteo_previous_runs", "model": "gfs_seamless", "variable": variable,
                      "forecast_lead_hours": lead_days * 24, "day_count": len(complete), "authenticated": False}


def build_historical_snapshot(event_ticker: str, markets: list[dict], candle_sets: dict[str, list[dict]], *,
                              forecast_mean_f: float, sigma_f: float, extreme: str, target_date: date) -> dict | None:
    """Build a synthetic quote snapshot using only candles completed by local midnight."""
    cutoff = int(datetime.combine(target_date, time.min, LOCAL).timestamp())
    quoted = []
    candle_times = []
    for market in markets:
        usable = [row for row in candle_sets.get(str(market.get("ticker")), [])
                  if int(row.get("end_period_ts") or 0) <= cutoff]
        if not usable:
            continue
        candle = max(usable, key=lambda row: int(row.get("end_period_ts") or 0))
        yes_ask = _close(candle.get("yes_ask")); yes_bid = _close(candle.get("yes_bid"))
        if yes_ask is None or yes_bid is None:
            continue
        quoted.append({**market, "yes_ask_dollars": yes_ask, "no_ask_dollars": 1 - yes_bid,
                       "yes_ask_size_fp": "1", "yes_bid_size_fp": "1"})
        candle_times.append(int(candle["end_period_ts"]))
    if not quoted:
        return None
    snapshot = score_event(event_ticker, quoted, forecast_mean_f=forecast_mean_f, sigma_f=sigma_f,
                           extreme=extreme, target_date=target_date)
    snapshot["decision_cutoff"] = datetime.fromtimestamp(cutoff, timezone.utc).isoformat()
    snapshot["latest_quote_timestamp"] = datetime.fromtimestamp(max(candle_times), timezone.utc).isoformat()
    snapshot["historical_quote_market_count"] = len(quoted)
    return snapshot


def historical_dallas_backtest(*, days: int = 30, sigma_f: float = 2.5, lead_days: int = 1,
                               minimum_edge: float = .05) -> dict:
    days = max(7, min(90, int(days)))
    cases, metadata = _collect_historical_cases(days, lead_days)
    if not metadata.get("ok"):
        return metadata
    results = _evaluate_cases(cases, sigma_f=sigma_f, minimum_edge=minimum_edge)
    return {**metadata, **_summary(results), "minimum_model_edge_after_fee": minimum_edge,
            "events": results}


def walk_forward_dallas(*, days: int = 60, lead_days: int = 1,
                        sigma_grid: tuple[float, ...] = (1.5, 2.0, 2.5, 3.0, 3.5, 4.0),
                        edge_grid: tuple[float, ...] = (.03, .05, .08, .10, .15)) -> dict:
    """Tune on the oldest two-thirds, then report one untouched validation result."""
    days = max(21, min(90, int(days)))
    cases, metadata = _collect_historical_cases(days, lead_days)
    if not metadata.get("ok"):
        return metadata
    ordered_dates = sorted({case["target"] for case in cases})
    split_index = max(1, int(len(ordered_dates) * 2 / 3))
    calibration_dates = set(ordered_dates[:split_index])
    calibration_cases = [case for case in cases if case["target"] in calibration_dates]
    validation_cases = [case for case in cases if case["target"] not in calibration_dates]
    grid = []
    for sigma in sigma_grid:
        for edge in edge_grid:
            summary = _summary(_evaluate_cases(calibration_cases, sigma_f=sigma, minimum_edge=edge))
            grid.append({"sigma_f": sigma, "minimum_edge": edge, **summary})
    viable = [row for row in grid if row["paper_trade_count"] >= max(5, len(calibration_dates) // 4)]
    ranked = viable or grid
    ranked.sort(key=lambda row: (row["paper_pnl_per_one_contract_each_trade"],
                                 row["paper_win_rate"] or 0, row["paper_trade_count"]), reverse=True)
    selected = ranked[0]
    validation_results = _evaluate_cases(validation_cases, sigma_f=selected["sigma_f"],
                                         minimum_edge=selected["minimum_edge"])
    validation = _summary(validation_results)
    calibration_profitable = selected["paper_pnl_per_one_contract_each_trade"] > 0
    out_of_sample_profitable = (validation["paper_trade_count"] >= 5 and
                                validation["paper_pnl_per_one_contract_each_trade"] > 0)
    passes = calibration_profitable and out_of_sample_profitable
    return {**metadata, "method": "chronological_two_thirds_calibration_one_third_validation",
            "split": {"calibration_start": ordered_dates[0].isoformat() if ordered_dates else None,
                      "calibration_end": ordered_dates[split_index - 1].isoformat() if ordered_dates else None,
                      "validation_start": ordered_dates[split_index].isoformat() if len(ordered_dates) > split_index else None,
                      "validation_end": ordered_dates[-1].isoformat() if ordered_dates else None},
            "grid_size": len(grid), "selected_parameters": {"sigma_f": selected["sigma_f"],
                                                               "minimum_edge": selected["minimum_edge"]},
            "calibration": {key: value for key, value in selected.items()
                            if key not in {"sigma_f", "minimum_edge", "events"}},
            "validation": validation, "validation_events": validation_results,
            "calibration_profitable": calibration_profitable,
            "out_of_sample_profitable": out_of_sample_profitable,
            "model_retained": passes,
            "verdict": "retain_for_forward_validation" if passes else "reject_or_redesign_current_weather_model",
            "eligible": False, "execution_enabled": False}


def _collect_historical_cases(days: int, lead_days: int) -> tuple[list[dict], dict]:
    today = datetime.now(LOCAL).date()
    start_date, end_date = today - timedelta(days=days), today - timedelta(days=1)
    forecasts, forecast_transport = fetch_archived_gfs(start_date, end_date, lead_days=lead_days)
    if forecast_transport.get("error"):
        return [], {"ok": False, "forecast_transport": forecast_transport, "execution_enabled": False}
    rows, transports, cases = [], {}, []
    min_settled = int(datetime.combine(start_date, time.min, LOCAL).timestamp())
    for extreme, series_ticker in SERIES.items():
        markets, transport = fetch_settled_series_markets(series_ticker, min_settled_ts=min_settled)
        transports[series_ticker] = transport
        for market in markets:
            rows.append((extreme, series_ticker, market))
    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for extreme, series_ticker, market in rows:
        grouped[(extreme, series_ticker, str(market.get("event_ticker")))].append(market)
    candle_errors = []
    for (extreme, series_ticker, event_ticker), markets in sorted(grouped.items()):
        target = _ticker_date(event_ticker)
        values = forecasts.get(target) if target else None
        if not target or not values or not start_date <= target <= end_date:
            continue
        open_times = [_timestamp(row.get("open_time")) for row in markets]
        start_ts = min((value for value in open_times if value), default=int(datetime.combine(target - timedelta(days=2), time.min, LOCAL).timestamp()))
        cutoff = int(datetime.combine(target, time.min, LOCAL).timestamp())
        candles, candle_transport = fetch_event_candlesticks(series_ticker, event_ticker, start_ts=start_ts,
                                                              end_ts=cutoff, period_interval=60)
        if candle_transport.get("error"):
            candle_errors.append({"event_ticker": event_ticker, "error": candle_transport.get("error")})
            continue
        forecast = max(values) if extreme == "high" else min(values)
        cases.append({"event_ticker": event_ticker, "markets": markets, "candles": candles,
                      "forecast": forecast, "extreme": extreme, "target": target})
    metadata = {"ok": True, "location": "Dallas / CLIDFW", "requested_days": days,
                "forecast_transport": forecast_transport, "market_transports": transports,
                "decision_policy": "latest completed hourly Kalshi candle no later than 00:00 America/Chicago on target date",
                "candle_errors": candle_errors,
                "limitations": ["archived GFS is not the NWS forecaster grid or Kalshi settlement source",
                                "hourly candle closing asks are historical quote proxies, not guaranteed fills",
                                "one-contract results exclude bankroll sizing and opportunity scarcity"],
                "eligible": False, "blockers": ["historical_proxy_requires_validation_against_forward_snapshots",
                                                   "settlement_source_model_mismatch"],
                "research_only": True, "execution_enabled": False}
    return cases, metadata


def _evaluate_cases(cases: list[dict], *, sigma_f: float, minimum_edge: float) -> list[dict]:
    results = []
    for case in cases:
        snapshot = build_historical_snapshot(case["event_ticker"], case["markets"], case["candles"],
                                             forecast_mean_f=case["forecast"], sigma_f=sigma_f,
                                             extreme=case["extreme"], target_date=case["target"])
        calibration = calibrate_snapshot(snapshot, {row["ticker"]: row for row in case["markets"]}) if snapshot else None
        if calibration:
            best_edge = max(float(row.get("model_edge_after_fee") or -999) for row in snapshot["candidates"])
            calibration["paper_trade"]["model_edge_after_fee"] = round(best_edge, 6)
            calibration["paper_trade"]["taken"] = best_edge >= minimum_edge
            results.append(calibration)
    return results


def _summary(results: list[dict]) -> dict:
    trades = [row["paper_trade"] for row in results if row["paper_trade"]["taken"]]
    pnl = [row["realized_pnl_per_contract"] for row in trades]
    briers = [row["brier_score"] for row in results]
    return {"event_count": len(results), "average_brier_score": round(sum(briers) / len(briers), 8) if briers else None,
            "paper_trade_count": len(trades),
            "paper_pnl_per_one_contract_each_trade": round(sum(pnl), 6),
            "paper_profitable_event_count": sum(value > 0 for value in pnl),
            "paper_win_rate": round(sum(value > 0 for value in pnl) / len(pnl), 6) if pnl else None}


def _close(row) -> float | None:
    try:
        value = float((row or {}).get("close_dollars"))
        return value if 0 <= value <= 1 else None
    except (TypeError, ValueError):
        return None


def _timestamp(value) -> int | None:
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError):
        return None
