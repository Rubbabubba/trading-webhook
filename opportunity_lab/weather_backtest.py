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
    today = datetime.now(LOCAL).date()
    start_date, end_date = today - timedelta(days=days), today - timedelta(days=1)
    forecasts, forecast_transport = fetch_archived_gfs(start_date, end_date, lead_days=lead_days)
    if forecast_transport.get("error"):
        return {"ok": False, "forecast_transport": forecast_transport, "execution_enabled": False}
    rows, transports, results = [], {}, []
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
        snapshot = build_historical_snapshot(event_ticker, markets, candles, forecast_mean_f=forecast,
                                             sigma_f=sigma_f, extreme=extreme, target_date=target)
        calibration = calibrate_snapshot(snapshot, {row["ticker"]: row for row in markets}) if snapshot else None
        if calibration:
            best_edge = max(float(row.get("model_edge_after_fee") or -999) for row in snapshot["candidates"])
            calibration["paper_trade"]["model_edge_after_fee"] = round(best_edge, 6)
            calibration["paper_trade"]["taken"] = best_edge >= minimum_edge
            results.append(calibration)
    trades = [row["paper_trade"] for row in results if row["paper_trade"]["taken"]]
    pnl = [row["realized_pnl_per_contract"] for row in trades]
    briers = [row["brier_score"] for row in results]
    return {"ok": True, "location": "Dallas / CLIDFW", "requested_days": days,
            "forecast_transport": forecast_transport, "market_transports": transports,
            "decision_policy": "latest completed hourly Kalshi candle no later than 00:00 America/Chicago on target date",
            "minimum_model_edge_after_fee": minimum_edge,
            "event_count": len(results), "average_brier_score": round(sum(briers) / len(briers), 8) if briers else None,
            "paper_trade_count": len(trades),
            "paper_pnl_per_one_contract_each_trade": round(sum(pnl), 6),
            "paper_profitable_event_count": sum(value > 0 for value in pnl),
            "paper_win_rate": round(sum(value > 0 for value in pnl) / len(pnl), 6) if pnl else None,
            "events": results, "candle_errors": candle_errors,
            "limitations": ["archived GFS is not the NWS forecaster grid or Kalshi settlement source",
                            "hourly candle closing asks are historical quote proxies, not guaranteed fills",
                            "one-contract results exclude bankroll sizing and opportunity scarcity"],
            "eligible": False, "blockers": ["historical_proxy_requires_validation_against_forward_snapshots",
                                               "settlement_source_model_mismatch"],
            "research_only": True, "execution_enabled": False}


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
