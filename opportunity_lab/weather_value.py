"""Read-only NWS forecast proxy for Kalshi daily Dallas temperature markets."""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .kalshi_market_data import fetch_series_markets


DFW_COORDS = (32.8998, -97.0403)
SERIES = {"high": "KXHIGHTDAL", "low": "KXLOWTDAL"}
MONTHS = {month.upper(): number for number, month in enumerate(
    ("", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")) if month}


def collect_dallas_weather(*, sigma_f: float = 2.5) -> dict:
    hourly, forecast_transport = fetch_nws_hourly(*DFW_COORDS)
    if forecast_transport.get("error"):
        return {"ok": False, "forecast_transport": forecast_transport, "execution_enabled": False}
    events, transports = [], {}
    for extreme, series_ticker in SERIES.items():
        markets, series, transport = fetch_series_markets(series_ticker)
        transports[series_ticker] = transport
        if transport.get("error"):
            continue
        by_event = {}
        for market in markets:
            by_event.setdefault(market.get("event_ticker"), []).append(market)
        for event_ticker, event_markets in by_event.items():
            target = _ticker_date(event_ticker)
            temperatures = _temperatures_for_date(hourly, target)
            if not target or not temperatures:
                continue
            mean = max(temperatures) if extreme == "high" else min(temperatures)
            events.append(score_event(event_ticker, event_markets, forecast_mean_f=mean, sigma_f=sigma_f,
                                      extreme=extreme, target_date=target, series=series))
    events.sort(key=lambda row: row["best_model_edge_after_fee"], reverse=True)
    return {"ok": True, "location": "Dallas / CLIDFW", "forecast_source": "NWS hourly point forecast at DFW Airport",
            "forecast_transport": forecast_transport, "market_transports": transports, "event_count": len(events),
            "events": events, "best_candidate": events[0] if events else None,
            "model_status": "uncalibrated_research_proxy", "research_only": True, "execution_enabled": False}


def fetch_nws_hourly(latitude: float, longitude: float) -> tuple[list[dict], dict]:
    point, first = _get(f"https://api.weather.gov/points/{latitude:.4f},{longitude:.4f}")
    if first.get("error"):
        return [], {**first, "stage": "point_lookup"}
    url = (point.get("properties") or {}).get("forecastHourly")
    if not url:
        return [], {"error": "forecast_hourly_url_missing", "stage": "point_lookup"}
    forecast, second = _get(url)
    periods = (forecast.get("properties") or {}).get("periods") or []
    return periods, {**second, "method": "nws_public_rest", "path": url, "period_count": len(periods),
                     "authenticated": False}


def score_event(event_ticker: str, markets: list[dict], *, forecast_mean_f: float, sigma_f: float,
                extreme: str, target_date: date, series: dict | None = None) -> dict:
    candidates = []
    for market in markets:
        probability = _market_probability(market, forecast_mean_f, sigma_f)
        if probability is None:
            continue
        for side, chance_key, ask_key, size_key in (
            ("yes", probability, "yes_ask_dollars", "yes_ask_size_fp"),
            ("no", 1 - probability, "no_ask_dollars", "yes_bid_size_fp"),
        ):
            ask = _number(market.get(ask_key)); size = _number(market.get(size_key))
            if ask is None or not 0 < ask < 1 or not size or size <= 0:
                continue
            fee = math.ceil(0.07 * ask * (1 - ask) * 100 - 1e-12) / 100
            edge = chance_key - ask - fee
            candidates.append({"ticker": market.get("ticker"), "outcome": market.get("yes_sub_title"),
                               "side": side, "model_probability": round(chance_key, 6),
                               "ask": ask, "displayed_size": size, "estimated_taker_fee_per_contract": fee,
                               "model_edge_after_fee": round(edge, 6), "model_roi_on_cost_pct": round(edge / (ask + fee) * 100, 6),
                               "positive_model_edge": edge > 0})
    candidates.sort(key=lambda row: row["model_edge_after_fee"], reverse=True)
    sources = list((series or {}).get("settlement_sources") or [])
    return {"event_ticker": event_ticker, "target_date": target_date.isoformat(), "extreme": extreme,
            "forecast_mean_f": forecast_mean_f, "assumed_error_sigma_f": sigma_f,
            "settlement_sources": sources, "candidate_count": len(candidates), "candidates": candidates,
            "best_model_edge_after_fee": candidates[0]["model_edge_after_fee"] if candidates else -1,
            "positive_model_edge_count": sum(row["positive_model_edge"] for row in candidates),
            "eligible": False,
            "blockers": ["forecast_error_not_calibrated", "nws_proxy_differs_from_weather_company_settlement",
                         "series_fee_and_account_eligibility_not_verified"], "execution_enabled": False}


def _market_probability(market: dict, mean: float, sigma: float) -> float | None:
    sigma = max(.1, float(sigma))
    strike_type = market.get("strike_type")
    floor = _number(market.get("floor_strike")); cap = _number(market.get("cap_strike"))
    cdf = lambda value: .5 * (1 + math.erf((value - mean) / (sigma * math.sqrt(2))))
    if strike_type == "less" and cap is not None:
        return cdf(cap - .5)
    if strike_type == "greater" and floor is not None:
        return 1 - cdf(floor + .5)
    if strike_type == "between" and floor is not None and cap is not None:
        return max(0.0, cdf(cap + .5) - cdf(floor - .5))
    return None


def _ticker_date(ticker: str | None) -> date | None:
    match = re.search(r"-(\d{2})([A-Z]{3})(\d{2})(?:-|$)", str(ticker or ""))
    if not match or match.group(2) not in MONTHS:
        return None
    return date(2000 + int(match.group(1)), MONTHS[match.group(2)], int(match.group(3)))


def _temperatures_for_date(periods: list[dict], target: date | None) -> list[float]:
    if not target:
        return []
    values, hours = [], []
    for period in periods:
        try:
            local = datetime.fromisoformat(period["startTime"]).astimezone(ZoneInfo("America/Chicago"))
            value = float(period["temperature"])
            if local.date() == target and str(period.get("temperatureUnit") or "F").upper() == "F":
                values.append(value)
                hours.append(local.hour)
        except (KeyError, TypeError, ValueError):
            continue
    return values if hours and min(hours) <= 1 and max(hours) >= 22 and len(set(hours)) >= 20 else []


def _number(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get(url: str) -> tuple[dict, dict]:
    request = Request(url, headers={"Accept": "application/geo+json", "User-Agent": "OpportunityLab/1.0 (research)"})
    try:
        with urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8")), {"status_code": response.status}
    except HTTPError as exc:
        return {}, {"status_code": exc.code, "error": f"nws_http_{exc.code}"}
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {}, {"error": f"nws_transport_error:{type(exc).__name__}"}
