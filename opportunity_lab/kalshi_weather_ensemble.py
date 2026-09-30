"""Fail-closed, shadow-only weather ensemble research helpers.

The model deliberately keeps forecast collection separate from order execution.
It uses two independent Open-Meteo ensemble families and accepts a market only
when its Kalshi series, settlement location and temperature bucket are known.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
import re
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
MODEL_SUFFIXES = {
    "gfs": "ncep_gefs_seamless",
    "ecmwf_ifs": "ecmwf_ifs025_ensemble",
}
MODEL_REQUEST = "gfs_seamless,ecmwf_ifs025"


@dataclass(frozen=True)
class WeatherSeries:
    series: str
    station: str
    name: str
    latitude: float
    longitude: float
    timezone: str
    extreme: str
    rule_tokens: tuple[str, ...]


# Station mappings are limited to current Kalshi daily-temperature series whose
# live rule text names the corresponding NOAA CLI station. New series fail
# closed until their settlement station is explicitly registered.
_STATIONS = {
    "KNYC": ("New York Central Park", 40.7794, -73.9692, "America/New_York", ("central park", "new york")),
    "KMDW": ("Chicago Midway", 41.7868, -87.7522, "America/Chicago", ("midway", "chicago")),
    "KMIA": ("Miami International", 25.7933, -80.2906, "America/New_York", ("miami",)),
    "KLAX": ("Los Angeles International", 33.9381, -118.3889, "America/Los_Angeles", ("los angeles", "lax")),
    "KATL": ("Atlanta Hartsfield", 33.6367, -84.4281, "America/New_York", ("atlanta",)),
    "KAUS": ("Austin-Bergstrom", 30.1950, -97.6700, "America/Chicago", ("austin",)),
    "KPHL": ("Philadelphia International", 39.8744, -75.2424, "America/New_York", ("philadelphia",)),
    "KDEN": ("Denver International", 39.8466, -104.6564, "America/Denver", ("denver",)),
    "KHOU": ("Houston Hobby", 29.6454, -95.2789, "America/Chicago", ("houston", "houston-hobby", "houston hobby")),
    "KDCA": ("Washington Reagan National", 38.8512, -77.0402, "America/New_York", ("reagan", "washington")),
    "KBOS": ("Boston Logan", 42.3656, -71.0096, "America/New_York", ("boston", "logan")),
    "KPHX": ("Phoenix Sky Harbor", 33.4373, -112.0078, "America/Phoenix", ("phoenix", "sky harbor")),
    "KDFW": ("Dallas-Fort Worth", 32.8998, -97.0403, "America/Chicago", ("dallas", "fort worth", "dfw")),
    "KSFO": ("San Francisco International", 37.6213, -122.3790, "America/Los_Angeles", ("san francisco", "sfo")),
    "KSEA": ("Seattle-Tacoma", 47.4502, -122.3088, "America/Los_Angeles", ("seattle", "tacoma")),
    "KLAS": ("Las Vegas Harry Reid", 36.0840, -115.1537, "America/Los_Angeles", ("las vegas", "harry reid")),
    "KMSY": ("New Orleans Louis Armstrong", 29.9934, -90.2580, "America/Chicago", ("new orleans",)),
    "KMSP": ("Minneapolis-St Paul", 44.8848, -93.2223, "America/Chicago", ("minneapolis", "st. paul", "st paul")),
    "KSAT": ("San Antonio International", 29.5337, -98.4698, "America/Chicago", ("san antonio",)),
    "KOKC": ("Oklahoma City Will Rogers", 35.3931, -97.6007, "America/Chicago", ("oklahoma city", "will rogers")),
    "KEWR": ("Newark Liberty", 40.6895, -74.1745, "America/New_York", ("newark", "ewr")),
    "KSAN": ("San Diego International", 32.7338, -117.1933, "America/Los_Angeles", ("san diego", "san")),
    "KSDF": ("Louisville Muhammad Ali", 38.1744, -85.7360, "America/Kentucky/Louisville", ("louisville", "sdf")),
    "KTTN": ("Trenton Mercer", 40.2767, -74.8135, "America/New_York", ("trenton", "ttn")),
}


def _series(series, station, extreme):
    name, lat, lon, tz, tokens = _STATIONS[station]
    return WeatherSeries(series, station, name, lat, lon, tz, extreme, tokens)


_HIGH_SERIES = {
    "KXHIGHNY": "KNYC", "KXHIGHCHI": "KMDW", "KXHIGHMIA": "KMIA",
    "KXHIGHLAX": "KLAX", "KXHIGHPHIL": "KPHL", "KXHIGHAUS": "KAUS",
    "KXHIGHDEN": "KDEN", "KXHIGHTATL": "KATL", "KXHIGHTBOS": "KBOS",
    "KXHIGHTDAL": "KDFW", "KXHIGHTDC": "KDCA", "KXHIGHTEWR": "KEWR",
    "KXHIGHTHOU": "KHOU", "KXHIGHTLV": "KLAS", "KXHIGHTMIN": "KMSP",
    "KXHIGHTNOLA": "KMSY", "KXHIGHTOKC": "KOKC", "KXHIGHTPHX": "KPHX",
    "KXHIGHTSAN": "KSAN", "KXHIGHTSATX": "KSAT", "KXHIGHTSDF": "KSDF",
    "KXHIGHTSEA": "KSEA", "KXHIGHTSFO": "KSFO", "KXHIGHTTTN": "KTTN",
}
_LOW_SERIES = {
    "KXLOWTATL": "KATL", "KXLOWTAUS": "KAUS", "KXLOWTBOS": "KBOS",
    "KXLOWTCHI": "KMDW", "KXLOWTDAL": "KDFW", "KXLOWTDC": "KDCA",
    "KXLOWTDEN": "KDEN", "KXLOWTEWR": "KEWR", "KXLOWTHOU": "KHOU",
    "KXLOWTLAX": "KLAX", "KXLOWTLV": "KLAS", "KXLOWTMIA": "KMIA",
    "KXLOWTMIN": "KMSP", "KXLOWTNOLA": "KMSY", "KXLOWTNYC": "KNYC",
    "KXLOWTOKC": "KOKC", "KXLOWTPHIL": "KPHL", "KXLOWTPHX": "KPHX",
    "KXLOWTSAN": "KSAN", "KXLOWTSATX": "KSAT", "KXLOWTSDF": "KSDF",
    "KXLOWTSEA": "KSEA", "KXLOWTSFO": "KSFO", "KXLOWTTTN": "KTTN",
}
WEATHER_SERIES = {
    **{name: _series(name, station, "high") for name, station in _HIGH_SERIES.items()},
    **{name: _series(name, station, "low") for name, station in _LOW_SERIES.items()},
}


def weather_market_spec(market: dict, *, verify_rules: bool = True) -> WeatherSeries | None:
    event = str(market.get("event_ticker") or "").upper()
    series = next((name for name in sorted(WEATHER_SERIES, key=len, reverse=True)
                   if event == name or event.startswith(name + "-")), None)
    if series is None:
        return None
    spec = WEATHER_SERIES[series]
    if not verify_rules:
        return spec
    text = " ".join(str(market.get(key) or "") for key in (
        "title", "subtitle", "rules_primary", "rules_secondary",
    )).lower()
    return spec if any(token in text for token in spec.rule_tokens) else None


def parse_target_date(event_ticker: str | None) -> date | None:
    months = {name: index for index, name in enumerate(
        ("", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")) if name}
    match = re.search(r"-(\d{2})([A-Z]{3})(\d{2})(?:-|$)", str(event_ticker or "").upper())
    if not match or match.group(2) not in months:
        return None
    try:
        return date(2000 + int(match.group(1)), months[match.group(2)], int(match.group(3)))
    except ValueError:
        return None


def parse_temperature_bucket(market: dict) -> tuple[float, float] | None:
    """Return inclusive integer-temperature bounds as half-open float edges."""
    text = " ".join(str(market.get(key) or "") for key in (
        "yes_sub_title", "title", "subtitle", "rules_primary",
    )).replace("°", "")
    number = r"(-?\d+(?:\.\d+)?)"
    between = re.search(number + r"\s*(?:to|through|-)\s*" + number, text, re.I)
    below = re.search(number + r"\s*(?:or\s+)?(?:below|lower|less)", text, re.I)
    above = re.search(number + r"\s*(?:or\s+)?(?:above|higher|greater)", text, re.I)
    if between:
        low, high = map(float, between.groups())
        return (low - .5, high + .5) if low <= high else None
    if below:
        return -math.inf, float(below.group(1)) + .5
    if above:
        return float(above.group(1)) - .5, math.inf
    return None


def coherent_event(markets: list[dict]) -> tuple[list[dict], str | None]:
    parsed = []
    for market in markets:
        bucket = parse_temperature_bucket(market)
        if bucket is None:
            return [], "unparsed_bucket"
        parsed.append({**market, "weather_bucket": bucket})
    parsed.sort(key=lambda row: (row["weather_bucket"][0], row["weather_bucket"][1]))
    if not parsed or parsed[0]["weather_bucket"][0] != -math.inf or parsed[-1]["weather_bucket"][1] != math.inf:
        return [], "incomplete_tails"
    for left, right in zip(parsed, parsed[1:]):
        if abs(left["weather_bucket"][1] - right["weather_bucket"][0]) > 1e-9:
            return [], "bucket_gap_or_overlap"
    return parsed, None


def member_probabilities(markets: list[dict], members: list[float]) -> dict[str, float]:
    if not members:
        return {}
    result = {}
    for market in markets:
        low, high = market["weather_bucket"]
        result[str(market["ticker"])] = sum(low <= value < high for value in members) / len(members)
    return result


def weather_event_observation(event_id: str, markets: list[dict], forecasts: dict,
                              observed_at: str, *, minimum_raw_edge=.12,
                              fee_and_model_stress=.02,
                              maximum_model_disagreement=.15,
                              maximum_model_mean_difference_f=3.0) -> dict:
    observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    bucket = int(observed.timestamp()) // 1800 * 1800
    coherent, error = coherent_event(markets)
    spec = weather_market_spec(markets[0]) if markets else None
    target = parse_target_date(event_id)
    result = {
        "observation_id": f"{event_id}:{bucket}", "event_id": event_id,
        "decision_bucket": bucket, "observed_at": observed_at,
        "target_date": target.isoformat() if target else None,
        "station": spec.station if spec else None, "series": spec.series if spec else None,
        "extreme": spec.extreme if spec else None, "signal": None,
        "expiration_time": max((str(row.get("expiration_time") or row.get("close_time") or "")
                                for row in markets), default=""),
        "execution_enabled": False, "fill_assumed": False,
    }
    if not spec or not target or error:
        return {**result, "classification": error or "unregistered_station"}
    model_rows = {}
    for model, payload in forecasts.items():
        members = list(payload.get(spec.extreme) or [])
        minimum = 20 if model == "gfs" else 40
        if len(members) < minimum:
            return {**result, "classification": f"insufficient_{model}_members"}
        model_rows[model] = {
            "member_count": len(members), "forecast_hash": payload.get("forecast_hash"),
            "probabilities": member_probabilities(coherent, members),
            "mean_f": sum(members) / len(members),
        }
    if set(model_rows) != set(MODEL_SUFFIXES):
        return {**result, "classification": "missing_model_family"}
    centers = [row["mean_f"] for row in model_rows.values()]
    if max(centers) - min(centers) > maximum_model_mean_difference_f:
        return {**result, "classification": "model_family_center_disagreement",
                "models": model_rows,
                "maximum_model_mean_difference_f": maximum_model_mean_difference_f}
    contracts = []
    for market in coherent:
        ticker = str(market["ticker"])
        probabilities = {model: row["probabilities"][ticker] for model, row in model_rows.items()}
        consensus = sum(probabilities.values()) / len(probabilities)
        try:
            yes_bid = float(market["yes_bid_dollars"]); yes_ask = float(market["yes_ask_dollars"])
            no_bid = float(market["no_bid_dollars"]); no_ask = float(market["no_ask_dollars"])
        except (KeyError, TypeError, ValueError):
            continue
        market_mid = max(0.0, min(1.0, (yes_bid + yes_ask) / 2))
        row = {"ticker": ticker, "bucket": list(market["weather_bucket"]),
               "model_probabilities": probabilities, "consensus_probability": consensus,
               "market_mid_probability": market_mid, "yes_ask": yes_ask, "no_ask": no_ask}
        choices = []
        for side, ask, probability, size_key in (
            ("yes", yes_ask, consensus, "yes_ask_size_fp"),
            ("no", no_ask, 1 - consensus, "no_ask_size_fp"),
        ):
            try:
                depth = float(market.get(size_key) or 0)
            except (TypeError, ValueError):
                depth = 0
            per_model = [value if side == "yes" else 1 - value for value in probabilities.values()]
            raw_edge = probability - ask
            if (all(value > ask for value in per_model)
                    and max(per_model) - min(per_model) <= maximum_model_disagreement
                    and .10 <= ask <= .90 and depth >= 1
                    and raw_edge >= minimum_raw_edge):
                choices.append({"ticker": ticker, "side": side, "ask": ask,
                                "displayed_ask_depth": depth, "model_probability": probability,
                                "raw_edge": raw_edge,
                                "stressed_edge": raw_edge - fee_and_model_stress,
                                "model_side_probabilities": per_model})
        row["eligible_sides"] = choices
        contracts.append(row)
    market_total = sum(row["market_mid_probability"] for row in contracts)
    if market_total > 0:
        for row in contracts:
            row["normalized_market_probability"] = row["market_mid_probability"] / market_total
    candidates = [candidate for row in contracts for candidate in row["eligible_sides"]]
    candidates.sort(key=lambda row: (-row["stressed_edge"], row["ticker"], row["side"]))
    result.update({"classification": "signal" if candidates else "no_qualified_edge",
                   "models": model_rows, "contracts": contracts,
                   "signal": candidates[0] if candidates else None,
                   "minimum_raw_edge": minimum_raw_edge,
                   "fee_and_model_stress": fee_and_model_stress,
                   "maximum_model_disagreement": maximum_model_disagreement,
                   "maximum_model_mean_difference_f": maximum_model_mean_difference_f})
    return result


def fetch_ensemble(spec: WeatherSeries, start: date, end: date, *, opener=urlopen) -> tuple[dict, dict]:
    params = {"latitude": spec.latitude, "longitude": spec.longitude,
              "daily": "temperature_2m_max,temperature_2m_min",
              "temperature_unit": "fahrenheit", "timezone": spec.timezone,
              "start_date": start.isoformat(), "end_date": end.isoformat(),
              "models": MODEL_REQUEST}
    url = ENSEMBLE_URL + "?" + urlencode(params)
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "OpportunityLab/1.0 (research)"})
    try:
        with opener(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
            status = getattr(response, "status", 200)
    except HTTPError as exc:
        return {}, {"error": f"open_meteo_http_{exc.code}", "status_code": exc.code}
    except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        return {}, {"error": f"open_meteo_transport_error:{type(exc).__name__}"}
    daily = payload.get("daily") or {}; dates = daily.get("time") or []
    output = {}
    for model, suffix in MODEL_SUFFIXES.items():
        for extreme, variable in (("high", "temperature_2m_max"), ("low", "temperature_2m_min")):
            keys = [key for key in daily if key.startswith(variable) and key.endswith(suffix)]
            for key in keys:
                for index, value in enumerate(daily.get(key) or []):
                    if index >= len(dates) or value is None:
                        continue
                    row = output.setdefault(dates[index], {}).setdefault(model, {"high": [], "low": []})
                    row[extreme].append(float(value))
    for day in output.values():
        for model, row in day.items():
            encoded = json.dumps({"high": row["high"], "low": row["low"]}, separators=(",", ":"))
            row["forecast_hash"] = hashlib.sha256(encoded.encode()).hexdigest()
    return output, {"status_code": status, "provider": "open_meteo",
                    "models": list(MODEL_SUFFIXES), "station": spec.station,
                    "start_date": start.isoformat(), "end_date": end.isoformat()}


def score_resolution(observation: dict, expiration_value: float) -> dict:
    contracts = list(observation.get("contracts") or [])
    if not contracts:
        raise ValueError("missing_contracts")
    outcomes = [int(float(row["bucket"][0]) <= expiration_value < float(row["bucket"][1]))
                for row in contracts]
    if sum(outcomes) != 1:
        raise ValueError("nonunique_winning_bucket")
    model = [float(row["consensus_probability"]) for row in contracts]
    market = [float(row.get("normalized_market_probability") or 0) for row in contracts]
    brier = lambda values: sum((probability - outcome) ** 2
                               for probability, outcome in zip(values, outcomes)) / len(outcomes)
    def rps(values):
        return sum((sum(values[:index + 1]) - sum(outcomes[:index + 1])) ** 2
                   for index in range(len(values) - 1)) / max(1, len(values) - 1)
    signal = observation.get("signal")
    net = None
    if signal:
        ticker_index = next(index for index, row in enumerate(contracts)
                            if row["ticker"] == signal["ticker"])
        won = bool(outcomes[ticker_index]) == (signal["side"] == "yes")
        net = (1.0 if won else 0.0) - float(signal["ask"]) - float(observation["fee_and_model_stress"])
    return {"expiration_value": expiration_value, "model_brier": brier(model),
            "market_brier": brier(market), "brier_delta_vs_market": brier(model) - brier(market),
            "model_rps": rps(model), "market_rps": rps(market),
            "rps_delta_vs_market": rps(model) - rps(market),
            "cost_stressed_net_dollars": net, "hypothetical_only": True}
