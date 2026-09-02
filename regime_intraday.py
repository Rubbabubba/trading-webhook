"""Pure SPY/QQQ regime-routed intraday signal engine.

This module deliberately contains no broker or FastAPI dependencies.  It turns
one-minute bars into auditable underlying signals and an *option intent*; it
never selects a contract or submits an order.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
from math import sqrt
from statistics import fmean, pstdev
from typing import Any


REGIME_INTRADAY_VERSION = "v1-spy-qqq-orb-vwap-router"


@dataclass(frozen=True)
class RegimeIntradayConfig:
    symbols: tuple[str, ...] = ("SPY", "QQQ")
    trade_symbols: tuple[str, ...] = ("SPY", "QQQ")
    momentum_enabled: bool = True
    mean_reversion_enabled: bool = True
    opening_range_minutes: int = 30
    min_bars: int = 40
    momentum_volume_ratio: float = 1.20
    momentum_break_buffer_pct: float = 0.0003
    momentum_max_vwap_extension_pct: float = 0.008
    trend_efficiency_min: float = 0.34
    range_efficiency_max: float = 0.24
    mean_reversion_min_vwap_atr: float = 1.0
    mean_reversion_max_vwap_atr: float = 2.75
    stop_atr: float = 0.75
    target_r: float = 2.0
    option_min_dte: int = 7
    option_max_dte: int = 21
    option_target_delta_low: float = 0.55
    option_target_delta_high: float = 0.70
    option_max_spread_pct: float = 0.08


def _num(row: dict, key: str, fallback: str | None = None) -> float:
    value = row.get(key)
    if value is None and fallback:
        value = row.get(fallback)
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _time_key(row: dict) -> datetime:
    value = row.get("ts_ny") or row.get("timestamp") or row.get("ts_utc")
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (float(length) + 1.0)
    out = values[0]
    for value in values[1:]:
        out = alpha * value + (1.0 - alpha) * out
    return out


def _session_vwap(bars: list[dict]) -> float:
    volume = sum(max(0.0, _num(row, "volume", "v")) for row in bars)
    if volume <= 0:
        return _num(bars[-1], "close", "c") if bars else 0.0
    dollars = 0.0
    for row in bars:
        vol = max(0.0, _num(row, "volume", "v"))
        typical = (
            _num(row, "high", "h")
            + _num(row, "low", "l")
            + _num(row, "close", "c")
        ) / 3.0
        dollars += typical * vol
    return dollars / volume


def _atr(bars: list[dict], length: int = 20) -> float:
    rows = bars[-max(2, length + 1) :]
    ranges: list[float] = []
    for index in range(1, len(rows)):
        high = _num(rows[index], "high", "h")
        low = _num(rows[index], "low", "l")
        prior = _num(rows[index - 1], "close", "c")
        ranges.append(max(high - low, abs(high - prior), abs(low - prior)))
    return fmean(ranges) if ranges else 0.0


def _efficiency(closes: list[float], length: int = 20) -> float:
    values = closes[-max(2, length + 1) :]
    if len(values) < 2:
        return 0.0
    path = sum(abs(values[i] - values[i - 1]) for i in range(1, len(values)))
    return abs(values[-1] - values[0]) / path if path > 0 else 0.0


def _market_features(symbol: str, bars: list[dict], config: RegimeIntradayConfig) -> dict:
    ordered = sorted((dict(row) for row in bars if isinstance(row, dict)), key=_time_key)
    closes = [_num(row, "close", "c") for row in ordered]
    opening = ordered[: max(1, config.opening_range_minutes)]
    recent_vol = [_num(row, "volume", "v") for row in ordered]
    baseline = recent_vol[-23:-3]
    recent = recent_vol[-3:]
    volume_ratio = (fmean(recent) / fmean(baseline)) if recent and baseline and fmean(baseline) > 0 else 0.0
    vwap = _session_vwap(ordered)
    price = closes[-1] if closes else 0.0
    atr = _atr(ordered)
    return {
        "symbol": symbol,
        "bars": len(ordered),
        "price": price,
        "prior_close": closes[-2] if len(closes) > 1 else price,
        "recent_closes": closes[-5:],
        "vwap": vwap,
        "vwap_distance_pct": ((price / vwap) - 1.0) if vwap > 0 else 0.0,
        "vwap_distance_atr": ((price - vwap) / atr) if atr > 0 else 0.0,
        "ema_fast": _ema(closes[-40:], 9),
        "ema_slow": _ema(closes[-60:], 20),
        "efficiency": _efficiency(closes),
        "atr_1m": atr,
        "opening_high": max((_num(row, "high", "h") for row in opening), default=0.0),
        "opening_low": min((_num(row, "low", "l") for row in opening), default=0.0),
        "volume_ratio": volume_ratio,
        "last_open": _num(ordered[-1], "open", "o") if ordered else 0.0,
        "last_high": _num(ordered[-1], "high", "h") if ordered else 0.0,
        "last_low": _num(ordered[-1], "low", "l") if ordered else 0.0,
        "last_ts": _time_key(ordered[-1]).isoformat() if ordered else None,
        "ready": len(ordered) >= config.min_bars and price > 0 and vwap > 0 and atr > 0,
    }


def classify_regime(features: dict[str, dict], config: RegimeIntradayConfig) -> dict:
    ready = [features[s] for s in config.symbols if s in features and features[s].get("ready")]
    if len(ready) != len(config.symbols):
        return {"name": "not_ready", "direction": None, "trade_allowed": False, "reason": "both_symbols_not_ready"}
    bullish = all(r["price"] > r["vwap"] and r["ema_fast"] > r["ema_slow"] for r in ready)
    bearish = all(r["price"] < r["vwap"] and r["ema_fast"] < r["ema_slow"] for r in ready)
    avg_efficiency = fmean(r["efficiency"] for r in ready)
    if avg_efficiency >= config.trend_efficiency_min and (bullish or bearish):
        return {
            "name": "trend",
            "direction": "bullish" if bullish else "bearish",
            "trade_allowed": True,
            "reason": "spy_qqq_vwap_ema_alignment",
            "average_efficiency": round(avg_efficiency, 4),
        }
    if avg_efficiency <= config.range_efficiency_max and not (bullish or bearish):
        return {"name": "range", "direction": None, "trade_allowed": True, "reason": "low_efficiency_mixed_alignment", "average_efficiency": round(avg_efficiency, 4)}
    return {"name": "transition", "direction": None, "trade_allowed": False, "reason": "ambiguous_regime", "average_efficiency": round(avg_efficiency, 4)}


def _trade_plan(symbol: str, strategy: str, side: str, feature: dict, config: RegimeIntradayConfig) -> dict:
    entry = float(feature["price"])
    atr = float(feature["atr_1m"])
    if side == "buy":
        stop = entry - config.stop_atr * atr
        target = entry + config.target_r * (entry - stop)
        option_type = "call"
    else:
        stop = entry + config.stop_atr * atr
        target = entry - config.target_r * (stop - entry)
        option_type = "put"
    session = str(feature.get("last_ts") or "")[:10]
    identity = "|".join([session, symbol, strategy, side, f"{float(feature.get('opening_high') or 0):.4f}", f"{float(feature.get('opening_low') or 0):.4f}"])
    signal_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return {
        "signal_id": signal_id,
        "symbol": symbol,
        "strategy": strategy,
        "underlying_side": side,
        "entry_price": round(entry, 4),
        "stop_price": round(stop, 4),
        "target_price": round(target, 4),
        "target_r": config.target_r,
        "option_intent": {
            "underlying": symbol,
            "underlying_price": round(entry, 4),
            "option_type": option_type,
            "position": "defined_risk_debit",
            "min_dte": config.option_min_dte,
            "max_dte": config.option_max_dte,
            "target_delta_range": [config.option_target_delta_low, config.option_target_delta_high],
            "max_bid_ask_spread_pct": config.option_max_spread_pct,
            "limit_orders_only": True,
            "live_submission": False,
        },
    }


def evaluate_regime_intraday(bars_by_symbol: dict[str, list[dict]], config: RegimeIntradayConfig | None = None) -> dict[str, Any]:
    cfg = config or RegimeIntradayConfig()
    features = {symbol: _market_features(symbol, list(bars_by_symbol.get(symbol) or []), cfg) for symbol in cfg.symbols}
    regime = classify_regime(features, cfg)
    signals: list[dict] = []
    if regime["name"] == "trend" and cfg.momentum_enabled:
        direction = regime["direction"]
        for symbol, feature in features.items():
            if symbol not in cfg.trade_symbols or not feature["ready"]:
                continue
            bullish = direction == "bullish"
            level = feature["opening_high"] * (1.0 + cfg.momentum_break_buffer_pct) if bullish else feature["opening_low"] * (1.0 - cfg.momentum_break_buffer_pct)
            recent_closes = list(feature.get("recent_closes") or [])
            crossed = any(
                (recent_closes[i] >= level and recent_closes[i - 1] < level)
                if bullish
                else (recent_closes[i] <= level and recent_closes[i - 1] > level)
                for i in range(1, len(recent_closes))
            )
            extension_ok = abs(feature["vwap_distance_pct"]) <= cfg.momentum_max_vwap_extension_pct
            if crossed and extension_ok and feature["volume_ratio"] >= cfg.momentum_volume_ratio:
                signals.append(_trade_plan(symbol, "opening_range_momentum", "buy" if bullish else "sell", feature, cfg))
    elif regime["name"] == "range" and cfg.mean_reversion_enabled:
        for symbol, feature in features.items():
            if symbol not in cfg.trade_symbols:
                continue
            distance = float(feature["vwap_distance_atr"])
            stretched = cfg.mean_reversion_min_vwap_atr <= abs(distance) <= cfg.mean_reversion_max_vwap_atr
            bullish_reversal = distance < 0 and feature["price"] > feature["last_open"] and feature["price"] > feature["prior_close"]
            bearish_reversal = distance > 0 and feature["price"] < feature["last_open"] and feature["price"] < feature["prior_close"]
            if stretched and (bullish_reversal or bearish_reversal):
                plan = _trade_plan(symbol, "vwap_mean_reversion", "buy" if bullish_reversal else "sell", feature, cfg)
                plan["target_price"] = round(float(feature["vwap"]), 4)
                signals.append(plan)
    return {
        "ok": True,
        "version": REGIME_INTRADAY_VERSION,
        "mode": "shadow_underlying_signal_with_option_intent",
        "live_submission": False,
        "regime": regime,
        "signals": signals,
        "signal_count": len(signals),
        "features": features,
        "config": asdict(cfg),
    }
