"""Research-only complementary signal sleeves; none are wired to execution."""

from __future__ import annotations

from typing import Any

from regime_intraday import RegimeIntradayConfig, _trade_plan, evaluate_regime_intraday


def _result(base: dict[str, Any], signals: list[dict], name: str) -> dict[str, Any]:
    return {**base, "mode": f"research_only_{name}", "signals": signals, "signal_count": len(signals), "live_submission": False}


def trend_pullback_candidate(bars_by_symbol: dict[str, list[dict]], config: RegimeIntradayConfig) -> dict[str, Any]:
    base = evaluate_regime_intraday(bars_by_symbol, config)
    regime, features = dict(base.get("regime") or {}), dict(base.get("features") or {})
    signals = []
    if regime.get("name") == "trend":
        bullish = regime.get("direction") == "bullish"
        for symbol in config.trade_symbols:
            feature = dict(features.get(symbol) or {})
            if not feature.get("ready"):
                continue
            distance = float(feature.get("vwap_distance_atr") or 0)
            touched_fast = float(feature.get("last_low") or 0) <= float(feature.get("ema_fast") or 0) if bullish else float(feature.get("last_high") or 0) >= float(feature.get("ema_fast") or 0)
            reversal = float(feature.get("price") or 0) > float(feature.get("last_open") or 0) if bullish else float(feature.get("price") or 0) < float(feature.get("last_open") or 0)
            aligned_near_vwap = 0 <= distance <= 0.9 if bullish else -0.9 <= distance <= 0
            if touched_fast and reversal and aligned_near_vwap:
                signals.append(_trade_plan(symbol, "trend_pullback", "buy" if bullish else "sell", feature, config))
    return _result(base, signals, "trend_pullback")


def failed_breakout_fade_candidate(bars_by_symbol: dict[str, list[dict]], config: RegimeIntradayConfig) -> dict[str, Any]:
    base = evaluate_regime_intraday(bars_by_symbol, config)
    regime, features = dict(base.get("regime") or {}), dict(base.get("features") or {})
    signals = []
    if regime.get("name") in {"range", "transition"}:
        for symbol in config.trade_symbols:
            feature = dict(features.get(symbol) or {})
            closes = list(feature.get("recent_closes") or [])
            if not feature.get("ready") or len(closes) < 2:
                continue
            high, low, price = float(feature.get("opening_high") or 0), float(feature.get("opening_low") or 0), float(feature.get("price") or 0)
            failed_high = closes[-2] > high and price < high and price < float(feature.get("last_open") or 0)
            failed_low = closes[-2] < low and price > low and price > float(feature.get("last_open") or 0)
            if failed_high or failed_low:
                signals.append(_trade_plan(symbol, "failed_breakout_fade", "sell" if failed_high else "buy", feature, config))
    return _result(base, signals, "failed_breakout_fade")


def relative_strength_divergence_candidate(bars_by_symbol: dict[str, list[dict]], config: RegimeIntradayConfig) -> dict[str, Any]:
    base = evaluate_regime_intraday(bars_by_symbol, config)
    regime, features = dict(base.get("regime") or {}), dict(base.get("features") or {})
    spy, qqq = dict(features.get("SPY") or {}), dict(features.get("QQQ") or {})
    signals = []
    if regime.get("name") in {"range", "transition"} and spy.get("ready") and qqq.get("ready"):
        divergence = float(spy.get("vwap_distance_atr") or 0) - float(qqq.get("vwap_distance_atr") or 0)
        if abs(divergence) >= 1.0:
            symbol = "SPY" if "SPY" in config.trade_symbols else next(iter(config.trade_symbols), "")
            feature = dict(features.get(symbol) or {})
            if symbol and feature:
                signals.append(_trade_plan(symbol, "relative_strength_divergence", "sell" if divergence > 0 else "buy", feature, config))
    return _result(base, signals, "relative_strength_divergence")
