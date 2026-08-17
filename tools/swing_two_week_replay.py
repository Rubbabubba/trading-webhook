#!/usr/bin/env python3
"""
Offline two-week swing replay harness.

Read-only:
- Fetches historical Alpaca daily bars.
- Simulates the current swing config against recent completed sessions.
- Writes JSON/CSV reports under TradingDiagnostics.
- Does not import app.py.
- Does not submit broker orders.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
from dataclasses import dataclass, asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame

    try:
        from alpaca.data.enums import Adjustment as AlpacaAdjustment
        from alpaca.data.enums import DataFeed as AlpacaDataFeed
    except Exception:
        AlpacaAdjustment = None
        AlpacaDataFeed = None
except Exception as exc:
    raise SystemExit(
        "Missing alpaca-py. Install requirements first: pip install -r requirements.txt"
    ) from exc


PATCH_VERSION = "patch-448-swing-two-week-scenario-matrix-goal-calibration"
NY_TZ = "America/New_York"

DEFAULT_SYMBOLS = (
    "SPY,QQQ,IWM,AAPL,MSFT,AMZN,GOOGL,META,NVDA,AMD,AVGO,TSLA,CRM,ORCL,"
    "ADBE,NOW,PLTR,NET,CRWD,PANW,SNOW,SHOP,SQ,UBER,COIN,SMCI,MU,INTC,"
    "NFLX,AMAT,ARM,MRVL,VRT,ANET,APP,ZS,DDOG,LRCX,KLAC,SPCX,MA"
)


def getenv_any(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def getenv_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def getenv_int(name: str, default: int) -> int:
    try:
        return int(float(str(os.getenv(name, default)).strip()))
    except Exception:
        return default


def getenv_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def normalize_symbols(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in str(raw or "").split(","):
        sym = part.strip().upper()
        if not sym or sym in seen:
            continue
        out.append(sym)
        seen.add(sym)
    return out


def pct_env(name: str, default: float) -> float:
    value = getenv_float(name, default)
    if abs(value) > 1.5:
        return value / 100.0
    return value


def sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    return sum(values[-length:]) / float(length)


def atr(highs: list[float], lows: list[float], closes: list[float], length: int = 14) -> float | None:
    if len(closes) < length + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(closes)):
        trs.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        )
    if len(trs) < length:
        return None
    return sum(trs[-length:]) / float(length)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


@dataclass
class ReplayConfig:
    symbols: list[str]
    test_days: int
    warmup_days: int
    risk_per_trade_dollars: float
    max_open_positions: int
    max_entries_per_day: int
    min_price: float
    min_avg_dollar_volume: float
    fast_ma_days: int
    slow_ma_days: int
    breakout_lookback_days: int
    breakout_buffer_pct: float
    target_r_mult: float
    production_min_rank_score: float
    production_max_below_breakout_pct: float
    production_max_above_breakout_pct: float
    production_min_close_to_high_pct: float
    production_min_return_20d_pct: float
    production_max_risk_per_share_pct: float
    mean_reversion_enabled: bool
    mean_reversion_only_weak_tape: bool
    mean_reversion_risk_multiplier: float
    mean_reversion_target_pct: float
    mean_reversion_stop_pct: float
    stall_exit_days: int
    stall_exit_min_r: float
    allow_same_day_exits: bool
    same_bar_policy: str
    data_feed: str
    data_adjustment: str


@dataclass
class SimPosition:
    symbol: str
    strategy: str
    entry_date: str
    entry_price: float
    qty: float
    stop_price: float
    target_price: float
    risk_per_share: float
    rank_score: float
    reasons: list[str]
    max_unrealized_r: float = 0.0
    min_unrealized_r: float = 0.0
    holding_days: int = 0


def build_config(args: argparse.Namespace) -> ReplayConfig:
    symbols_raw = (
        args.symbols
        or os.getenv("SWING_REPLAY_SYMBOLS")
        or os.getenv("SCANNER_UNIVERSE_SYMBOLS")
        or os.getenv("ALLOWED_SYMBOLS")
        or DEFAULT_SYMBOLS
    )

    return ReplayConfig(
        symbols=normalize_symbols(symbols_raw),
        test_days=int(args.days or getenv_int("SWING_REPLAY_TEST_DAYS", 10)),
        warmup_days=int(args.warmup_days or getenv_int("SWING_REPLAY_WARMUP_DAYS", 100)),
        risk_per_trade_dollars=getenv_float("SWING_RISK_PER_TRADE_DOLLARS", getenv_float("RISK_DOLLARS", 30.0)),
        max_open_positions=getenv_int("SWING_MAX_OPEN_POSITIONS", 12),
        max_entries_per_day=getenv_int(
            "SWING_PRODUCTION_RESET_MAX_ENTRIES_PER_SCAN",
            getenv_int("SCANNER_MAX_ENTRIES_PER_SCAN", 2),
        ),
        min_price=getenv_float("SWING_MIN_PRICE", 5.0),
        min_avg_dollar_volume=getenv_float("SWING_PRODUCTION_RESET_MIN_AVG_DOLLAR_VOLUME", 20_000_000.0),
        fast_ma_days=getenv_int("SWING_FAST_MA_DAYS", 10),
        slow_ma_days=getenv_int("SWING_SLOW_MA_DAYS", 30),
        breakout_lookback_days=getenv_int("SWING_BREAKOUT_LOOKBACK_DAYS", 20),
        breakout_buffer_pct=pct_env("SWING_BREAKOUT_BUFFER_PCT", 0.005),
        target_r_mult=getenv_float("SWING_TARGET_R_MULT", 2.0),
        production_min_rank_score=getenv_float("SWING_PRODUCTION_RESET_MIN_RANK_SCORE", 103.0),
        production_max_below_breakout_pct=pct_env("SWING_PRODUCTION_RESET_MAX_BELOW_BREAKOUT_PCT", 0.07),
        production_max_above_breakout_pct=pct_env("SWING_PRODUCTION_RESET_MAX_ABOVE_BREAKOUT_PCT", 0.15),
        production_min_close_to_high_pct=pct_env("SWING_PRODUCTION_RESET_MIN_CLOSE_TO_HIGH_PCT", 0.65),
        production_min_return_20d_pct=pct_env("SWING_PRODUCTION_RESET_MIN_RETURN_20D_PCT", -0.10),
        production_max_risk_per_share_pct=pct_env("SWING_PRODUCTION_RESET_MAX_RISK_PER_SHARE_PCT", 0.12),
        mean_reversion_enabled=getenv_bool("SWING_MEAN_REVERSION_ENABLED", True),
        mean_reversion_only_weak_tape=getenv_bool("SWING_MEAN_REVERSION_ONLY_WHEN_REGIME_UNFAVORABLE", True),
        mean_reversion_risk_multiplier=getenv_float("SWING_MEAN_REVERSION_RISK_MULTIPLIER", 0.5),
        mean_reversion_target_pct=pct_env("SWING_MEAN_REVERSION_TARGET_PCT", 0.03),
        mean_reversion_stop_pct=pct_env("SWING_MEAN_REVERSION_STOP_PCT", 0.02),
        stall_exit_days=getenv_int("SWING_STALL_EXIT_DAYS", 3),
        stall_exit_min_r=getenv_float("SWING_STALL_EXIT_MIN_R", 0.50),
        allow_same_day_exits=getenv_bool("SWING_REPLAY_ALLOW_SAME_DAY_EXITS", True),
        same_bar_policy=os.getenv("SWING_REPLAY_SAME_BAR_POLICY", "conservative_stop_first").strip().lower(),
        data_feed=os.getenv("DATA_FEED", "iex").strip().lower() or "iex",
        data_adjustment=os.getenv("DATA_ADJUSTMENT", "raw").strip().lower() or "raw",
    )


def alpaca_enum_feed(raw: str):
    if AlpacaDataFeed is None:
        return raw
    return getattr(AlpacaDataFeed, raw.upper(), AlpacaDataFeed.IEX)


def alpaca_enum_adjustment(raw: str):
    if AlpacaAdjustment is None:
        return raw
    mapping = {
        "raw": AlpacaAdjustment.RAW,
        "split": AlpacaAdjustment.SPLIT,
        "dividend": AlpacaAdjustment.DIVIDEND,
        "all": AlpacaAdjustment.ALL,
    }
    return mapping.get(raw.lower(), AlpacaAdjustment.RAW)


def fetch_daily_bars(config: ReplayConfig) -> dict[str, list[dict]]:
    key = getenv_any("APCA_API_KEY_ID", "ALPACA_KEY_ID", "ALPACA_API_KEY_ID")
    secret = getenv_any("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET_KEY")
    if not key or not secret:
        raise SystemExit("Missing Alpaca API keys in APCA_API_KEY_ID/APCA_API_SECRET_KEY or ALPACA equivalents.")

    client = StockHistoricalDataClient(key, secret)

    end = datetime.now(timezone.utc)
    fetch_days = max(45, config.warmup_days + config.test_days * 3 + 10)
    start = end - timedelta(days=fetch_days)

    req = StockBarsRequest(
        symbol_or_symbols=config.symbols,
        timeframe=TimeFrame.Day,
        start=start,
        end=end,
        adjustment=alpaca_enum_adjustment(config.data_adjustment),
        feed=alpaca_enum_feed(config.data_feed),
    )
    result = client.get_stock_bars(req)
    data = getattr(result, "data", {}) or {}

    out: dict[str, list[dict]] = {}
    for symbol in config.symbols:
        rows: list[dict] = []
        for bar in data.get(symbol, []) or []:
            ts = getattr(bar, "timestamp", None)
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            rows.append(
                {
                    "date": ts.date().isoformat(),
                    "ts_utc": ts.isoformat(),
                    "open": safe_float(getattr(bar, "open", 0.0)),
                    "high": safe_float(getattr(bar, "high", 0.0)),
                    "low": safe_float(getattr(bar, "low", 0.0)),
                    "close": safe_float(getattr(bar, "close", 0.0)),
                    "volume": safe_float(getattr(bar, "volume", 0.0)),
                    "vwap": safe_float(getattr(bar, "vwap", 0.0)),
                }
            )
        rows.sort(key=lambda r: r["date"])
        out[symbol] = rows
    return out


def bar_by_date(rows: list[dict]) -> dict[str, dict]:
    return {str(row.get("date")): row for row in rows or []}


def prior_rows(rows: list[dict], session_date: str) -> list[dict]:
    return [row for row in rows if str(row.get("date")) < session_date]


def evaluate_breakout(symbol: str, history: list[dict], config: ReplayConfig) -> dict:
    reasons: list[str] = []
    need = max(config.slow_ma_days + 5, config.breakout_lookback_days + 2, 25)
    if len(history) < need:
        return {"symbol": symbol, "strategy": "daily_breakout", "eligible": False, "reasons": ["insufficient_daily_bars"]}

    closes = [safe_float(r.get("close")) for r in history]
    highs = [safe_float(r.get("high")) for r in history]
    lows = [safe_float(r.get("low")) for r in history]
    vols = [safe_float(r.get("volume")) for r in history]

    close = closes[-1]
    high = highs[-1]
    low = lows[-1]
    fast = sma(closes, config.fast_ma_days)
    slow = sma(closes, config.slow_ma_days)
    atr_14 = atr(highs, lows, closes, 14) or max(close * 0.01, 0.01)
    atr_pct = atr_14 / max(close, 1e-9)

    avg_dollar_volume_20d = sum(closes[-20 + i] * vols[-20 + i] for i in range(20)) / 20.0
    return_20d = (close / closes[-21] - 1.0) if len(closes) >= 21 and closes[-21] > 0 else 0.0
    breakout_level = max(highs[-(config.breakout_lookback_days + 1) : -1])
    trailing_low = min(lows[-5:])
    close_to_high = close / max(high, 1e-9)
    breakout_distance = close / max(breakout_level, 1e-9) - 1.0
    stop_price = min(trailing_low, breakout_level * (1.0 - config.breakout_buffer_pct))
    risk_per_share = max(close - stop_price, close * 0.0025)
    risk_per_share_pct = risk_per_share / max(close, 1e-9)

    score = 0.0
    if close >= config.min_price:
        score += 10
    else:
        reasons.append("price_below_min")

    if avg_dollar_volume_20d >= config.min_avg_dollar_volume:
        score += min(20.0, avg_dollar_volume_20d / max(config.min_avg_dollar_volume, 1.0) * 10.0)
    else:
        reasons.append("avg_dollar_volume_below_min")

    if fast and slow and close > fast > slow:
        score += 25
    else:
        reasons.append("trend_filter_failed")

    if return_20d >= config.production_min_return_20d_pct:
        score += min(20.0, return_20d * 200.0)
    else:
        reasons.append("return_20d_below_min")

    if close_to_high >= config.production_min_close_to_high_pct:
        score += 12
    else:
        reasons.append("close_not_near_high")

    if breakout_distance >= -config.production_max_below_breakout_pct:
        score += 18
    else:
        reasons.append("too_far_below_breakout")

    if breakout_distance > config.production_max_above_breakout_pct:
        reasons.append("too_far_above_breakout")

    if risk_per_share_pct > config.production_max_risk_per_share_pct:
        reasons.append("risk_per_share_too_wide")

    score += max(0.0, min(10.0, ((high - low) / max(close, 1e-9)) * 100.0))
    score += min(5.0, atr_pct * 100.0)

    if score < config.production_min_rank_score:
        reasons.append("rank_score_below_min")

    qty = round(config.risk_per_trade_dollars / max(risk_per_share, 1e-9), 4)
    if qty <= 0:
        reasons.append("qty_zero")

    return {
        "symbol": symbol,
        "strategy": "daily_breakout",
        "eligible": not reasons,
        "reasons": sorted(set(reasons)),
        "rank_score": round(score, 4),
        "signal_close": round(close, 4),
        "stop_price": round(stop_price, 4),
        "risk_per_share": round(risk_per_share, 4),
        "qty": qty,
        "target_r": config.target_r_mult,
        "breakout_level": round(breakout_level, 4),
        "breakout_distance_pct": round(breakout_distance * 100.0, 4),
        "close_to_high_pct": round(close_to_high * 100.0, 4),
        "return_20d_pct": round(return_20d * 100.0, 4),
        "risk_per_share_pct": round(risk_per_share_pct * 100.0, 4),
        "avg_dollar_volume_20d": round(avg_dollar_volume_20d, 2),
    }


def evaluate_mean_reversion(symbol: str, history: list[dict], config: ReplayConfig, weak_tape: bool) -> dict:
    if not config.mean_reversion_enabled:
        return {"symbol": symbol, "strategy": "daily_mean_reversion", "eligible": False, "reasons": ["mean_reversion_disabled"]}

    if config.mean_reversion_only_weak_tape and not weak_tape:
        return {"symbol": symbol, "strategy": "daily_mean_reversion", "eligible": False, "reasons": ["regime_not_unfavorable"]}

    need = max(config.slow_ma_days + 5, 25)
    if len(history) < need:
        return {"symbol": symbol, "strategy": "daily_mean_reversion", "eligible": False, "reasons": ["insufficient_daily_bars"]}

    closes = [safe_float(r.get("close")) for r in history]
    highs = [safe_float(r.get("high")) for r in history]
    lows = [safe_float(r.get("low")) for r in history]
    vols = [safe_float(r.get("volume")) for r in history]

    close = closes[-1]
    fast = sma(closes, config.fast_ma_days)
    slow = sma(closes, config.slow_ma_days)
    avg_dollar_volume_20d = sum(closes[-20 + i] * vols[-20 + i] for i in range(20)) / 20.0

    reasons: list[str] = []
    if close < config.min_price:
        reasons.append("price_below_min")
    if avg_dollar_volume_20d < config.min_avg_dollar_volume:
        reasons.append("avg_dollar_volume_below_min")
    if not fast or close > fast * 0.985:
        reasons.append("not_pulled_back_enough")
    if not slow or close < slow * 0.90:
        reasons.append("mean_reversion_too_deep")

    stop_price = close * (1.0 - config.mean_reversion_stop_pct)
    risk_per_share = max(close - stop_price, close * 0.0025)
    qty = round((config.risk_per_trade_dollars * config.mean_reversion_risk_multiplier) / max(risk_per_share, 1e-9), 4)

    return {
        "symbol": symbol,
        "strategy": "daily_mean_reversion",
        "eligible": not reasons,
        "reasons": sorted(set(reasons)),
        "rank_score": 75.0 if not reasons else 0.0,
        "signal_close": round(close, 4),
        "stop_price": round(stop_price, 4),
        "risk_per_share": round(risk_per_share, 4),
        "qty": qty,
        "target_r": config.mean_reversion_target_pct / max(config.mean_reversion_stop_pct, 1e-9),
        "avg_dollar_volume_20d": round(avg_dollar_volume_20d, 2),
    }


def regime_weak(spy_history: list[dict], config: ReplayConfig) -> bool:
    closes = [safe_float(r.get("close")) for r in spy_history]
    fast = sma(closes, config.fast_ma_days)
    slow = sma(closes, config.slow_ma_days)
    if not closes or fast is None or slow is None:
        return True
    return not (closes[-1] > fast > slow)


def simulate_position_exit(position: SimPosition, day_bar: dict, config: ReplayConfig) -> tuple[dict | None, SimPosition]:
    open_px = safe_float(day_bar.get("open"))
    high = safe_float(day_bar.get("high"))
    low = safe_float(day_bar.get("low"))
    close = safe_float(day_bar.get("close"))
    date = str(day_bar.get("date"))

    position.holding_days += 1
    high_r = (high - position.entry_price) / max(position.risk_per_share, 1e-9)
    low_r = (low - position.entry_price) / max(position.risk_per_share, 1e-9)
    close_r = (close - position.entry_price) / max(position.risk_per_share, 1e-9)
    position.max_unrealized_r = max(position.max_unrealized_r, high_r)
    position.min_unrealized_r = min(position.min_unrealized_r, low_r)

    hit_stop = low <= position.stop_price
    hit_target = high >= position.target_price

    exit_reason = None
    exit_price = None

    if hit_stop and hit_target:
        if config.same_bar_policy == "optimistic_target_first":
            exit_reason = "target_same_bar"
            exit_price = position.target_price
        else:
            exit_reason = "stop_same_bar_conservative"
            exit_price = position.stop_price
    elif hit_stop:
        exit_reason = "stop"
        exit_price = position.stop_price
    elif hit_target:
        exit_reason = "target"
        exit_price = position.target_price
    elif config.stall_exit_days > 0 and position.holding_days >= config.stall_exit_days and close_r < config.stall_exit_min_r:
        exit_reason = "stall_exit"
        exit_price = close

    if exit_reason is None:
        return None, position

    pnl = (float(exit_price) - position.entry_price) * position.qty
    r_mult = pnl / max(position.risk_per_share * position.qty, 1e-9)

    return {
        "symbol": position.symbol,
        "strategy": position.strategy,
        "entry_date": position.entry_date,
        "exit_date": date,
        "entry_price": round(position.entry_price, 4),
        "exit_price": round(float(exit_price), 4),
        "qty": round(position.qty, 4),
        "pnl": round(pnl, 2),
        "r_mult": round(r_mult, 4),
        "exit_reason": exit_reason,
        "holding_days": position.holding_days,
        "max_unrealized_r": round(position.max_unrealized_r, 4),
        "min_unrealized_r": round(position.min_unrealized_r, 4),
        "rank_score": position.rank_score,
        "entry_reasons": ",".join(position.reasons),
    }, position


def run_replay(config: ReplayConfig, bars_map: dict[str, list[dict]]) -> dict:
    all_dates = sorted({row["date"] for rows in bars_map.values() for row in rows})
    test_dates = all_dates[-config.test_days :]
    by_symbol_date = {sym: bar_by_date(rows) for sym, rows in bars_map.items()}

    open_positions: list[SimPosition] = []
    trades: list[dict] = []
    daily_rows: list[dict] = []
    candidate_rows: list[dict] = []
    missed_rows: list[dict] = []

    for session_date in test_dates:
        realized_today = 0.0
        exited_indices: set[int] = set()

        for idx, pos in enumerate(open_positions):
            bar = by_symbol_date.get(pos.symbol, {}).get(session_date)
            if not bar:
                continue
            trade, _ = simulate_position_exit(pos, bar, config)
            if trade:
                trades.append(trade)
                realized_today += safe_float(trade.get("pnl"))
                exited_indices.add(idx)

        open_positions = [pos for idx, pos in enumerate(open_positions) if idx not in exited_indices]

        spy_history = prior_rows(bars_map.get("SPY", []), session_date)
        weak_tape = regime_weak(spy_history, config)

        candidates: list[dict] = []
        for symbol in config.symbols:
            if any(p.symbol == symbol for p in open_positions):
                continue

            history = prior_rows(bars_map.get(symbol, []), session_date)
            day_bar = by_symbol_date.get(symbol, {}).get(session_date)
            if not day_bar:
                continue

            breakout = evaluate_breakout(symbol, history, config)
            mean_rev = evaluate_mean_reversion(symbol, history, config, weak_tape)

            for row in [breakout, mean_rev]:
                row["session_date"] = session_date
                candidate_rows.append(row)

            candidates.extend([breakout, mean_rev])

        eligible = [c for c in candidates if c.get("eligible")]
        eligible.sort(key=lambda r: (safe_float(r.get("rank_score")), r.get("strategy") == "daily_mean_reversion"), reverse=True)

        slots = max(0, min(config.max_entries_per_day, config.max_open_positions - len(open_positions)))
        selected = eligible[:slots]

        for row in eligible[slots:]:
            missed = dict(row)
            missed["missed_reason"] = "capacity_or_daily_entry_slot"
            missed_rows.append(missed)

        for candidate in selected:
            symbol = str(candidate["symbol"])
            day_bar = by_symbol_date.get(symbol, {}).get(session_date)
            if not day_bar:
                continue

            entry_price = safe_float(day_bar.get("open") or candidate.get("signal_close"))
            stop_price = safe_float(candidate.get("stop_price"))
            risk_per_share = max(entry_price - stop_price, entry_price * 0.0025)
            qty = round(config.risk_per_trade_dollars / max(risk_per_share, 1e-9), 4)
            if candidate.get("strategy") == "daily_mean_reversion":
                qty = round(qty * config.mean_reversion_risk_multiplier, 4)
                target_price = entry_price * (1.0 + config.mean_reversion_target_pct)
            else:
                target_price = entry_price + risk_per_share * config.target_r_mult

            pos = SimPosition(
                symbol=symbol,
                strategy=str(candidate.get("strategy")),
                entry_date=session_date,
                entry_price=entry_price,
                qty=max(qty, 0.0),
                stop_price=stop_price,
                target_price=target_price,
                risk_per_share=risk_per_share,
                rank_score=safe_float(candidate.get("rank_score")),
                reasons=list(candidate.get("reasons") or []),
            )

            if pos.qty <= 0:
                continue

            if config.allow_same_day_exits:
                trade, updated = simulate_position_exit(pos, day_bar, config)
                if trade:
                    trades.append(trade)
                    realized_today += safe_float(trade.get("pnl"))
                else:
                    open_positions.append(updated)
            else:
                open_positions.append(pos)

        unrealized_today = 0.0
        for pos in open_positions:
            bar = by_symbol_date.get(pos.symbol, {}).get(session_date)
            if bar:
                unrealized_today += (safe_float(bar.get("close")) - pos.entry_price) * pos.qty

        daily_rows.append(
            {
                "date": session_date,
                "realized_pnl": round(realized_today, 2),
                "open_positions": len(open_positions),
                "unrealized_pnl_mark": round(unrealized_today, 2),
                "eligible_count": len(eligible),
                "selected_count": len(selected),
                "selected_symbols": ",".join([str(r.get("symbol")) for r in selected]),
            }
        )

    final_date = test_dates[-1] if test_dates else None
    for pos in open_positions:
        if not final_date:
            continue
        bar = by_symbol_date.get(pos.symbol, {}).get(final_date)
        if not bar:
            continue
        exit_price = safe_float(bar.get("close"))
        pnl = (exit_price - pos.entry_price) * pos.qty
        trades.append(
            {
                "symbol": pos.symbol,
                "strategy": pos.strategy,
                "entry_date": pos.entry_date,
                "exit_date": final_date,
                "entry_price": round(pos.entry_price, 4),
                "exit_price": round(exit_price, 4),
                "qty": round(pos.qty, 4),
                "pnl": round(pnl, 2),
                "r_mult": round(pnl / max(pos.risk_per_share * pos.qty, 1e-9), 4),
                "exit_reason": "final_mark_to_close",
                "holding_days": pos.holding_days,
                "max_unrealized_r": round(pos.max_unrealized_r, 4),
                "min_unrealized_r": round(pos.min_unrealized_r, 4),
                "rank_score": pos.rank_score,
                "entry_reasons": ",".join(pos.reasons),
            }
        )

    return {
        "test_dates": test_dates,
        "trades": trades,
        "daily_rows": daily_rows,
        "candidate_rows": candidate_rows,
        "missed_rows": missed_rows,
    }


def summarize(rows: list[dict], daily_rows: list[dict]) -> dict:
    pnl_values = [safe_float(r.get("pnl")) for r in rows]
    wins = [p for p in pnl_values if p > 0]
    losses = [p for p in pnl_values if p < 0]
    r_values = [safe_float(r.get("r_mult")) for r in rows if r.get("r_mult") is not None]

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for day in daily_rows:
        equity += safe_float(day.get("realized_pnl"))
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)

    by_strategy: dict[str, dict] = {}
    by_symbol: dict[str, dict] = {}
    by_exit: dict[str, dict] = {}

    def add_bucket(target: dict[str, dict], key: str, pnl: float, r_mult: float) -> None:
        bucket = target.setdefault(key, {"count": 0, "wins": 0, "losses": 0, "pnl": 0.0, "r_values": []})
        bucket["count"] += 1
        bucket["wins"] += int(pnl > 0)
        bucket["losses"] += int(pnl < 0)
        bucket["pnl"] += pnl
        bucket["r_values"].append(r_mult)

    for row in rows:
        pnl = safe_float(row.get("pnl"))
        r_mult = safe_float(row.get("r_mult"))
        add_bucket(by_strategy, str(row.get("strategy") or "unknown"), pnl, r_mult)
        add_bucket(by_symbol, str(row.get("symbol") or "unknown"), pnl, r_mult)
        add_bucket(by_exit, str(row.get("exit_reason") or "unknown"), pnl, r_mult)

    def finalize(bucket_map: dict[str, dict]) -> list[dict]:
        out = []
        for key, value in bucket_map.items():
            count = int(value["count"])
            out.append(
                {
                    "key": key,
                    "count": count,
                    "wins": int(value["wins"]),
                    "losses": int(value["losses"]),
                    "win_rate": round((value["wins"] / count) if count else 0.0, 4),
                    "pnl": round(value["pnl"], 2),
                    "avg_r": round(statistics.mean(value["r_values"]), 4) if value["r_values"] else 0.0,
                }
            )
        out.sort(key=lambda r: safe_float(r["pnl"]), reverse=True)
        return out

    return {
        "trade_count": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round((len(wins) / len(rows)) if rows else 0.0, 4),
        "gross_pnl": round(sum(pnl_values), 2),
        "avg_trade_pnl": round(statistics.mean(pnl_values), 2) if pnl_values else 0.0,
        "avg_r": round(statistics.mean(r_values), 4) if r_values else 0.0,
        "max_drawdown": round(max_drawdown, 2),
        "daily_average_realized_pnl": round(
            statistics.mean([safe_float(d.get("realized_pnl")) for d in daily_rows]), 2
        )
        if daily_rows
        else 0.0,
        "by_strategy": finalize(by_strategy),
        "by_symbol": finalize(by_symbol),
        "by_exit_reason": finalize(by_exit),
    }


def parse_number_list(raw: str, defaults: list[float]) -> list[float]:
    if not str(raw or "").strip():
        return list(defaults)

    out: list[float] = []
    for part in str(raw or "").split(","):
        try:
            value = float(part.strip())
            if math.isfinite(value):
                out.append(value)
        except Exception:
            continue

    return out or list(defaults)


def parse_int_list(raw: str, defaults: list[int]) -> list[int]:
    return [int(v) for v in parse_number_list(raw, [float(v) for v in defaults])]


def scenario_name(config: ReplayConfig) -> str:
    return (
        f"risk_{int(config.risk_per_trade_dollars)}"
        f"_entries_{int(config.max_entries_per_day)}"
        f"_target_{str(config.target_r_mult).replace('.', 'p')}"
        f"_stall_{int(config.stall_exit_days)}"
    )


def goal_score(summary: dict, low_goal: float = 100.0, high_goal: float = 200.0) -> float:
    daily_avg = safe_float(summary.get("daily_average_realized_pnl"))
    drawdown = abs(safe_float(summary.get("max_drawdown")))
    trade_count = safe_float(summary.get("trade_count"))
    win_rate = safe_float(summary.get("win_rate"))
    avg_r = safe_float(summary.get("avg_r"))

    goal_component = min(daily_avg / max(low_goal, 1.0), 1.5)
    drawdown_penalty = min(drawdown / max(high_goal * 2.0, 1.0), 1.0)
    sample_penalty = 0.25 if trade_count < 8 else 0.0

    return round(
        (goal_component * 100.0)
        + (win_rate * 25.0)
        + (avg_r * 20.0)
        - (drawdown_penalty * 30.0)
        - (sample_penalty * 100.0),
        4,
    )


def scenario_recommendation(summary: dict, low_goal: float = 100.0) -> str:
    daily_avg = safe_float(summary.get("daily_average_realized_pnl"))
    drawdown = abs(safe_float(summary.get("max_drawdown")))
    win_rate = safe_float(summary.get("win_rate"))
    trade_count = int(safe_float(summary.get("trade_count")))

    if trade_count < 8:
        return "insufficient_trade_sample"
    if daily_avg >= low_goal and win_rate >= 0.50 and drawdown <= low_goal * 2.5:
        return "goal_candidate_review_for_live_calibration"
    if daily_avg > 0 and win_rate >= 0.50:
        return "profitable_but_below_goal"
    if daily_avg > 0:
        return "profitable_but_quality_weak"
    return "reject_negative_expectancy"


def run_scenario_matrix(base_config: ReplayConfig, bars_map: dict[str, list[dict]]) -> dict:
    risk_values = parse_number_list(
        os.getenv("SWING_REPLAY_SCENARIO_RISK_DOLLARS", ""),
        [
            float(base_config.risk_per_trade_dollars),
            40.0,
            50.0,
            60.0,
        ],
    )
    entry_values = parse_int_list(
        os.getenv("SWING_REPLAY_SCENARIO_ENTRIES_PER_DAY", ""),
        [
            int(base_config.max_entries_per_day),
            3,
            4,
        ],
    )
    target_values = parse_number_list(
        os.getenv("SWING_REPLAY_SCENARIO_TARGET_R", ""),
        [
            float(base_config.target_r_mult),
            1.5,
        ],
    )
    stall_values = parse_int_list(
        os.getenv("SWING_REPLAY_SCENARIO_STALL_DAYS", ""),
        [
            int(base_config.stall_exit_days),
            2,
            4,
        ],
    )

    seen: set[str] = set()
    rows: list[dict] = []

    for risk in risk_values:
        for entries in entry_values:
            for target_r in target_values:
                for stall_days in stall_values:
                    scenario_config = replace(
                        base_config,
                        risk_per_trade_dollars=float(risk),
                        max_entries_per_day=int(entries),
                        target_r_mult=float(target_r),
                        stall_exit_days=int(stall_days),
                    )
                    name = scenario_name(scenario_config)
                    if name in seen:
                        continue
                    seen.add(name)

                    replay = run_replay(scenario_config, bars_map)
                    summary = summarize(replay["trades"], replay["daily_rows"])
                    rows.append(
                        {
                            "scenario": name,
                            "risk_per_trade_dollars": round(float(risk), 2),
                            "max_entries_per_day": int(entries),
                            "target_r_mult": round(float(target_r), 4),
                            "stall_exit_days": int(stall_days),
                            "trade_count": summary.get("trade_count"),
                            "win_rate": summary.get("win_rate"),
                            "gross_pnl": summary.get("gross_pnl"),
                            "daily_average_realized_pnl": summary.get("daily_average_realized_pnl"),
                            "avg_trade_pnl": summary.get("avg_trade_pnl"),
                            "avg_r": summary.get("avg_r"),
                            "max_drawdown": summary.get("max_drawdown"),
                            "goal_score": goal_score(summary),
                            "recommendation": scenario_recommendation(summary),
                        }
                    )

    rows.sort(
        key=lambda row: (
            safe_float(row.get("goal_score")),
            safe_float(row.get("daily_average_realized_pnl")),
            safe_float(row.get("avg_r")),
        ),
        reverse=True,
    )

    goal_candidates = [
        row for row in rows
        if str(row.get("recommendation")) == "goal_candidate_review_for_live_calibration"
    ]

    return {
        "enabled": True,
        "scenario_count": len(rows),
        "risk_values": risk_values,
        "entry_values": entry_values,
        "target_values": target_values,
        "stall_values": stall_values,
        "rows": rows,
        "top_scenarios": rows[:10],
        "goal_candidates": goal_candidates[:10],
        "recommended_action": (
            "review_goal_candidates_before_live_env_change"
            if goal_candidates
            else "no_goal_candidate_found_review_top_profitable_scenarios"
        ),
    }

def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run offline two-week swing replay.")
    parser.add_argument("--days", type=int, default=None, help="Completed market sessions to replay. Default: env or 10.")
    parser.add_argument("--warmup-days", type=int, default=None, help="Historical warmup calendar days. Default: env or 100.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols. Default: env universe.")
    parser.add_argument(
        "--output-dir",
        default=str(Path.home() / "TradingDiagnostics" / "swing_two_week_replay"),
        help="Output directory.",
    )
    args = parser.parse_args()

    config = build_config(args)
    output_dir = Path(args.output_dir)
    archive_dir = output_dir / "archive"
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir = archive_dir / stamp
    run_dir.mkdir(parents=True, exist_ok=True)

    bars_map = fetch_daily_bars(config)
    replay = run_replay(config, bars_map)
    summary = summarize(replay["trades"], replay["daily_rows"])
    scenario_matrix = run_scenario_matrix(config, bars_map)

    payload = {
        "ok": True,
        "patch_version": PATCH_VERSION,
        "mode": "offline_two_week_swing_replay",
        "read_only": True,
        "does_not_submit_orders": True,
        "does_not_change_state": True,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "Uses daily bars and next-session open approximation.",
            "Does not model live spreads, Alpaca rejects, partial fills, or intraday quote timing.",
            "Does not import app.py, so this is a current-config strategy proxy rather than an exact live runtime replay.",
        ],
        "config": asdict(config),
        "bar_coverage": {
            "symbol_count": len(config.symbols),
            "symbols_with_bars": len([s for s in config.symbols if bars_map.get(s)]),
            "symbols_missing_bars": [s for s in config.symbols if not bars_map.get(s)],
            "bars_by_symbol": {s: len(bars_map.get(s) or []) for s in config.symbols},
        },
        "summary": summary,
        "scenario_matrix": {
            "enabled": bool(scenario_matrix.get("enabled")),
            "scenario_count": scenario_matrix.get("scenario_count"),
            "top_scenarios": scenario_matrix.get("top_scenarios"),
            "goal_candidates": scenario_matrix.get("goal_candidates"),
            "recommended_action": scenario_matrix.get("recommended_action"),
        },
        "test_dates": replay["test_dates"],
        "recommended_action": (
            "review_trade_and_missed_opportunity_csv_before_live_gate_changes"
            if summary["trade_count"]
            else "strategy_proxy_found_no_trades_review_candidate_rejections"
        ),
    }

    latest_summary = output_dir / "latest_summary.json"
    latest_config = output_dir / "latest_config_snapshot.json"
    latest_trades = output_dir / "latest_trades.csv"
    latest_daily = output_dir / "latest_daily_pnl.csv"
    latest_candidates = output_dir / "latest_candidate_rows.csv"
    latest_missed = output_dir / "latest_missed_opportunities.csv"
    latest_matrix = output_dir / "latest_scenario_matrix.csv"
    latest_matrix_json = output_dir / "latest_scenario_matrix.json"

    output_dir.mkdir(parents=True, exist_ok=True)

    latest_summary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    latest_config.write_text(json.dumps(asdict(config), indent=2, sort_keys=True), encoding="utf-8")
    write_csv(latest_trades, replay["trades"])
    write_csv(latest_daily, replay["daily_rows"])
    write_csv(latest_candidates, replay["candidate_rows"])
    write_csv(latest_missed, replay["missed_rows"])
    write_csv(latest_matrix, scenario_matrix["rows"])
    latest_matrix_json.write_text(json.dumps(scenario_matrix, indent=2, sort_keys=True), encoding="utf-8")

    (run_dir / "summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    (run_dir / "config_snapshot.json").write_text(json.dumps(asdict(config), indent=2, sort_keys=True), encoding="utf-8")
    write_csv(run_dir / "trades.csv", replay["trades"])
    write_csv(run_dir / "daily_pnl.csv", replay["daily_rows"])
    write_csv(run_dir / "candidate_rows.csv", replay["candidate_rows"])
    write_csv(run_dir / "missed_opportunities.csv", replay["missed_rows"])
    write_csv(run_dir / "scenario_matrix.csv", scenario_matrix["rows"])
    (run_dir / "scenario_matrix.json").write_text(json.dumps(scenario_matrix, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())