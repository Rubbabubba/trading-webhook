#!/usr/bin/env python3
"""
Minute-bar swing replay with scan-cycle simulation.

Read-only:
- Fetches Alpaca daily + 1m bars.
- Simulates scanner cycles during regular market hours.
- Simulates daily entry budget, per-scan entry cap, protective limit fills,
  stop/target exits, and final mark-to-close.
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
from dataclasses import asdict, dataclass, replace
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

try:
    from alpaca.data.enums import Adjustment as AlpacaAdjustment
    from alpaca.data.enums import DataFeed as AlpacaDataFeed
except Exception:
    AlpacaAdjustment = None
    AlpacaDataFeed = None


PATCH_VERSION = "patch-454-first-2k-window-replay-symbol-sleeve-attribution-matrix"
NY = ZoneInfo("America/New_York")

DEFAULT_SYMBOLS = (
    "SPY,QQQ,IWM,AAPL,MSFT,AMZN,GOOGL,META,NVDA,AMD,AVGO,TSLA,CRM,ORCL,"
    "ADBE,NOW,PLTR,NET,CRWD,PANW,SNOW,SHOP,SQ,UBER,COIN,SMCI,MU,INTC,"
    "NFLX,AMAT,ARM,MRVL,VRT,ANET,APP,ZS,DDOG,LRCX,KLAC,SPCX,MA"
)


@dataclass
class Config:
    symbols: list[str]
    test_days: int
    warmup_days: int
    scan_interval_minutes: int
    scan_start: str
    scan_end: str
    risk_per_trade_dollars: float
    max_entries_per_day: int
    max_entries_per_scan: int
    max_open_positions: int
    min_price: float
    min_avg_dollar_volume: float
    fast_ma_days: int
    slow_ma_days: int
    breakout_lookback_days: int
    breakout_buffer_pct: float
    target_r_mult: float
    min_rank_score: float
    max_below_breakout_pct: float
    max_above_breakout_pct: float
    min_close_to_high_pct: float
    min_return_20d_pct: float
    max_risk_per_share_pct: float
    entry_limit_offset_pct: float
    same_bar_policy: str
    data_feed: str
    data_adjustment: str
    start_date: str
    end_date: str
    include_current_session: bool


@dataclass
class Position:
    symbol: str
    entry_date: str
    entry_ts: str
    entry_price: float
    qty: float
    stop_price: float
    target_price: float
    risk_per_share: float
    rank_score: float
    max_r: float = 0.0
    min_r: float = 0.0


def getenv_any(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


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


def getenv_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_float_list_env(name: str, defaults: list[float]) -> list[float]:
    raw = os.getenv(name, "")
    values = []
    for part in str(raw or "").split(","):
        try:
            values.append(float(part.strip()))
        except Exception:
            pass
    return sorted(set(values or defaults))


def parse_int_list_env(name: str, defaults: list[int]) -> list[int]:
    raw = os.getenv(name, "")
    values = []
    for part in str(raw or "").split(","):
        try:
            values.append(int(float(part.strip())))
        except Exception:
            pass
    return sorted(set(values or defaults))


def pct_env(name: str, default: float) -> float:
    value = getenv_float(name, default)
    return value / 100.0 if abs(value) > 1.5 else value


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def symbols_from_env(raw: str) -> list[str]:
    out = []
    seen = set()
    for part in str(raw or "").split(","):
        sym = part.strip().upper()
        if sym and sym not in seen:
            out.append(sym)
            seen.add(sym)
    return out


def parse_hhmm(value: str) -> time:
    hour, minute = str(value).split(":")[:2]
    return time(int(hour), int(minute))


def sma(values: list[float], n: int) -> float | None:
    if len(values) < n:
        return None
    return sum(values[-n:]) / float(n)


def atr(highs: list[float], lows: list[float], closes: list[float], n: int = 14) -> float | None:
    if len(closes) < n + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    return sum(trs[-n:]) / float(n) if len(trs) >= n else None


def enum_feed(raw: str):
    if AlpacaDataFeed is None:
        return raw
    return getattr(AlpacaDataFeed, raw.upper(), AlpacaDataFeed.IEX)


def enum_adjustment(raw: str):
    if AlpacaAdjustment is None:
        return raw
    return {
        "raw": AlpacaAdjustment.RAW,
        "split": AlpacaAdjustment.SPLIT,
        "dividend": AlpacaAdjustment.DIVIDEND,
        "all": AlpacaAdjustment.ALL,
    }.get(raw.lower(), AlpacaAdjustment.RAW)


def build_config(args: argparse.Namespace) -> Config:
    symbols_raw = (
        args.symbols
        or os.getenv("SWING_REPLAY_SYMBOLS")
        or os.getenv("SCANNER_UNIVERSE_SYMBOLS")
        or os.getenv("ALLOWED_SYMBOLS")
        or DEFAULT_SYMBOLS
    )
    return Config(
        symbols=symbols_from_env(symbols_raw),
        test_days=int(args.days or getenv_int("SWING_REPLAY_TEST_DAYS", 10)),
        warmup_days=int(args.warmup_days or getenv_int("SWING_REPLAY_WARMUP_DAYS", 100)),
        scan_interval_minutes=getenv_int("SWING_MINUTE_REPLAY_SCAN_INTERVAL_MINUTES", 5),
        scan_start=os.getenv("SWING_MINUTE_REPLAY_SCAN_START", "09:35"),
        scan_end=os.getenv("SWING_MINUTE_REPLAY_SCAN_END", "15:30"),
        risk_per_trade_dollars=getenv_float("SWING_RISK_PER_TRADE_DOLLARS", getenv_float("RISK_DOLLARS", 30.0)),
        max_entries_per_day=getenv_int("SWING_MAX_NEW_ENTRIES_PER_DAY", 4),
        max_entries_per_scan=getenv_int(
            "SWING_PRODUCTION_RESET_MAX_ENTRIES_PER_SCAN",
            getenv_int("SCANNER_MAX_ENTRIES_PER_SCAN", 2),
        ),
        max_open_positions=getenv_int("SWING_MAX_OPEN_POSITIONS", 12),
        min_price=getenv_float("SWING_MIN_PRICE", 5.0),
        min_avg_dollar_volume=getenv_float("SWING_PRODUCTION_RESET_MIN_AVG_DOLLAR_VOLUME", 20_000_000.0),
        fast_ma_days=getenv_int("SWING_FAST_MA_DAYS", 10),
        slow_ma_days=getenv_int("SWING_SLOW_MA_DAYS", 30),
        breakout_lookback_days=getenv_int("SWING_BREAKOUT_LOOKBACK_DAYS", 20),
        breakout_buffer_pct=pct_env("SWING_BREAKOUT_BUFFER_PCT", 0.005),
        target_r_mult=getenv_float("SWING_TARGET_R_MULT", 2.0),
        min_rank_score=getenv_float("SWING_PRODUCTION_RESET_MIN_RANK_SCORE", 103.0),
        max_below_breakout_pct=pct_env("SWING_PRODUCTION_RESET_MAX_BELOW_BREAKOUT_PCT", 0.07),
        max_above_breakout_pct=pct_env("SWING_PRODUCTION_RESET_MAX_ABOVE_BREAKOUT_PCT", 0.15),
        min_close_to_high_pct=pct_env("SWING_PRODUCTION_RESET_MIN_CLOSE_TO_HIGH_PCT", 0.65),
        min_return_20d_pct=pct_env("SWING_PRODUCTION_RESET_MIN_RETURN_20D_PCT", -0.10),
        max_risk_per_share_pct=pct_env("SWING_PRODUCTION_RESET_MAX_RISK_PER_SHARE_PCT", 0.12),
        entry_limit_offset_pct=pct_env("SWING_REPLAY_ENTRY_LIMIT_OFFSET_PCT", 0.003),
        same_bar_policy=os.getenv("SWING_REPLAY_SAME_BAR_POLICY", "conservative_stop_first").strip().lower(),
        data_feed=os.getenv("DATA_FEED", "iex").strip().lower() or "iex",
        data_adjustment=os.getenv("DATA_ADJUSTMENT", "raw").strip().lower() or "raw",
        start_date=str(args.start_date or os.getenv("SWING_MINUTE_REPLAY_START_DATE", "") or "").strip(),
        end_date=str(args.end_date or os.getenv("SWING_MINUTE_REPLAY_END_DATE", "") or "").strip(),
        include_current_session=bool(args.include_current_session or getenv_bool("SWING_MINUTE_REPLAY_INCLUDE_CURRENT_SESSION", False)),
    )


def client_from_env() -> StockHistoricalDataClient:
    key = getenv_any("APCA_API_KEY_ID", "ALPACA_KEY_ID", "ALPACA_API_KEY_ID")
    secret = getenv_any("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET_KEY")
    if not key or not secret:
        raise SystemExit("Missing Alpaca API keys.")
    return StockHistoricalDataClient(key, secret)


def fetch_bars(client: StockHistoricalDataClient, config: Config, timeframe: TimeFrame) -> dict[str, list[dict]]:
    requested_start = parse_iso_date_or_none(config.start_date)
    requested_end = parse_iso_date_or_none(config.end_date)

    if requested_end:
        end = datetime.fromisoformat(requested_end).replace(hour=23, minute=59, second=59, tzinfo=NY).astimezone(timezone.utc)
    else:
        end = datetime.now(timezone.utc)

    if requested_start:
        start = datetime.fromisoformat(requested_start).replace(tzinfo=NY).astimezone(timezone.utc) - timedelta(days=max(10, config.warmup_days))
    else:
        days = max(45, config.warmup_days + config.test_days * 3 + 10)
        start = end - timedelta(days=days)

    req = StockBarsRequest(
        symbol_or_symbols=config.symbols,
        timeframe=timeframe,
        start=start,
        end=end,
        adjustment=enum_adjustment(config.data_adjustment),
        feed=enum_feed(config.data_feed),
    )
    result = client.get_stock_bars(req)
    data = getattr(result, "data", {}) or {}
    out: dict[str, list[dict]] = {}
    for symbol in config.symbols:
        bars = data.get(symbol, []) or []
        rows = []
        for bar in bars:
            ts = getattr(bar, "timestamp", None)
            if not ts:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            ts_ny = ts.astimezone(NY)
            rows.append({
                "symbol": symbol,
                "date": ts_ny.date().isoformat(),
                "ts_utc": ts.isoformat(),
                "ts_ny": ts_ny.isoformat(),
                "open": safe_float(getattr(bar, "open", 0.0)),
                "high": safe_float(getattr(bar, "high", 0.0)),
                "low": safe_float(getattr(bar, "low", 0.0)),
                "close": safe_float(getattr(bar, "close", 0.0)),
                "volume": safe_float(getattr(bar, "volume", 0.0)),
            })
        rows.sort(key=lambda r: r["ts_utc"])
        out[symbol] = rows
    return out

def regular_minutes(rows: list[dict]) -> list[dict]:
    out = []
    regular_start = time(9, 30)
    regular_end = time(16, 0)
    for row in rows:
        ts = datetime.fromisoformat(str(row["ts_ny"]))
        local_time = ts.time()
        if regular_start <= local_time <= regular_end:
            out.append(row)
    return out


def prior_daily_rows(rows: list[dict], session_date: str) -> list[dict]:
    return [r for r in rows if str(r.get("date")) < session_date]


def evaluate_candidate(symbol: str, history: list[dict], day_rows_so_far: list[dict], config: Config) -> dict:
    reasons = []
    need = max(config.slow_ma_days + 5, config.breakout_lookback_days + 2, 25)
    if len(history) < need or not day_rows_so_far:
        return {"symbol": symbol, "eligible": False, "reasons": ["insufficient_bars"]}

    closes = [safe_float(r["close"]) for r in history]
    highs = [safe_float(r["high"]) for r in history]
    lows = [safe_float(r["low"]) for r in history]
    vols = [safe_float(r["volume"]) for r in history]

    price = safe_float(day_rows_so_far[-1]["close"])
    session_high = max(safe_float(r["high"]) for r in day_rows_so_far)
    fast = sma(closes, config.fast_ma_days)
    slow = sma(closes, config.slow_ma_days)
    atr_14 = atr(highs, lows, closes, 14) or max(price * 0.01, 0.01)

    avg_dollar_volume_20d = sum(closes[-20 + i] * vols[-20 + i] for i in range(20)) / 20.0
    ret_20 = (price / closes[-20] - 1.0) if len(closes) >= 20 and closes[-20] > 0 else 0.0
    breakout_level = max(highs[-config.breakout_lookback_days:])
    breakout_distance = price / max(breakout_level, 1e-9) - 1.0
    close_to_high = price / max(session_high, 1e-9)

    stop_price = min(min(lows[-5:]), breakout_level * (1.0 - config.breakout_buffer_pct))
    risk_per_share = max(price - stop_price, price * 0.0025)
    risk_per_share_pct = risk_per_share / max(price, 1e-9)

    score = 0.0
    if price >= config.min_price:
        score += 10
    else:
        reasons.append("price_below_min")

    if avg_dollar_volume_20d >= config.min_avg_dollar_volume:
        score += min(20.0, avg_dollar_volume_20d / max(config.min_avg_dollar_volume, 1.0) * 10.0)
    else:
        reasons.append("avg_dollar_volume_below_min")

    if fast and slow and price > fast > slow:
        score += 25
    else:
        reasons.append("trend_filter_failed")

    if ret_20 >= config.min_return_20d_pct:
        score += min(20.0, ret_20 * 200.0)
    else:
        reasons.append("return_20d_below_min")

    if close_to_high >= config.min_close_to_high_pct:
        score += 12
    else:
        reasons.append("close_not_near_high")

    if breakout_distance >= -config.max_below_breakout_pct:
        score += 18
    else:
        reasons.append("too_far_below_breakout")

    if breakout_distance > config.max_above_breakout_pct:
        reasons.append("too_far_above_breakout")

    if risk_per_share_pct > config.max_risk_per_share_pct:
        reasons.append("risk_per_share_too_wide")

    score += min(10.0, max(0.0, ((session_high - price) / max(price, 1e-9)) * 100.0))
    score += min(5.0, (atr_14 / max(price, 1e-9)) * 100.0)

    if score < config.min_rank_score:
        reasons.append("rank_score_below_min")

    if stop_price >= price:
        reasons.append("entry_gap_below_or_at_stop")

    return {
        "symbol": symbol,
        "strategy": "daily_breakout",
        "eligible": not reasons,
        "reasons": ",".join(sorted(set(reasons))),
        "rank_score": round(score, 4),
        "price": round(price, 4),
        "stop_price": round(stop_price, 4),
        "risk_per_share": round(risk_per_share, 4),
        "target_price": round(price + risk_per_share * config.target_r_mult, 4),
        "breakout_level": round(breakout_level, 4),
        "breakout_distance_pct": round(breakout_distance * 100.0, 4),
        "return_20d_pct": round(ret_20 * 100.0, 4),
        "risk_per_share_pct": round(risk_per_share_pct * 100.0, 4),
    }


def fill_limit(candidate: dict, future_rows: list[dict], config: Config) -> tuple[dict | None, str]:
    limit_price = safe_float(candidate["price"]) * (1.0 + config.entry_limit_offset_pct)
    for row in future_rows:
        open_px = safe_float(row["open"])
        low = safe_float(row["low"])
        if open_px <= limit_price:
            return {**row, "fill_price": open_px, "limit_price": limit_price}, "open_at_or_below_limit"
        if low <= limit_price:
            return {**row, "fill_price": limit_price, "limit_price": limit_price}, "touched_limit"
    return None, "limit_not_filled"


def exit_check(position: Position, row: dict, config: Config) -> dict | None:
    open_px = safe_float(row["open"])
    high = safe_float(row["high"])
    low = safe_float(row["low"])
    close = safe_float(row["close"])

    high_r = (high - position.entry_price) / max(position.risk_per_share, 1e-9)
    low_r = (low - position.entry_price) / max(position.risk_per_share, 1e-9)
    position.max_r = max(position.max_r, high_r)
    position.min_r = min(position.min_r, low_r)

    hit_stop = low <= position.stop_price
    hit_target = high >= position.target_price

    reason = None
    px = None
    if hit_stop and hit_target:
        if open_px <= position.stop_price:
            reason = "gap_stop"
            px = open_px
        elif open_px >= position.target_price and config.same_bar_policy == "optimistic_target_first":
            reason = "target_gap"
            px = open_px
        elif config.same_bar_policy == "optimistic_target_first":
            reason = "target_same_bar"
            px = position.target_price
        else:
            reason = "stop_same_bar_conservative"
            px = position.stop_price
    elif hit_stop:
        reason = "gap_stop" if open_px <= position.stop_price else "stop"
        px = open_px if open_px <= position.stop_price else position.stop_price
    elif hit_target:
        reason = "target_gap" if open_px >= position.target_price else "target"
        px = open_px if open_px >= position.target_price else position.target_price

    if reason is None:
        return None

    pnl = (safe_float(px) - position.entry_price) * position.qty
    return {
        "symbol": position.symbol,
        "entry_date": position.entry_date,
        "entry_ts": position.entry_ts,
        "exit_date": row["date"],
        "exit_ts": row["ts_ny"],
        "entry_price": round(position.entry_price, 4),
        "exit_price": round(safe_float(px), 4),
        "qty": round(position.qty, 4),
        "pnl": round(pnl, 2),
        "r_mult": round(pnl / max(position.qty * position.risk_per_share, 1e-9), 4),
        "exit_reason": reason,
        "max_r": round(position.max_r, 4),
        "min_r": round(position.min_r, 4),
        "rank_score": round(position.rank_score, 4),
    }


def summarize(trades: list[dict], daily_rows: list[dict]) -> dict:
    pnls = [safe_float(t.get("pnl")) for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    r_vals = [safe_float(t.get("r_mult")) for t in trades]

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for row in daily_rows:
        equity += safe_float(row.get("realized_pnl"))
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)

    return {
        "trade_count": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
        "gross_pnl": round(sum(pnls), 2),
        "avg_trade_pnl": round(statistics.mean(pnls), 2) if pnls else 0.0,
        "avg_r": round(statistics.mean(r_vals), 4) if r_vals else 0.0,
        "daily_average_realized_pnl": round(statistics.mean([safe_float(r.get("realized_pnl")) for r in daily_rows]), 2) if daily_rows else 0.0,
        "max_drawdown": round(max_dd, 2),
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

def parse_iso_date_or_none(raw: str) -> str | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date().isoformat()
    except Exception:
        return None


def select_test_dates(all_dates: list[str], config: Config) -> tuple[list[str], dict]:
    today_ny = datetime.now(NY).date().isoformat()
    requested_start_date = parse_iso_date_or_none(config.start_date)
    requested_end_date = parse_iso_date_or_none(config.end_date)

    excluded_current_session = False
    if not config.include_current_session:
        filtered = [d for d in all_dates if d < today_ny]
        excluded_current_session = today_ny in set(all_dates)
    else:
        filtered = list(all_dates)

    if requested_start_date:
        filtered = [d for d in filtered if d >= requested_start_date]

    if requested_end_date:
        filtered = [d for d in filtered if d <= requested_end_date]

    if requested_start_date or requested_end_date:
        test_dates = filtered
    else:
        test_dates = filtered[-config.test_days:]

    metadata = {
        "completed_sessions_only": not config.include_current_session,
        "include_current_session": bool(config.include_current_session),
        "excluded_current_session": excluded_current_session,
        "current_ny_date": today_ny,
        "requested_start_date": requested_start_date,
        "requested_end_date": requested_end_date,
        "effective_start_date": test_dates[0] if test_dates else None,
        "effective_end_date": test_dates[-1] if test_dates else None,
        "available_dates": all_dates,
        "effective_test_dates": test_dates,
        "explicit_window": bool(requested_start_date or requested_end_date),
    }
    return test_dates, metadata

def run_with_data(config: Config, daily_map: dict[str, list[dict]], minute_map: dict[str, list[dict]], collect_rows: bool = True) -> dict:
    dates = sorted({r["date"] for rows in minute_map.values() for r in rows})
    test_dates, session_filter = select_test_dates(dates, config)

    open_positions: list[Position] = []
    trades: list[dict] = []
    candidate_rows: list[dict] = []
    submit_rows: list[dict] = []
    daily_rows: list[dict] = []

    scan_start = parse_hhmm(config.scan_start)
    scan_end = parse_hhmm(config.scan_end)

    for session_date in test_dates:
        entries_today = 0
        realized_today = 0.0
        daily_candidate_count = 0
        daily_eligible_count = 0
        daily_submitted_count = 0
        daily_filled_count = 0
        session_rows_by_symbol = {
            sym: [r for r in rows if r["date"] == session_date]
            for sym, rows in minute_map.items()
        }
        all_ts = sorted({r["ts_ny"] for rows in session_rows_by_symbol.values() for r in rows})

        for ts_str in all_ts:
            ts = datetime.fromisoformat(ts_str)
            current_minute_rows = {
                sym: next((r for r in rows if r["ts_ny"] == ts_str), None)
                for sym, rows in session_rows_by_symbol.items()
            }

            still_open = []
            for pos in open_positions:
                row = current_minute_rows.get(pos.symbol)
                if row:
                    trade = exit_check(pos, row, config)
                    if trade:
                        trades.append(trade)
                        realized_today += safe_float(trade["pnl"])
                        continue
                still_open.append(pos)
            open_positions = still_open

            local_time = ts.time()
            if local_time < scan_start or local_time > scan_end:
                continue

            minutes_from_open = (ts.hour * 60 + ts.minute) - (9 * 60 + 30)
            if minutes_from_open < 0 or minutes_from_open % max(1, config.scan_interval_minutes) != 0:
                continue

            if entries_today >= config.max_entries_per_day or len(open_positions) >= config.max_open_positions:
                continue

            candidates = []
            active_symbols = {p.symbol for p in open_positions}
            for sym in config.symbols:
                if sym in active_symbols:
                    continue
                rows_today = [r for r in session_rows_by_symbol.get(sym, []) if r["ts_ny"] <= ts_str]
                if not rows_today:
                    continue
                candidate = evaluate_candidate(sym, prior_daily_rows(daily_map.get(sym, []), session_date), rows_today, config)
                candidate["session_date"] = session_date
                candidate["scan_ts"] = ts_str
                daily_candidate_count += 1
                if collect_rows:
                    candidate_rows.append(candidate)
                if candidate.get("eligible"):
                    daily_eligible_count += 1
                    candidates.append(candidate)

            candidates.sort(key=lambda c: safe_float(c.get("rank_score")), reverse=True)
            slots = min(
                config.max_entries_per_scan,
                config.max_entries_per_day - entries_today,
                config.max_open_positions - len(open_positions),
            )

            for candidate in candidates[:slots]:
                sym = str(candidate["symbol"])
                future_rows = [r for r in session_rows_by_symbol.get(sym, []) if r["ts_ny"] >= ts_str]
                fill_row, fill_reason = fill_limit(candidate, future_rows, config)
                daily_submitted_count += 1
                submit_row = {
                    "session_date": session_date,
                    "scan_ts": ts_str,
                    "symbol": sym,
                    "rank_score": candidate.get("rank_score"),
                    "submitted": True,
                    "fill_reason": fill_reason,
                    "limit_price": round(safe_float(candidate["price"]) * (1.0 + config.entry_limit_offset_pct), 4),
                }

                if not fill_row:
                    if collect_rows:
                        submit_rows.append({**submit_row, "filled": False})
                    continue

                entry_price = safe_float(fill_row["fill_price"])
                stop_price = safe_float(candidate["stop_price"])
                if stop_price >= entry_price:
                    if collect_rows:
                        submit_rows.append({**submit_row, "filled": False, "fill_reason": "fill_invalid_stop_at_or_above_entry"})
                    continue

                risk_per_share = max(entry_price - stop_price, entry_price * 0.0025)
                qty = round(config.risk_per_trade_dollars / max(risk_per_share, 1e-9), 4)
                target_price = entry_price + risk_per_share * config.target_r_mult

                open_positions.append(Position(
                    symbol=sym,
                    entry_date=session_date,
                    entry_ts=fill_row["ts_ny"],
                    entry_price=entry_price,
                    qty=qty,
                    stop_price=stop_price,
                    target_price=target_price,
                    risk_per_share=risk_per_share,
                    rank_score=safe_float(candidate["rank_score"]),
                ))
                entries_today += 1
                daily_filled_count += 1
                if collect_rows:
                    submit_rows.append({**submit_row, "filled": True, "fill_ts": fill_row["ts_ny"], "fill_price": round(entry_price, 4), "qty": qty})

        unrealized = 0.0
        for pos in open_positions:
            last_row = session_rows_by_symbol.get(pos.symbol, [])[-1] if session_rows_by_symbol.get(pos.symbol) else None
            if last_row:
                unrealized += (safe_float(last_row["close"]) - pos.entry_price) * pos.qty

        daily_rows.append({
            "date": session_date,
            "realized_pnl": round(realized_today, 2),
            "unrealized_mark": round(unrealized, 2),
            "entries_today": entries_today,
            "open_positions": len(open_positions),
            "candidate_count": daily_candidate_count,
            "eligible_count": daily_eligible_count,
            "submitted_count": daily_submitted_count,
            "filled_count": daily_filled_count,
        })

    final_date = test_dates[-1] if test_dates else None
    if final_date:
        for pos in open_positions:
            rows = [r for r in minute_map.get(pos.symbol, []) if r["date"] == final_date]
            if not rows:
                continue
            px = safe_float(rows[-1]["close"])
            pnl = (px - pos.entry_price) * pos.qty
            trades.append({
                "symbol": pos.symbol,
                "entry_date": pos.entry_date,
                "entry_ts": pos.entry_ts,
                "exit_date": final_date,
                "exit_ts": rows[-1]["ts_ny"],
                "entry_price": round(pos.entry_price, 4),
                "exit_price": round(px, 4),
                "qty": round(pos.qty, 4),
                "pnl": round(pnl, 2),
                "r_mult": round(pnl / max(pos.qty * pos.risk_per_share, 1e-9), 4),
                "exit_reason": "final_mark_to_close",
                "max_r": round(pos.max_r, 4),
                "min_r": round(pos.min_r, 4),
                "rank_score": round(pos.rank_score, 4),
            })

    return {
        "test_dates": test_dates,
        "session_filter": session_filter,
        "trades": trades,
        "candidate_rows": candidate_rows,
        "submit_rows": submit_rows,
        "daily_rows": daily_rows,
        "summary": summarize(trades, daily_rows),
    }

def symbol_attribution_rows(trades: list[dict], scenario: str = "baseline") -> list[dict]:
    by_symbol: dict[str, list[dict]] = {}
    for trade in trades:
        symbol = str(trade.get("symbol") or "").upper()
        if not symbol:
            continue
        by_symbol.setdefault(symbol, []).append(trade)

    rows = []
    for symbol, items in sorted(by_symbol.items()):
        pnl_values = [safe_float(t.get("pnl")) for t in items]
        r_values = [safe_float(t.get("r_mult")) for t in items]
        wins = len([p for p in pnl_values if p > 0])
        losses = len([p for p in pnl_values if p < 0])
        exit_reasons = [str(t.get("exit_reason") or "") for t in items]
        rows.append({
            "scenario": scenario,
            "symbol": symbol,
            "trade_count": len(items),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / len(items), 4) if items else 0.0,
            "gross_pnl": round(sum(pnl_values), 2),
            "avg_pnl": round(statistics.mean(pnl_values), 2) if pnl_values else 0.0,
            "avg_r": round(statistics.mean(r_values), 4) if r_values else 0.0,
            "target_count": len([x for x in exit_reasons if x.startswith("target")]),
            "stop_count": len([x for x in exit_reasons if "stop" in x]),
            "final_mark_count": len([x for x in exit_reasons if x == "final_mark_to_close"]),
        })
    rows.sort(key=lambda r: safe_float(r.get("gross_pnl")), reverse=True)
    return rows

def build_scenario_configs(config: Config) -> list[tuple[str, Config, dict]]:
    risk_values = parse_float_list_env(
        "SWING_MINUTE_REPLAY_MATRIX_RISK_DOLLARS",
        sorted(set([config.risk_per_trade_dollars, 45.0, 60.0])),
    )
    daily_values = parse_int_list_env(
        "SWING_MINUTE_REPLAY_MATRIX_DAILY_ENTRIES",
        sorted(set([config.max_entries_per_day, 3, 5])),
    )
    scan_values = parse_int_list_env(
        "SWING_MINUTE_REPLAY_MATRIX_SCAN_ENTRIES",
        sorted(set([config.max_entries_per_scan, 1, 2])),
    )
    target_values = parse_float_list_env(
        "SWING_MINUTE_REPLAY_MATRIX_TARGET_R",
        sorted(set([config.target_r_mult, 1.5, 2.0])),
    )
    rank_values = parse_float_list_env(
        "SWING_MINUTE_REPLAY_MATRIX_MIN_RANK",
        sorted(set([config.min_rank_score, 100.0, 106.0])),
    )
    below_values = parse_float_list_env(
        "SWING_MINUTE_REPLAY_MATRIX_MAX_BELOW_BREAKOUT_PCT",
        sorted(set([config.max_below_breakout_pct, 0.05, 0.09])),
    )

    scenarios: list[tuple[str, Config, dict]] = []
    seen = set()
    for risk in risk_values:
        for daily in daily_values:
            for scan in scan_values:
                for target in target_values:
                    for rank in rank_values:
                        for below in below_values:
                            overrides = {
                                "risk_per_trade_dollars": float(risk),
                                "max_entries_per_day": int(daily),
                                "max_entries_per_scan": int(scan),
                                "target_r_mult": float(target),
                                "min_rank_score": float(rank),
                                "max_below_breakout_pct": float(below),
                            }
                            key = tuple(sorted(overrides.items()))
                            if key in seen:
                                continue
                            seen.add(key)
                            name = (
                                f"risk_{risk:g}_daily_{daily}_scan_{scan}_"
                                f"target_{str(target).replace('.', 'p')}_"
                                f"rank_{rank:g}_below_{round(below * 100, 2):g}pct"
                            )
                            scenarios.append((name, replace(config, **overrides), overrides))
    return scenarios


def scenario_matrix(config: Config, daily_map: dict[str, list[dict]], minute_map: dict[str, list[dict]]) -> dict:
    rows = []
    best_result = None
    best_name = None
    best_overrides = None
    symbol_matrix_rows = []

    if not getenv_bool("SWING_MINUTE_REPLAY_SCENARIO_MATRIX_ENABLED", True):
        return {"rows": rows, "best_scenario": None, "best_trades": [], "best_daily_rows": [], "symbol_matrix": [], "best_symbol_rows": []}

    for name, scenario_config, overrides in build_scenario_configs(config):
        result = run_with_data(scenario_config, daily_map, minute_map, collect_rows=False)
        summary = result["summary"]
        test_dates = result["test_dates"]
        gross_per_day = safe_float(summary.get("gross_pnl")) / max(len(test_dates), 1)
        realized_goal_days = len([
            r for r in result["daily_rows"]
            if safe_float(r.get("realized_pnl")) >= 100.0
        ])
        losing_days = len([
            r for r in result["daily_rows"]
            if safe_float(r.get("realized_pnl")) < 0.0
        ])
        row = {
            "scenario": name,
            "gross_pnl": summary.get("gross_pnl"),
            "gross_per_day": round(gross_per_day, 2),
            "daily_average_realized_pnl": summary.get("daily_average_realized_pnl"),
            "trade_count": summary.get("trade_count"),
            "win_rate": summary.get("win_rate"),
            "avg_r": summary.get("avg_r"),
            "max_drawdown": summary.get("max_drawdown"),
            "realized_goal_days": realized_goal_days,
            "losing_days": losing_days,
            "goal_100_realized_avg_met": safe_float(summary.get("daily_average_realized_pnl")) >= 100.0,
            "goal_100_gross_per_day_met": gross_per_day >= 100.0,
            "risk_per_trade_dollars": overrides["risk_per_trade_dollars"],
            "max_entries_per_day": overrides["max_entries_per_day"],
            "max_entries_per_scan": overrides["max_entries_per_scan"],
            "target_r_mult": overrides["target_r_mult"],
            "min_rank_score": overrides["min_rank_score"],
            "max_below_breakout_pct": overrides["max_below_breakout_pct"],
        }
        rows.append(row)
        symbol_matrix_rows.extend(symbol_attribution_rows(result.get("trades") or [], scenario=name))

        if best_result is None:
            best_result = result
            best_name = name
            best_overrides = overrides
        else:
            current_key = (
                safe_float(row.get("gross_per_day")),
                safe_float(row.get("avg_r")),
                -abs(safe_float(row.get("max_drawdown"))),
            )
            best_summary = best_result["summary"]
            best_key = (
                safe_float(best_summary.get("gross_pnl")) / max(len(best_result.get("test_dates") or []), 1),
                safe_float(best_summary.get("avg_r")),
                -abs(safe_float(best_summary.get("max_drawdown"))),
            )
            if current_key > best_key:
                best_result = result
                best_name = name
                best_overrides = overrides

    rows.sort(
        key=lambda r: (
            safe_float(r.get("gross_per_day")),
            safe_float(r.get("avg_r")),
            -abs(safe_float(r.get("max_drawdown"))),
        ),
        reverse=True,
    )

    best_scenario = None
    best_trades = []
    best_daily_rows = []
    best_symbol_rows = []
    if best_result is not None:
        best_summary = best_result["summary"]
        best_scenario = {
            "scenario": best_name,
            "overrides": best_overrides or {},
            "summary": best_summary,
            "test_dates": best_result.get("test_dates") or [],
            "gross_per_day": round(
                safe_float(best_summary.get("gross_pnl")) / max(len(best_result.get("test_dates") or []), 1),
                2,
            ),
        }
        best_trades = [
            {"scenario": best_name, **row}
            for row in best_result.get("trades", [])
        ]
        best_daily_rows = [
            {"scenario": best_name, **row}
            for row in best_result.get("daily_rows", [])
        ]
        best_symbol_rows = symbol_attribution_rows(best_result.get("trades") or [], scenario=str(best_name or "best"))

    return {
        "rows": rows,
        "best_scenario": best_scenario,
        "best_trades": best_trades,
        "best_daily_rows": best_daily_rows,
        "symbol_matrix": symbol_matrix_rows,
        "best_symbol_rows": best_symbol_rows,
    }

def run(config: Config) -> dict:
    client = client_from_env()
    daily_map = fetch_bars(client, config, TimeFrame.Day)
    minute_map = {s: regular_minutes(v) for s, v in fetch_bars(client, config, TimeFrame.Minute).items()}

    base_result = run_with_data(config, daily_map, minute_map, collect_rows=True)
    matrix_bundle = scenario_matrix(config, daily_map, minute_map)

    return {
        **base_result,
        "scenario_matrix": matrix_bundle.get("rows") or [],
        "best_scenario": matrix_bundle.get("best_scenario"),
        "best_scenario_trades": matrix_bundle.get("best_trades") or [],
        "best_scenario_daily_rows": matrix_bundle.get("best_daily_rows") or [],
        "symbol_attribution": symbol_attribution_rows(base_result.get("trades") or [], scenario="baseline"),
        "best_scenario_symbol_attribution": matrix_bundle.get("best_symbol_rows") or [],
        "scenario_symbol_attribution_matrix": matrix_bundle.get("symbol_matrix") or [],
        "positive_scenario_count": len([
            r for r in matrix_bundle.get("rows") or []
            if safe_float(r.get("gross_pnl")) > 0.0
        ]),
        "positive_avg_r_scenario_count": len([
            r for r in matrix_bundle.get("rows") or []
            if safe_float(r.get("avg_r")) > 0.0
        ]),
        "bar_coverage": {
            "symbols": len(config.symbols),
            "symbols_with_daily_bars": len([s for s in config.symbols if daily_map.get(s)]),
            "symbols_with_minute_bars": len([s for s in config.symbols if minute_map.get(s)]),
            "missing_daily": [s for s in config.symbols if not daily_map.get(s)],
            "missing_minute": [s for s in config.symbols if not minute_map.get(s)],
            "minute_rows_by_symbol": {s: len(minute_map.get(s) or []) for s in config.symbols},
        },
    }

def main() -> int:
    parser = argparse.ArgumentParser(description="Run minute-bar swing replay.")
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--warmup-days", type=int, default=None)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--include-current-session", action="store_true")
    parser.add_argument("--output-dir", default=str(Path.home() / "TradingDiagnostics" / "swing_minute_replay"))
    args = parser.parse_args()

    config = build_config(args)
    output_dir = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir = output_dir / "archive" / stamp
    run_dir.mkdir(parents=True, exist_ok=True)

    result = run(config)
    payload = {
        "ok": True,
        "patch_version": PATCH_VERSION,
        "mode": "minute_bar_swing_replay_scan_cycle_simulation",
        "read_only": True,
        "does_not_submit_orders": True,
        "does_not_change_state": True,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "Offline replay; does not import app.py.",
            "Uses simplified production-contract proxy.",
            "Models protective limit fills from 1m bars, not actual broker queue priority.",
            "Intrabar stop/target order is conservative unless SWING_REPLAY_SAME_BAR_POLICY changes it.",
        ],
        "config": asdict(config),
        "bar_coverage": result["bar_coverage"],
        "summary": result["summary"],
        "scenario_matrix_enabled": getenv_bool("SWING_MINUTE_REPLAY_SCENARIO_MATRIX_ENABLED", True),
        "scenario_count": len(result.get("scenario_matrix") or []),
        "positive_scenario_count": result.get("positive_scenario_count"),
        "positive_avg_r_scenario_count": result.get("positive_avg_r_scenario_count"),
        "best_scenario": result.get("best_scenario"),
        "top_scenarios": (result.get("scenario_matrix") or [])[:10],
        "session_filter": result.get("session_filter") or {},
        "test_dates": result["test_dates"],
        "symbol_attribution_top": (result.get("symbol_attribution") or [])[:15],
        "best_scenario_symbol_attribution_top": (result.get("best_scenario_symbol_attribution") or [])[:15],
        "recommended_action": "compare_first_2k_window_against_recent_window_before_live_risk_changes",
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "latest_config_snapshot.json").write_text(json.dumps(asdict(config), indent=2, sort_keys=True), encoding="utf-8")
    write_csv(output_dir / "latest_trades.csv", result["trades"])
    write_csv(output_dir / "latest_daily_pnl.csv", result["daily_rows"])
    write_csv(output_dir / "latest_candidate_rows.csv", result["candidate_rows"])
    write_csv(output_dir / "latest_submit_rows.csv", result["submit_rows"])
    write_csv(output_dir / "latest_scenario_matrix.csv", result.get("scenario_matrix") or [])
    (output_dir / "latest_scenario_matrix.json").write_text(
        json.dumps(result.get("scenario_matrix") or [], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_csv(output_dir / "latest_best_scenario_trades.csv", result.get("best_scenario_trades") or [])
    write_csv(output_dir / "latest_best_scenario_daily_pnl.csv", result.get("best_scenario_daily_rows") or [])
    write_csv(output_dir / "latest_symbol_attribution.csv", result.get("symbol_attribution") or [])
    write_csv(output_dir / "latest_best_scenario_symbol_attribution.csv", result.get("best_scenario_symbol_attribution") or [])
    write_csv(output_dir / "latest_scenario_symbol_attribution_matrix.csv", result.get("scenario_symbol_attribution_matrix") or [])

    (run_dir / "summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    (run_dir / "config_snapshot.json").write_text(json.dumps(asdict(config), indent=2, sort_keys=True), encoding="utf-8")
    write_csv(run_dir / "trades.csv", result["trades"])
    write_csv(run_dir / "daily_pnl.csv", result["daily_rows"])
    write_csv(run_dir / "candidate_rows.csv", result["candidate_rows"])
    write_csv(run_dir / "submit_rows.csv", result["submit_rows"])
    write_csv(run_dir / "scenario_matrix.csv", result.get("scenario_matrix") or [])
    (run_dir / "scenario_matrix.json").write_text(
        json.dumps(result.get("scenario_matrix") or [], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_csv(run_dir / "best_scenario_trades.csv", result.get("best_scenario_trades") or [])
    write_csv(run_dir / "best_scenario_daily_pnl.csv", result.get("best_scenario_daily_rows") or [])
    write_csv(run_dir / "symbol_attribution.csv", result.get("symbol_attribution") or [])
    write_csv(run_dir / "best_scenario_symbol_attribution.csv", result.get("best_scenario_symbol_attribution") or [])
    write_csv(run_dir / "scenario_symbol_attribution_matrix.csv", result.get("scenario_symbol_attribution_matrix") or [])

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())