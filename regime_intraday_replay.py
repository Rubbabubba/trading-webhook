"""No-lookahead replay and validation for the regime-routed intraday engine."""

from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime
from itertools import product
from statistics import fmean
from typing import Any, Callable, Iterable

from regime_intraday import RegimeIntradayConfig, evaluate_regime_intraday


def _dt(row: dict) -> datetime:
    value = row.get("ts_ny") or row.get("timestamp") or row.get("ts_utc") or row.get("t")
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _num(row: dict, key: str, short: str) -> float:
    try:
        return float(row.get(key, row.get(short)) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def split_sessions(bars_by_symbol: dict[str, list[dict]], symbols: Iterable[str]) -> dict[str, dict[str, list[dict]]]:
    sessions: dict[str, dict[str, list[dict]]] = {}
    for symbol in symbols:
        for row in sorted((dict(r) for r in bars_by_symbol.get(symbol, []) if isinstance(r, dict)), key=_dt):
            stamp = _dt(row)
            session = stamp.date().isoformat()
            sessions.setdefault(session, {}).setdefault(symbol, []).append(row)
    return sessions


def _outcome(signal: dict, future: list[dict]) -> dict:
    side = str(signal["underlying_side"])
    entry = float(signal["entry_price"])
    stop = float(signal["stop_price"])
    target = float(signal["target_price"])
    risk = abs(entry - stop)
    exit_price = entry
    exit_reason = "no_future_bar"
    exit_ts = None
    for row in future:
        high, low = _num(row, "high", "h"), _num(row, "low", "l")
        stop_hit = low <= stop if side == "buy" else high >= stop
        target_hit = high >= target if side == "buy" else low <= target
        if stop_hit:  # Deliberately pessimistic when both levels trade in one minute.
            exit_price, exit_reason, exit_ts = stop, "stop", _dt(row).isoformat()
            break
        if target_hit:
            exit_price, exit_reason, exit_ts = target, "target", _dt(row).isoformat()
            break
    else:
        if future:
            exit_price = _num(future[-1], "close", "c")
            exit_reason, exit_ts = "eod", _dt(future[-1]).isoformat()
    points = (exit_price - entry) if side == "buy" else (entry - exit_price)
    return {
        **signal,
        "exit_price": round(exit_price, 4),
        "exit_reason": exit_reason,
        "exit_ts": exit_ts,
        "realized_r": round(points / risk, 4) if risk else 0.0,
    }


def replay_sessions(
    bars_by_symbol: dict[str, list[dict]],
    config: RegimeIntradayConfig | None = None,
    *,
    max_trades_per_day: int = 2,
    evaluator: Callable[[dict[str, list[dict]], RegimeIntradayConfig], dict[str, Any]] = evaluate_regime_intraday,
) -> dict[str, Any]:
    """Replay completed one-minute bars, allowing only one position at a time."""
    cfg = config or RegimeIntradayConfig()
    sessions = split_sessions(bars_by_symbol, cfg.symbols)
    trades: list[dict] = []
    setup_observations: list[dict] = []
    regime_counts: dict[str, int] = {}
    accepted_sessions = 0
    for session, day in sorted(sessions.items()):
        if any(len(day.get(symbol, [])) < cfg.min_bars for symbol in cfg.symbols):
            continue
        accepted_sessions += 1
        timeline = sorted({_dt(row) for symbol in cfg.symbols for row in day.get(symbol, [])})
        arrivals = {symbol: {_dt(row): row for row in day.get(symbol, [])} for symbol in cfg.symbols}
        prefix = {symbol: [] for symbol in cfg.symbols}
        seen: set[str] = set()
        daily_trades = 0
        blocked_until: datetime | None = None
        for stamp in timeline:
            for symbol in cfg.symbols:
                row = arrivals[symbol].get(stamp)
                if row is not None:
                    prefix[symbol].append(row)
            if daily_trades >= max(1, int(max_trades_per_day)) or (blocked_until and stamp <= blocked_until):
                continue
            if any(len(prefix[symbol]) < cfg.min_bars for symbol in cfg.symbols):
                continue
            scan = evaluator(prefix, cfg)
            name = str((scan.get("regime") or {}).get("name") or "unknown")
            regime_counts[name] = regime_counts.get(name, 0) + 1
            for row in list(scan.get("setup_proximity") or []):
                setup_observations.append({"session": session, "ts": stamp.isoformat(), **dict(row)})
            candidates = [s for s in scan.get("signals", []) if str(s.get("signal_id")) not in seen]
            if not candidates:
                continue
            signal = dict(candidates[0])
            seen.add(str(signal.get("signal_id")))
            symbol = str(signal["symbol"])
            future = [row for row in day.get(symbol, []) if _dt(row) > stamp]
            trade = _outcome(signal, future)
            trade.update({"session": session, "entry_ts": stamp.isoformat(), "regime": name})
            trades.append(trade)
            daily_trades += 1
            if trade.get("exit_ts"):
                blocked_until = datetime.fromisoformat(str(trade["exit_ts"]).replace("Z", "+00:00"))
            else:
                break
    return _report(trades, len(sessions), accepted_sessions, regime_counts, cfg, setup_observations)


def _report(trades: list[dict], session_count: int, accepted_sessions: int, regime_counts: dict, cfg: RegimeIntradayConfig, setup_observations: list[dict] | None = None) -> dict:
    values = [float(row.get("realized_r") or 0.0) for row in trades]
    equity = peak = drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    grouped: dict[str, dict[str, Any]] = {}
    for field in ("symbol", "strategy", "exit_reason"):
        for key in sorted({str(row.get(field)) for row in trades}):
            subset = [float(row["realized_r"]) for row in trades if str(row.get(field)) == key]
            grouped[f"{field}:{key}"] = {"count": len(subset), "win_rate": round(sum(v > 0 for v in subset) / len(subset), 4), "average_r": round(fmean(subset), 4)}
    observations = list(setup_observations or [])
    blockers: dict[str, int] = {}
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in observations:
        gate = str(row.get("next_gate") or "unknown")
        blockers[gate] = blockers.get(gate, 0) + 1
        symbol = str(row.get("symbol") or "unknown")
        bucket = by_symbol.setdefault(symbol, {"observations": 0, "signal_ready": 0, "next_gate_counts": {}, "closest_misses": []})
        bucket["observations"] += 1
        bucket["signal_ready"] += int(bool(row.get("underlying_signal_ready")))
        bucket["next_gate_counts"][gate] = bucket["next_gate_counts"].get(gate, 0) + 1
        if not row.get("underlying_signal_ready"):
            bucket["closest_misses"].append({key: row.get(key) for key in ("session", "ts", "next_gate", "vwap_distance_atr", "distance_to_nearest_band_edge_atr", "data_ready", "regime_ready", "stretch_ready", "reversal_ready")})
    for bucket in by_symbol.values():
        bucket["closest_misses"] = sorted(bucket["closest_misses"], key=lambda row: (-(sum(bool(row.get(field)) for field in ("data_ready", "regime_ready", "stretch_ready", "reversal_ready"))), abs(float(row.get("distance_to_nearest_band_edge_atr") or 0))))[:10]
    return {
        "ok": True,
        "session_count": session_count,
        "accepted_session_count": accepted_sessions,
        "trade_count": len(trades),
        "win_rate": round(sum(v > 0 for v in values) / len(values), 4) if values else None,
        "average_r": round(fmean(values), 4) if values else None,
        "total_r": round(sum(values), 4),
        "max_drawdown_r": round(drawdown, 4),
        "profit_factor_r": round(sum(v for v in values if v > 0) / abs(sum(v for v in values if v < 0)), 4) if any(v < 0 for v in values) else None,
        "regime_evaluations": regime_counts,
        "setup_gate_analysis": {"observation_count": len(observations), "next_gate_counts": blockers, "by_symbol": by_symbol,
                                "note": "Completed-bar rule observations, not signal probabilities."},
        "attribution": grouped,
        "config": asdict(cfg),
        "trades": trades,
    }


def cost_adjusted_report(report: dict[str, Any], *, risk_dollars: float = 100.0, round_trip_cost_r: float = 0.12) -> dict[str, Any]:
    """Apply a conservative fixed options-friction estimate to replay outcomes."""
    trades = list(report.get("trades") or [])
    net_r = [float(row.get("realized_r") or 0.0) - abs(float(round_trip_cost_r)) for row in trades]
    daily: dict[str, float] = {}
    for row, value in zip(trades, net_r):
        session = str(row.get("session") or "unknown")
        daily[session] = daily.get(session, 0.0) + value * float(risk_dollars)
    observed_sessions = max(int(report.get("accepted_session_count") or report.get("session_count") or 0), len(daily))
    values = list(daily.values())
    return {
        "trade_count": len(trades), "risk_dollars": float(risk_dollars), "round_trip_cost_r": float(round_trip_cost_r),
        "net_total_r": round(sum(net_r), 4), "net_average_r": round(fmean(net_r), 4) if net_r else None,
        "net_total_dollars": round(sum(net_r) * float(risk_dollars), 2),
        "average_daily_dollars": round(sum(values) / observed_sessions, 2) if observed_sessions else None,
        "days_at_or_above_100": sum(value >= 100 for value in values), "days_at_or_above_200": sum(value >= 200 for value in values),
        "observed_sessions": observed_sessions, "days_with_trades": len(values),
        "daily_goal_100_rate": round(sum(value >= 100 for value in values) / observed_sessions, 4) if observed_sessions else None,
        "daily_goal_200_rate": round(sum(value >= 200 for value in values) / observed_sessions, 4) if observed_sessions else None,
        "daily_net_dollars": daily,
    }


def threshold_sensitivity(bars_by_symbol: dict[str, list[dict]], base: RegimeIntradayConfig | None = None) -> list[dict]:
    cfg = base or RegimeIntradayConfig()
    rows = []
    # Keep this intentionally bounded: it runs inside diagnostics on production.
    for trend, volume, range_eff, stretch in product((0.34, 0.40), (1.2, 1.4), (0.22, 0.27), (1.0, 1.25)):
        candidate = replace(cfg, trend_efficiency_min=trend, momentum_volume_ratio=volume, range_efficiency_max=range_eff, mean_reversion_min_vwap_atr=stretch)
        result = replay_sessions(bars_by_symbol, candidate)
        rows.append({"parameters": {"trend_efficiency_min": trend, "momentum_volume_ratio": volume, "range_efficiency_max": range_eff, "mean_reversion_min_vwap_atr": stretch}, **{k: result[k] for k in ("trade_count", "win_rate", "average_r", "total_r", "max_drawdown_r")}})
    return sorted(rows, key=lambda row: (float(row.get("average_r") or -999), int(row["trade_count"])), reverse=True)


def walk_forward(bars_by_symbol: dict[str, list[dict]], base: RegimeIntradayConfig | None = None, *, train_fraction: float = 0.7) -> dict:
    cfg = base or RegimeIntradayConfig()
    sessions = split_sessions(bars_by_symbol, cfg.symbols)
    dates = sorted(sessions)
    cut = max(1, min(len(dates) - 1, int(len(dates) * train_fraction))) if len(dates) > 1 else len(dates)
    train_dates, test_dates = set(dates[:cut]), set(dates[cut:])
    subset = lambda chosen: {symbol: [row for row in bars_by_symbol.get(symbol, []) if _dt(row).date().isoformat() in chosen] for symbol in cfg.symbols}
    ranked = threshold_sensitivity(subset(train_dates), cfg)
    best = ranked[0] if ranked else None
    selected = replace(cfg, **dict((best or {}).get("parameters") or {}))
    return {"train_sessions": sorted(train_dates), "test_sessions": sorted(test_dates), "selected_parameters": dict((best or {}).get("parameters") or {}), "train": replay_sessions(subset(train_dates), selected), "test": replay_sessions(subset(test_dates), selected), "top_train_candidates": ranked[:10]}


def mean_reversion_walk_forward(
    bars_by_symbol: dict[str, list[dict]],
    base: RegimeIntradayConfig | None = None,
    *,
    train_fraction: float = 0.7,
    risk_dollars: float = 100.0,
    round_trip_cost_r: float = 0.12,
) -> dict[str, Any]:
    """Tune a bounded mean-reversion grid on train data and freeze it for test."""
    cfg = replace(base or RegimeIntradayConfig(), momentum_enabled=False, mean_reversion_enabled=True)
    sessions = split_sessions(bars_by_symbol, cfg.symbols)
    dates = sorted(sessions)
    cut = max(1, min(len(dates) - 1, int(len(dates) * train_fraction))) if len(dates) > 1 else len(dates)
    train_dates, test_dates = set(dates[:cut]), set(dates[cut:])

    def subset(chosen: set[str]) -> dict[str, list[dict]]:
        return {symbol: [row for row in bars_by_symbol.get(symbol, []) if _dt(row).date().isoformat() in chosen] for symbol in cfg.symbols}

    candidates = []
    for range_efficiency, min_stretch in product((0.20, 0.24, 0.27, 0.30), (1.0, 1.25)):
        candidate_cfg = replace(cfg, range_efficiency_max=range_efficiency, mean_reversion_min_vwap_atr=min_stretch)
        raw = replay_sessions(subset(train_dates), candidate_cfg)
        net = cost_adjusted_report(raw, risk_dollars=risk_dollars, round_trip_cost_r=round_trip_cost_r)
        count = int(raw.get("trade_count") or 0)
        score = float(net.get("net_average_r") or -999) - 0.02 * float(raw.get("max_drawdown_r") or 0)
        candidates.append({
            "parameters": {"range_efficiency_max": range_efficiency, "mean_reversion_min_vwap_atr": min_stretch},
            "eligible": count >= 8 and float(net.get("net_average_r") or 0) > 0,
            "selection_score": round(score, 4), "trade_count": count, "raw_average_r": raw.get("average_r"),
            "max_drawdown_r": raw.get("max_drawdown_r"), "cost_adjusted": net,
        })
    candidates.sort(key=lambda row: (bool(row["eligible"]), float(row["selection_score"]), int(row["trade_count"])), reverse=True)
    selected_row = next((row for row in candidates if row["eligible"]), None)
    if not selected_row:
        return {"ready": False, "reason": "no_positive_train_candidate_with_minimum_sample", "train_sessions": len(train_dates), "test_sessions": len(test_dates), "candidates": candidates}
    selected_cfg = replace(cfg, **selected_row["parameters"])
    train_raw = replay_sessions(subset(train_dates), selected_cfg)
    test_raw = replay_sessions(subset(test_dates), selected_cfg)
    test_net = cost_adjusted_report(test_raw, risk_dollars=risk_dollars, round_trip_cost_r=round_trip_cost_r)
    return {
        "ready": True, "train_sessions": len(train_dates), "test_sessions": len(test_dates), "selected_parameters": selected_row["parameters"],
        "train": {key: value for key, value in train_raw.items() if key != "trades"} | {"cost_adjusted": cost_adjusted_report(train_raw, risk_dollars=risk_dollars, round_trip_cost_r=round_trip_cost_r)},
        "test": {key: value for key, value in test_raw.items() if key != "trades"} | {"cost_adjusted": test_net},
        "out_of_sample_positive": bool((test_net.get("net_average_r") or 0) > 0), "candidates": candidates,
    }


def rolling_mean_reversion_walk_forward(
    bars_by_symbol: dict[str, list[dict]],
    base: RegimeIntradayConfig | None = None,
    *,
    train_sessions: int = 20,
    test_sessions: int = 5,
    risk_dollars: float = 100.0,
    round_trip_cost_r: float = 0.12,
) -> dict[str, Any]:
    """Repeat bounded tuning across chronological, non-overlapping test windows."""
    cfg = base or RegimeIntradayConfig()
    dates = sorted(split_sessions(bars_by_symbol, cfg.symbols))
    train_size, test_size = max(5, int(train_sessions)), max(1, int(test_sessions))
    folds = []
    for test_start in range(train_size, len(dates), test_size):
        test_end = min(len(dates), test_start + test_size)
        if test_end <= test_start:
            continue
        chosen = set(dates[test_start - train_size:test_end])
        window = {symbol: [row for row in bars_by_symbol.get(symbol, []) if _dt(row).date().isoformat() in chosen] for symbol in cfg.symbols}
        result = mean_reversion_walk_forward(
            window,
            cfg,
            train_fraction=train_size / (train_size + (test_end - test_start)),
            risk_dollars=risk_dollars,
            round_trip_cost_r=round_trip_cost_r,
        )
        folds.append({"train_start": dates[test_start - train_size], "train_end": dates[test_start - 1], "test_start": dates[test_start], "test_end": dates[test_end - 1], **result})
    ready_folds = [fold for fold in folds if fold.get("ready")]
    positive = [fold for fold in ready_folds if fold.get("out_of_sample_positive")]
    return {
        "ready": bool(ready_folds),
        "fold_count": len(folds),
        "ready_fold_count": len(ready_folds),
        "positive_fold_count": len(positive),
        "positive_fold_fraction": round(len(positive) / len(ready_folds), 4) if ready_folds else None,
        "folds": folds,
    }


def chronological_holdout(
    bars_by_symbol: dict[str, list[dict]],
    config: RegimeIntradayConfig,
    *,
    train_fraction: float = 0.7,
    risk_dollars: float = 100.0,
) -> dict[str, Any]:
    """Evaluate frozen parameters on an untouched chronological final segment."""
    dates = sorted(split_sessions(bars_by_symbol, config.symbols))
    cut = max(1, min(len(dates) - 1, int(len(dates) * train_fraction))) if len(dates) > 1 else len(dates)

    def run(chosen: set[str]) -> dict[str, Any]:
        subset = {symbol: [row for row in bars_by_symbol.get(symbol, []) if _dt(row).date().isoformat() in chosen] for symbol in config.symbols}
        raw = replay_sessions(subset, config)
        return {key: value for key, value in raw.items() if key != "trades"} | {
            "cost_012": cost_adjusted_report(raw, risk_dollars=risk_dollars, round_trip_cost_r=0.12),
            "cost_030": cost_adjusted_report(raw, risk_dollars=risk_dollars, round_trip_cost_r=0.30),
        }

    return {
        "train_sessions": cut,
        "test_sessions": len(dates) - cut,
        "parameters_frozen": True,
        "train": run(set(dates[:cut])),
        "test": run(set(dates[cut:])),
    }
