"""Long/flat crypto regime model and cost-aware chronological replay."""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass, replace
from statistics import fmean, median, pstdev


@dataclass(frozen=True)
class CryptoRegimeConfig:
    fast_window: int = 24
    slow_window: int = 96
    volatility_window: int = 48
    max_hourly_volatility: float = 0.025
    stop_loss_pct: float = 0.04
    trailing_stop_pct: float = 0.06
    maker_fee_bps: float = 15.0
    slippage_bps: float = 3.0


PARAMETER_GRID = (
    (12, 72, .03, .05),
    (24, 96, .04, .06),
    (24, 168, .04, .08),
    (48, 168, .05, .08),
)


def _drawdown(returns: list[float]) -> float:
    equity = peak = 1.0
    worst = 0.0
    for value in returns:
        equity *= 1 + value
        peak = max(peak, equity)
        worst = max(worst, 1 - equity / peak)
    return worst


def replay_crypto_regime(bars: list[dict], config: CryptoRegimeConfig | None = None) -> dict:
    """Replay completed bars only; signals at bar i fill at bar i+1 open."""
    cfg = config or CryptoRegimeConfig()
    rows = sorted((dict(row) for row in bars), key=lambda row: row["ts_utc"])
    closes = [float(row["close"]) for row in rows]
    minimum = max(cfg.slow_window, cfg.volatility_window) + 1
    position = None
    trades: list[dict] = []
    fee_rate = (cfg.maker_fee_bps + cfg.slippage_bps) / 10_000.0
    for index in range(minimum, len(rows) - 1):
        fast = fmean(closes[index - cfg.fast_window + 1:index + 1])
        slow = fmean(closes[index - cfg.slow_window + 1:index + 1])
        hourly_returns = [closes[j] / closes[j - 1] - 1 for j in range(index - cfg.volatility_window + 2, index + 1)]
        volatility = pstdev(hourly_returns) if len(hourly_returns) > 1 else 0.0
        next_row = rows[index + 1]
        fill = float(next_row["open"])
        bullish = fast > slow and volatility <= cfg.max_hourly_volatility
        if position is None and bullish:
            position = {"entry": fill * (1 + fee_rate), "entry_ts": next_row["ts_utc"], "peak": fill}
            continue
        if position is None:
            continue
        position["peak"] = max(float(position["peak"]), float(rows[index]["high"]))
        stop = float(position["entry"]) * (1 - cfg.stop_loss_pct)
        trail = float(position["peak"]) * (1 - cfg.trailing_stop_pct)
        stop_price = max(stop, trail)
        stop_hit = float(next_row["low"]) <= stop_price
        if stop_hit or not bullish:
            raw_exit = min(fill, stop_price) if stop_hit else fill
            exit_price = raw_exit * (1 - fee_rate)
            net_return = exit_price / float(position["entry"]) - 1
            trades.append({"entry_ts": position["entry_ts"].isoformat(), "exit_ts": next_row["ts_utc"].isoformat(), "entry_price": round(float(position["entry"]), 8), "exit_price": round(exit_price, 8), "net_return": net_return, "reason": "stop" if stop_hit else "regime_exit"})
            position = None
    values = [float(row["net_return"]) for row in trades]
    years = max((rows[-1]["ts_utc"] - rows[0]["ts_utc"]).total_seconds() / (365.25 * 86400), 1 / 365.25) if len(rows) > 1 else 0
    compounded = 1.0
    for value in values:
        compounded *= 1 + value
    return {
        "ok": True, "bar_count": len(rows), "trade_count": len(trades), "config": asdict(cfg),
        "net_return": round(compounded - 1, 6), "annualized_return": round(compounded ** (1 / years) - 1, 6) if years and compounded > 0 else None,
        "win_rate": round(sum(value > 0 for value in values) / len(values), 4) if values else None,
        "average_trade": round(fmean(values), 6) if values else None, "max_drawdown": round(_drawdown(values), 6),
        "trades": trades,
    }


def _rank_candidates(bars: list[dict], *, maker_fee_bps: float = 15.0, slippage_bps: float = 3.0) -> list[dict]:
    candidates = []
    for fast, slow, stop, trail in PARAMETER_GRID:
        cfg = replace(CryptoRegimeConfig(), fast_window=fast, slow_window=slow, stop_loss_pct=stop, trailing_stop_pct=trail, maker_fee_bps=maker_fee_bps, slippage_bps=slippage_bps)
        report = replay_crypto_regime(bars, cfg)
        score = float(report["net_return"]) - float(report["max_drawdown"])
        candidates.append({"config": cfg, "score": round(score, 6), "report": report})
    return sorted(candidates, key=lambda row: (row["score"], row["report"]["trade_count"]), reverse=True)


def walk_forward_crypto(bars: list[dict], *, train_fraction: float = 0.7) -> dict:
    """Select a bounded grid on the oldest data and report untouched newer data."""
    ordered = sorted(bars, key=lambda row: row["ts_utc"])
    cut = max(1, min(len(ordered) - 1, int(len(ordered) * train_fraction))) if len(ordered) > 1 else len(ordered)
    train, test = ordered[:cut], ordered[cut:]
    candidates = _rank_candidates(train)
    selected = candidates[0] if candidates else {"config": CryptoRegimeConfig(), "report": {}}
    test_report = replay_crypto_regime(test, selected["config"])
    stability = []
    for row in candidates:
        result = replay_crypto_regime(test, row["config"])
        stability.append({"config": asdict(row["config"]), "train_score": row["score"], "test_net_return": result["net_return"], "test_max_drawdown": result["max_drawdown"], "test_trade_count": result["trade_count"]})
    return {"train_bars": len(train), "test_bars": len(test), "selected_config": asdict(selected["config"]), "train": selected["report"], "test": test_report, "out_of_sample_positive": bool(test_report.get("net_return", 0) > 0), "candidate_count": len(candidates), "parameter_stability": stability}


def buy_and_hold_benchmark(bars: list[dict], *, fee_bps: float = 15.0, slippage_bps: float = 3.0) -> dict:
    rows = sorted(bars, key=lambda row: row["ts_utc"])
    if len(rows) < 2:
        return {"bar_count": len(rows), "net_return": None}
    friction = (fee_bps + slippage_bps) / 10_000.0
    entry = float(rows[0]["open"]) * (1 + friction)
    exit_price = float(rows[-1]["close"]) * (1 - friction)
    return {"bar_count": len(rows), "entry_ts": rows[0]["ts_utc"].isoformat(), "exit_ts": rows[-1]["ts_utc"].isoformat(), "net_return": round(exit_price / entry - 1, 6)}


def rolling_walk_forward_crypto(bars: list[dict], *, folds: int = 5) -> dict:
    """Repeated expanding-window selection with non-overlapping forward tests."""
    ordered = sorted(bars, key=lambda row: row["ts_utc"])
    fold_count = max(2, min(10, int(folds)))
    initial_train = int(len(ordered) * .5)
    test_size = max(1, (len(ordered) - initial_train) // fold_count)
    results = []
    for number in range(fold_count):
        test_start = initial_train + number * test_size
        test_end = len(ordered) if number == fold_count - 1 else min(len(ordered), test_start + test_size)
        if test_start >= len(ordered) or test_end <= test_start:
            continue
        train, test = ordered[:test_start], ordered[test_start:test_end]
        ranked = _rank_candidates(train)
        selected = ranked[0]
        report = replay_crypto_regime(test, selected["config"])
        results.append({"fold": number + 1, "train_bars": len(train), "test_bars": len(test), "test_start": test[0]["ts_utc"].isoformat(), "test_end": test[-1]["ts_utc"].isoformat(), "selected_config": asdict(selected["config"]), "net_return": report["net_return"], "max_drawdown": report["max_drawdown"], "trade_count": report["trade_count"]})
    positive = sum(float(row["net_return"]) > 0 for row in results)
    return {"fold_count": len(results), "positive_fold_count": positive, "positive_fold_fraction": round(positive / len(results), 4) if results else None, "aggregate_net_return": round(sum(float(row["net_return"]) for row in results), 6), "folds": results}


def cost_sensitivity(bars: list[dict], config: CryptoRegimeConfig) -> list[dict]:
    scenarios = (("optimistic", 10.0, 1.0), ("base", 15.0, 3.0), ("taker_like", 25.0, 8.0), ("stressed", 35.0, 15.0))
    output = []
    for name, fee, slippage in scenarios:
        report = replay_crypto_regime(bars, replace(config, maker_fee_bps=fee, slippage_bps=slippage))
        output.append({"scenario": name, "fee_bps_per_side": fee, "slippage_bps_per_side": slippage, "net_return": report["net_return"], "max_drawdown": report["max_drawdown"], "trade_count": report["trade_count"]})
    return output


def monte_carlo_trades(trades: list[dict], *, simulations: int = 2000, seed: int = 17) -> dict:
    values = [float(row["net_return"]) for row in trades]
    if not values:
        return {"simulation_count": 0, "trade_count": 0, "reason": "no_closed_trades"}
    rng = random.Random(seed)
    totals, drawdowns = [], []
    for _ in range(max(100, min(10_000, int(simulations)))):
        sample = [rng.choice(values) for _ in values]
        equity = 1.0
        for value in sample:
            equity *= 1 + value
        totals.append(equity - 1)
        drawdowns.append(_drawdown(sample))
    totals.sort()
    drawdowns.sort()
    percentile = lambda rows, p: rows[min(len(rows) - 1, int((len(rows) - 1) * p))]
    return {"simulation_count": len(totals), "trade_count": len(values), "seed": seed, "median_net_return": round(median(totals), 6), "p05_net_return": round(percentile(totals, .05), 6), "p95_net_return": round(percentile(totals, .95), 6), "p95_max_drawdown": round(percentile(drawdowns, .95), 6), "loss_probability": round(sum(value < 0 for value in totals) / len(totals), 4)}


def crypto_research_suite(bars: list[dict]) -> dict:
    holdout = walk_forward_crypto(bars)
    selected = CryptoRegimeConfig(**holdout["selected_config"])
    test = holdout["test"]
    return {"bar_count": len(bars), "benchmark": buy_and_hold_benchmark(bars), "chronological_holdout": holdout, "rolling_walk_forward": rolling_walk_forward_crypto(bars), "cost_sensitivity_on_holdout": cost_sensitivity(sorted(bars, key=lambda row: row["ts_utc"])[holdout["train_bars"]:], selected), "monte_carlo_holdout_trades": monte_carlo_trades(test.get("trades") or []), "research_only": True, "execution_enabled": False}
