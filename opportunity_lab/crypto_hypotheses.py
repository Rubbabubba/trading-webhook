"""Distinct crypto breakout and BTC/ETH relative-strength research."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from .crypto_regime import CryptoRegimeConfig, replay_crypto_regime


BREAKOUT_GRID = tuple(
    CryptoRegimeConfig("breakout", 24, slow, vol, lookback, .01, max_vol, stop, trail)
    for slow in (72, 168)
    for vol in (24, 72)
    for lookback in (24, 72, 168)
    for max_vol in (.015, .025)
    for stop, trail in ((.03, .05), (.05, .09))
)


@dataclass(frozen=True)
class RelativeStrengthConfig:
    lookback: int = 72
    rebalance_hours: int = 24
    minimum_absolute_momentum: float = .01
    minimum_relative_edge: float = .005
    fee_bps: float = 15.0
    slippage_bps: float = 3.0


RELATIVE_GRID = tuple(
    RelativeStrengthConfig(lookback, rebalance, absolute, relative)
    for lookback in (24, 72, 168)
    for rebalance in (6, 24, 72)
    for absolute in (0, .01, .03)
    for relative in (.005, .02)
)


def crypto_hypothesis_walk_forward(btc: list[dict], eth: list[dict]) -> dict:
    breakout = {"BTC/USD": _breakout_walk_forward(btc), "ETH/USD": _breakout_walk_forward(eth)}
    relative = _relative_walk_forward(btc, eth)
    retained = [name for name, row in {**breakout, "btc_eth_relative_strength": relative}.items()
                if row.get("model_retained")]
    return {"strategy_families": ["volatility_filtered_breakout", "btc_eth_relative_strength"],
            "breakout": breakout, "relative_strength": relative, "retained_hypotheses": retained,
            "research_only": True, "execution_enabled": False}


def _breakout_walk_forward(bars: list[dict]) -> dict:
    ordered = sorted(bars, key=lambda row: row["ts_utc"])
    if len(ordered) < 500:
        return {"valid": False, "reason": "insufficient_bars", "model_retained": False}
    split = int(len(ordered) * 2 / 3)
    candidates = []
    for config in BREAKOUT_GRID:
        report = replay_crypto_regime(ordered[:split], config)
        candidates.append({"config": config, "report": report,
                           "score": report["net_return"] - report["max_drawdown"]})
    viable = [row for row in candidates if row["report"]["trade_count"] >= 8]
    selected = max(viable or candidates, key=lambda row: (row["score"], row["report"]["net_return"]))
    validation = replay_crypto_regime(ordered[split:], selected["config"])
    retained = selected["report"]["net_return"] > 0 and validation["trade_count"] >= 3 and validation["net_return"] > 0
    return {"valid": True, "grid_size": len(BREAKOUT_GRID), "selected_config": asdict(selected["config"]),
            "calibration": _compact(selected["report"]), "validation": _compact(validation),
            "model_retained": retained, "verdict": "retain_for_forward_validation" if retained else "continue_retuning"}


def replay_relative_strength(btc: list[dict], eth: list[dict], config: RelativeStrengthConfig) -> dict:
    btc_map = {row["ts_utc"]: row for row in btc}; eth_map = {row["ts_utc"]: row for row in eth}
    stamps = sorted(set(btc_map).intersection(eth_map))
    fee = (config.fee_bps + config.slippage_bps) / 10_000.0
    equity, peak, drawdown, position, trades = 1.0, 1.0, 0.0, None, []
    for index in range(config.lookback, len(stamps) - 1, config.rebalance_hours):
        now, prior, next_stamp = stamps[index], stamps[index - config.lookback], stamps[index + 1]
        momenta = {"BTC": float(btc_map[now]["close"]) / float(btc_map[prior]["close"]) - 1,
                   "ETH": float(eth_map[now]["close"]) / float(eth_map[prior]["close"]) - 1}
        strongest = max(momenta, key=momenta.get); weakest = min(momenta, key=momenta.get)
        desired = strongest if momenta[strongest] >= config.minimum_absolute_momentum and momenta[strongest] - momenta[weakest] >= config.minimum_relative_edge else None
        if desired == position:
            continue
        if position:
            exit_price = float((btc_map if position == "BTC" else eth_map)[next_stamp]["open"]) * (1 - fee)
            trade = trades[-1]; trade["exit_ts"] = next_stamp.isoformat(); trade["exit_price"] = exit_price
            trade["net_return"] = exit_price / trade["entry_price"] - 1
            equity *= 1 + trade["net_return"]; peak = max(peak, equity); drawdown = max(drawdown, 1 - equity / peak)
        position = desired
        if position:
            entry = float((btc_map if position == "BTC" else eth_map)[next_stamp]["open"]) * (1 + fee)
            trades.append({"symbol": position, "entry_ts": next_stamp.isoformat(), "entry_price": entry})
    closed = [row for row in trades if "net_return" in row]
    return {"bar_count": len(stamps), "trade_count": len(closed), "net_return": round(equity - 1, 6),
            "win_rate": round(sum(row["net_return"] > 0 for row in closed) / len(closed), 4) if closed else None,
            "max_drawdown": round(drawdown, 6), "config": asdict(config), "trades": closed}


def _relative_walk_forward(btc: list[dict], eth: list[dict]) -> dict:
    common = sorted(set(row["ts_utc"] for row in btc).intersection(row["ts_utc"] for row in eth))
    if len(common) < 500:
        return {"valid": False, "reason": "insufficient_aligned_bars", "model_retained": False}
    split_stamp = common[int(len(common) * 2 / 3)]
    btc_train = [row for row in btc if row["ts_utc"] < split_stamp]; btc_test = [row for row in btc if row["ts_utc"] >= split_stamp]
    eth_train = [row for row in eth if row["ts_utc"] < split_stamp]; eth_test = [row for row in eth if row["ts_utc"] >= split_stamp]
    candidates = []
    for config in RELATIVE_GRID:
        report = replay_relative_strength(btc_train, eth_train, config)
        candidates.append({"config": config, "report": report, "score": report["net_return"] - report["max_drawdown"]})
    viable = [row for row in candidates if row["report"]["trade_count"] >= 8]
    selected = max(viable or candidates, key=lambda row: (row["score"], row["report"]["net_return"]))
    validation = replay_relative_strength(btc_test, eth_test, selected["config"])
    retained = selected["report"]["net_return"] > 0 and validation["trade_count"] >= 3 and validation["net_return"] > 0
    return {"valid": True, "grid_size": len(RELATIVE_GRID), "selected_config": asdict(selected["config"]),
            "calibration": _compact(selected["report"]), "validation": _compact(validation),
            "model_retained": retained, "verdict": "retain_for_forward_validation" if retained else "continue_retuning"}


def _compact(report: dict) -> dict:
    return {key: report.get(key) for key in ("bar_count", "trade_count", "net_return", "annualized_return",
                                              "win_rate", "average_trade", "max_drawdown") if key in report}
