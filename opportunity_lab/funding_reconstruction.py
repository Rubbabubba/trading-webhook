"""Approximate Coinbase CDE hourly funding from aligned hourly closes."""

from __future__ import annotations

from statistics import mean


def reconstruct_hourly_funding(future_candles: list[dict], spot_candles: list[dict], *, total_cost_bps: float = 72.0) -> dict:
    futures = {int(row["timestamp"]): float(row["close"]) for row in future_candles if row.get("close")}
    spots = {int(row["timestamp"]): float(row["close"]) for row in spot_candles if row.get("close")}
    timestamps = sorted(set(futures).intersection(spots))
    if len(timestamps) < 24:
        return {"valid": False, "reason": "insufficient_aligned_hourly_candles", "aligned_hours": len(timestamps), "execution_enabled": False}
    rates: list[float] = []
    previous = 0.0
    for timestamp in timestamps:
        premium = ((futures[timestamp] - spots[timestamp]) / spots[timestamp]) / 24.0
        rate = 0.75 * premium + 0.25 * previous
        rates.append(rate)
        previous = rate
    entry_basis_bps = (futures[timestamps[0]] / spots[timestamps[0]] - 1.0) * 10_000.0
    exit_basis_bps = (futures[timestamps[-1]] / spots[timestamps[-1]] - 1.0) * 10_000.0
    funding_bps = sum(rates) * 10_000.0
    basis_capture_bps = entry_basis_bps - exit_basis_bps
    net_pnl_bps = funding_bps + basis_capture_bps - float(total_cost_bps)
    hours = len(timestamps)
    elapsed_hours = max(1.0, (timestamps[-1] - timestamps[0]) / 3600.0)
    rolling = {
        str(days): _rolling_holds(timestamps, futures, spots, rates, days=days, total_cost_bps=float(total_cost_bps))
        for days in (7, 30, 90)
    }
    curve = []
    cumulative_funding_bps = 0.0
    initial_basis_bps = entry_basis_bps
    for timestamp, rate in zip(timestamps, rates):
        cumulative_funding_bps += rate * 10_000.0
        current_basis_bps = (futures[timestamp] / spots[timestamp] - 1.0) * 10_000.0
        curve.append(cumulative_funding_bps + initial_basis_bps - current_basis_bps - float(total_cost_bps))
    peak = curve[0]
    max_drawdown_bps = 0.0
    for value in curve:
        peak = max(peak, value)
        max_drawdown_bps = max(max_drawdown_bps, peak - value)
    return {
        "valid": True,
        "method": "hourly_close_proxy_for_published_cde_formula",
        "official_history": False,
        "limitations": [
            "uses hourly closes instead of Coinbase twenty-sample three-minute representative-price TWAP",
            "does not model missed fills, intrahour entry timing, margin calls, or liquidation",
        ],
        "aligned_hours": hours,
        "elapsed_hours": round(elapsed_hours, 3),
        "coverage_days": round(elapsed_hours / 24.0, 3),
        "observation_density": round(hours / (elapsed_hours + 1.0), 6),
        "first_timestamp": timestamps[0],
        "last_timestamp": timestamps[-1],
        "average_hourly_funding_bps": round(mean(rates) * 10_000.0, 6),
        "positive_funding_fraction": round(sum(rate > 0 for rate in rates) / hours, 6),
        "funding_pnl_bps": round(funding_bps, 4),
        "entry_basis_bps": round(entry_basis_bps, 4),
        "exit_basis_bps": round(exit_basis_bps, 4),
        "basis_capture_bps": round(basis_capture_bps, 4),
        "total_cost_bps": round(float(total_cost_bps), 4),
        "net_pnl_bps": round(net_pnl_bps, 4),
        "return_on_fully_collateralized_capital_pct": round(net_pnl_bps / 10_000.0 / 2.0 * 100.0, 6),
        "annualized_return_on_fully_collateralized_capital_pct": round((net_pnl_bps / 10_000.0 / 2.0) * (8760.0 / elapsed_hours) * 100.0, 6),
        "max_carry_curve_drawdown_bps": round(max_drawdown_bps, 4),
        "rolling_holding_periods": rolling,
        "profitable": net_pnl_bps > 0,
        "execution_enabled": False,
    }


def conditional_carry_walk_forward(future_candles: list[dict], spot_candles: list[dict], *,
                                   total_cost_bps: float = 139.0) -> dict:
    """Select a conditional carry rule on older data and test it on untouched history."""
    futures = {int(row["timestamp"]): float(row["close"]) for row in future_candles if row.get("close")}
    spots = {int(row["timestamp"]): float(row["close"]) for row in spot_candles if row.get("close")}
    timestamps = sorted(set(futures).intersection(spots))
    if len(timestamps) < 24 * 30:
        return {"valid": False, "reason": "insufficient_aligned_hourly_candles", "aligned_hours": len(timestamps),
                "execution_enabled": False}
    rates, bases, previous = [], [], 0.0
    for stamp in timestamps:
        basis = (futures[stamp] / spots[stamp] - 1.0) * 10_000.0
        rate = 0.75 * (basis / 10_000.0 / 24.0) + 0.25 * previous
        rates.append(rate * 10_000.0)
        bases.append(basis)
        previous = rate
    split = int(len(timestamps) * 2 / 3)
    grid = []
    for threshold in (.02, .05, .10, .20, .40):
        for persistence in (6, 12, 24):
            for hold in (24, 72, 168, 336):
                for maximum_basis in (25, 50, 100, 200):
                    trades = _conditional_trades(timestamps, rates, bases, 0, split, threshold=threshold,
                                                  persistence=persistence, hold=hold, maximum_basis=maximum_basis,
                                                  total_cost_bps=total_cost_bps)
                    grid.append({"minimum_hourly_funding_bps": threshold, "persistence_hours": persistence,
                                 "holding_hours": hold, "maximum_entry_basis_bps": maximum_basis,
                                 **_carry_summary(trades)})
    viable = [row for row in grid if row["trade_count"] >= 3]
    ranked = viable or grid
    ranked.sort(key=lambda row: (row["total_net_pnl_bps"], row["average_net_pnl_bps"] or -999,
                                 row["trade_count"]), reverse=True)
    selected = ranked[0]
    validation_trades = _conditional_trades(
        timestamps, rates, bases, split, len(timestamps), threshold=selected["minimum_hourly_funding_bps"],
        persistence=selected["persistence_hours"], hold=selected["holding_hours"],
        maximum_basis=selected["maximum_entry_basis_bps"], total_cost_bps=total_cost_bps)
    validation = _carry_summary(validation_trades)
    retained = (selected["total_net_pnl_bps"] > 0 and validation["trade_count"] >= 2
                and validation["total_net_pnl_bps"] > 0)
    return {"valid": True, "strategy": "conditional_long_spot_short_cde", "aligned_hours": len(timestamps),
            "total_cost_bps_per_trade": total_cost_bps, "grid_size": len(grid),
            "method": "parameter_grid_on_oldest_two_thirds_then_untouched_newest_third",
            "split_timestamp": timestamps[split],
            "selected_parameters": {key: selected[key] for key in ("minimum_hourly_funding_bps", "persistence_hours",
                                                                     "holding_hours", "maximum_entry_basis_bps")},
            "calibration": {key: value for key, value in selected.items() if key not in {
                "minimum_hourly_funding_bps", "persistence_hours", "holding_hours", "maximum_entry_basis_bps"}},
            "validation": validation, "validation_trades": validation_trades,
            "model_retained": retained, "verdict": "retain_for_forward_validation" if retained else "continue_retuning",
            "limitations": ["funding is reconstructed from hourly closes rather than official three-minute TWAP samples",
                            "maker-price fills and uninterrupted hedge availability are assumed",
                            "margin calls, liquidation, and tax treatment are not modeled"],
            "eligible": False, "execution_enabled": False}


def cost_recovery_carry_walk_forward(future_candles: list[dict], spot_candles: list[dict], *,
                                     total_cost_bps: float = 139.0) -> dict:
    """Test long-duration exits that wait for gross carry to recover costs."""
    timestamps, rates, bases = _carry_series(future_candles, spot_candles)
    if len(timestamps) < 24 * 120:
        return {"valid": False, "reason": "insufficient_aligned_hourly_candles", "aligned_hours": len(timestamps),
                "execution_enabled": False}
    split = int(len(timestamps) * 2 / 3)
    grid = []
    for threshold in (.02, .05, .10, .20):
        for persistence in (12, 24, 48):
            for maximum_hold in (24 * 30, 24 * 60, 24 * 90):
                for target in (10, 25, 50):
                    trades = _cost_recovery_trades(timestamps, rates, bases, 0, split, threshold=threshold,
                                                   persistence=persistence, maximum_hold=maximum_hold,
                                                   target_net_bps=target, total_cost_bps=total_cost_bps)
                    grid.append({"minimum_hourly_funding_bps": threshold, "persistence_hours": persistence,
                                 "maximum_holding_days": maximum_hold // 24, "target_net_bps": target,
                                 **_carry_summary(trades)})
    viable = [row for row in grid if row["trade_count"] >= 2]
    ranked = viable or grid
    ranked.sort(key=lambda row: (row["total_net_pnl_bps"], row["average_net_pnl_bps"] or -999), reverse=True)
    selected = ranked[0]
    validation_trades = _cost_recovery_trades(
        timestamps, rates, bases, split, len(timestamps), threshold=selected["minimum_hourly_funding_bps"],
        persistence=selected["persistence_hours"], maximum_hold=selected["maximum_holding_days"] * 24,
        target_net_bps=selected["target_net_bps"], total_cost_bps=total_cost_bps)
    validation = _carry_summary(validation_trades)
    retained = selected["total_net_pnl_bps"] > 0 and validation["trade_count"] >= 1 and validation["total_net_pnl_bps"] > 0
    return {"valid": True, "strategy": "long_duration_cost_recovery_eth_carry", "aligned_hours": len(timestamps),
            "total_cost_bps_per_trade": total_cost_bps, "grid_size": len(grid),
            "method": "parameter_grid_on_oldest_two_thirds_then_untouched_newest_third",
            "split_timestamp": timestamps[split],
            "selected_parameters": {key: selected[key] for key in ("minimum_hourly_funding_bps", "persistence_hours",
                                                                     "maximum_holding_days", "target_net_bps")},
            "calibration": {key: value for key, value in selected.items() if key not in {
                "minimum_hourly_funding_bps", "persistence_hours", "maximum_holding_days", "target_net_bps"}},
            "validation": validation, "validation_trades": validation_trades,
            "model_retained": retained, "verdict": "retain_for_forward_validation" if retained else "continue_retuning",
            "limitations": ["funding is reconstructed from hourly closes rather than official three-minute TWAP samples",
                            "positions are assumed continuously hedged and maker-filled on both legs",
                            "margin calls, liquidation, borrow constraints, and taxes are not modeled"],
            "eligible": False, "execution_enabled": False}


def _carry_series(future_candles: list[dict], spot_candles: list[dict]):
    futures = {int(row["timestamp"]): float(row["close"]) for row in future_candles if row.get("close")}
    spots = {int(row["timestamp"]): float(row["close"]) for row in spot_candles if row.get("close")}
    timestamps = sorted(set(futures).intersection(spots))
    rates, bases, previous = [], [], 0.0
    for stamp in timestamps:
        basis = (futures[stamp] / spots[stamp] - 1.0) * 10_000.0
        rate = 0.75 * (basis / 10_000.0 / 24.0) + 0.25 * previous
        rates.append(rate * 10_000.0); bases.append(basis); previous = rate
    return timestamps, rates, bases


def _cost_recovery_trades(timestamps, rates, bases, start, stop, *, threshold, persistence,
                          maximum_hold, target_net_bps, total_cost_bps):
    trades, index = [], max(start, persistence - 1)
    while index + 1 < stop:
        if min(rates[index - persistence + 1:index + 1]) < threshold:
            index += 1
            continue
        entry, cumulative = index, 0.0
        final = min(entry + maximum_hold, stop - 1)
        exit_reason = "maximum_hold"
        for current in range(entry, final + 1):
            cumulative += rates[current]
            gross = cumulative + bases[entry] - bases[current]
            if current > entry and gross >= total_cost_bps + target_net_bps:
                final, exit_reason = current, "cost_and_profit_target_recovered"
                break
        gross = cumulative + bases[entry] - bases[final]
        net = gross - total_cost_bps
        trades.append({"entry_timestamp": timestamps[entry], "exit_timestamp": timestamps[final],
                       "holding_hours": final - entry, "exit_reason": exit_reason,
                       "entry_basis_bps": round(bases[entry], 4), "exit_basis_bps": round(bases[final], 4),
                       "funding_pnl_bps": round(cumulative, 4), "gross_pnl_bps": round(gross, 4),
                       "net_pnl_bps": round(net, 4)})
        index = final + 1
    return trades


def _conditional_trades(timestamps, rates, bases, start, stop, *, threshold, persistence, hold,
                        maximum_basis, total_cost_bps):
    trades, index = [], max(start, persistence - 1)
    while index + hold < stop:
        recent = rates[index - persistence + 1:index + 1]
        if min(recent) >= threshold and bases[index] <= maximum_basis:
            end = index + hold
            funding = sum(rates[index:end + 1])
            pnl = funding + bases[index] - bases[end] - total_cost_bps
            trades.append({"entry_timestamp": timestamps[index], "exit_timestamp": timestamps[end],
                           "entry_basis_bps": round(bases[index], 4), "exit_basis_bps": round(bases[end], 4),
                           "funding_pnl_bps": round(funding, 4), "net_pnl_bps": round(pnl, 4)})
            index = end + 1
        else:
            index += 1
    return trades


def _carry_summary(trades: list[dict]) -> dict:
    pnl = [float(row["net_pnl_bps"]) for row in trades]
    cumulative = peak = drawdown = 0.0
    for value in pnl:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return {"trade_count": len(trades), "profitable_trade_count": sum(value > 0 for value in pnl),
            "win_rate": round(sum(value > 0 for value in pnl) / len(pnl), 6) if pnl else None,
            "total_net_pnl_bps": round(sum(pnl), 4),
            "average_net_pnl_bps": round(mean(pnl), 4) if pnl else None,
            "return_on_fully_collateralized_capital_pct": round(sum(pnl) / 200.0, 6),
            "maximum_trade_curve_drawdown_bps": round(drawdown, 4)}


def _rolling_holds(timestamps, futures, spots, rates, *, days: int, total_cost_bps: float) -> dict:
    horizon = days * 86400
    cumulative = [0.0]
    for rate in rates:
        cumulative.append(cumulative[-1] + rate * 10_000.0)
    outcomes = []
    end_index = 0
    for start_index in range(0, len(timestamps), 24):
        target = timestamps[start_index] + horizon
        end_index = max(end_index, start_index + 1)
        while end_index < len(timestamps) and timestamps[end_index] < target:
            end_index += 1
        if end_index >= len(timestamps):
            break
        entry_basis = (futures[timestamps[start_index]] / spots[timestamps[start_index]] - 1.0) * 10_000.0
        exit_basis = (futures[timestamps[end_index]] / spots[timestamps[end_index]] - 1.0) * 10_000.0
        funding = cumulative[end_index + 1] - cumulative[start_index]
        outcomes.append(funding + entry_basis - exit_basis - total_cost_bps)
    if not outcomes:
        return {"sample_count": 0, "reason": "insufficient_history"}
    ordered = sorted(outcomes)
    return {
        "sample_count": len(outcomes),
        "profitable_fraction": round(sum(value > 0 for value in outcomes) / len(outcomes), 6),
        "average_net_pnl_bps": round(mean(outcomes), 4),
        "median_net_pnl_bps": round(ordered[len(ordered) // 2], 4),
        "minimum_net_pnl_bps": round(ordered[0], 4),
        "maximum_net_pnl_bps": round(ordered[-1], 4),
    }
