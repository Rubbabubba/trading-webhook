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
