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
    return {
        "valid": True,
        "method": "hourly_close_proxy_for_published_cde_formula",
        "official_history": False,
        "limitations": [
            "uses hourly closes instead of Coinbase twenty-sample three-minute representative-price TWAP",
            "does not model missed fills, intrahour entry timing, margin calls, or liquidation",
        ],
        "aligned_hours": hours,
        "coverage_days": round((timestamps[-1] - timestamps[0]) / 86400.0, 3),
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
        "annualized_return_on_fully_collateralized_capital_pct": round((net_pnl_bps / 10_000.0 / 2.0) * (8760.0 / hours) * 100.0, 6),
        "profitable": net_pnl_bps > 0,
        "execution_enabled": False,
    }
