"""Venue-neutral economics and backtesting for long-spot/short-derivative carry."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import mean
from typing import Iterable


@dataclass(frozen=True)
class BasisInputs:
    spot_ask: float
    derivative_bid: float
    funding_rate_bps: float
    funding_interval_hours: float = 8.0
    holding_hours: float = 24.0
    expected_exit_basis_bps: float = 0.0
    spot_round_trip_fee_bps: float = 50.0
    derivative_round_trip_fee_bps: float = 12.0
    round_trip_slippage_bps: float = 10.0
    derivative_leverage: float = 1.0
    available_capital: float = 0.0
    spot_ask_size: float = 0.0
    derivative_bid_size: float = 0.0
    minimum_net_bps: float = 5.0
    minimum_annualized_return_pct: float = 0.0


def evaluate_basis(inputs: BasisInputs) -> dict:
    """Evaluate an executable long-spot/short-derivative carry entry.

    Positive funding means shorts receive funding. Prices use the executable sides
    (spot ask and derivative bid), never mid prices.
    """
    _validate(inputs)
    entry_basis_bps = (inputs.derivative_bid / inputs.spot_ask - 1.0) * 10_000.0
    funding_events = inputs.holding_hours / inputs.funding_interval_hours
    expected_funding_bps = inputs.funding_rate_bps * funding_events
    expected_basis_capture_bps = entry_basis_bps - inputs.expected_exit_basis_bps
    estimated_cost_bps = (
        inputs.spot_round_trip_fee_bps
        + inputs.derivative_round_trip_fee_bps
        + inputs.round_trip_slippage_bps
    )
    gross_pnl_bps = expected_basis_capture_bps + expected_funding_bps
    net_pnl_bps = gross_pnl_bps - estimated_cost_bps

    capital_multiplier = 1.0 + (1.0 / inputs.derivative_leverage)
    max_notional_from_capital = inputs.available_capital / capital_multiplier if inputs.available_capital else 0.0
    depth_limits = [value for value in (inputs.spot_ask_size, inputs.derivative_bid_size) if value > 0]
    max_notional_from_depth = min(depth_limits) if depth_limits else 0.0
    limits = [value for value in (max_notional_from_capital, max_notional_from_depth) if value > 0]
    executable_notional = min(limits) if limits else 0.0
    expected_profit = executable_notional * net_pnl_bps / 10_000.0
    holding_days = inputs.holding_hours / 24.0
    annualized_return_pct = (net_pnl_bps / 10_000.0) / capital_multiplier * (365.0 / holding_days) * 100.0
    break_even_funding_bps = (estimated_cost_bps - expected_basis_capture_bps) / funding_events

    blockers: list[str] = []
    if executable_notional <= 0:
        blockers.append("capital_or_executable_depth_missing")
    if net_pnl_bps < inputs.minimum_net_bps:
        blockers.append("net_edge_below_minimum")
    if annualized_return_pct < inputs.minimum_annualized_return_pct:
        blockers.append("annualized_return_below_minimum")

    return {
        "strategy": "crypto_basis",
        "direction": "long_spot_short_derivative",
        "funding_sign_convention": "positive_rate_is_received_by_short",
        "entry_basis_bps": round(entry_basis_bps, 4),
        "expected_basis_capture_bps": round(expected_basis_capture_bps, 4),
        "expected_funding_bps": round(expected_funding_bps, 4),
        "gross_pnl_bps": round(gross_pnl_bps, 4),
        "estimated_cost_bps": round(estimated_cost_bps, 4),
        "net_pnl_bps": round(net_pnl_bps, 4),
        "break_even_funding_bps_per_interval": round(break_even_funding_bps, 4),
        "capital_required_per_notional": round(capital_multiplier, 6),
        "executable_notional": round(executable_notional, 2),
        "expected_profit": round(expected_profit, 2),
        "annualized_return_on_capital_pct": round(annualized_return_pct, 4),
        "eligible": not blockers,
        "blockers": blockers,
        "inputs": asdict(inputs),
        "execution_enabled": False,
    }


def backtest_funding(
    funding_rates_bps: Iterable[float],
    *,
    entry_basis_bps: float = 0.0,
    exit_basis_bps: float = 0.0,
    total_cost_bps: float = 72.0,
    derivative_leverage: float = 1.0,
) -> dict:
    """Backtest one completed carry holding period from normalized funding events."""
    rates = [float(rate) for rate in funding_rates_bps]
    if not rates:
        return {"valid": False, "reason": "no_funding_observations", "execution_enabled": False}
    if derivative_leverage < 1:
        raise ValueError("derivative_leverage must be at least 1")
    funding_bps = sum(rates)
    basis_capture_bps = float(entry_basis_bps) - float(exit_basis_bps)
    gross_pnl_bps = funding_bps + basis_capture_bps
    net_pnl_bps = gross_pnl_bps - float(total_cost_bps)
    capital_multiplier = 1.0 + 1.0 / derivative_leverage
    return {
        "valid": True,
        "observation_count": len(rates),
        "positive_funding_fraction": round(sum(rate > 0 for rate in rates) / len(rates), 6),
        "average_funding_bps": round(mean(rates), 6),
        "funding_pnl_bps": round(funding_bps, 4),
        "basis_capture_bps": round(basis_capture_bps, 4),
        "gross_pnl_bps": round(gross_pnl_bps, 4),
        "total_cost_bps": round(float(total_cost_bps), 4),
        "net_pnl_bps": round(net_pnl_bps, 4),
        "return_on_capital_pct": round((net_pnl_bps / 10_000.0) / capital_multiplier * 100.0, 6),
        "profitable": net_pnl_bps > 0,
        "execution_enabled": False,
    }


def _validate(inputs: BasisInputs) -> None:
    if inputs.spot_ask <= 0 or inputs.derivative_bid <= 0:
        raise ValueError("spot_ask and derivative_bid must be positive")
    if inputs.funding_interval_hours <= 0 or inputs.holding_hours <= 0:
        raise ValueError("funding_interval_hours and holding_hours must be positive")
    if inputs.derivative_leverage < 1:
        raise ValueError("derivative_leverage must be at least 1")
    for name in ("spot_round_trip_fee_bps", "derivative_round_trip_fee_bps", "round_trip_slippage_bps"):
        if getattr(inputs, name) < 0:
            raise ValueError(f"{name} cannot be negative")
