"""Venue-neutral, execution-disabled sports and prediction arbitrage math."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class OutcomeQuote:
    outcome: str
    venue: str
    decimal_odds: float
    max_stake: float
    commission_rate: float = 0.0

    @property
    def effective_decimal_odds(self) -> float:
        return 1.0 + (self.decimal_odds - 1.0) * (1.0 - self.commission_rate)


def american_to_decimal(odds: float) -> float:
    if odds == 0:
        raise ValueError("American odds cannot be zero")
    return 1.0 + (odds / 100.0 if odds > 0 else 100.0 / abs(odds))


def scan_arbitrage(
    quotes: list[OutcomeQuote],
    *,
    bankroll: float,
    stake_increment: float = 0.01,
    minimum_profit: float = 0.01,
    rules_compatible: bool = False,
) -> dict:
    if len(quotes) < 2:
        raise ValueError("at least two outcome quotes are required")
    if bankroll <= 0 or stake_increment <= 0:
        raise ValueError("bankroll and stake_increment must be positive")
    outcomes = [quote.outcome.strip() for quote in quotes]
    if any(not outcome for outcome in outcomes) or len(set(outcomes)) != len(outcomes):
        raise ValueError("each mutually exclusive outcome must appear exactly once")
    for quote in quotes:
        if quote.decimal_odds <= 1 or quote.max_stake <= 0:
            raise ValueError("decimal_odds must exceed 1 and max_stake must be positive")
        if not 0 <= quote.commission_rate < 1:
            raise ValueError("commission_rate must be between 0 and 1")

    inverse_sum = sum(1.0 / quote.effective_decimal_odds for quote in quotes)
    theoretical_edge_pct = (1.0 / inverse_sum - 1.0) * 100.0
    maximum_total = min(
        bankroll,
        *(quote.max_stake * quote.effective_decimal_odds * inverse_sum for quote in quotes),
    )
    stakes = [math.floor((maximum_total / (quote.effective_decimal_odds * inverse_sum)) / stake_increment) * stake_increment for quote in quotes]
    total_stake = sum(stakes)
    legs = []
    payouts = []
    for quote, stake in zip(quotes, stakes):
        payout = stake * quote.effective_decimal_odds
        payouts.append(payout)
        legs.append({
            "outcome": quote.outcome,
            "venue": quote.venue,
            "decimal_odds": quote.decimal_odds,
            "effective_decimal_odds": round(quote.effective_decimal_odds, 6),
            "commission_rate": quote.commission_rate,
            "stake": round(stake, 2),
            "net_payout_if_wins": round(payout, 2),
        })
    worst_payout = min(payouts)
    guaranteed_profit = worst_payout - total_stake
    guaranteed_roi_pct = guaranteed_profit / total_stake * 100.0 if total_stake else 0.0
    blockers = []
    if inverse_sum >= 1:
        blockers.append("no_theoretical_arbitrage")
    if guaranteed_profit < minimum_profit:
        blockers.append("rounded_guaranteed_profit_below_minimum")
    if not rules_compatible:
        blockers.append("market_rules_not_confirmed_compatible")
    return {
        "strategy": "sports_prediction_arb",
        "outcome_count": len(quotes),
        "venue_count": len({quote.venue for quote in quotes}),
        "implied_probability_sum": round(inverse_sum, 8),
        "theoretical_edge_pct": round(theoretical_edge_pct, 6),
        "total_stake": round(total_stake, 2),
        "worst_case_payout": round(worst_payout, 2),
        "guaranteed_profit": round(guaranteed_profit, 2),
        "guaranteed_roi_pct": round(guaranteed_roi_pct, 6),
        "legs": legs,
        "eligible": not blockers,
        "blockers": blockers,
        "rules_compatible": rules_compatible,
        "execution_enabled": False,
    }
