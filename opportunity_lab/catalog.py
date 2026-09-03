"""Locked candidate registry for the Opportunity Lab roadmap."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Candidate:
    key: str
    rank: int
    name: str
    category: str
    initial_mode: str
    execution_style: str
    required_adapters: tuple[str, ...]
    status: str
    rationale: str


CANDIDATES = (
    Candidate("crypto_basis", 1, "Crypto funding/basis arbitrage", "market_neutral", "backtest_shadow", "two_leg", ("crypto_spot", "crypto_derivatives"), "active", "Active market-neutral research candidate; requires a derivatives venue and matched two-leg accounting."),
    Candidate("sports_prediction_arb", 2, "Sports/prediction-market arbitrage scanner", "arbitrage", "monitor_alert", "multi_venue", ("odds_feed", "prediction_market"), "queued", "Good scanner fit; execution and settlement rules vary by venue."),
    Candidate("crypto_regime", 3, "Crypto regime trading", "directional", "backtest_shadow", "single_venue", ("alpaca_crypto",), "researched_rejected", "Rejected in current form after full-coverage hourly, four-hour, and daily validation failed out of sample."),
    Candidate("prediction_market_making", 4, "Prediction-market market making", "market_making", "backtest_shadow", "single_venue", ("prediction_market",), "queued", "API-friendly, but sensitive to liquidity and resolution risk."),
    Candidate("cross_exchange_crypto", 5, "Cross-exchange crypto arbitrage", "arbitrage", "monitor_only", "multi_venue", ("crypto_exchange_a", "crypto_exchange_b"), "queued", "Requires pre-funded inventory and careful transfer accounting."),
    Candidate("triangular_crypto", 6, "Triangular crypto arbitrage", "arbitrage", "monitor_only", "three_leg", ("crypto_exchange_orderbook",), "queued", "Must clear three-leg fees, depth, latency, and fill risk."),
    Candidate("matched_betting", 7, "Matched betting/promotions", "promotion", "manual_assist", "multi_venue", ("sportsbook_offers", "hedge_venue"), "queued", "Potentially attractive but finite and account-dependent."),
)


def candidate_catalog() -> list[dict]:
    return [asdict(candidate) for candidate in CANDIDATES]
