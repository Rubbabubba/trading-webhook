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
    Candidate("crypto_basis", 1, "Crypto funding/basis arbitrage", "market_neutral", "backtest_shadow", "two_leg", ("crypto_spot", "crypto_derivatives"), "research_retained", "ETH retained as long-duration research; BTC rejected and current fees prevent dependable short-horizon carry."),
    Candidate("sports_prediction_arb", 2, "Sports/prediction-market arbitrage scanner", "arbitrage", "monitor_alert", "multi_venue", ("odds_feed", "prediction_market"), "active", "Active scanner build; execution and settlement rules vary by venue."),
    Candidate("crypto_regime", 3, "Crypto regime trading", "directional", "backtest_shadow", "single_venue", ("alpaca_crypto",), "retuning", "Original formulation failed out of sample; candidate retained for genuinely different regime, volatility, and relative-strength hypotheses."),
    Candidate("prediction_market_making", 4, "Prediction-market market making", "market_making", "backtest_shadow", "single_venue", ("prediction_market",), "research", "Conservative public-quote screen active; fill history, queue position, series fees, and adverse selection remain unverified."),
    Candidate("cross_exchange_crypto", 5, "Cross-exchange crypto arbitrage", "arbitrage", "monitor_only", "multi_venue", ("coinbase_public_orderbook", "kraken_public_orderbook"), "research", "Depth-aware Coinbase/Kraken public monitor active; requires pre-funded inventory and careful transfer accounting."),
    Candidate("triangular_crypto", 6, "Triangular crypto arbitrage", "arbitrage", "monitor_only", "three_leg", ("kraken_public_orderbook",), "research", "Depth-aware Kraken BTC/USD, ETH/USD, and ETH/BTC cycle monitor active; must clear three fees, latency, and fill risk."),
    Candidate("matched_betting", 7, "Matched betting/promotions", "promotion", "manual_assist", "multi_venue", ("sportsbook_offers", "hedge_venue"), "queued", "Potentially attractive but finite and account-dependent."),
    Candidate("weather_prediction_value", 8, "Weather prediction-market value", "forecast_value", "monitor_only", "single_venue", ("nws_forecast", "kalshi_prediction_market"), "forward_validation", "Bias-corrected expanding-window GFS v2 passed its initial chronological holdout with a small edge; forward executable-price validation remains required."),
)


def candidate_catalog() -> list[dict]:
    return [asdict(candidate) for candidate in CANDIDATES]
