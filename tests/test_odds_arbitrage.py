import pytest

from opportunity_lab.odds_arbitrage import OutcomeQuote, american_to_decimal, scan_arbitrage


def test_american_odds_conversion():
    assert american_to_decimal(150) == pytest.approx(2.5)
    assert american_to_decimal(-200) == pytest.approx(1.5)


def test_two_way_arb_allocates_guaranteed_profit():
    result = scan_arbitrage([
        OutcomeQuote("A", "venue-a", 2.1, 1000),
        OutcomeQuote("B", "venue-b", 2.1, 1000),
    ], bankroll=1000, rules_compatible=True)
    assert result["eligible"] is True
    assert result["guaranteed_profit"] > 49
    assert result["guaranteed_roi_pct"] > 4.9


def test_commission_can_remove_arb():
    result = scan_arbitrage([
        OutcomeQuote("yes", "venue-a", 2.01, 100, .1),
        OutcomeQuote("no", "venue-b", 2.01, 100, .1),
    ], bankroll=100, rules_compatible=True)
    assert result["eligible"] is False
    assert "no_theoretical_arbitrage" in result["blockers"]


def test_unconfirmed_rules_block_apparent_arb():
    result = scan_arbitrage([
        OutcomeQuote("home", "a", 2.2, 100), OutcomeQuote("away", "b", 2.2, 100)
    ], bankroll=100, rules_compatible=False)
    assert result["eligible"] is False
    assert "market_rules_not_confirmed_compatible" in result["blockers"]


def test_duplicate_outcomes_rejected():
    with pytest.raises(ValueError):
        scan_arbitrage([OutcomeQuote("yes", "a", 2, 10), OutcomeQuote("yes", "b", 2, 10)], bankroll=10)
