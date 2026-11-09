from fractions import Fraction

import pytest

from opportunity_lab.kalshi_wallet_monitor import price_book


def test_production_book_keeps_displayed_depth_above_accounting_limit():
    quote = {"orderbook_fp": {
        "yes_dollars": [["0.6100", "263844.13"]],
        "no_dollars": [["0.3700", "2644760.78"], ["0.3800", "5779587.72"]],
    }}
    assert price_book(quote, "yes") == tuple(map(
        Fraction, ("0.61", "0.62", "263844.13", "5779587.72")))
    assert price_book(quote, "no") == tuple(map(
        Fraction, ("0.38", "0.39", "5779587.72", "263844.13")))


@pytest.mark.parametrize("no_levels", [
    [],
    [["0.38", "0"]],
    [["0.38", "NaN"]],
    [["0.38", "1"], ["0.38", "2"]],
    [["0.39", "10"]],
])
def test_invalid_public_depth_stays_unavailable(no_levels):
    quote = {"orderbook_fp": {"yes_dollars": [["0.61", "10"]], "no_dollars": no_levels}}
    with pytest.raises(ValueError):
        price_book(quote, "yes")
