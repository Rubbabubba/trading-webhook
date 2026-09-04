from opportunity_lab.triangular_crypto import scan_triangular


def _book(symbol, bid, ask, size=10000):
    return {"symbol": symbol, "bids": [{"price": bid, "size": size}], "asks": [{"price": ask, "size": size}]}


def test_triangle_finds_profitable_forward_cycle_without_fees():
    books = {"BTC/USD": _book("BTC/USD", 99, 100), "ETH/USD": _book("ETH/USD", 59, 60),
             "ETH/BTC": _book("ETH/BTC", .49, .5)}
    result = scan_triangular(books, starting_usd=1000, taker_fee=0)
    assert result["best_cycle"]["cycle"] == "USD_BTC_ETH_USD"
    assert result["best_cycle"]["final_usd"] == 1180
    assert result["best_cycle"]["profitable"] is True
    assert result["execution_enabled"] is False


def test_three_fees_compound_and_can_consume_edge():
    books = {"BTC/USD": _book("BTC/USD", 99, 100), "ETH/USD": _book("ETH/USD", 51, 52),
             "ETH/BTC": _book("ETH/BTC", .49, .5)}
    result = scan_triangular(books, starting_usd=1000, taker_fee=.008)
    assert result["best_cycle"]["net_profit_usd"] < 0
    assert result["profitable_cycle_count"] == 0


def test_inadequate_depth_marks_cycle_incomplete():
    books = {"BTC/USD": _book("BTC/USD", 99, 100, size=.001), "ETH/USD": _book("ETH/USD", 51, 52),
             "ETH/BTC": _book("ETH/BTC", .49, .5)}
    result = scan_triangular(books, starting_usd=1000, taker_fee=0)
    forward = next(row for row in result["cycles"] if row["cycle"] == "USD_BTC_ETH_USD")
    assert forward["complete"] is False
    assert forward["profitable"] is False
