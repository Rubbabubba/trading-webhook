from opportunity_lab.cross_exchange_crypto import scan_cross_exchange


def _book(venue, bid, ask, size=1):
    return {"venue": venue, "bids": [{"price": bid, "size": size}], "asks": [{"price": ask, "size": size}]}


def test_scanner_rejects_headline_spread_consumed_by_fees():
    result = scan_cross_exchange("BTC", _book("coinbase", 100, 101), _book("kraken", 103, 104),
                                 max_notional=101, coinbase_taker_fee=.012, kraken_taker_fee=.008)
    best = result["best_direction"]
    assert best["gross_spread_profit"] == 2
    assert best["net_profit"] < 0
    assert result["profitable_direction_count"] == 0
    assert result["execution_enabled"] is False


def test_scanner_sweeps_depth_and_caps_notional():
    coinbase = {"venue": "coinbase", "asks": [{"price": 100, "size": 1}, {"price": 102, "size": 2}], "bids": [{"price": 99, "size": 3}]}
    kraken = {"venue": "kraken", "bids": [{"price": 105, "size": 1.5}, {"price": 104, "size": 1}], "asks": [{"price": 106, "size": 3}]}
    best = scan_cross_exchange("ETH", coinbase, kraken, max_notional=202, coinbase_taker_fee=0, kraken_taker_fee=0)["best_direction"]
    assert best["base_quantity"] == 2
    assert best["buy_cost"] == 202
    assert best["sell_proceeds"] == 209.5
    assert best["profitable"] is True
