from fractions import Fraction
from types import SimpleNamespace

import pytest

from opportunity_lab.kalshi_wallet_demo_exit import close_evidence, exit_limit, matching_sale


@pytest.fixture
def copied_sale():
    entry = {"proxy_wallet": "wallet-a", "condition_id": "game-a", "token_id": "yes-token"}
    return entry, {**entry, "side": "SELL", "type": "TRADE", "timestamp": 110,
                   "observed_at": 111, "size": 50, "price": "0.60"}


@pytest.mark.parametrize(("change", "now", "accepted"), [
    ({}, 112, True),
    ({"proxy_wallet": "wallet-b"}, 112, False),
    ({"token_id": "no-token"}, 112, False),
    ({"condition_id": "game-b"}, 112, False),
    ({"type": "REDEEM"}, 112, False),
    ({"timestamp": 100}, 112, False),
    ({}, 139, False),
])
def test_exit_requires_a_new_sell_on_the_copied_wallet_and_outcome(copied_sale, change, now, accepted):
    entry, sale = copied_sale
    assert matching_sale(entry, {**sale, **change}, filled_at=103, now=now, offsets=(-.5, .5)) is accepted


@pytest.mark.parametrize(("remaining", "last_event_at", "result"), [
    (15, 110, "trim"),
    (0, 110, "close"),
    (0, 109, "unknown"),
])
def test_only_a_verified_full_close_can_reduce_the_demo_position(copied_sale, remaining, last_event_at, result):
    _, sale = copied_sale
    position = {key: sale[key] for key in ("proxy_wallet", "condition_id", "token_id")}

    def read(_, params):
        assert params["filter_amount"] == 0
        assert params["user"] == sale["proxy_wallet"]
        status = params["status"]
        row = {**position, "current_size": remaining, "last_event_at": last_event_at, "status": status}
        data = [row] if status == ("OPEN" if remaining else "CLOSED") else []
        return {"data": data, "pagination": {"has_more": False, "next_cursor": None}}, 111, 112, None, None

    if result == "unknown":
        with pytest.raises(ValueError, match="wallet_exit_full_close_unverified"):
            close_evidence(sale, read)
    elif result == "trim":
        assert close_evidence(sale, read) is None
    else:
        assert close_evidence(sale, read)["closed_position"]["current_size"] == 0


def test_exit_uses_the_demo_bid_net_of_fees(copied_sale):
    _, sale = copied_sale
    quote = {"environment": "demo", "market": {"status": "active"},
             "side": "yes", "started_at": 111, "observed_at": 112}
    monitor = SimpleNamespace(price_book=lambda *_: (Fraction(58, 100), Fraction(61, 100), 1, 2))
    assert exit_limit(sale, quote, now=113, fee=2, monitor=monitor) == 58
    with pytest.raises(ValueError, match="wallet_exit_price_or_depth_guard"):
        exit_limit({**sale, "price": "0.6001"}, quote, now=113, fee=2, monitor=monitor)
