from datetime import datetime, timezone, timedelta

from opportunity_lab.kalshi_demo_v4_worker import (
    limit_price_cents, one_contract_frame, select_markets,
)


def quote(ticker="T"):
    at = datetime.now(timezone.utc).timestamp()
    return {
        "environment": "demo", "ticker": ticker, "started_at": at,
        "observed_at": at,
        "orderbook_fp": {
            "yes_dollars": [["0.39", "5.00"]],
            "no_dollars": [["0.60", "6.00"]],
        },
    }


def test_aggressive_one_contract_limits_cover_yes_and_no_books():
    frame = one_contract_frame(quote(), book_id="book")
    assert limit_price_cents(frame, "yes", "buy") == 41
    assert limit_price_cents(frame, "yes", "sell") == 38
    assert limit_price_cents(frame, "no", "buy") == 62
    assert limit_price_cents(frame, "no", "sell") == 59


class Markets:
    def get(self, ticker=None, *, params=None):
        if ticker:
            return {"market": self.rows[0]}, 1.0, 1.1
        return {"markets": self.rows}, 1.0, 1.1

    def quote(self, payload):
        return quote(payload["ticker"])

    rows = [
        {
            "ticker": "GOOD", "event_ticker": "EVENT", "status": "active",
            "market_type": "binary", "exchange_index": 0, "volume_24h_fp": "10",
            "close_time": (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat(),
        },
        {
            "ticker": "OTHER-SHARD", "event_ticker": "EVENT2", "status": "active",
            "market_type": "binary", "exchange_index": 1, "volume_24h_fp": "20",
            "close_time": (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat(),
        },
    ]


def test_demo_selection_requires_shard_zero_active_two_sided_market():
    selected = select_markets(Markets(), now=datetime.now(timezone.utc).timestamp())
    assert [row["ticker"] for row in selected] == ["GOOD"]
