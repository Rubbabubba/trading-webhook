from datetime import datetime, timezone
import hashlib

from opportunity_lab.kalshi_live_preflight import check


class Account:
    key_id = "synthetic-key"
    def __init__(self): self.paths = []
    def get(self, path, params):
        self.paths.append(path)
        assert path == "/portfolio/balance" and params == {"subaccount": 0}
        return {"balance": 4500, "portfolio_value": 0}
    def pages(self, path, field, **params):
        self.paths.append(path)
        assert path in ("/portfolio/positions", "/portfolio/orders")
        return []


class Markets:
    def get(self, **params):
        return {"markets": [{"market_type": "binary", "exchange_index": 0,
                             "ticker": "T", "event_ticker": "EVENT"}]}, 0, 0
    def quote(self, payload):
        return {"environment": "production", "observed_at": datetime.now(timezone.utc).timestamp(),
                "orderbook_fp": {"yes_dollars": [[".06", "1"]], "no_dollars": [[".92", "1"]]}}
    def get_event(self, event):
        return {"event": {"event_ticker": event, "series_ticker": "SERIES"}}, 0, datetime.now(timezone.utc).timestamp()
    def get_series(self, series):
        return {"series": {"ticker": series, "fee_type": "quadratic", "fee_multiplier": 1}}, 0, datetime.now(timezone.utc).timestamp()


def test_preflight_uses_only_reads_reports_fingerprint_and_never_authorizes():
    account = Account()
    result = check(account, Markets())
    assert account.paths == ["/portfolio/balance", "/portfolio/positions", "/portfolio/orders"]
    assert result["available_cash_cents"] == 4500
    assert result["account_key_sha256"] == hashlib.sha256(account.key_id.encode()).hexdigest()
    assert result["account_verified"] and result["quote_verified"] and result["fee_verified"]
    assert result["execution_enabled"] is False
