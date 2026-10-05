from datetime import datetime, timezone, timedelta
import json

from opportunity_lab import kalshi_live_worker as worker
from opportunity_lab.kalshi_live_execution import LiveBinaryJournal, LiveBinaryBroker
from test_kalshi_live_execution import grant, LiveExchange, live_quote


class Markets:
    def __init__(self, journal): self.j = journal
    def quote(self, payload): return live_quote(self.j, payload["client_order_id"])


def test_cycle_fills_once_waits_for_settlement_and_reconciles_revocation(tmp_path, monkeypatch):
    j = LiveBinaryJournal(tmp_path / "journal.db", grant(), approval_verifier=lambda digest: True)
    x = LiveExchange(j); b = LiveBinaryBroker(j, x)
    plan = {"client_id": "one", "ticker": "T", "event_id": "EVENT", "outcome": "yes", "price_cents": 8, "fee_reserve_cents": 1}
    monkeypatch.setattr(worker, "select_entry", lambda *args: plan)
    assert worker.cycle(b, Markets(j), tmp_path)["filled"] == 1
    assert worker.cycle(b, Markets(j), tmp_path)["state"] == "awaiting_settlement"
    j.approval_verifier = lambda digest: False
    assert worker.cycle(b, Markets(j), tmp_path)["new_entries"] is False
    assert x.posts == 1
    j.close()


def test_no_grant_never_loads_credentials_or_opens_journal(tmp_path, monkeypatch):
    class Authority:
        def __init__(self, *args): pass
        def fetch(self): return None
    monkeypatch.setattr(worker, "OwnerAuthority", Authority)
    monkeypatch.delenv("KALSHI_LIVE_API_KEY_ID", raising=False)
    report = worker.run(tmp_path, once=True)
    assert report["state"] == "awaiting_owner_grant" and report["ai_tokens"] == 0
    assert not (tmp_path / "journal.sqlite3").exists()


def test_restart_after_revocation_can_cancel_but_cannot_reenter(tmp_path):
    path = tmp_path / "journal.db"
    j = LiveBinaryJournal(path, grant(), approval_verifier=lambda digest: True)
    x = LiveExchange(j); x.resting_only = True; b = LiveBinaryBroker(j, x)
    j.reserve("one", "T", 1, 8, 1, event_id="EVENT", outcome="yes", action="buy", account_snapshot=b.snapshot())
    b.submit("one", quote_snapshot=live_quote(j, "one")); j.close()
    j = LiveBinaryJournal(path, grant(), approval_verifier=lambda digest: False, recovery_only=True)
    x.j = j; b = LiveBinaryBroker(j, x)
    assert worker.cycle(b, Markets(j), tmp_path)["new_entries"] is False
    assert j.get("one")["state"] == "terminal" and x.posts == 1
    j.close()


def test_catalog_selector_requires_matching_book_and_fee_metadata(tmp_path):
    j = LiveBinaryJournal(tmp_path / "journal.db", grant(), approval_verifier=lambda digest: True)
    now = datetime.now(timezone.utc)
    class Catalog:
        multiplier = "1"
        def get(self, **kwargs):
            return {"cursor": "next-page", "markets": [{"ticker": "T", "event_ticker": "EVENT", "close_time": (now + timedelta(hours=1)).isoformat()}]}, 0, 0
        def quote(self, payload):
            at = datetime.now(timezone.utc).timestamp()
            return {"environment": "production", "observed_at": at,
                    "orderbook_fp": {"yes_dollars": [[".06", "1"]], "no_dollars": [[".92", "1"]]}}
        def get_event(self, event):
            at = datetime.now(timezone.utc).timestamp()
            return {"event": {"event_ticker": event, "series_ticker": "SERIES"}}, at, at
        def get_series(self, series):
            at = datetime.now(timezone.utc).timestamp()
            return {"series": {"ticker": series, "fee_type": "quadratic", "fee_multiplier": self.multiplier}}, at, at
    markets = Catalog()
    plan = worker.select_entry(markets, j, tmp_path)
    assert plan["price_cents"] == 8 and plan["fee_reserve_cents"] == 5
    assert plan["fee_basis"]["actual_fee_verified"] is False
    assert json.loads((tmp_path / "catalog.json").read_text())["cursor"] == "next-page"
    markets.multiplier = "10"
    assert worker.select_entry(markets, j, tmp_path) is None
    j.close()
