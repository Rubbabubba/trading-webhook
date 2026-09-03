import json
from datetime import date
from urllib.parse import parse_qs, urlparse

import pytest
import regime_intraday_options as module


INTENT = {"underlying": "SPY", "option_type": "call", "underlying_price": 100, "min_dte": 7, "max_dte": 21}
LONG = "SPY260918C00100000"
SHORT = "SPY260918C00101000"


def quote(bid, ask, delta=.6):
    return {"latestQuote": {"bp": bid, "ap": ask}, "greeks": {"delta": delta}}


def transport(monkeypatch, pages):
    calls = []
    iterator = iter(pages)
    class Response:
        def __init__(self, data): self.data = data
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return json.dumps(self.data).encode()
    def open_page(request, timeout):
        calls.append(parse_qs(urlparse(request.full_url).query))
        data = next(iterator)
        if isinstance(data, Exception): raise data
        return Response(data)
    monkeypatch.setattr(module, "urlopen", open_page)
    return calls


def test_second_page_partner_selected_and_filters_preserved(monkeypatch):
    calls = transport(monkeypatch, [
        {"snapshots": {LONG: quote(1.95, 2)}, "next_page_token": "page2"},
        {"snapshots": {SHORT: quote(1.5, 1.55, .4)}}])
    chain = module.fetch_option_chain("key", "secret", "SPY", intent=INTENT, as_of=date(2026, 9, 3))
    plan = module.select_debit_spread(chain, INTENT, as_of=date(2026, 9, 3))
    assert chain["chain_diagnostics"]["complete"]
    assert plan["status"] == "selected"
    assert plan["max_loss_dollars"] == 50
    assert calls[1]["page_token"] == ["page2"]
    for call in calls:
        assert call["type"] == ["call"]
        assert call["expiration_date_gte"] == ["2026-09-10"]
        assert call["expiration_date_lte"] == ["2026-09-24"]


@pytest.mark.parametrize("failure", ["limit", "repeat", "request", "invalid"])
def test_incomplete_chain_blocks_entries(monkeypatch, failure):
    page = {"snapshots": {LONG: quote(1.95, 2), SHORT: quote(1.5, 1.55)}, "next_page_token": "more"}
    second = {"repeat": page, "request": TimeoutError(), "invalid": {}}.get(failure, page)
    transport(monkeypatch, [page, second])
    chain = module.fetch_option_chain("key", "secret", "SPY", max_pages=1 if failure == "limit" else 2)
    assert not chain["chain_diagnostics"]["complete"]
    assert module.select_debit_spread(chain, INTENT)["status"] == "incomplete_option_chain"


def test_rejection_counts_and_empty_chain():
    plan = module.select_debit_spread({"snapshots": {LONG: quote(1, 2)}}, INTENT, as_of=date(2026, 9, 3))
    assert plan["diagnostics"]["rejections"] == {"long_bid_ask_spread": 1}
    assert plan["candidate_count"] == 0
    empty = module.select_debit_spread({"snapshots": {}, "chain_diagnostics": {"complete": True}}, INTENT)
    assert empty["diagnostics"]["snapshot_count"] == 0


def test_held_expiration_is_not_restricted_to_entry_window(monkeypatch):
    calls = transport(monkeypatch, [{"snapshots": {}}])
    module.fetch_option_chain("key", "secret", "SPY", expiration="2026-09-04")
    assert calls[0]["expiration_date"] == ["2026-09-04"]
    assert "expiration_date_gte" not in calls[0]


def test_total_time_budget(monkeypatch):
    clock = iter([0, 21])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(clock))
    calls = transport(monkeypatch, [])
    chain = module.fetch_option_chain("key", "secret", "SPY", timeout=20)
    assert not calls
    assert chain["chain_diagnostics"]["reason"] == "time_budget"
