from datetime import datetime, timedelta, timezone
import sqlite3

from opportunity_lab.kalshi_official_release_probe import (
    capture_publication, capture_quotes, fetch_gdp_publication, init,
    parse_gdp_publication, parse_schedule, register_watchlist, refresh_schedule, status,
)


def _market(release):
    return {"ticker": "KXGDP-26OCT30-T4.0", "event_ticker": "KXGDP-26OCT30",
            "status": "active", "market_type": "binary",
            "title": "Will real GDP increase by more than 4%?",
            "rules_primary": "BEA seasonally adjusted annualized Advance Estimate",
            "rules_secondary": "", "close_time": (release + timedelta(days=1)).isoformat()}


class DemoQuote:
    def __init__(self, at):
        self.at = at
        self.calls = 0

    def quote(self, payload):
        self.calls += 1
        return {"environment": "demo", "ticker": payload["ticker"],
                "started_at": self.at.timestamp() - 0.2,
                "observed_at": self.at.timestamp(),
                "orderbook_fp": {"yes_dollars": [["0.4000", "10.00"]],
                                 "no_dollars": [["0.5000", "12.00"]]}}


def test_bea_schedule_watchlist_and_future_demo_capture():
    release = datetime.now(timezone.utc) + timedelta(minutes=3)
    earlier = release - timedelta(days=1)
    db = sqlite3.connect(":memory:", isolation_level=None)
    init(db)
    payload = {"Gross Domestic Product": {"release_dates": [release.isoformat()]},
               "Personal Income and Outlays": {"release_dates": [(release + timedelta(days=30)).isoformat()]}}
    assert parse_schedule(payload, earlier)[0][0] == "Gross Domestic Product"
    assert refresh_schedule(db, earlier, lambda: payload)
    assert not refresh_schedule(db, earlier + timedelta(minutes=1), lambda: {})
    market = _market(release)
    bad = {**market, "ticker": "KXGDP-OTHER", "rules_primary": "unrelated source"}
    assert register_watchlist(db, [bad, market], earlier) == 1
    assert status(db, earlier)["watchlist_contracts"] == 1
    assert status(db, earlier)["contract_rule_versions"] == 1
    assert status(db, earlier)["next_releases"][0]["close_timing_review"] == 1
    client = DemoQuote(datetime.now(timezone.utc))
    assert capture_quotes(db, client, earlier) == 0
    assert capture_quotes(db, client, client.at) == 1
    assert status(db, client.at)["demo_quote_snapshots"] == 1
    assert status(db, client.at)["profitability_evidence"] is False
    db.close()


def test_changed_contract_rules_are_preserved_as_separate_versions():
    release = datetime.now(timezone.utc) + timedelta(days=1)
    db = sqlite3.connect(":memory:", isolation_level=None)
    init(db)
    db.execute("INSERT INTO official_release_schedule VALUES(?,?,?,?)",
               ("Gross Domestic Product", release.isoformat(),
                datetime.now(timezone.utc).isoformat(), "official"))
    first = _market(release)
    assert register_watchlist(db, [first], datetime.now(timezone.utc)) == 1
    changed = {**first, "rules_primary": first["rules_primary"] + " revised"}
    assert register_watchlist(db, [changed], datetime.now(timezone.utc)) == 0
    report = status(db, datetime.now(timezone.utc))
    assert report["contract_rule_versions"] == 2
    assert report["contracts_with_rule_changes"] == 1
    hashes = {row[0] for row in db.execute(
        "SELECT rules_sha256 FROM official_release_contract_terms")}
    assert len(hashes) == 2
    assert db.execute("SELECT rules_sha256 FROM official_release_watchlist").fetchone()[0] in hashes
    db.close()


def test_gdp_rule_template_requires_exact_strike_and_never_claims_outcome():
    release = datetime.now(timezone.utc) + timedelta(days=1)
    db = sqlite3.connect(":memory:", isolation_level=None)
    init(db)
    db.execute("INSERT INTO official_release_schedule VALUES(?,?,?,?)",
               ("Gross Domestic Product", release.isoformat(),
                datetime.now(timezone.utc).isoformat(), "official"))
    market = {**_market(release), "strike_type": "greater", "floor_strike": 4,
              "rules_primary": "Resolves Yes if real GDP (as measured by the BEA's seasonally adjusted and annualized Advance Estimate) increases by more than 4.0."}
    assert register_watchlist(db, [market], datetime.now(timezone.utc)) == 1
    assert status(db, datetime.now(timezone.utc))["parsed_gdp_templates"] == 1
    assert db.execute("SELECT json_extract(terms_json,'$.mapping.state') FROM official_release_contract_terms").fetchone()[0] == "template_parsed_outcome_unverified"
    changed = {**market, "floor_strike": 3}
    assert register_watchlist(db, [changed], datetime.now(timezone.utc)) == 0
    assert status(db, datetime.now(timezone.utc))["parsed_gdp_templates"] == 0
    db.close()


def test_watchlist_rejects_market_that_closes_before_release():
    release = datetime.now(timezone.utc) + timedelta(days=1)
    db = sqlite3.connect(":memory:", isolation_level=None)
    init(db)
    db.execute("INSERT INTO official_release_schedule VALUES(?,?,?,?)",
               ("Gross Domestic Product", release.isoformat(),
                datetime.now(timezone.utc).isoformat(), "official"))
    market = _market(release)
    market["close_time"] = (release - timedelta(minutes=1)).isoformat()
    assert register_watchlist(db, [market], datetime.now(timezone.utc)) == 0
    db.close()


def test_gdp_publication_capture_is_versioned_and_never_claims_settlement():
    now = datetime.now(timezone.utc)
    release = now - timedelta(minutes=1)
    scheduled = release.isoformat()
    db = sqlite3.connect(":memory:", isolation_level=None)
    init(db)
    db.execute("INSERT INTO official_release_schedule VALUES(?,?,?,?)",
               ("Gross Domestic Product", scheduled, now.isoformat(), "official"))
    market = {**_market(release), "strike_type": "greater", "floor_strike": 1,
              "rules_primary": "Resolves Yes if real GDP (as measured by the BEA's seasonally adjusted and annualized Advance Estimate) increases by more than 1.0."}
    assert register_watchlist(db, [market], now) == 1
    title = "GDP (Advance Estimate), 3rd Quarter 2026"
    # Use a fixed October schedule for the title mapping, independent of the
    # current test date; the live capture uses the stored BEA release date.
    october = datetime(2026, 10, 29, 12, 30, tzinfo=timezone.utc).isoformat()
    body = (f"<h1>{title}</h1><p>Real gross domestic product (GDP) increased "
            "at an annual rate of 1.5 percent in the third quarter.</p>").encode()
    assert parse_gdp_publication(body, october) == ("1.5", "gdp_advance_text_parsed_unverified")
    assert parse_gdp_publication(b"<h1>Other release</h1>", october) == (None, "title_mismatch")
    # Adapt the document title to the current release date for storage.
    from opportunity_lab.kalshi_official_release_probe import _gdp_advance_title
    live_body = body.replace(title.encode(), _gdp_advance_title(scheduled).encode())
    source = ("https://www.bea.gov/news/2026/gdp-advance-estimate", live_body,
              now - timedelta(seconds=1), now)
    fetch = lambda _: source
    assert capture_publication(db, now, fetch) == 1
    assert capture_publication(db, now, fetch) == 0
    report = status(db, now)
    assert report["publication_versions"] == 1
    assert report["first_gdp_annualized_percent"] == "1.5"
    assert report["publication_value_conflict"] is False
    assert report["contract_comparisons"][0]["source_implied_result"] == "yes"
    assert report["profitability_evidence"] is False
    db.close()


def test_gdp_publication_discovery_requires_matching_bea_link(monkeypatch):
    import opportunity_lab.kalshi_official_release_probe as probe
    scheduled = datetime(2026, 10, 29, 12, 30, tzinfo=timezone.utc).isoformat()
    index = (b'<a href="/news/2026/gdp-advance-estimate-3rd-quarter-2026">'
             b'GDP (Advance Estimate), 3rd Quarter 2026</a>')
    page = b"<h1>GDP (Advance Estimate), 3rd Quarter 2026</h1>"
    calls = []
    def fake_fetch(url):
        calls.append(url)
        return url, index if len(calls) == 1 else page, datetime.now(timezone.utc), datetime.now(timezone.utc)
    monkeypatch.setattr(probe, "_bea_html", fake_fetch)
    assert fetch_gdp_publication(scheduled)[1] == page
    assert len(calls) == 2
    calls.clear()
    def no_match(url):
        calls.append(url)
        return url, b'<a href="https://other.example/news/2026/x">GDP (Advance Estimate), 3rd Quarter 2026</a>', datetime.now(timezone.utc), datetime.now(timezone.utc)
    monkeypatch.setattr(probe, "_bea_html", no_match)
    assert fetch_gdp_publication(scheduled) is None
    assert len(calls) == 1
