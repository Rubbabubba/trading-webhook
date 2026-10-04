from datetime import datetime, timedelta, timezone
import sqlite3

from opportunity_lab.kalshi_official_release_probe import (
    capture_quotes, init, parse_schedule, register_watchlist, refresh_schedule, status,
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
