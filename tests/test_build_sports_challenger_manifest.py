from datetime import datetime, timezone

from tools.build_sports_challenger_manifest import SOURCES, build


class Markets:
    def __init__(self, rows):
        self.rows = rows

    def get(self, *, params):
        rows = self.rows if params["series_ticker"] == "KXNFLGAME" else []
        return {"markets": rows, "cursor": ""}, 1.0, 1.1


def _market(ticker, subtitle):
    return {
        "ticker": ticker,
        "event_ticker": "KXNFLGAME-26OCT20AB",
        "status": "active",
        "market_type": "binary",
        "yes_sub_title": subtitle,
        "rules_primary": "Winner",
        "rules_secondary": "Final score",
    }


def test_manifest_covers_all_open_upcoming_events_without_a_day_ceiling():
    rows = [_market("KXNFLGAME-26OCT20AB-A", "Alpha"),
            _market("KXNFLGAME-26OCT20AB-B", "Beta")]

    def fetch(_url):
        return {"events": [{
            "id": "game-1", "name": "Alpha at Beta", "date": "2026-10-20T20:00:00Z",
            "competitions": [{"competitors": [
                {"id": "a", "homeAway": "away", "team": {"displayName": "Alpha"}},
                {"id": "b", "homeAway": "home", "team": {"displayName": "Beta"}},
            ]}],
        }]}

    result = build(datetime(2026, 9, 28, tzinfo=timezone.utc), client=Markets(rows), fetch=fetch)
    assert result["last_date"] is None
    assert result["horizon_policy"] == "all_currently_open_upcoming_events_in_registered_series"
    assert result["mapped_events"] == 1
    assert result["coverage"]["nfl"]["catalog_accounted_for"] == 1
    assert result["coverage"]["nfl"]["unmatched_catalog_events"] == []
    assert set(result["coverage"]) == set(SOURCES)


def test_manifest_lists_every_unmatched_catalog_event_explicitly():
    rows = [_market("KXNFLGAME-26OCT20AB-A", "Alpha"),
            _market("KXNFLGAME-26OCT20AB-B", "Beta")]
    result = build(datetime(2026, 9, 28, tzinfo=timezone.utc), client=Markets(rows),
                   fetch=lambda _url: {"events": []})
    coverage = result["coverage"]["nfl"]
    assert coverage["catalog_events"] == coverage["catalog_accounted_for"] == 1
    assert coverage["mapped_events"] == 0
    assert coverage["unmatched_catalog_events"][0]["event_ticker"] == "KXNFLGAME-26OCT20AB"
