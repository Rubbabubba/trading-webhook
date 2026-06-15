import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _dashboard_request(path="/dashboard", query_string=b""):
    return app.Request({"type": "http", "headers": [], "query_string": query_string, "method": "GET", "path": path})


def test_patch_204_received_scan_request_is_ready_not_in_flight():
    view = app._dashboard_scanner_status_view("received", False, "scan_request")

    assert view["ready"] is True
    assert view["healthy"] is True
    assert view["health"] == "healthy"


def test_patch_204_summary_dashboard_skips_heavy_analytics(monkeypatch):
    monkeypatch.setattr(app, "_p175_trade_quality_analytics", lambda **kwargs: (_ for _ in ()).throw(AssertionError("summary dashboard should skip heavy analytics")))

    body = app.dashboard(_dashboard_request()).body.decode("utf-8")

    assert "Heavy rank, symbol, holding-period, exit-attribution, and follow-through analytics are skipped" in body
    assert "/dashboard?detail=full" in body


def test_patch_204_full_dashboard_keeps_heavy_analytics(monkeypatch):
    monkeypatch.setattr(app, "_p175_trade_quality_analytics", lambda **kwargs: {
        "open_book_risk": {"open_positions": 0, "max_open_positions": 7},
        "missing_field_counts": {},
        "rejected_setup_follow_through": {"focus_reasons": {}},
    })

    body = app.dashboard(_dashboard_request(query_string=b"detail=full")).body.decode("utf-8")

    assert "Closed Trades by Rank Bucket" in body
    assert "Intraday Signal Debug" in body