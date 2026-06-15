import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_203_scanner_success_status_is_dashboard_healthy():
    view = app._dashboard_scanner_status_view("success", False, "scan_ok")

    assert view["healthy"] is True
    assert view["ready"] is True
    assert view["health"] == "healthy"


def test_patch_203_scanner_received_in_flight_is_warm_not_failed():
    view = app._dashboard_scanner_status_view("received", True, "scan_start")

    assert view["healthy"] is True
    assert view["ready"] is True
    assert view["health"] == "in_flight"


def test_patch_203_scanner_error_remains_unhealthy():
    view = app._dashboard_scanner_status_view("success", False, "scan_error")

    assert view["healthy"] is False
    assert view["ready"] is False
    assert view["health"] == "unhealthy"


def test_patch_203_dashboard_has_snapshot_freshness_and_no_store(monkeypatch):
    monkeypatch.setattr(app, "count_open_positions_allowed", lambda: (_ for _ in ()).throw(AssertionError("dashboard must stay snapshot-only")))
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})

    resp = app.dashboard(req)
    body = resp.body.decode("utf-8")

    assert "Snapshot freshness" in body
    assert "scanner_status_normalized" in body
    assert resp.headers["Cache-Control"] == "no-store, max-age=0"