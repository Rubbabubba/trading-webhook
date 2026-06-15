import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _dashboard_request(query_string=b""):
    return app.Request({"type": "http", "headers": [], "query_string": query_string, "method": "GET", "path": "/dashboard"})


def test_patch_205_capacity_advisory_prefers_staged_raise_to_10_or_12():
    advisory = app._swing_capacity_advisory(current_max=7, current_open_positions=6, closed_trades=87, win_rate=0.5862, avg_r=0.96)

    assert advisory["safe_to_12"] is True
    assert advisory["safe_to_15"] is False
    assert advisory["recommended_next_max"] == 10
    assert advisory["recommended_action"] == "raise_to_10_or_12_in_steps"


def test_patch_205_capacity_advisory_allows_12_before_15():
    advisory = app._swing_capacity_advisory(current_max=10, current_open_positions=6, closed_trades=87, win_rate=0.5862, avg_r=0.96)

    assert advisory["safe_to_12"] is True
    assert advisory["safe_to_15"] is False
    assert advisory["recommended_next_max"] == 12


def test_patch_205_dashboard_shows_render_timing_and_capacity_advisory():
    body = app.dashboard(_dashboard_request()).body.decode("utf-8")

    assert "server_render_ms" in body
    assert "capacity_next_step" in body
    assert "safe_to_12" in body
    assert "safe_to_15" in body