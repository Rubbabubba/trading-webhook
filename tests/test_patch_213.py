import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _rows_ready():
    rows = []
    rows.append({"symbol": "OLD", "exit_reason": "stall_exit", "gross_pnl": -80.0, "pnl_r": -2.4, "ts_utc": "2026-06-17T13:00:00+00:00"})
    for i in range(10):
        rows.append({"symbol": f"WIN{i}", "exit_reason": "target", "gross_pnl": 25.0, "pnl_r": 1.0, "ts_utc": "2026-06-18T15:00:00+00:00"})
    rows.append({"symbol": "STAL", "exit_reason": "stall_exit", "gross_pnl": 2.0, "pnl_r": 0.1, "ts_utc": "2026-06-18T16:00:00+00:00"})
    return rows


def test_patch_213_post_tuning_validation_can_unlock_40(monkeypatch):
    monkeypatch.setattr(app, "SWING_STALL_TUNING_START_UTC", "2026-06-18T14:00:00+00:00")
    payload = app._post_tuning_exit_validation(perf_state={"closed_trades": _rows_ready(), "by_strategy": {}, "kill_switch": {}})

    assert payload["ok"] is True
    assert payload["patch_version"].startswith("patch-213")
    assert payload["mode"] == "read_only_validation"
    assert payload["post_tuning_window_configured"] is True
    assert payload["post_tuning_closed_trades"] == 11
    assert payload["post_tuning_stall_exits"] == 1
    assert payload["post_tuning_negative_stall_exits"] == 0
    assert payload["post_tuning_validation_ready"] is True
    assert payload["ready_for_40_after_post_tuning"] is True
    assert payload["risk_scale_unlock_candidate"] is True
    assert payload["risk_scale_unlock_blockers"] == []


def test_patch_213_post_tuning_validation_blocks_without_start(monkeypatch):
    monkeypatch.setattr(app, "SWING_STALL_TUNING_START_UTC", "")
    payload = app._post_tuning_exit_validation(perf_state={"closed_trades": _rows_ready(), "by_strategy": {}, "kill_switch": {}})

    assert payload["post_tuning_window_configured"] is False
    assert "post_tuning_start_not_configured" in payload["risk_scale_unlock_blockers"]
    assert payload["risk_scale_unlock_candidate"] is False


def test_patch_213_diagnostics_endpoint(monkeypatch):
    monkeypatch.setattr(app, "_recompute_strategy_performance_state", lambda: {"closed_trades": _rows_ready(), "by_strategy": {}, "kill_switch": {}})
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    monkeypatch.setattr(app, "SWING_STALL_TUNING_START_UTC", "2026-06-18T14:00:00+00:00")
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/post_tuning_exit_validation"})

    payload = app.diagnostics_post_tuning_exit_validation(req)

    assert payload["ok"] is True
    assert payload["risk_scale_unlock_candidate"] is True


def test_patch_213_dashboard_renders_post_tuning_validation():
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")

    assert "Post-Tuning Exit Validation" in body
    assert "risk_scale_unlock_candidate" in body
    assert "/diagnostics/post_tuning_exit_validation" in body