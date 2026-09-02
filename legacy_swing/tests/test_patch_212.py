import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _monitor_rows():
    return [
        {"symbol": "OLD", "exit_reason": "stall_exit", "gross_pnl": -40.0, "pnl_r": -1.0, "ts_utc": "2026-06-17T13:00:00+00:00"},
        {"symbol": "KEEP", "exit_reason": "target", "gross_pnl": 50.0, "pnl_r": 1.2, "ts_utc": "2026-06-18T13:00:00+00:00"},
        {"symbol": "NEW1", "exit_reason": "stall_exit", "gross_pnl": -12.0, "pnl_r": -0.3, "ts_utc": "2026-06-18T14:01:00+00:00"},
        {"symbol": "NEW2", "exit_reason": "stall_exit", "gross_pnl": 9.0, "pnl_r": 0.4, "ts_utc": "2026-06-18T15:01:00+00:00"},
    ]


def test_patch_212_monitor_uses_post_tuning_window(monkeypatch):
    monkeypatch.setattr(app, "SWING_STALL_MIN_R", 0.50)
    monkeypatch.setattr(app, "SWING_STALL_TUNING_BASELINE_MIN_R", 0.25)
    monkeypatch.setattr(app, "SWING_STALL_EXIT_DAYS", 3)
    monkeypatch.setattr(app, "SWING_STALL_TUNING_START_UTC", "2026-06-18T14:00:00+00:00")

    payload = app._stall_exit_tuning_monitor(perf_state={"closed_trades": _monitor_rows(), "by_strategy": {}, "kill_switch": {}})

    assert payload["ok"] is True
    assert payload["patch_version"].startswith("patch-212")
    assert payload["mode"] == "read_only_monitor"
    assert payload["active_SWING_STALL_MIN_R"] == 0.50
    assert payload["active_SWING_STALL_EXIT_DAYS"] == 3
    assert payload["stall_tuning_active"] is True
    assert payload["stall_tuning_changed_from_default"] is True
    assert payload["closed_trades_since_tuning"] == 2
    assert payload["stall_exits_since_tuning"] == 2
    assert payload["negative_stall_exit_count_since_tuning"] == 1
    assert payload["stall_exit_avg_r_since_tuning"] == 0.05
    assert payload["stall_exit_worst_r_since_tuning"] == -0.3
    assert payload["config"]["read_only"] is True


def test_patch_212_monitor_endpoint(monkeypatch):
    monkeypatch.setattr(app, "_recompute_strategy_performance_state", lambda: {"closed_trades": _monitor_rows(), "by_strategy": {}, "kill_switch": {}})
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    monkeypatch.setattr(app, "SWING_STALL_TUNING_START_UTC", "2026-06-18T14:00:00+00:00")
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/stall_exit_tuning_monitor"})

    payload = app.diagnostics_stall_exit_tuning_monitor(req)

    assert payload["ok"] is True
    assert payload["stall_exits_since_tuning"] == 2
    assert payload["recent_stall_exits"]


def test_patch_212_dashboard_renders_stall_monitor():
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")

    assert "Stall Tuning Monitor" in body
    assert "active_SWING_STALL_MIN_R" in body
    assert "negative_stall_exit_count_since_tuning" in body
    assert "/diagnostics/stall_exit_tuning_monitor" in body