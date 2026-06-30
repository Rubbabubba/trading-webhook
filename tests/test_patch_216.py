import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_216_snapshot_age_view_has_mild_and_critical_states(monkeypatch):
    monkeypatch.setenv("DASHBOARD_SNAPSHOT_WARNING_SEC", "300")
    monkeypatch.setenv("DASHBOARD_SNAPSHOT_CRITICAL_SEC", "600")
    generated = datetime(2026, 6, 22, 15, 0, tzinfo=timezone.utc)

    mild = app._dashboard_snapshot_age_view((generated - timedelta(seconds=180)).isoformat(), generated_utc=generated, stale_after_sec=120)
    critical = app._dashboard_snapshot_age_view((generated - timedelta(seconds=900)).isoformat(), generated_utc=generated, stale_after_sec=120)

    assert mild["fresh"] is False
    assert mild["status"] == "mild_stale"
    assert mild["severity"] == "warn"
    assert critical["status"] == "critical_stale"
    assert critical["severity"] == "critical"
    assert critical["thresholds"]["warning_sec"] == 300


def test_patch_216_summary_dashboard_defers_heavy_tuning(monkeypatch):
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    monkeypatch.setattr(app, "_swing_profit_acceleration_simulator", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("summary dashboard should defer tuning simulator")))
    monkeypatch.setattr(app, "_swing_stall_exit_drilldown", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("summary dashboard should defer stall drilldown")))
    monkeypatch.setattr(app, "_post_tuning_exit_validation", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("summary dashboard should defer post-tuning validation")))
    monkeypatch.setattr(app, "_stall_exit_tuning_monitor", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("summary dashboard should defer stall monitor")))

    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")

    assert "patch-222-recovered-attribution-backfill" in body
    assert "snapshot_status" in body
    assert "performance_analytics_ms" in body
    assert "open_dashboard_detail_full_for_post_tuning_validation" in body
    assert "Post-Tuning Exit Validation" in body


def test_patch_216_full_dashboard_keeps_heavy_diagnostics(monkeypatch):
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    calls = {"sim": 0, "drill": 0, "post": 0, "monitor": 0}

    def fake_sim(*args, **kwargs):
        calls["sim"] += 1
        return {"mode": "read_only_simulation", "best_tuning_candidate": {}, "scenarios": []}

    def fake_drill(*args, **kwargs):
        calls["drill"] += 1
        return {"stall_exit_summary": {}, "recommended_exit_tuning": {}, "by_symbol": [], "exit_tightening_simulations": []}

    def fake_post(*args, **kwargs):
        calls["post"] += 1
        return {"post_tuning_validation_ready": False, "risk_scale_unlock_candidate": False, "risk_scale_unlock_blockers": [], "recommended_action": "continue"}

    def fake_monitor(*args, **kwargs):
        calls["monitor"] += 1
        return {"assessment": "monitoring", "stall_tuning_active": True}

    monkeypatch.setattr(app, "_swing_profit_acceleration_simulator", fake_sim)
    monkeypatch.setattr(app, "_swing_stall_exit_drilldown", fake_drill)
    monkeypatch.setattr(app, "_post_tuning_exit_validation", fake_post)
    monkeypatch.setattr(app, "_stall_exit_tuning_monitor", fake_monitor)

    req = app.Request({"type": "http", "headers": [], "query_string": b"detail=full", "method": "GET", "path": "/dashboard"})
    body = app.dashboard(req).body.decode("utf-8")

    assert "Swing Tuning Simulator" in body
    assert "Stall Exit Drilldown" in body
    assert calls == {"sim": 1, "drill": 1, "post": 1, "monitor": 1}