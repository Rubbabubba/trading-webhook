from datetime import datetime, timedelta, timezone

import app


def test_worker_exit_started_watchdog_flags_stale(monkeypatch):
    monkeypatch.setattr(app, "WORKER_EXIT_STARTED_STALE_SEC", 10)
    app.LAST_EXIT_HEARTBEAT.clear()
    app.LAST_EXIT_HEARTBEAT.update({
        "ts_utc": (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat(),
        "ts_ny": "2026-06-24T10:00:00-04:00",
        "status": "started",
    })
    out = app._worker_exit_status_snapshot(limit=5)
    assert out["patch_version"] == "patch-220-worker-position-truth"
    assert out["started_stale"] is True
    assert out["healthy"] is False
    assert out["recommended_action"] == "investigate_worker_exit_started_without_completion"


def test_worker_exit_started_watchdog_allows_fresh_started(monkeypatch):
    monkeypatch.setattr(app, "WORKER_EXIT_STARTED_STALE_SEC", 120)
    app.LAST_EXIT_HEARTBEAT.clear()
    app.LAST_EXIT_HEARTBEAT.update({
        "ts_utc": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),
        "ts_ny": "2026-06-24T10:00:00-04:00",
        "status": "started",
    })
    out = app._worker_exit_status_snapshot(limit=5)
    assert out["started_stale"] is False
    assert out["healthy"] is True


def test_position_truth_detects_plan_broker_and_snapshot_mismatches(monkeypatch):
    monkeypatch.setattr(app, "POSITION_TRUTH_STALE_SEC", 3600)
    snapshot = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "reason": "unit_test",
        "positions": [{"symbol": "PANW", "qty": 1}],
        "active_plans": {"PANW": {"active": True}, "INTC": {"active": True}},
    }
    reconcile = {
        "broker_symbols": ["PANW", "AMD"],
        "active_plan_symbols": ["PANW", "INTC"],
        "open_order_symbols": ["MSFT"],
        "open_order_count": 1,
        "missing_from_plans": ["AMD"],
        "stale_active_plans": ["INTC"],
        "issue_total": 2,
        "health_grade": "warn",
    }
    out = app._position_truth_snapshot(snapshot=snapshot, reconcile=reconcile, live=True)
    assert out["status"] == "mismatch"
    assert out["mismatch_count"] >= 3
    assert "INTC" in out["plans_without_broker_positions"]
    assert "AMD" in out["broker_positions_without_active_plans"]
    assert "MSFT" in out["open_orders_without_plans"]
    assert out["recommended_action"] == "run_diagnostics_reconcile_and_verify_broker_positions"


def test_position_truth_endpoint_returns_worker_and_reconcile(monkeypatch):
    monkeypatch.setattr(app, "read_positions_snapshot", lambda: {"ts_utc": datetime.now(timezone.utc).isoformat(), "positions": [], "active_plans": {}})
    monkeypatch.setattr(app, "build_reconcile_snapshot", lambda: {"broker_symbols": [], "active_plan_symbols": [], "open_order_symbols": [], "open_order_count": 0, "issue_total": 0, "health_grade": "healthy"})
    req = type("Req", (), {"headers": {}, "query_params": {}})()
    out = app.diagnostics_position_truth(req)
    assert out["ok"] is True
    assert out["patch_version"] == "patch-220-worker-position-truth"
    assert "worker_exit_status" in out
    assert "reconcile_snapshot" in out