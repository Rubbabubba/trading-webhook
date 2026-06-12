import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def test_patch_202_hybrid_metrics_accept_precomputed_capacity(monkeypatch):
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [{"latest_return_pct": 0.01, "latest_r": 0.2}])
    monkeypatch.setattr(app, "HYBRID_PROOF_MIN_SHADOW_TRADES", 1)
    monkeypatch.setattr(app, "HYBRID_PROOF_MIN_AVG_R", 0.1)
    monkeypatch.setattr(app, "HYBRID_PROOF_REQUIRE_EOD_FLAT_SESSIONS", 0)
    monkeypatch.setattr(app, "LAST_EOD_FLATTEN_STATUS", {"fully_flat": True, "residual_count": 0})
    monkeypatch.setattr(app, "count_open_positions_allowed", lambda: (_ for _ in ()).throw(AssertionError("broker position call should not run")))

    metrics = app._hybrid_proof_metrics(capacity_plan={"sleeves_configured": True})

    assert metrics["proof_ready_for_paper"] is True
    assert metrics["avg_r"] == 0.2


def test_patch_202_dashboard_hybrid_panel_uses_snapshot_capacity(monkeypatch):
    monkeypatch.setattr(app, "HYBRID_PROOF_LEDGER", [{"symbol": "SPY", "signal": "INTRADAY_VWAP_RECLAIM", "latest_r": 0.25, "latest_return_pct": 0.006}])
    monkeypatch.setattr(app, "LAST_EOD_FLATTEN_STATUS", {"fully_flat": True, "residual_count": 0})
    monkeypatch.setattr(app, "count_open_positions_allowed", lambda: (_ for _ in ()).throw(AssertionError("dashboard must stay snapshot-only")))
    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/dashboard"})

    body = app.dashboard(req).body.decode("utf-8")

    assert "Hybrid Proof Ledger" in body
    assert "Hybrid Sleeve Readiness" in body