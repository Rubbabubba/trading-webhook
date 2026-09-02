import importlib

from fastapi.testclient import TestClient


def _load(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMIN_SECRET", "test-admin")
    monkeypatch.setenv("WORKER_SECRET", "test-worker")
    monkeypatch.setenv("REGIME_INTRADAY_LEDGER_PATH", str(tmp_path / "ledger.json"))
    import intraday_app
    return importlib.reload(intraday_app)


def test_intraday_app_has_no_swing_routes(monkeypatch, tmp_path):
    module = _load(monkeypatch, tmp_path)
    paths = {route.path for route in module.app.routes}
    assert "/dashboard/intraday" in paths
    assert "/worker/regime_intraday_scan" in paths
    assert not any("swing" in path for path in paths)
    assert "/worker/exit" not in paths
    assert "/worker/scan_entries" not in paths
    assert "/webhook" not in paths


def test_health_is_public_and_paper_only(monkeypatch, tmp_path):
    module = _load(monkeypatch, tmp_path)
    response = TestClient(module.app).get("/health")
    assert response.status_code == 200
    assert response.json()["paper_only"] is True
    assert response.json()["live_trading_enabled"] is False
    assert set(response.json()["systems"]) == {"regime_intraday"}


def test_operator_routes_require_auth(monkeypatch, tmp_path):
    module = _load(monkeypatch, tmp_path)
    client = TestClient(module.app)
    assert client.get("/diagnostics/regime_intraday_ledger").status_code == 401
    assert client.get("/diagnostics/regime_intraday_ledger", headers={"x-admin-secret": "test-admin"}).status_code == 200


def test_worker_requires_worker_secret(monkeypatch, tmp_path):
    module = _load(monkeypatch, tmp_path)
    client = TestClient(module.app)
    assert client.post("/worker/regime_intraday_paper_reconcile", json={}).status_code == 401
    response = client.post("/worker/regime_intraday_paper_reconcile", json={"worker_secret": "test-worker"})
    assert response.status_code == 200
    assert response.json()["live_submission"] is False


def test_paper_submit_gate_defaults_closed(monkeypatch, tmp_path):
    monkeypatch.setenv("REGIME_INTRADAY_PAPER_SUBMIT_ENABLED", "false")
    module = _load(monkeypatch, tmp_path)
    response = TestClient(module.app).post("/worker/regime_intraday_paper_roundtrip", json={"worker_secret": "test-worker", "signal_id": "x"})
    assert response.status_code == 409
    assert "gate is closed" in response.text
