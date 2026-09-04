from fastapi.testclient import TestClient

from opportunity_app import app


def test_worker_requires_separate_secret(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_WORKER_SECRET", "worker-only")
    response = TestClient(app).post("/worker/opportunity-lab/collect-kalshi", json={})
    assert response.status_code == 401


def test_worker_collects_and_persists_without_execution(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_WORKER_SECRET", "worker-only")
    monkeypatch.setattr("opportunity_app.fetch_open_events", lambda **kwargs: ([], {"pages": 1, "event_count": 0, "more_available": False}))
    monkeypatch.setattr("opportunity_app.save_kalshi_scan", lambda scan, transport: {"configured": True, "saved": True, "run_id": "x"})
    response = TestClient(app).post("/worker/opportunity-lab/collect-kalshi", json={"worker_secret": "worker-only"})
    assert response.status_code == 200
    assert response.json()["persistence"]["saved"] is True
    assert response.json()["execution_enabled"] is False


def test_operator_scan_can_persist(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_ADMIN_SECRET", "admin")
    monkeypatch.setattr("opportunity_app.fetch_open_events", lambda **kwargs: ([], {"pages": 1, "event_count": 0, "more_available": False}))
    monkeypatch.setattr("opportunity_app.save_kalshi_scan", lambda scan, transport: {"configured": True, "saved": True, "run_id": "x"})
    response = TestClient(app).post("/diagnostics/opportunity_lab/kalshi/scan", headers={"x-admin-secret": "admin"}, json={"persist": True})
    assert response.status_code == 200
    assert response.json()["persistence"]["saved"] is True


def test_scoreboard_route_is_protected():
    response = TestClient(app).get("/diagnostics/opportunity_lab/scoreboard")
    assert response.status_code == 401


def test_scoreboard_route_remains_non_executing(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_ADMIN_SECRET", "admin")
    monkeypatch.setattr("opportunity_app.kalshi_scoreboard", lambda **kwargs: {"verdict": "collecting_evidence"})
    response = TestClient(app).get(
        "/diagnostics/opportunity_lab/scoreboard?hours=72", headers={"x-admin-secret": "admin"})
    assert response.status_code == 200
    assert response.json()["kalshi"]["verdict"] == "collecting_evidence"
    assert response.json()["execution_enabled"] is False


def test_market_making_route_remains_non_executing(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_ADMIN_SECRET", "admin")
    monkeypatch.setattr("opportunity_app.fetch_open_events", lambda **kwargs: ([], {"pages": 1, "event_count": 0}))
    response = TestClient(app).post("/diagnostics/opportunity_lab/kalshi/market-making",
                                    headers={"x-admin-secret": "admin"}, json={"quote_size": 10})
    assert response.status_code == 200
    assert response.json()["market_making"]["market_count"] == 0
    assert response.json()["execution_enabled"] is False
