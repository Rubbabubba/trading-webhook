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
