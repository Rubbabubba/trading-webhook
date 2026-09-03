from datetime import datetime, timedelta, timezone

from opportunity_lab.catalog import CANDIDATES, candidate_catalog
from opportunity_lab.crypto_regime import CryptoRegimeConfig, crypto_research_suite, monte_carlo_trades, replay_crypto_regime, walk_forward_crypto
from opportunity_lab.models import Opportunity
from fastapi.testclient import TestClient
from opportunity_app import app


def _bars(count=500):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = []
    price = 100.0
    for index in range(count):
        price *= 1.001 if (index // 120) % 2 == 0 else .9995
        rows.append({"ts_utc": start + timedelta(hours=index), "open": price, "high": price * 1.002, "low": price * .998, "close": price, "volume": 10, "vwap": price})
    return rows


def test_all_locked_candidates_have_unique_rank_and_key():
    assert len(CANDIDATES) == 7
    assert len({row.key for row in CANDIDATES}) == 7
    assert sorted(row.rank for row in CANDIDATES) == list(range(1, 8))
    assert next(row for row in candidate_catalog() if row["key"] == "crypto_regime")["status"] == "active"


def test_shared_opportunity_computes_net_edge():
    row = Opportunity("test", "id", datetime.now(timezone.utc), ("A", "B"), 30, 12, 1000)
    assert row.net_edge_bps == 18
    assert row.as_dict()["net_edge_bps"] == 18


def test_crypto_replay_is_cost_aware_and_deterministic():
    result = replay_crypto_regime(_bars(), CryptoRegimeConfig(fast_window=12, slow_window=48, volatility_window=12))
    assert result["bar_count"] == 500
    assert result["trade_count"] > 0
    assert all("net_return" in row for row in result["trades"])
    assert result == replay_crypto_regime(_bars(), CryptoRegimeConfig(fast_window=12, slow_window=48, volatility_window=12))


def test_crypto_walk_forward_keeps_chronological_holdout():
    result = walk_forward_crypto(_bars())
    assert result["train_bars"] == 350
    assert result["test_bars"] == 150
    assert result["candidate_count"] == 4
    assert len(result["parameter_stability"]) == 4


def test_full_crypto_research_suite_is_reproducible():
    first = crypto_research_suite(_bars(1200))
    second = crypto_research_suite(_bars(1200))
    assert first == second
    assert first["benchmark"]["net_return"] is not None
    assert first["rolling_walk_forward"]["fold_count"] == 5
    assert len(first["cost_sensitivity_on_holdout"]) == 4
    assert first["execution_enabled"] is False


def test_monte_carlo_reports_empty_sample_without_inventing_evidence():
    assert monte_carlo_trades([])["reason"] == "no_closed_trades"


def test_opportunity_app_is_hard_closed(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_ADMIN_SECRET", "secret")
    client = TestClient(app)
    root = client.get("/").json()
    assert root["execution_enabled"] is False
    assert root["live_submission"] is False
    assert client.get("/diagnostics/opportunity_lab/catalog").status_code == 401
    catalog_response = client.get("/diagnostics/opportunity_lab/catalog", headers={"x-admin-secret": "secret"})
    assert catalog_response.status_code == 200
    assert len(catalog_response.json()["candidates"]) == 7


def test_regime_admin_secret_does_not_authorize_opportunity_lab(monkeypatch):
    monkeypatch.delenv("OPPORTUNITY_ADMIN_SECRET", raising=False)
    monkeypatch.setenv("ADMIN_SECRET", "regime-only")
    client = TestClient(app)
    assert client.get("/diagnostics/opportunity_lab/catalog", headers={"x-admin-secret": "regime-only"}).status_code == 401


def test_opportunity_dashboard_is_protected_and_execution_closed(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_ADMIN_SECRET", "secret")
    client = TestClient(app)
    assert client.get("/dashboard/opportunity-lab").status_code == 401
    response = client.get("/dashboard/opportunity-lab", headers={"x-admin-secret": "secret"})
    assert response.status_code == 200
    assert "execution hard-disabled" in response.text
    assert "/diagnostics/opportunity_lab/backtest/crypto" in response.text
