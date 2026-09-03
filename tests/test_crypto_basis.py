import pytest
from fastapi.testclient import TestClient

from opportunity_app import app
from opportunity_lab.crypto_basis import BasisInputs, backtest_funding, evaluate_basis


def test_basis_economics_include_both_legs_and_capital():
    result = evaluate_basis(BasisInputs(
        spot_ask=100.0,
        derivative_bid=101.0,
        funding_rate_bps=2.0,
        holding_hours=24,
        spot_round_trip_fee_bps=20,
        derivative_round_trip_fee_bps=10,
        round_trip_slippage_bps=5,
        derivative_leverage=2,
        available_capital=1500,
        spot_ask_size=5000,
        derivative_bid_size=3000,
    ))
    assert result["entry_basis_bps"] == pytest.approx(100)
    assert result["expected_funding_bps"] == pytest.approx(6)
    assert result["net_pnl_bps"] == pytest.approx(71)
    assert result["capital_required_per_notional"] == pytest.approx(1.5)
    assert result["executable_notional"] == pytest.approx(1000)
    assert result["expected_profit"] == pytest.approx(7.1)
    assert result["eligible"] is True


def test_negative_edge_is_rejected():
    result = evaluate_basis(BasisInputs(
        spot_ask=100, derivative_bid=100, funding_rate_bps=1,
        available_capital=1000, spot_ask_size=1000, derivative_bid_size=1000,
    ))
    assert result["eligible"] is False
    assert "net_edge_below_minimum" in result["blockers"]


def test_funding_backtest_uses_short_receipt_sign():
    result = backtest_funding([2, 2, -1], entry_basis_bps=20, exit_basis_bps=5, total_cost_bps=10)
    assert result["funding_pnl_bps"] == 3
    assert result["basis_capture_bps"] == 15
    assert result["net_pnl_bps"] == 8
    assert result["profitable"] is True


def test_basis_endpoint_is_protected(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_ADMIN_SECRET", "test-secret")
    client = TestClient(app)
    assert client.post("/diagnostics/opportunity_lab/basis/evaluate", json={}).status_code == 401


def test_basis_endpoint_validates(monkeypatch):
    monkeypatch.setenv("OPPORTUNITY_ADMIN_SECRET", "test-secret")
    client = TestClient(app)
    response = client.post(
        "/diagnostics/opportunity_lab/basis/evaluate",
        headers={"x-admin-secret": "test-secret"},
        json={"spot_ask": 0, "derivative_bid": 100, "funding_rate_bps": 1},
    )
    assert response.status_code == 400
