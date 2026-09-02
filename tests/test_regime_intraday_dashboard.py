from regime_intraday_dashboard import render_intraday_dashboard
from route_catalog import classify_path, is_sensitive_path


def test_intraday_dashboard_has_unambiguous_paper_identity():
    page = render_intraday_dashboard(scan={"config": {"symbols": ["SPY", "QQQ"], "trade_symbols": ["SPY"]}, "regime": {"name": "range"}, "signals": []}, ledger={}, readiness={"paper_ready": True, "live_ready": False}, scanner={})
    assert "PAPER ONLY" in page
    assert "LIVE INTRADAY CLOSED" in page
    assert "Legacy live swing account" not in page
    assert "System health" in page


def test_route_catalog_separates_active_intraday_from_legacy_research():
    assert classify_path("/dashboard/intraday", {"GET"})["owner"] == "regime_intraday"
    row = classify_path("/diagnostics/swing_tuning_simulator", {"GET"})
    assert row["lifecycle"] == "deprecation_candidate"
    assert classify_path("/worker/regime_intraday_paper_close", {"POST"})["sensitive"] is True
    assert is_sensitive_path("/dashboard") is True
    assert is_sensitive_path("/diagnostics/broker_backed_exposure_truth") is True
    assert is_sensitive_path("/diagnostics/regime_intraday_readiness") is False
