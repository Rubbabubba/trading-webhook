from regime_intraday_dashboard import render_intraday_dashboard
from route_catalog import classify_path, is_sensitive_path


def test_intraday_dashboard_has_unambiguous_paper_identity():
    page = render_intraday_dashboard(scan={"config": {"symbols": ["SPY", "QQQ"], "trade_symbols": ["SPY"]}, "regime": {"name": "range"}, "signals": []}, ledger={}, readiness={"paper_ready": True, "live_ready": False}, scanner={})
    assert "PAPER ONLY" in page
    assert "LIVE INTRADAY CLOSED" in page
    assert "Legacy live swing account" not in page
    assert "System health" in page
    assert "Detailed view" in page
    assert "Recorded paper P/L" in page
    assert "Underlying shadow simulation" not in page


def test_detailed_dashboard_preserves_metrics_and_navigation():
    page = render_intraday_dashboard(scan={}, ledger={}, readiness={}, scanner={}, view="detailed")
    for label in ("Underlying shadow simulation", "Alpaca paper execution", "Legacy records are preserved", "Candidate history", "Market data freshness", "Worker state", "Operating overview"):
        assert label in page


def test_overview_escapes_content_and_flags_missing_scan():
    page = render_intraday_dashboard(scan={"regime": {"name": "<script>"}}, ledger={}, readiness={}, scanner={})
    assert "Scan needs checking" in page
    assert "<script>" not in page
    assert "&lt;Script&gt;" in page


def test_overview_keeps_canceled_orders_out_of_active_list():
    page = render_intraday_dashboard(scan={}, ledger={"orders": {"old-id": {"status": "canceled"}, "review-id": {"status": "entry_requires_attention"}}}, readiness={}, scanner={})
    assert "old-id" not in page
    assert "review-id" in page
    assert "Paper order needs attention" in page


def test_route_catalog_separates_active_intraday_from_legacy_research():
    assert classify_path("/dashboard/intraday", {"GET"})["owner"] == "regime_intraday"
    row = classify_path("/diagnostics/swing_tuning_simulator", {"GET"})
    assert row["lifecycle"] == "deprecation_candidate"
    assert classify_path("/worker/regime_intraday_paper_close", {"POST"})["sensitive"] is True
    assert is_sensitive_path("/dashboard") is True
    assert is_sensitive_path("/diagnostics/broker_backed_exposure_truth") is True
    assert is_sensitive_path("/diagnostics/regime_intraday_readiness") is False
