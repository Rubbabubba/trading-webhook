from app import _record_paper_lifecycle, _paper_lifecycle_counts, _symbol_lifecycle_proof


def test_exit_armed_counts_as_exit_event(monkeypatch):
    import app
    app.PAPER_LIFECYCLE_HISTORY.clear()
    app.LAST_PAPER_LIFECYCLE.clear()
    _record_paper_lifecycle("exit", "armed", "AMZN", {"source": "test"})
    counts = _paper_lifecycle_counts()
    assert counts["exit_events"] >= 1


def test_planned_entry_does_not_count_as_fill(monkeypatch):
    import app
    app.PAPER_LIFECYCLE_HISTORY.clear()
    app.LAST_PAPER_LIFECYCLE.clear()
    app.TRADE_PLAN.clear()
    app.DECISIONS.clear()
    app._record_paper_lifecycle("entry", "planned", "PLTR", {})
    monkeypatch.setattr(app, "get_position", lambda sym: (0.0, "flat"))
    monkeypatch.setattr(app, "get_order_status", lambda oid: {})
    row = _symbol_lifecycle_proof("PLTR", active_scan={"summary": {"top_candidates": [], "selected_symbols": []}, "_scan_source": "memory"})
    assert row["fill_observed"] is False
