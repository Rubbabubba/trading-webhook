import app


def test_patch_version_092():
    assert app.PATCH_VERSION == "patch-092-paper-proof-recovered-fill-alignment"


def test_execution_proof_counts_recovered_filled_active_plan(monkeypatch):
    monkeypatch.setattr(app, '_active_truth_scan', lambda limit=20: {"summary": {"symbols": ["AMZN"], "selected_symbols": []}, "_scan_source": "scan_history_match"})
    monkeypatch.setattr(app, '_paper_execution_proof_snapshot', lambda limit=20: {
        "truth_source": "scan_history_match",
        "runtime_symbols": ["AMZN"],
        "selected_symbols": [],
        "rows": [{
            "symbol": "AMZN",
            "selected": False,
            "plan_created": True,
            "order_submitted": True,
            "fill_observed": True,
            "active_position": True,
            "exit_armed": True,
            "exit_event": {"status": "armed"},
            "latest_decision": {"action": "recovered_plan"},
            "decisions": [{"action": "recovered_plan", "event": "RECONCILE"}],
            "lifecycle_events": [],
            "current_stage": "filled",
        }],
    })
    snap = app._execution_proof_snapshot(limit=10)
    assert snap["selected_count"] == 0
    assert snap["planned_count"] == 1
    assert snap["submitted_count"] == 1
    assert snap["filled_count"] == 1
    assert snap["exit_armed_count"] == 1
    assert snap["items"][0]["symbol"] == "AMZN"


def test_paper_lifecycle_counts_credit_synthetic_exit_for_active_managed_position(monkeypatch):
    monkeypatch.setattr(app, 'PAPER_LIFECYCLE_HISTORY', [])
    monkeypatch.setattr(app, 'LAST_PAPER_LIFECYCLE', {})
    monkeypatch.setattr(app, 'TRADE_PLAN', {
        'AMZN': {'active': True, 'stop_price': 205.85, 'take_price': 221.68}
    })
    monkeypatch.setattr(app, 'get_position', lambda symbol: (5.68, 'long'))
    counts = app._paper_lifecycle_counts()
    assert counts['exit_events'] >= 1
    assert 'AMZN' in counts['synthetic_exit_symbols']
