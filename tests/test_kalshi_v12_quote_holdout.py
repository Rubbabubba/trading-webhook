import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone

from opportunity_lab.kalshi_v12_quote_holdout import evaluate


def test_future_holdout_excludes_prior_events_and_requires_frozen_code(tmp_path):
    start = datetime(2026, 10, 4, tzinfo=timezone.utc)
    strategy = tmp_path / "strategy.py"
    strategy.write_bytes(b"frozen strategy\n")
    registration = {
        "schema": "kalshi_v12_quote_holdout_v1",
        "version": "test", "strategy_id": "microprice_value_maker_v12_shadow",
        "registered_at": "2026-10-03T23:00:00Z",
        "holdout_start_at": "2026-10-04T00:00:00Z",
        "environment": "demo", "execution_enabled": False,
        "production_execution_enabled": False,
        "strategy_module_sha256": hashlib.sha256(strategy.read_bytes()).hexdigest(),
        "required_markout_seconds": [5, 30, 300],
        "minimum_completed_signals": 100,
        "minimum_independent_events": 30,
        "minimum_observation_days": 14,
        "minimum_active_days": 10,
    }
    registration_path = tmp_path / "registration.json"
    registration_path.write_text(json.dumps(registration))
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE v12_shadow_signals(id INTEGER PRIMARY KEY,event_id TEXT,observed_at REAL);
        CREATE TABLE v12_shadow_markouts(signal_id INTEGER,horizon_seconds INTEGER,observed_at REAL,stressed_cents TEXT);
    """)
    db.execute("INSERT INTO v12_shadow_signals VALUES(1,'prior',?)", (start.timestamp() - 1,))
    for signal_id in range(2, 123):
        event_id = "prior" if signal_id == 2 else f"new-{(signal_id - 3) // 4}"
        observed_at = (start + timedelta(days=(signal_id - 2) % 10)).timestamp()
        db.execute("INSERT INTO v12_shadow_signals VALUES(?,?,?)", (signal_id, event_id, observed_at))
        for horizon in (5, 30, 300):
            db.execute("INSERT INTO v12_shadow_markouts VALUES(?,?,?,?)", (signal_id, horizon, observed_at + horizon, "2.0"))
    db.execute("INSERT INTO v12_shadow_signals VALUES(123,'invalid-future-markout',?)", (start.timestamp(),))
    for horizon in (5, 30, 300):
        db.execute("INSERT INTO v12_shadow_markouts VALUES(?,?,?,?)", (123, horizon, start.timestamp() + 1, "1000"))
    result = evaluate(db, now=start + timedelta(days=15),
                      registration_path=registration_path, strategy_path=strategy)
    assert result["prior_event_signals_excluded"] == 1
    assert result["complete_signals"] == 120
    assert result["independent_events"] == 30
    assert result["passed"] is True
    strategy.write_text("changed\n")
    changed = evaluate(db, now=start + timedelta(days=15),
                       registration_path=registration_path, strategy_path=strategy)
    assert changed["gates"]["strategy_code_frozen"] is False
    assert changed["passed"] is False
