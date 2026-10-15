import hashlib
import json
import sqlite3

import pytest

from opportunity_lab.kalshi_historical_execution_audit import audit, audit_strategy, number


def database(path):
    db = sqlite3.connect(path)
    db.executescript("""CREATE TABLE v12_shadow_signals(
      id INTEGER PRIMARY KEY,ticker TEXT,observed_at REAL,outcome TEXT,price_cents INTEGER,event_id TEXT);
      CREATE TABLE v12_shadow_markouts(signal_id INTEGER,horizon_seconds INTEGER,
      observed_at REAL,yes_mid TEXT,gross_cents TEXT,stressed_cents TEXT);""")
    return db


def signal(db, sid, event, *, at=1000, late=0, outcome="yes"):
    db.execute("INSERT INTO v12_shadow_signals VALUES(?,?,?,?,?,?)",
               (sid, "T", at, outcome, 45, event))
    mid = "1/2"
    for horizon in (5, 30, 300):
        db.execute("INSERT INTO v12_shadow_markouts VALUES(?,?,?,?,?,?)",
                   (sid, horizon, at + horizon + late, mid, "5", "3"))
    db.commit()


def test_fraction_midpoints_and_execution_stress_do_not_prove_profit(tmp_path):
    path = tmp_path / "data.sqlite3"
    db = database(path)
    signal(db, 1, "A"); signal(db, 2, "B", outcome="no")
    db.close()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    result = audit(path, now=2000)
    summary = result["results"]["v12"]["punctual_complete"]
    assert summary["signals"] == 2
    assert summary["stress_sensitivity"]["0"]["5"]["net_quote_markout_cents"] == 6
    assert summary["stress_sensitivity"]["5"]["5"]["net_quote_markout_cents"] == -4
    assert result["profitability_proven"] is result["live_promotion_ready"] is False
    assert result["results"]["v10"]["state"] == "missing_tables"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    json.dumps(result, allow_nan=False)


def test_late_early_missing_and_invalid_marks_remain_in_denominator(tmp_path):
    db = database(tmp_path / "data.sqlite3")
    signal(db, 1, "LATE", late=6)
    signal(db, 2, "EARLY", late=-1)
    signal(db, 3, "MISSING")
    db.execute("DELETE FROM v12_shadow_markouts WHERE signal_id=3 AND horizon_seconds=300")
    signal(db, 4, "BAD")
    db.execute("UPDATE v12_shadow_markouts SET stressed_cents='NaN' WHERE signal_id=4")
    result = audit_strategy(db, "v12", now=2000)
    assert result["signals"] == result["excluded_signals"] == 4
    assert result["raw_complete_signals"] == 3
    assert result["punctual_complete"]["signals"] == 0
    assert result["exclusion_reasons_nonexclusive"] == {
        "late_markout": 1, "early_markout": 1, "missing_horizon": 1, "invalid_markout_number": 1}
    db.close()


def test_correlated_signals_do_not_create_independent_events(tmp_path):
    db = database(tmp_path / "data.sqlite3")
    for sid in range(1, 5):
        signal(db, sid, "ONE")
    result = audit_strategy(db, "v12", now=2000)["punctual_complete"]
    assert result["signals"] == 4 and result["independent_events"] == 1
    assert result["stress_sensitivity"]["0"]["5"]["event_clustered_95pct_lower_bound_cents"] is None
    db.close()


def test_bad_arithmetic_future_marks_and_bad_numbers_fail_closed(tmp_path):
    db = database(tmp_path / "data.sqlite3")
    signal(db, 1, "BAD")
    db.execute("UPDATE v12_shadow_markouts SET stressed_cents='4'")
    result = audit_strategy(db, "v12", now=1001)
    assert result["excluded_signals"] == 1
    assert result["exclusion_reasons_nonexclusive"]["markout_arithmetic_mismatch"] == 1
    assert result["exclusion_reasons_nonexclusive"]["future_markout"] == 1
    for value in (True, "NaN", "inf", "1/0", "wrong"):
        with pytest.raises(ValueError):
            number(value)
    db.close()
