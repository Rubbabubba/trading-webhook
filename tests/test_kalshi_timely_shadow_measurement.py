from fractions import Fraction
import json
import sqlite3

import pytest

from opportunity_lab.kalshi_timely_shadow_measurement import init, observe, pending_ticker, status


def database():
    db = sqlite3.connect(":memory:", isolation_level=None)
    for version in ("v10", "v11", "v12"):
        db.execute(f"CREATE TABLE {version}_shadow_signals(id INTEGER PRIMARY KEY,ticker,event_id,observed_at,outcome,price_cents)")
    init(db, 100)
    return db


def test_predeployment_signals_not_reused_and_restart_preserves_start():
    db = database()
    db.execute("INSERT INTO v12_shadow_signals VALUES(1,'T','OLD',99,'yes',45)")
    init(db, 200)
    observe(db, "T", now=104, yes_mid=Fraction(1, 2))
    assert status(db)["started_at"] == 100
    assert status(db)["strategies"]["v12"]["signals"] == 0
    db.close()


def test_due_windows_exactly_capture_and_expired_windows_remain_missing():
    db = database()
    db.execute("INSERT INTO v12_shadow_signals VALUES(1,'T','E',100,'yes',45)")
    observe(db, "T", now=100, yes_mid=Fraction(1, 2))
    assert pending_ticker(db, 104) is None
    assert pending_ticker(db, 105) == "T"
    observe(db, "T", now=106, yes_mid=Fraction(1, 2))
    assert pending_ticker(db, 106) is None
    observe(db, "T", now=136, yes_mid=Fraction(1, 2))  # late 30s window
    observe(db, "T", now=402, yes_mid=Fraction(1, 2))
    horizons = [r[0] for r in db.execute("SELECT horizon FROM timely_shadow_marks ORDER BY horizon")]
    assert horizons == [5, 300]
    assert status(db)["strategies"]["v12"]["complete_signals"] == 0
    assert pending_ticker(db, 500) is None
    db.close()


def test_all_horizons_same_event_counts_and_marks_not_overwritten():
    db = database()
    for sid in (1, 2):
        db.execute("INSERT INTO v12_shadow_signals VALUES(?,'T','E',100,'no',45)", (sid,))
    for now in (100, 105, 130, 400):
        observe(db, "T", now=now, yes_mid=Fraction(1, 2))
    observe(db, "T", now=401, yes_mid=Fraction(3, 4))
    result = status(db)
    assert result["strategies"]["v12"] == {"signals": 2, "complete_signals": 2, "independent_complete_events": 1}
    raw = db.execute("SELECT detail FROM timely_shadow_marks WHERE source_id=1 AND horizon=300").fetchone()[0]
    assert json.loads(raw)["stressed_cents"] == "3"
    assert result["promotion_ready"] is result["execution_enabled"] is False
    db.close()


def test_immutable_protocol_and_signal_capacity():
    db = database()
    for sid in range(501):
        db.execute("INSERT INTO v12_shadow_signals VALUES(?,'T','E',100,'yes',45)", (sid,))
    observe(db, "T", now=100, yes_mid=Fraction(1, 2))
    assert status(db)["strategies"]["v12"]["signals"] == 500
    db.execute("UPDATE timely_shadow_protocol SET detail='{}'")
    with pytest.raises(ValueError, match="protocol_changed"):
        init(db, 200)
    db.close()
