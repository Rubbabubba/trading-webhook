import hashlib
import json
import sqlite3

import pytest

from opportunity_lab.kalshi_high_edge_shadow import PROTOCOL, init
from opportunity_lab.kalshi_v15_release_review import audit, review


def setup(db):
    init(db, 100)


def signal(db, sid, event, *, missing=False, late=0):
    source = {"outcome": "yes", "price_cents": 40, "fair_value_cents": "46",
              "gross_edge_cents": "6", "strategy_id": "microprice_value_maker_v12_shadow"}
    candidate = dict(source, price_cents=42, strategy_id=PROTOCOL["schema"])
    detail = {"challenger": candidate, "frozen_v12": source, "frozen_v10": None}
    db.execute("INSERT INTO high_edge_v15_signals VALUES(?,?,?,?,?)", (sid, "T", event, 200, json.dumps(detail)))
    value = {"challenger": {"gross_cents": "8", "stressed_cents": "6"},
             "frozen_v12": {"gross_cents": "10", "stressed_cents": "8"},
             "frozen_v10": {"gross_cents": "0", "stressed_cents": "0", "no_trade": True}}
    for horizon in (5, 30, 300):
        if not missing or horizon != 300:
            db.execute("INSERT INTO high_edge_v15_marks VALUES(?,?,?,?)",
                       (sid, horizon, 200 + horizon + late, json.dumps(value)))


def test_incomplete_signals_preserved_and_no_single_event_confidence():
    db = sqlite3.connect(":memory:"); setup(db)
    signal(db, 1, "A"); signal(db, 2, "B", missing=True)
    result = review(db, now=1000)
    assert result["signals"] == 2 and result["complete_signals"] == 1
    assert result["incomplete_or_invalid_signals"] == 1
    assert result["exclusion_reasons_nonexclusive"]["missing_horizon"] == 1
    assert result["horizons"]["5"]["event_cluster_lcb_cents"] is None
    assert not result["original_quote_screen_passed"]
    db.close()


def test_passing_synthetic_quote_screen_never_becomes_profit_or_order_authority():
    db = sqlite3.connect(":memory:"); setup(db)
    for sid in range(100): signal(db, sid, str(sid % 40))
    result = review(db, now=1000)
    assert result["original_quote_screen_passed"]
    assert result["release_decision"] == "prepare_separate_demo_trial"
    assert not result["execution_enabled"] and not result["profitability_proven"]
    assert not result["live_promotion_ready"] and not result["unused_new_holdout_supplied"]
    assert "actual_fills_and_fees_missing" in result["blockers"]
    db.close()


@pytest.mark.parametrize("defect", ["late", "bad_mid", "cost", "source", "predates", "nan"])
def test_invalid_evidence_never_qualifies(defect):
    db = sqlite3.connect(":memory:"); setup(db); signal(db, 1, "A", late=6 if defect == "late" else 0)
    if defect == "predates": db.execute("UPDATE high_edge_v15_signals SET at=99")
    if defect == "source":
        row = json.loads(db.execute("SELECT detail FROM high_edge_v15_signals").fetchone()[0])
        row["challenger"]["price_cents"] = 41
        db.execute("UPDATE high_edge_v15_signals SET detail=?", (json.dumps(row),))
    if defect in ("bad_mid", "cost", "nan"):
        row = json.loads(db.execute("SELECT detail FROM high_edge_v15_marks LIMIT 1").fetchone()[0])
        if defect == "bad_mid": row["frozen_v12"] = {"gross_cents": "11", "stressed_cents": "9"}
        if defect == "cost": row["challenger"]["stressed_cents"] = "7"
        if defect == "nan": row["challenger"]["gross_cents"] = "NaN"
        db.execute("UPDATE high_edge_v15_marks SET detail=? WHERE horizon=5", (json.dumps(row),))
    assert review(db, now=1000)["complete_signals"] == 0
    db.close()


def test_protocol_integrity_and_read_only_snapshot(tmp_path):
    path = tmp_path / "worker.sqlite3"; db = sqlite3.connect(path); setup(db)
    signal(db, 1, "A"); db.commit(); db.close()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    report = audit(path, now=1000)
    assert report["complete_signals"] == 1
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    db = sqlite3.connect(path); db.execute("UPDATE high_edge_v15_protocol SET detail='{}'")
    with pytest.raises(ValueError, match="protocol_mismatch"): review(db, now=1000)
    db.close()
