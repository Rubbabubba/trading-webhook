"""Separate, bounded-time measurement of unchanged frozen maker decisions."""
import json
from pathlib import Path

from .kalshi_maker_v10 import stressed_markout

PROTOCOL = json.loads((Path(__file__).resolve().parents[1] /
    "configs/kalshi_historical_execution_audit_v1_20261015/timely_measurement_registration.json").read_text())


def init(db, now):
    db.executescript("""CREATE TABLE IF NOT EXISTS timely_shadow_protocol(
      id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS timely_shadow_signals(
      strategy TEXT,source_id INTEGER,ticker TEXT,event TEXT,at REAL,outcome TEXT,price INTEGER,
      PRIMARY KEY(strategy,source_id));
      CREATE TABLE IF NOT EXISTS timely_shadow_marks(
      strategy TEXT,source_id INTEGER,horizon INTEGER,at REAL,detail TEXT,
      PRIMARY KEY(strategy,source_id,horizon));""")
    raw = json.dumps(PROTOCOL, sort_keys=True)
    db.execute("INSERT OR IGNORE INTO timely_shadow_protocol VALUES(1,?,?)", (raw, now))
    if db.execute("SELECT detail FROM timely_shadow_protocol WHERE id=1").fetchone()[0] != raw:
        raise ValueError("timely_measurement_protocol_changed")


def observe(db, ticker, *, now, yes_mid):
    start = db.execute("SELECT started_at FROM timely_shadow_protocol WHERE id=1").fetchone()[0]
    for strategy in ("v10", "v11", "v12"):
        count = db.execute("SELECT count(*) FROM timely_shadow_signals WHERE strategy=?", (strategy,)).fetchone()[0]
        if count < PROTOCOL["maximum_signals_per_strategy"]:
            for sid, event, at, outcome, price in db.execute(
                    f"SELECT id,event_id,observed_at,outcome,price_cents FROM {strategy}_shadow_signals "
                    "WHERE ticker=? AND observed_at>=? AND observed_at<=? ORDER BY observed_at,id LIMIT ?",
                    (ticker, max(start, now - 305), now, PROTOCOL["maximum_signals_per_strategy"])).fetchall():
                if count >= PROTOCOL["maximum_signals_per_strategy"]:
                    break
                cursor = db.execute("INSERT OR IGNORE INTO timely_shadow_signals VALUES(?,?,?,?,?,?,?)",
                                    (strategy, sid, ticker, event, at, outcome, price))
                count += cursor.rowcount
    for strategy, sid, at, outcome, price in db.execute(
            "SELECT strategy,source_id,at,outcome,price FROM timely_shadow_signals WHERE ticker=? AND at>=?",
            (ticker, now - 305)).fetchall():
        for horizon in PROTOCOL["horizons_seconds"]:
            if horizon <= now - at <= horizon + PROTOCOL["maximum_lateness_seconds"]:
                value = stressed_markout({"outcome": outcome, "price_cents": price}, yes_mid)
                db.execute("INSERT OR IGNORE INTO timely_shadow_marks VALUES(?,?,?,?,?)",
                           (strategy, sid, horizon, now, json.dumps(value, sort_keys=True)))


def pending_ticker(db, now):
    due = []
    for strategy, sid, ticker, at in db.execute(
            "SELECT strategy,source_id,ticker,at FROM timely_shadow_signals WHERE at>=?", (now - 305,)):
        marked = {r[0] for r in db.execute(
            "SELECT horizon FROM timely_shadow_marks WHERE strategy=? AND source_id=?", (strategy, sid))}
        for horizon in PROTOCOL["horizons_seconds"]:
            deadline = at + horizon + PROTOCOL["maximum_lateness_seconds"]
            if horizon not in marked and at + horizon <= now <= deadline:
                due.append((deadline, strategy, sid, ticker))
    return min(due)[3] if due else None


def status(db):
    start = db.execute("SELECT started_at FROM timely_shadow_protocol WHERE id=1").fetchone()[0]
    results = {}
    for strategy in ("v10", "v11", "v12"):
        total = db.execute("SELECT count(*) FROM timely_shadow_signals WHERE strategy=?", (strategy,)).fetchone()[0]
        complete = db.execute("SELECT s.source_id,s.event FROM timely_shadow_signals s JOIN timely_shadow_marks m "
                              "ON m.strategy=s.strategy AND m.source_id=s.source_id WHERE s.strategy=? "
                              "GROUP BY s.source_id HAVING count(*)=3", (strategy,)).fetchall()
        results[strategy] = {"signals": total, "complete_signals": len(complete),
                             "independent_complete_events": len({r[1] for r in complete})}
    return {"schema": PROTOCOL["schema"], "started_at": start, "strategies": results,
            "maximum_lateness_seconds": 5, "execution_enabled": False, "fill_assumed": False,
            "promotion_ready": False,
            "legacy_warning": "Legacy markout labels do not guarantee horizon timing. Preserve originals; use explicit timestamp audits and separate prospective measurements."}
