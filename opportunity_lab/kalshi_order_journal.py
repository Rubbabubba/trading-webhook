"""Offline order-lifecycle rehearsal. No network transport or live activation."""
import json
import sqlite3
from decimal import Decimal


class Journal:
    def __init__(self, path):
        self.db = sqlite3.connect(path, isolation_level=None, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS intents(
            id TEXT PRIMARY KEY, payload TEXT NOT NULL, reserve INTEGER NOT NULL,
            state TEXT NOT NULL, filled INTEGER NOT NULL DEFAULT 0, broker_id TEXT);
          CREATE TABLE IF NOT EXISTS controls(id INTEGER PRIMARY KEY, stopped INTEGER NOT NULL);
          INSERT OR IGNORE INTO controls VALUES(1, 0);
        """)

    def close(self):
        self.db.close()

    def stop(self):
        self.db.execute("UPDATE controls SET stopped=1 WHERE id=1")

    def reserve(self, client_id, ticker, contracts, price_cents, fee_reserve_cents, *, cash_cents):
        # Initial rehearsal supports buy-YES only. Prices and quantities are whole units.
        for value in (contracts, price_cents, fee_reserve_cents, cash_cents):
            if type(value) is not int:
                raise ValueError("integer_units_required")
        if not client_id or not ticker or contracts < 1 or not 1 <= price_cents <= 99 or fee_reserve_cents < 0:
            raise ValueError("invalid_intent")
        cost = contracts * price_cents + fee_reserve_cents
        payload = json.dumps({"ticker": ticker, "client_order_id": client_id, "side": "bid",
            "count": f"{contracts}.00", "price": f"{price_cents / 100:.4f}",
            "time_in_force": "immediate_or_cancel", "self_trade_prevention_type": "taker_at_cross",
            "cancel_order_on_pause": True, "subaccount": 0}, sort_keys=True)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            old = self.db.execute("SELECT payload,reserve FROM intents WHERE id=?", (client_id,)).fetchone()
            if old:
                if old != (payload, cost):
                    raise ValueError("client_id_reused_for_different_intent")
                self.db.execute("COMMIT")
                return False
            if self.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0]:
                raise ValueError("stopped")
            if self.db.execute("SELECT 1 FROM intents WHERE state='uncertain'").fetchone():
                raise ValueError("unreconciled_submission")
            used = self.db.execute("SELECT COALESCE(SUM(reserve),0) FROM intents").fetchone()[0]
            if cost > 500 or used + cost > min(cash_cents, 50000, 5000):
                raise ValueError("rehearsal_budget_exceeded")
            self.db.execute("INSERT INTO intents(id,payload,reserve,state) VALUES(?,?,?,'reserved')",
                            (client_id, payload, cost))
            self.db.execute("COMMIT")
            return True
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def mark_submission_started(self, client_id):
        # Persist uncertainty BEFORE an external adapter would send anything.
        # A crash/timeout must never cause blind resubmission after restart.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0]:
                raise ValueError("stopped")
            if self.db.execute("SELECT 1 FROM intents WHERE state='uncertain'").fetchone():
                raise ValueError("unreconciled_submission")
            changed = self.db.execute("UPDATE intents SET state='uncertain' WHERE id=? AND state='reserved'",
                                      (client_id,)).rowcount
            if changed != 1:
                raise ValueError("submission_not_allowed")
            payload = json.loads(self.db.execute("SELECT payload FROM intents WHERE id=?", (client_id,)).fetchone()[0])
            self.db.execute("COMMIT")
            return payload
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def reconcile(self, client_id, *, broker_id, filled, remaining, terminal):
        if type(filled) is not int or type(remaining) is not int or min(filled, remaining) < 0 or not broker_id:
            raise ValueError("invalid_broker_observation")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT payload,filled,state,broker_id FROM intents WHERE id=?", (client_id,)).fetchone()
            if row is None or row[2] == 'reserved':
                raise ValueError("intent_not_submitted")
            count = int(Decimal(json.loads(row[0])["count"]))
            if filled < row[1] or filled + remaining > count or (terminal and remaining != 0):
                raise ValueError("inconsistent_fill_counts")
            if row[3] and row[3] != broker_id:
                raise ValueError("broker_identity_changed")
            state = "terminal" if terminal else "working"
            if row[2] == "terminal" and (not terminal or filled != row[1]):
                raise ValueError("terminal_order_changed")
            # Keep the full reserve even after cancellation with partial fills.
            # Settlement/position reconciliation must be added before releasing it.
            self.db.execute("UPDATE intents SET state=?,filled=?,broker_id=? WHERE id=?",
                            (state, filled, broker_id, client_id))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
