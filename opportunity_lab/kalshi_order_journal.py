"""Durable order-lifecycle journal. No network transport or live activation."""
import json
import sqlite3
from decimal import Decimal
from datetime import datetime, timezone
from fractions import Fraction


class Journal:
    def __init__(self, path, *, clock=None):
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.db = sqlite3.connect(path, isolation_level=None, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS intents(
            id TEXT PRIMARY KEY, payload TEXT NOT NULL, reserve INTEGER NOT NULL,
            state TEXT NOT NULL, filled INTEGER NOT NULL DEFAULT 0, broker_id TEXT);
          CREATE TABLE IF NOT EXISTS controls(id INTEGER PRIMARY KEY, stopped INTEGER NOT NULL);
          INSERT OR IGNORE INTO controls VALUES(1, 0);
          CREATE TABLE IF NOT EXISTS broker_evidence(
            client_id TEXT PRIMARY KEY, detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS risk_policy(id INTEGER PRIMARY KEY, daily_loss_cents INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS risk_stops(day TEXT PRIMARY KEY);
          CREATE TABLE IF NOT EXISTS settlements(ticker TEXT PRIMARY KEY, detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS journal_scope(id INTEGER PRIMARY KEY, environment TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS production_policy(id INTEGER PRIMARY KEY, detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS submission_quotes(client_id TEXT PRIMARY KEY, detail TEXT NOT NULL);
        """)

    def close(self):
        self.db.close()

    def bind_environment(self, environment):
        if environment not in ("demo", "production"):
            raise ValueError("invalid_journal_environment")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            old = self.db.execute("SELECT environment FROM journal_scope WHERE id=1").fetchone()
            if old and old[0] != environment:
                raise ValueError("journal_environment_mismatch")
            if not old and environment == "production" and self.db.execute("SELECT 1 FROM intents LIMIT 1").fetchone():
                raise ValueError("production_requires_empty_scoped_journal")
            self.db.execute("INSERT OR IGNORE INTO journal_scope VALUES(1,?)", (environment,))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def stop(self):
        self.db.execute("UPDATE controls SET stopped=1 WHERE id=1")

    def configure_production_policy(self, *, order_limit_cents, capital_limit_cents, daily_loss_limit_cents):
        policy = dict(order_limit_cents=order_limit_cents, capital_limit_cents=capital_limit_cents,
                      daily_loss_limit_cents=daily_loss_limit_cents)
        if any(type(v) is not int or v <= 0 for v in policy.values()) or order_limit_cents > capital_limit_cents:
            raise ValueError("invalid_production_policy")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.db.execute("SELECT environment FROM journal_scope WHERE id=1").fetchone() != ("production",):
                raise ValueError("production_journal_scope_required")
            detail = json.dumps(policy, sort_keys=True)
            self.db.execute("INSERT OR IGNORE INTO production_policy VALUES(1,?)", (detail,))
            if self.db.execute("SELECT detail FROM production_policy WHERE id=1").fetchone()[0] != detail:
                raise ValueError("production_policy_change_not_allowed")
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def _production_gate(self, snapshot, client_id=None, *, side="bid"):
        if self.db.execute("SELECT environment FROM journal_scope WHERE id=1").fetchone() != ("production",):
            return None
        from .kalshi_production_risk import evaluate
        row = self.db.execute("SELECT detail FROM production_policy WHERE id=1").fetchone()
        if row is None:
            raise ValueError("production_policy_missing")
        if snapshot is None:
            raise ValueError("production_snapshot_required")
        policy = json.loads(row[0])
        checks = evaluate(self.db, snapshot, {k:v for k,v in policy.items() if k != "order_limit_cents"},
                          now=self.clock(), reserved_client_id=client_id)
        loss_reached = "daily_loss_limit_reached" in checks["blockers"]
        if loss_reached:
            self.db.execute("INSERT OR IGNORE INTO risk_stops VALUES(?)", (checks["accounting"]["day"],))
        blockers = checks["blockers"]
        if side == "ask":
            # Only inventory-reducing exits may proceed through entry risk stops.
            # Scope, freshness, cash, hard stops and reconciliation still apply.
            blockers = [b for b in blockers if b not in
                        ("daily_loss_limit_reached", "daily_loss_stop_latched", "capital_limit_exceeded")]
        if blockers:
            if loss_reached:
                self.db.execute("COMMIT")
            raise ValueError("production_risk_blocked:" + ",".join(blockers))
        return policy

    def get(self, client_id):
        row = self.db.execute("SELECT payload,reserve,state,filled,broker_id FROM intents WHERE id=?",
                              (client_id,)).fetchone()
        if row is None:
            raise ValueError("unknown_intent")
        return dict(payload=json.loads(row[0]), reserve=row[1], state=row[2],
                    filled=row[3], broker_id=row[4])

    def records(self):
        return [self.get(row[0]) for row in self.db.execute("SELECT id FROM intents ORDER BY rowid")]

    def configure_risk(self, daily_loss_limit_cents):
        """Explicit rehearsal policy; cannot silently change an existing limit."""
        if type(daily_loss_limit_cents) is not int or daily_loss_limit_cents <= 0:
            raise ValueError("invalid_loss_limit")
        self.db.execute("INSERT OR IGNORE INTO risk_policy VALUES(1,?)", (daily_loss_limit_cents,))
        if self.db.execute("SELECT daily_loss_cents FROM risk_policy WHERE id=1").fetchone()[0] != daily_loss_limit_cents:
            raise ValueError("risk_policy_change_not_allowed")

    def accounting(self):
        from .kalshi_execution_risk import replay
        own_transaction = not self.db.in_transaction
        if own_transaction:
            self.db.execute("BEGIN")
        try:
            evidence = {r[0]: json.loads(r[1]) for r in self.db.execute("SELECT client_id,detail FROM broker_evidence")}
            settlements = [json.loads(r[0]) for r in self.db.execute("SELECT detail FROM settlements")]
            return replay(self.records(), evidence, as_of=self.clock(), settlements=settlements)
        finally:
            if own_transaction:
                self.db.execute("ROLLBACK")

    def record_settlement(self, row):
        """Persist broker evidence only if complete accounting validates atomically."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            ticker = row.get("ticker")
            old = self.db.execute("SELECT detail FROM settlements WHERE ticker=?", (ticker,)).fetchone()
            detail = json.dumps(row, sort_keys=True)
            if old and old[0] != detail:
                raise ValueError("settlement_history_changed")
            self.db.execute("INSERT OR IGNORE INTO settlements VALUES(?,?)", (ticker, detail))
            self.accounting()
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def _risk_gate(self, side):
        policy = self.db.execute("SELECT daily_loss_cents FROM risk_policy WHERE id=1").fetchone()
        if policy is None:
            return None
        state = self.accounting()
        if state["daily_low"] <= -Fraction(policy[0], 100):
            self.db.execute("INSERT OR IGNORE INTO risk_stops VALUES(?)", (state["day"],))
        if side == "bid" and self.db.execute("SELECT 1 FROM risk_stops WHERE day=?", (state["day"],)).fetchone():
            # Commit the stop latch even though this entry is rejected.
            self.db.execute("COMMIT")
            raise ValueError("daily_loss_stop")
        return state

    def reserve(self, client_id, ticker, contracts, price_cents, fee_reserve_cents, *, cash_cents, side="bid", account_snapshot=None):
        # Whole-contract YES buys and reduce-only YES exits; no short sales.
        for value in (contracts, price_cents, fee_reserve_cents, cash_cents):
            if type(value) is not int:
                raise ValueError("integer_units_required")
        if (not isinstance(client_id, str) or not client_id.strip()
                or not isinstance(ticker, str) or not ticker.strip() or side not in ("bid", "ask")
                or contracts < 1 or not 1 <= price_cents <= 99 or fee_reserve_cents < 0):
            raise ValueError("invalid_intent")
        cost = (contracts * price_cents if side == "bid" else 0) + fee_reserve_cents
        data = {"ticker": ticker, "client_order_id": client_id, "side": side,
            "count": f"{contracts}.00", "price": f"{price_cents / 100:.4f}",
            "time_in_force": "immediate_or_cancel", "self_trade_prevention_type": "taker_at_cross",
            "cancel_order_on_pause": True, "subaccount": 0}
        if side == "ask":
            data["reduce_only"] = True
        payload = json.dumps(data, sort_keys=True)
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
            if self.db.execute("SELECT 1 FROM settlements WHERE ticker=?", (ticker,)).fetchone():
                raise ValueError("market_already_settled")
            production = self._production_gate(account_snapshot, side=side)
            if production and cash_cents != account_snapshot["balance"]["balance"]:
                raise ValueError("production_cash_mismatch")
            if side == "ask":
                available = 0
                for record in self.records():
                    order = record["payload"]
                    if order["ticker"] != ticker:
                        continue
                    if order["side"] == "bid":
                        available += record["filled"]
                    else:
                        # Include reserved/unfilled exits so two exits cannot claim the same position.
                        available -= (record["filled"] if record["state"] == "terminal"
                                      else int(Decimal(order["count"])))
                if contracts > available:
                    raise ValueError("exit_exceeds_owned_position")
            risk = self.accounting() if production else self._risk_gate(side)
            used = (risk["capital_at_risk"] * 100 if risk is not None else
                    self.db.execute("SELECT COALESCE(SUM(reserve),0) FROM intents").fetchone()[0])
            # With reconciled accounting, reductions may still reserve their fees
            # at an exposure cap; they cannot increase owned inventory.
            if risk is None:
                over_budget = used + cost > min(cash_cents, 50000, 5000)
            elif side == "ask":
                over_budget = risk["pending_reserves"] * 100 + cost > cash_cents
            else:
                over_budget = (used + cost > (production["capital_limit_cents"] if production else 5000)
                               or risk["pending_reserves"] * 100 + cost > cash_cents)
            if cost > (production["order_limit_cents"] if production else 500) or over_budget:
                raise ValueError("rehearsal_budget_exceeded")
            self.db.execute("INSERT INTO intents(id,payload,reserve,state) VALUES(?,?,?,'reserved')",
                            (client_id, payload, cost))
            self.db.execute("COMMIT")
            return True
        except Exception:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def acknowledge(self, client_id, broker_id, filled):
        """Bind a create response without clearing uncertainty before full reconciliation."""
        if not isinstance(broker_id, str) or not broker_id.strip() or type(filled) is not int:
            raise ValueError("invalid_acknowledgement")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            record = self.get(client_id)
            if record["state"] != "uncertain" or not record["filled"] <= filled <= int(Decimal(record["payload"]["count"])):
                raise ValueError("invalid_acknowledgement")
            if record["broker_id"] not in (None, broker_id) or self.db.execute(
                    "SELECT 1 FROM intents WHERE broker_id=? AND id!=?", (broker_id, client_id)).fetchone():
                raise ValueError("broker_identity_changed")
            self.db.execute("UPDATE intents SET broker_id=?,filled=? WHERE id=?", (broker_id, filled, client_id))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def mark_cancel_started(self, client_id):
        # Cancellation remains available while stopped; it never releases the reservation.
        changed = self.db.execute("UPDATE intents SET state='uncertain' WHERE id=? AND state='working' AND broker_id IS NOT NULL",
                                  (client_id,)).rowcount
        if changed != 1:
            raise ValueError("cancel_not_allowed")

    def mark_reconciliation_started(self, client_id):
        self.db.execute("UPDATE intents SET state='uncertain' WHERE id=? AND state='working'", (client_id,))

    def mark_submission_started(self, client_id, *, account_snapshot=None, quote_snapshot=None):
        # Persist uncertainty BEFORE an external adapter would send anything.
        # A crash/timeout must never cause blind resubmission after restart.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0]:
                raise ValueError("stopped")
            if self.db.execute("SELECT 1 FROM intents WHERE state='uncertain'").fetchone():
                raise ValueError("unreconciled_submission")
            record = self.get(client_id)
            production = self._production_gate(account_snapshot, client_id, side=record["payload"]["side"])
            if production:
                if record["payload"]["side"] == "ask":
                    owned = self.accounting()["positions"].get(record["payload"]["ticker"], 0)
                    if record["payload"].get("reduce_only") is not True or Decimal(record["payload"]["count"]) > owned:
                        raise ValueError("exit_exceeds_owned_position")
                from .kalshi_quote_guard import validate
                validate(self.get(client_id)["payload"], quote_snapshot, now=self.clock())
                self.db.execute("INSERT INTO submission_quotes VALUES(?,?)",
                                (client_id,json.dumps(quote_snapshot,sort_keys=True)))
            if not production:
                self._risk_gate(self.get(client_id)["payload"]["side"])
            changed = self.db.execute("UPDATE intents SET state='uncertain' WHERE id=? AND state='reserved'",
                                      (client_id,)).rowcount
            if changed != 1:
                raise ValueError("submission_not_allowed")
            payload = json.loads(self.db.execute("SELECT payload FROM intents WHERE id=?", (client_id,)).fetchone()[0])
            self.db.execute("COMMIT")
            return payload
        except Exception:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def reconcile(self, client_id, *, broker_id, filled, remaining, terminal, evidence=None):
        if (type(filled) is not int or type(remaining) is not int
                or type(terminal) is not bool or min(filled, remaining) < 0
                or not isinstance(broker_id, str) or not broker_id.strip()):
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
            if self.db.execute("SELECT 1 FROM intents WHERE broker_id=? AND id!=?",
                               (broker_id, client_id)).fetchone():
                raise ValueError("broker_identity_reused")
            state = "terminal" if terminal else "working"
            if row[2] == "terminal" and (not terminal or filled != row[1]):
                raise ValueError("terminal_order_changed")
            if evidence is not None:
                old = self.db.execute("SELECT detail FROM broker_evidence WHERE client_id=?", (client_id,)).fetchone()
                if old:
                    old_fills = {f["fill_id"]: f for f in json.loads(old[0])["fills"]}
                    new_fills = {f["fill_id"]: f for f in evidence["fills"]}
                    if any(new_fills.get(key) != value for key, value in old_fills.items()):
                        raise ValueError("historical_fill_changed")
                self.db.execute("INSERT OR REPLACE INTO broker_evidence VALUES(?,?)",
                                (client_id, json.dumps(evidence, sort_keys=True)))
            # Preserve the original reservation as audit evidence. Opted-in risk
            # accounting calculates effective exposure from reconciled fills;
            # legacy journals continue retaining their full reservations.
            self.db.execute("UPDATE intents SET state=?,filled=?,broker_id=? WHERE id=?",
                            (state, filled, broker_id, client_id))
            self.validate_reconciliation()
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def validate_reconciliation(self):
        """Extension hook executed inside the evidence/state transaction."""
