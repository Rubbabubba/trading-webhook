"""Separate production execution controls, dormant without verified approval.

The initial runner supports collateralized one-contract buy-to-settlement only.
Transport mutations retain the existing no-retry and uncertainty journal rules.
"""
from __future__ import annotations

import hashlib
import json
import re

from .kalshi_binary_journal import BinaryJournal
from .kalshi_order_journal import Journal
from .kalshi_binary_broker import BinaryDemoBroker
from .kalshi_demo_broker import DemoClient
from .kalshi_strategy_factory import fingerprint


def grant_hash(grant):
    return hashlib.sha256(json.dumps(grant, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_grant(grant, approval_verifier, *, recovery_only=False):
    required = {"schema", "approval_id", "strategy_id", "spec", "spec_hash", "evidence_sha256",
                "account_key_sha256", "limits", "limit_revision"}
    if not isinstance(grant, dict) or set(grant) != required or grant["schema"] != "kalshi_live_grant_v1":
        raise ValueError("invalid_live_grant")
    if fingerprint(grant["spec"]) != grant["spec_hash"]:
        raise ValueError("live_strategy_hash_mismatch")
    if any(not isinstance(grant[key], str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{4,120}", grant[key])
           for key in ("approval_id", "strategy_id")):
        raise ValueError("invalid_live_identity")
    for key in ("evidence_sha256", "account_key_sha256"):
        value = grant[key]
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("invalid_live_binding")
    fields = {"capital_cents", "daily_loss_cents", "total_loss_cents", "event_exposure_cents",
              "portfolio_exposure_cents", "order_contracts", "open_orders", "quote_age_seconds", "spread_cents"}
    limits = grant["limits"]
    if not isinstance(limits, dict) or set(limits) != fields or any(type(v) is not int or v <= 0 for v in limits.values()):
        raise ValueError("invalid_live_limits")
    if not (limits["daily_loss_cents"] <= limits["total_loss_cents"] <= limits["capital_cents"]
            and limits["event_exposure_cents"] <= limits["portfolio_exposure_cents"] <= limits["capital_cents"]
            and limits["order_contracts"] == limits["open_orders"] == 1
            and limits["quote_age_seconds"] <= 5 and limits["spread_cents"] <= 99):
        raise ValueError("unsupported_live_pilot_limits")
    if type(grant["limit_revision"]) is not int or grant["limit_revision"] < 1:
        raise ValueError("live_limit_revision_missing")
    # A boolean in a model card or a local config is not an approval verifier.
    if not callable(approval_verifier) or (not recovery_only and approval_verifier(grant_hash(grant)) is not True):
        raise ValueError("verified_owner_approval_required")
    return grant


class LiveBinaryJournal(BinaryJournal):
    environment = "production"

    def __init__(self, path, grant, *, approval_verifier, clock=None, recovery_only=False):
        if recovery_only:
            from pathlib import Path
            import sqlite3
            db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
            try:
                if json.loads(db.execute("SELECT detail FROM live_grant WHERE id=1").fetchone()[0]) != grant:
                    raise ValueError("existing_live_grant_required_for_recovery")
            finally:
                db.close()
        self.grant = validate_grant(grant, approval_verifier, recovery_only=recovery_only)
        self.approval_verifier = approval_verifier
        self.live_limits = grant["limits"]
        self._pending_scope = None
        super().__init__(path, clock=clock,
                         order_limit_cents=self.live_limits["event_exposure_cents"],
                         capital_limit_cents=self.live_limits["portfolio_exposure_cents"],
                         daily_loss_cents=self.live_limits["daily_loss_cents"])
        self.db.executescript("CREATE TABLE IF NOT EXISTS live_grant(id INTEGER PRIMARY KEY,detail TEXT NOT NULL);"
                             "CREATE TABLE IF NOT EXISTS live_scope(client_id TEXT PRIMARY KEY,event_id TEXT NOT NULL);"
                             "CREATE TABLE IF NOT EXISTS live_bootstrap(id INTEGER PRIMARY KEY,cash_cents INTEGER NOT NULL);")
        encoded = json.dumps(grant, sort_keys=True)
        self.db.execute("INSERT OR IGNORE INTO live_grant VALUES(1,?)", (encoded,))
        if self.db.execute("SELECT detail FROM live_grant WHERE id=1").fetchone()[0] != encoded:
            self.close(); raise ValueError("live_grant_changed")

    def bind_environment(self, environment):
        if environment != "production":
            raise ValueError("production_journal_required")
        return Journal.bind_environment(self, environment)

    def reserve(self, client_id, ticker, contracts, price_cents, fee_reserve_cents, *, event_id,
                outcome, action, account_snapshot, order_mode="ioc"):
        if action != "buy" or order_mode != "ioc" or contracts != 1:
            raise ValueError("live_pilot_buy_one_ioc_only")
        if not isinstance(event_id, str) or not event_id or len(event_id) > 120:
            raise ValueError("invalid_live_event")
        old = self.db.execute("SELECT event_id FROM live_scope WHERE client_id=?", (client_id,)).fetchone()
        if old and old[0] != event_id:
            raise ValueError("live_event_binding_changed")
        self._pending_scope = client_id, event_id
        try:
            return super().reserve(client_id, ticker, contracts, price_cents, fee_reserve_cents,
                                   outcome=outcome, action=action, account_snapshot=account_snapshot, order_mode=order_mode)
        finally:
            self._pending_scope = None

    def _gate(self, intent, ticker, snapshot, *, exclude=None):
        if self.approval_verifier(grant_hash(self.grant)) is not True:
            raise ValueError("live_approval_revoked")
        # Compare the verified transport identity before every reservation and write.
        if snapshot.get("account_key_sha256") != self.grant["account_key_sha256"]:
            raise ValueError("live_account_binding_mismatch")
        state = self.accounting(exclude=exclude)
        if not self.db.execute("SELECT 1 FROM live_bootstrap").fetchone():
            if self.records() or state["positions"] or snapshot["balance"]["balance"] < self.live_limits["capital_cents"]:
                raise ValueError("live_pilot_funding_or_flat_start_missing")
            self.db.execute("INSERT INTO live_bootstrap VALUES(1,?)", (snapshot["balance"]["balance"],))
        scope = self._pending_scope
        if scope is None and exclude:
            row = self.db.execute("SELECT event_id FROM live_scope WHERE client_id=?", (exclude,)).fetchone()
            scope = (exclude, row[0]) if row else None
        if scope is None:
            raise ValueError("live_event_scope_missing")
        risk = intent["count"] * intent["price_cents"] + intent["fee_cents"]
        at_risk = state["capital_at_risk"] * 100
        # A worst-case exposure reserve also covers unmarked/open-position losses.
        if (at_risk + risk > self.live_limits["daily_loss_cents"] + min(0, state["daily_low"] * 100)
                or at_risk + risk > self.live_limits["total_loss_cents"] + min(0, state["realized"] * 100)):
            raise ValueError("live_loss_budget_exhausted")
        pending = [r for r in self.records() if r["payload"]["client_order_id"] != exclude and r["state"] != "terminal"]
        if pending:
            raise ValueError("live_serial_order_required")
        # The pilot never re-enters a parent event. This bounds correlated contracts.
        if self.db.execute("SELECT 1 FROM live_scope WHERE event_id=? AND client_id!=?", (scope[1], scope[0])).fetchone():
            raise ValueError("live_event_already_entered")
        if risk > self.live_limits["event_exposure_cents"]:
            raise ValueError("live_event_exposure_exceeded")
        reserve = super()._gate(intent, ticker, snapshot, exclude=exclude)
        self.db.execute("INSERT OR IGNORE INTO live_scope VALUES(?,?)", scope)
        return reserve

    def mark_submission_started(self, client_id, *, account_snapshot=None, quote_snapshot=None, demo_probe=False):
        if demo_probe or not isinstance(quote_snapshot, dict):
            raise ValueError("production_quote_required")
        from .kalshi_shadow import price_book
        record = self.get(client_id)
        bid, ask, _, _ = price_book(quote_snapshot, record["intent"]["outcome"])
        if (ask - bid) * 100 > self.live_limits["spread_cents"]:
            raise ValueError("live_spread_limit")
        if not 0 <= self.clock().timestamp() - quote_snapshot["observed_at"] <= self.live_limits["quote_age_seconds"]:
            raise ValueError("live_quote_age_limit")
        spec = self.grant["spec"]; market = quote_snapshot.get("market") or {}
        low, high = map(int, spec["price_bin"].split("-"))
        price = record["intent"]["price_cents"]
        from .kalshi_external_sleeves import SPORTS_PREFIXES, PRICE_BINS
        event = market.get("event_ticker", "")
        stratum = "sports" if event.upper().startswith(SPORTS_PREFIXES) else "non_sports"
        family = market.get("category") or event.split("-")[0]
        observed_bin = next((f"{a}-{b}" for a, b in PRICE_BINS if a <= price <= b), None)
        from datetime import datetime
        expiry = datetime.fromisoformat((market.get("expiration_time") or market["close_time"]).replace("Z", "+00:00"))
        if (observed_bin != spec["price_bin"] or stratum != spec["stratum"]
                or not 300 <= expiry.timestamp() - self.clock().timestamp() <= 86400
                or spec.get("side", "either") not in ("either", record["intent"]["outcome"])
                or spec.get("family", "*") not in ("*", family)
                or market.get("market_type") != "binary" or market.get("exchange_index", 0) != 0
                or market.get("event_ticker") != self.db.execute("SELECT event_id FROM live_scope WHERE client_id=?", (client_id,)).fetchone()[0]):
            raise ValueError("live_strategy_scope_mismatch")
        return super().mark_submission_started(client_id, account_snapshot=account_snapshot, quote_snapshot=quote_snapshot)


class LiveClient(DemoClient):
    BASE_URL = "https://external-api.kalshi.com/trade-api/v2"

    def __init__(self, key_id, key_path, grant, *, approval_verifier, demo_key_id=None, recovery_only=False, **kwargs):
        validate_grant(grant, approval_verifier, recovery_only=recovery_only)
        if key_id == demo_key_id or hashlib.sha256(key_id.encode()).hexdigest() != grant["account_key_sha256"]:
            raise ValueError("separate_live_key_required")
        super().__init__(key_id, key_path, **kwargs)


class LiveBinaryBroker(BinaryDemoBroker):
    def __init__(self, journal, client):
        if not isinstance(journal, LiveBinaryJournal) or not isinstance(client, LiveClient):
            raise ValueError("isolated_live_transport_and_journal_required")
        self.journal, self.client = journal, client
        self.quarantined_reserve_cents = 0

    def _check_other_journals(self):
        self.quarantined_reserve_cents = 0

    def snapshot(self):
        result = super().snapshot()
        result["account_key_sha256"] = hashlib.sha256(self.client.key_id.encode()).hexdigest()
        return result

    def recover(self):
        for record in self.journal.records():
            cid = record["payload"]["client_order_id"]
            if record["state"] == "reserved":
                self.journal.abandon_reserved(cid)
            elif record["state"] in ("uncertain", "working"):
                current = self.refresh(cid)
                if current["state"] == "working":
                    self.cancel(cid)
        self.reconcile_settlements()
        return self.reconcile_positions()
