"""Isolated, zero-AI production pilot. No grant means no account mutations.

Run only on a separate service with production-only secrets and its own disk.
Never add this worker's credentials to the Demo worker or a model prompt.
"""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.parse import urlparse
from urllib.request import Request, build_opener

from .kalshi_account_monitor import NoRedirect
from .kalshi_demo_market_data import DemoMarkets
from .kalshi_live_execution import LiveBinaryJournal, LiveBinaryBroker, LiveClient, grant_hash
from .kalshi_shadow import price_book
from .kalshi_external_sleeves import SPORTS_PREFIXES, PRICE_BINS
from .kalshi_factory_fee_probe import fee_basis


def write_json(path, value):
    path = Path(path); temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


class OwnerAuthority:
    def __init__(self, url, token):
        parsed = urlparse(url)
        if (parsed.scheme != "https" or parsed.path != "/execution/kalshi/grant"
                or parsed.username or parsed.password or parsed.query or parsed.fragment or not token):
            raise ValueError("configured_live_owner_authority_required")
        self.url, self.token = url, token
        self.opener = build_opener(NoRedirect())

    def fetch(self):
        request = Request(self.url, headers={"Authorization": "Bearer " + self.token})
        with self.opener.open(request, timeout=5) as response:
            raw = response.read(32769)
        if len(raw) > 32768:
            raise ValueError("live_authority_response_too_large")
        value = json.loads(raw)
        if value.get("schema") != "kalshi_live_authority_v1":
            raise ValueError("invalid_live_authority_response")
        return value.get("grant")

    def verify(self, digest):
        try:
            grant = self.fetch()
            return isinstance(grant, dict) and grant_hash(grant) == digest
        except Exception:
            return False

    def report(self, value):
        endpoint = urlparse(self.url)._replace(path="/execution/kalshi/status").geturl()
        request = Request(endpoint, json.dumps(value).encode(),
                          {"Authorization": "Bearer " + self.token, "Content-Type": "application/json"}, method="POST")
        with self.opener.open(request, timeout=5) as response:
            if response.status != 200: raise ValueError("live_status_not_accepted")


class ProductionMarkets(DemoMarkets):
    BASE = LiveClient.BASE_URL

    def quote(self, payload):
        market, _, _ = self.get(payload["ticker"]); row = market["market"]
        if (row.get("ticker") != payload["ticker"] or row.get("status") != "active"
                or row.get("market_type") != "binary" or row.get("exchange_index", 0) != 0):
            raise ValueError("production_market_not_active_binary")
        data, start, end = self.get(payload["ticker"], book=True, params={"depth": 20})
        return dict(data, environment="production", ticker=payload["ticker"], started_at=start, observed_at=end, market=row)


def select_entry(markets, journal, root):
    """Rotate through catalog pages; inspect at most three matching books/cycle."""
    root = Path(root); state_path = root / "catalog.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {"cursor": ""}
    spec = journal.grant["spec"]
    params = {"status": "open", "limit": 200}
    if state["cursor"]: params["cursor"] = state["cursor"]
    page, _, _ = markets.get(params=params)
    rows = page.get("markets")
    if not isinstance(rows, list) or len(rows) > 200 or not isinstance(page.get("cursor", ""), str):
        raise ValueError("invalid_production_catalog")
    # Catalog progress survives restarts even when a quote/fee request fails.
    write_json(state_path, {"cursor": page.get("cursor", "")})
    low, high = map(int, spec["price_bin"].split("-")); checked = 0
    for row in rows:
        ticker, event = row.get("ticker"), row.get("event_ticker")
        if not isinstance(ticker, str) or not isinstance(event, str) or not ticker or not event:
            continue
        stratum = "sports" if event.upper().startswith(SPORTS_PREFIXES) else "non_sports"
        family = row.get("category") or event.split("-")[0]
        if stratum != spec["stratum"] or spec.get("family", "*") not in ("*", family): continue
        if journal.db.execute("SELECT 1 FROM live_scope WHERE event_id=?", (event,)).fetchone(): continue
        try:
            expiry = datetime.fromisoformat((row.get("expiration_time") or row["close_time"]).replace("Z", "+00:00"))
            if not 300 <= expiry.timestamp() - journal.clock().timestamp() <= 86400: continue
        except (ValueError, TypeError, KeyError): continue
        if checked >= 3: break
        checked += 1
        quote = markets.quote({"ticker": ticker})
        for side in ("yes", "no"):
            if spec.get("side", "either") not in ("either", side): continue
            bid, ask, _, depth = price_book(quote, side)
            price = int((Decimal(ask.numerator) / Decimal(ask.denominator) * 100).to_integral_value(rounding=ROUND_CEILING))
            observed_bin = next((f"{a}-{b}" for a, b in PRICE_BINS if a <= price <= b), None)
            if observed_bin != spec["price_bin"] or depth < 1 or (ask - bid) * 100 > journal.live_limits["spread_cents"]: continue
            event_data, _, event_at = markets.get_event(event)
            series_data, _, series_at = markets.get_series(event_data["event"]["series_ticker"])
            basis = fee_basis({"observed_at": datetime.fromtimestamp(quote["observed_at"], timezone.utc).isoformat(),
                               "event_id": event, "price_cents": price, "market_type": "binary", "exchange_index": 0},
                              event_data["event"], series_data["series"], fetched_at=max(event_at, series_at))
            if basis["model_fee_upper_bound_cents"] > 2:
                continue
            cid = "live-" + hashlib.sha256((journal.grant["approval_id"] + ":" + event).encode()).hexdigest()[:32]
            return {"client_id": cid, "ticker": ticker, "event_id": event, "outcome": side, "price_cents": price,
                    "fee_reserve_cents": 5, "fee_basis": basis}
    return None


def cycle(broker, markets, root):
    """Reconcile first, including during revocation; no uncertain write retries."""
    broker.recover()
    journal = broker.journal
    if journal.approval_verifier(grant_hash(journal.grant)) is not True:
        return {"state": "owner_authority_unavailable_or_revoked", "new_entries": False}
    if journal.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0]:
        return {"state": "stopped", "new_entries": False}
    if any(record["state"] != "terminal" for record in journal.records()):
        return {"state": "awaiting_order_reconciliation", "new_entries": False}
    if journal.accounting()["positions"]:
        return {"state": "awaiting_settlement", "new_entries": False}
    plan = select_entry(markets, journal, root)
    if plan is None: return {"state": "scanning", "new_entries": False}
    snapshot = broker.snapshot()
    journal.reserve(plan["client_id"], plan["ticker"], 1, plan["price_cents"], plan["fee_reserve_cents"],
                    event_id=plan["event_id"], outcome=plan["outcome"], action="buy", account_snapshot=snapshot)
    try:
        result = broker.submit(plan["client_id"], quote_provider=markets.quote)
        if result["state"] == "working": result = broker.cancel(plan["client_id"])
    except Exception:
        if journal.get(plan["client_id"])["state"] == "reserved": journal.abandon_reserved(plan["client_id"])
        raise
    return {"state": result["state"], "new_entries": True, "filled": result["filled"],
            "strategy_id": journal.grant["strategy_id"], "client_id": plan["client_id"]}


def run(root, *, once=False):
    root = Path(root); root.mkdir(parents=True, exist_ok=True)
    authority = OwnerAuthority(os.environ.get("LIFE_OS_KALSHI_LIVE_GRANT_URL", ""),
                               os.environ.get("LIFE_OS_KALSHI_LIVE_TOKEN", ""))
    journal = broker = None
    while True:
        try:
            if broker is None:
                cached = root / "owner-grant.json"
                # Recover previously approved orders even when the authority
                # is down/revoked; _gate still forbids every new submission.
                grant = json.loads(cached.read_text()) if cached.exists() else authority.fetch()
                if grant is None:
                    result = {"state": "awaiting_owner_grant", "new_entries": False}
                else:
                    recovering = (root / "journal.sqlite3").exists()
                    journal = LiveBinaryJournal(root / "journal.sqlite3", grant, approval_verifier=authority.verify, recovery_only=recovering)
                    write_json(cached, grant)
                    client = LiveClient(os.environ.get("KALSHI_LIVE_API_KEY_ID", ""),
                                        os.environ.get("KALSHI_LIVE_PRIVATE_KEY_PATH", ""), grant,
                                        approval_verifier=authority.verify, recovery_only=recovering,
                                        demo_key_id=os.environ.get("KALSHI_DEMO_API_KEY_ID"))
                    broker = LiveBinaryBroker(journal, client); markets = ProductionMarkets()
                    result = cycle(broker, markets, root)
            else:
                result = cycle(broker, markets, root)
        except Exception as error:
            if broker is None and journal is not None:
                journal.close(); journal = None
            result = {"state": "blocked", "new_entries": False, "error_type": type(error).__name__}
            # Log no response bodies, credentials, request headers or key paths.
        result.update(schema="kalshi_live_status_v1", environment="production",
                      generated_at=datetime.now(timezone.utc).isoformat(), ai_tokens=0, ai_cost_usd=0)
        if broker is not None:
            try:
                ledger = journal.accounting()
                result.update(net_pnl_cents=round(float(ledger["realized"] * 100), 2),
                              fees_cents=round(float(ledger["fees"] * 100), 2),
                              open_positions=len(ledger["positions"]),
                              unresolved_orders=sum(r["state"] != "terminal" for r in journal.records()))
            except Exception:
                result.update(state="blocked", error_type="AccountingError", new_entries=False)
        write_json(root / "status.json", result)
        try:
            authority.report(result)
        except Exception:
            pass  # Reporting outages cannot cause an order retry or halt recovery.
        if once:
            if journal: journal.close()
            return result
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--root", required=True); parser.add_argument("--once", action="store_true")
    args = parser.parse_args(); run(args.root, once=args.once)
