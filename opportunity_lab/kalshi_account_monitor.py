"""Authenticated production account observer. GET-only; never submits orders."""
from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import sqlite3
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener, HTTPRedirectHandler

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

BASE = "https://external-api.kalshi.com"
ROOT = "/trade-api/v2"
ALLOWED = {"/portfolio/balance", "/portfolio/positions", "/portfolio/orders"}
CAPITAL_CEILING_CENTS = 50_000


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward account authorization to a redirected host.
        return None


class AccountClient:
    def __init__(self, key_id, key_path, *, opener=None, clock=time.time, sleep=time.sleep):
        self.key_id = key_id
        self.key = serialization.load_pem_private_key(Path(key_path).read_bytes(), password=None)
        self.opener = opener or build_opener(NoRedirect())
        self.clock, self.sleep, self.next_request = clock, sleep, 0.0

    def get(self, path, params=None):
        if path not in ALLOWED:
            raise ValueError("endpoint_not_allowed")
        for attempt in range(3):
            self.sleep(max(0, self.next_request - self.clock()))
            stamp = str(int(self.clock() * 1000))
            signature = self.key.sign((stamp + "GET" + ROOT + path).encode(),
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256())
            url = BASE + ROOT + path
            if params:
                url += "?" + urlencode(params)
            request = Request(url, method="GET", headers={
                "KALSHI-ACCESS-KEY": self.key_id,
                "KALSHI-ACCESS-TIMESTAMP": stamp,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
                "Accept": "application/json"})
            self.next_request = self.clock() + 2
            try:
                with self.opener.open(request, timeout=20) as response:
                    return json.load(response)
            except HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504) or attempt == 2:
                    raise RuntimeError(f"kalshi_http_{error.code}") from None
                self.next_request = self.clock() + 30 * 2 ** attempt
        raise RuntimeError("unreachable")

    def pages(self, path, field, **params):
        rows, seen, cursor = [], set(), None
        for _ in range(100):
            query = {**params, "limit": 100}
            if cursor:
                query["cursor"] = cursor
            result = self.get(path, query)
            if not isinstance(result.get(field), list):
                raise ValueError("invalid_account_response")
            rows.extend(result[field])
            cursor = result.get("cursor")
            if not cursor:
                return rows
            if cursor in seen:
                raise ValueError("repeated_account_cursor")
            seen.add(cursor)
        raise ValueError("account_pagination_incomplete")


def collect(client):
    # All endpoints use the same explicit primary account scope.
    started = time.time()
    balance = client.get("/portfolio/balance", {"subaccount": 0})
    positions = client.pages("/portfolio/positions", "market_positions", subaccount=0,
                             count_filter="position")
    orders = client.pages("/portfolio/orders", "orders", subaccount=0, status="resting")
    for field in ("balance", "portfolio_value"):
        if type(balance.get(field)) is not int or balance[field] < 0:
            raise ValueError("invalid_balance_response")
    return {"started_at": started, "observed_at": time.time(), "balance": balance,
            "positions": positions, "resting_orders": orders, "subaccount": 0}


def summarize(snapshot):
    cash = snapshot["balance"]["balance"]
    return {"account_connected": True, "observed_at": snapshot["observed_at"],
            "available_cash_cents": cash,
            "portfolio_value_cents": snapshot["balance"]["portfolio_value"],
            "configured_capital_ceiling_cents": CAPITAL_CEILING_CENTS,
            "cash_within_ceiling_cents": min(cash, CAPITAL_CEILING_CENTS),
            "funding_target_met": cash >= CAPITAL_CEILING_CENTS,
            "position_rows": len(snapshot["positions"]),
            "resting_orders": len(snapshot["resting_orders"]),
            "execution_enabled": False, "live_ready": False,
            "stage": "account_observer",
            "remaining_work": ["order_lifecycle_adapter_and_fault_tests",
                "strategy_release_evidence", "host_deployment_and_restart_test",
                "user_live_activation"]}


def write_json(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="sports_paper/production_readiness_20260914")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=int, default=300)
    args = parser.parse_args()
    if args.interval < 60:
        parser.error("interval must be at least 60 seconds")
    directory = Path(args.output)
    directory.mkdir(parents=True, exist_ok=True)
    from .college_football_paper import lock_process
    lock = lock_process(directory / "observer.lock")
    db = sqlite3.connect(directory / "account.sqlite3", timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS snapshots(at REAL PRIMARY KEY, detail TEXT NOT NULL)")
    try:
        client = AccountClient(os.environ["KALSHI_API_KEY_ID"], os.environ["KALSHI_PRIVATE_KEY_PATH"])
        while True:
            try:
                snapshot = collect(client)
                with db:
                    db.execute("INSERT INTO snapshots VALUES(?, ?)",
                               (snapshot["observed_at"], json.dumps(snapshot)))
                status = summarize(snapshot)
            except Exception as error:
                # Account bodies, key paths and authorization headers never go to logs.
                status = {"observed_at": time.time(), "account_connected": False,
                          "execution_enabled": False, "live_ready": False,
                          "error": type(error).__name__}
                if isinstance(error, RuntimeError) and str(error).startswith("kalshi_http_"):
                    status["error"] = str(error)
            write_json(directory / "status.json", status)
            print(json.dumps(status), flush=True)
            if not args.watch:
                return 0 if status["account_connected"] else 1
            time.sleep(args.interval)
    finally:
        db.close()
        lock.close()


if __name__ == "__main__":
    raise SystemExit(main())
