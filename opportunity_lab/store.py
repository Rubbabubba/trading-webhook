"""Isolated PostgreSQL persistence for Opportunity Lab observations."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone


def configured() -> bool:
    return bool((os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip())


def save_kalshi_scan(scan: dict, transport: dict) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "saved": False, "error": "opportunity_database_not_configured"}
    import psycopg

    run_id = str(uuid.uuid4())
    observed_at = datetime.now(timezone.utc)
    pairs = list(scan.get("mutually_exclusive_no_pairs") or [])
    summary = {
        "events_received": scan.get("events_received"),
        "candidate_count": scan.get("candidate_count"),
        "price_dislocation_count": scan.get("price_dislocation_count"),
        "mutually_exclusive_no_pair_count": scan.get("mutually_exclusive_no_pair_count"),
        "category_filter": scan.get("category_filter"),
        "fee_model": scan.get("fee_model"),
    }
    with psycopg.connect(url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS opportunity_lab")
            cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.scan_runs (
                run_id uuid PRIMARY KEY, observed_at timestamptz NOT NULL, source text NOT NULL,
                pages integer NOT NULL, event_count integer NOT NULL, more_available boolean NOT NULL,
                no_pair_count integer NOT NULL, summary jsonb NOT NULL
            )""")
            cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.pair_opportunities (
                run_id uuid NOT NULL REFERENCES opportunity_lab.scan_runs(run_id) ON DELETE CASCADE,
                event_ticker text NOT NULL, leg_key text NOT NULL, estimated_net_profit numeric NOT NULL,
                estimated_net_roi_pct numeric NOT NULL, annualized_return_pct numeric,
                payload jsonb NOT NULL, PRIMARY KEY (run_id, event_ticker, leg_key)
            )""")
            cursor.execute(
                "INSERT INTO opportunity_lab.scan_runs VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                (run_id, observed_at, "kalshi_public_market_data", int(transport.get("pages") or 0),
                 int(transport.get("event_count") or 0), bool(transport.get("more_available")), len(pairs), json.dumps(summary)),
            )
            for pair in pairs:
                leg_key = "|".join(sorted(str(leg.get("ticker") or "") for leg in pair.get("legs") or []))
                cursor.execute(
                    "INSERT INTO opportunity_lab.pair_opportunities VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    (run_id, pair.get("event_ticker"), leg_key, pair.get("estimated_net_profit") or 0,
                     pair.get("estimated_net_roi_pct") or 0, pair.get("annualized_return_pct"), json.dumps(pair)),
                )
    return {"configured": True, "saved": True, "run_id": run_id, "observed_at": observed_at.isoformat(), "pair_rows": len(pairs)}


def recent_runs(limit: int = 50) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "runs": [], "error": "opportunity_database_not_configured"}
    import psycopg

    limit = max(1, min(500, int(limit)))
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("""SELECT run_id::text, observed_at, pages, event_count, more_available,
            no_pair_count, summary FROM opportunity_lab.scan_runs ORDER BY observed_at DESC LIMIT %s""", (limit,))
        rows = cursor.fetchall()
    return {"configured": True, "runs": [{
        "run_id": row[0], "observed_at": row[1].isoformat(), "pages": row[2], "event_count": row[3],
        "more_available": row[4], "no_pair_count": row[5], "summary": row[6],
    } for row in rows]}
