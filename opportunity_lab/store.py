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
    closest_pairs = list(scan.get("closest_no_pairs") or [])
    maker_rows = list((scan.get("market_making") or {}).get("candidates") or [])
    summary = {
        "events_received": scan.get("events_received"),
        "candidate_count": scan.get("candidate_count"),
        "price_dislocation_count": scan.get("price_dislocation_count"),
        "mutually_exclusive_no_pair_count": scan.get("mutually_exclusive_no_pair_count"),
        "closest_no_pair_count": scan.get("closest_no_pair_count"),
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
            cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.near_miss_observations (
                run_id uuid NOT NULL REFERENCES opportunity_lab.scan_runs(run_id) ON DELETE CASCADE,
                event_ticker text NOT NULL, leg_key text NOT NULL, estimated_net_profit numeric NOT NULL,
                estimated_net_roi_pct numeric NOT NULL, shortfall_to_break_even numeric NOT NULL,
                payload jsonb NOT NULL, PRIMARY KEY (run_id, event_ticker)
            )""")
            cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.market_making_observations (
                run_id uuid NOT NULL REFERENCES opportunity_lab.scan_runs(run_id) ON DELETE CASCADE,
                ticker text NOT NULL, conservative_net_profit numeric NOT NULL,
                conservative_roi_pct numeric NOT NULL, payload jsonb NOT NULL,
                PRIMARY KEY (run_id, ticker)
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
            for pair in closest_pairs:
                leg_key = "|".join(sorted(str(leg.get("ticker") or "") for leg in pair.get("legs") or []))
                cursor.execute(
                    "INSERT INTO opportunity_lab.near_miss_observations VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    (run_id, pair.get("event_ticker"), leg_key, pair.get("estimated_net_profit") or 0,
                     pair.get("estimated_net_roi_pct") or 0, pair.get("shortfall_to_break_even") or 0, json.dumps(pair)),
                )
            for row in maker_rows:
                conservative = row.get("scenarios", {}).get("conservative", {})
                cursor.execute(
                    "INSERT INTO opportunity_lab.market_making_observations VALUES (%s,%s,%s,%s,%s::jsonb)",
                    (run_id, row.get("ticker"), conservative.get("estimated_net_profit") or 0,
                     conservative.get("estimated_roi_on_quote_capital_pct") or 0, json.dumps(row)),
                )
    return {"configured": True, "saved": True, "run_id": run_id, "observed_at": observed_at.isoformat(),
            "pair_rows": len(pairs), "near_miss_rows": len(closest_pairs), "market_making_rows": len(maker_rows)}


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


def kalshi_scoreboard(*, hours: int = 72, required_runs: int = 60) -> dict:
    """Summarize fee-adjusted evidence and issue a mechanical research verdict."""
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "error": "opportunity_database_not_configured"}
    import psycopg

    hours = max(1, min(24 * 30, int(hours)))
    required_runs = max(1, int(required_runs))
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("""SELECT count(*), coalesce(sum(event_count), 0), min(observed_at), max(observed_at),
            coalesce(sum(no_pair_count), 0)
            FROM opportunity_lab.scan_runs
            WHERE observed_at >= now() - (%s * interval '1 hour')""", (hours,))
        run_count, events_scanned, first_at, last_at, profitable_pair_hits = cursor.fetchone()
        cursor.execute("""SELECT max(estimated_net_profit), max(estimated_net_roi_pct)
            FROM opportunity_lab.pair_opportunities p JOIN opportunity_lab.scan_runs r USING (run_id)
            WHERE r.observed_at >= now() - (%s * interval '1 hour')""", (hours,))
        best_profit, best_profitable_roi = cursor.fetchone()
        cursor.execute("""SELECT estimated_net_profit, estimated_net_roi_pct, shortfall_to_break_even,
            event_ticker, leg_key FROM opportunity_lab.near_miss_observations n
            JOIN opportunity_lab.scan_runs r USING (run_id)
            WHERE r.observed_at >= now() - (%s * interval '1 hour')
            ORDER BY estimated_net_roi_pct DESC, estimated_net_profit DESC LIMIT 1""", (hours,))
        closest = cursor.fetchone()

    span_hours = ((last_at - first_at).total_seconds() / 3600) if first_at and last_at else 0.0
    evidence_complete = run_count >= required_runs and span_hours >= hours - 2
    closest_roi = float(closest[1]) if closest else None
    if profitable_pair_hits:
        verdict = "investigate_execution_feasibility"
        rationale = "At least one displayed pair remained positive after the conservative fee estimate."
    elif not evidence_complete:
        verdict = "collecting_evidence"
        rationale = f"Need at least {required_runs} runs spanning approximately {hours} hours before rejection."
    elif closest_roi is not None and closest_roi >= -0.25:
        verdict = "continue_high_frequency_validation"
        rationale = "No profit yet, but the best observation was within 0.25% ROI of break-even."
    else:
        verdict = "reject_current_strategy"
        rationale = "No fee-adjusted profit and the closest observation was not near break-even."
    return {
        "configured": True, "strategy": "kalshi_mutually_exclusive_no_pairs", "window_hours": hours,
        "required_runs": required_runs, "run_count": run_count, "events_scanned": int(events_scanned),
        "first_observed_at": first_at.isoformat() if first_at else None,
        "last_observed_at": last_at.isoformat() if last_at else None,
        "observation_span_hours": round(span_hours, 3), "evidence_complete": evidence_complete,
        "profitable_pair_hits": int(profitable_pair_hits),
        "best_estimated_net_profit": float(best_profit) if best_profit is not None else None,
        "best_profitable_roi_pct": float(best_profitable_roi) if best_profitable_roi is not None else None,
        "closest_observation": ({"estimated_net_profit": float(closest[0]), "estimated_net_roi_pct": float(closest[1]),
            "shortfall_to_break_even": float(closest[2]), "event_ticker": closest[3], "leg_key": closest[4]} if closest else None),
        "verdict": verdict, "rationale": rationale, "execution_enabled": False,
    }
