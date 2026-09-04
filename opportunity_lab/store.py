"""Isolated PostgreSQL persistence for Opportunity Lab observations."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from .prediction_market_making import replay_quote
from .weather_value import calibrate_snapshot


def configured() -> bool:
    return bool((os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip())


def save_cross_exchange_scans(scans: list[dict]) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "saved": False, "error": "opportunity_database_not_configured"}
    import psycopg

    observed_at = datetime.now(timezone.utc)
    rows = [row for row in scans if row.get("ok") and row.get("scan")]
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS opportunity_lab")
        cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.cross_exchange_observations (
            observation_id uuid PRIMARY KEY, observed_at timestamptz NOT NULL, symbol text NOT NULL,
            profitable_direction_count integer NOT NULL, best_net_profit numeric NOT NULL,
            best_roi_pct numeric NOT NULL, payload jsonb NOT NULL
        )""")
        for row in rows:
            scan, best = row["scan"], row["scan"]["best_direction"]
            cursor.execute("INSERT INTO opportunity_lab.cross_exchange_observations VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)",
                           (str(uuid.uuid4()), observed_at, row["symbol"], scan["profitable_direction_count"],
                            best["net_profit"], best["roi_on_fully_collateralized_capital_pct"], json.dumps(row)))
    return {"configured": True, "saved": True, "observed_at": observed_at.isoformat(), "row_count": len(rows)}


def save_triangular_scan(row: dict) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "saved": False, "error": "opportunity_database_not_configured"}
    if not row.get("ok") or not row.get("scan"):
        return {"configured": True, "saved": False, "error": "triangular_scan_failed"}
    import psycopg

    scan, best = row["scan"], row["scan"]["best_cycle"]
    observed_at = datetime.now(timezone.utc)
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS opportunity_lab")
        cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.triangular_observations (
            observation_id uuid PRIMARY KEY, observed_at timestamptz NOT NULL, venue text NOT NULL,
            profitable_cycle_count integer NOT NULL, best_net_profit numeric NOT NULL,
            best_roi_pct numeric NOT NULL, payload jsonb NOT NULL
        )""")
        cursor.execute("INSERT INTO opportunity_lab.triangular_observations VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)",
                       (str(uuid.uuid4()), observed_at, row["venue"], scan["profitable_cycle_count"],
                        best["net_profit_usd"], best["roi_pct"], json.dumps(row)))
    return {"configured": True, "saved": True, "observed_at": observed_at.isoformat(), "row_count": 1}


def save_weather_scan(row: dict) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "saved": False, "error": "opportunity_database_not_configured"}
    if not row.get("ok"):
        return {"configured": True, "saved": False, "error": "weather_scan_failed"}
    import psycopg

    observed_at, events = datetime.now(timezone.utc), list(row.get("events") or [])
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS opportunity_lab")
        cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.weather_value_observations (
            observation_id uuid PRIMARY KEY, observed_at timestamptz NOT NULL, event_ticker text NOT NULL,
            target_date date NOT NULL, best_model_edge numeric NOT NULL, positive_candidate_count integer NOT NULL,
            payload jsonb NOT NULL
        )""")
        for event in events:
            cursor.execute("INSERT INTO opportunity_lab.weather_value_observations VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)",
                           (str(uuid.uuid4()), observed_at, event["event_ticker"], event["target_date"],
                            event["best_model_edge_after_fee"], event["positive_model_edge_count"], json.dumps(event)))
    return {"configured": True, "saved": True, "observed_at": observed_at.isoformat(), "row_count": len(events)}


def reconcile_weather_settlements(markets: list[dict]) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "saved": False, "error": "opportunity_database_not_configured"}
    import psycopg

    settled = {row.get("ticker"): row for row in markets if row.get("ticker") and row.get("result") in {"yes", "no"}}
    calibrated = []
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS opportunity_lab")
        cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.weather_calibrations (
            observation_id uuid PRIMARY KEY REFERENCES opportunity_lab.weather_value_observations(observation_id) ON DELETE CASCADE,
            calibrated_at timestamptz NOT NULL, event_ticker text NOT NULL, forecast_horizon_hours numeric NOT NULL,
            brier_score numeric NOT NULL, forecast_error_lower_bound_f numeric,
            paper_realized_pnl numeric NOT NULL, paper_profitable boolean NOT NULL, payload jsonb NOT NULL
        )""")
        if not settled:
            return {"configured": True, "saved": True, "calibration_rows": 0, "settled_market_count": 0}
        event_tickers = list({row.get("event_ticker") for row in markets if row.get("event_ticker")})
        cursor.execute("""SELECT w.observation_id, w.observed_at, w.payload
            FROM opportunity_lab.weather_value_observations w
            LEFT JOIN opportunity_lab.weather_calibrations c USING (observation_id)
            WHERE c.observation_id IS NULL AND w.event_ticker = ANY(%s)""", (event_tickers,))
        for observation_id, observed_at, snapshot in cursor.fetchall():
            result = calibrate_snapshot(snapshot, settled)
            if not result:
                continue
            target_end = datetime.combine(datetime.fromisoformat(result["target_date"]).date(), time(23, 59),
                                          tzinfo=ZoneInfo("America/Chicago"))
            horizon = max(0.0, (target_end - observed_at.astimezone(ZoneInfo("America/Chicago"))).total_seconds() / 3600)
            pnl = result["paper_trade"]["realized_pnl_per_contract"]
            result["forecast_horizon_hours"] = round(horizon, 4)
            cursor.execute("INSERT INTO opportunity_lab.weather_calibrations VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                           (observation_id, datetime.now(timezone.utc), result["event_ticker"], horizon,
                            result["brier_score"], result["forecast_error_lower_bound_f"], pnl, pnl > 0, json.dumps(result)))
            calibrated.append(result)
    return {"configured": True, "saved": True, "calibration_rows": len(calibrated),
            "settled_market_count": len(settled),
            "paper_profitable_rows": sum(row["paper_trade"]["realized_pnl_per_contract"] > 0 for row in calibrated)}


def weather_scoreboard(*, hours: int = 72) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "error": "opportunity_database_not_configured"}
    import psycopg

    hours = max(1, min(24 * 30, int(hours)))
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.weather_value_observations (
            observation_id uuid PRIMARY KEY, observed_at timestamptz NOT NULL, event_ticker text NOT NULL,
            target_date date NOT NULL, best_model_edge numeric NOT NULL, positive_candidate_count integer NOT NULL,
            payload jsonb NOT NULL
        )""")
        cursor.execute("""SELECT count(*), min(observed_at), max(observed_at),
            coalesce(sum(positive_candidate_count),0), max(best_model_edge)
            FROM opportunity_lab.weather_value_observations
            WHERE observed_at >= now() - (%s * interval '1 hour')""", (hours,))
        count, first_at, last_at, positives, best_edge = cursor.fetchone()
        cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.weather_calibrations (
            observation_id uuid PRIMARY KEY REFERENCES opportunity_lab.weather_value_observations(observation_id) ON DELETE CASCADE,
            calibrated_at timestamptz NOT NULL, event_ticker text NOT NULL, forecast_horizon_hours numeric NOT NULL,
            brier_score numeric NOT NULL, forecast_error_lower_bound_f numeric,
            paper_realized_pnl numeric NOT NULL, paper_profitable boolean NOT NULL, payload jsonb NOT NULL
        )""")
        cursor.execute("""SELECT count(*), count(DISTINCT event_ticker), avg(brier_score),
            avg(forecast_error_lower_bound_f), sum(paper_realized_pnl), count(*) FILTER (WHERE paper_profitable)
            FROM opportunity_lab.weather_calibrations""")
        calibration_count, settled_events, avg_brier, avg_error_floor, paper_pnl, paper_wins = cursor.fetchone()
    evidence_complete = settled_events >= 30
    if not evidence_complete:
        verdict = "collecting_calibration_evidence"
    elif paper_pnl is not None and paper_pnl > 0 and avg_brier is not None and avg_brier < .15:
        verdict = "investigate_execution_feasibility"
    else:
        verdict = "reject_or_retune_weather_model"
    return {"configured": True, "strategy": "dallas_daily_temperature_value", "window_hours": hours,
            "observation_count": count, "first_observed_at": first_at.isoformat() if first_at else None,
            "last_observed_at": last_at.isoformat() if last_at else None,
            "positive_uncalibrated_model_candidates": int(positives),
            "best_uncalibrated_model_edge": float(best_edge) if best_edge is not None else None,
            "calibration": {"snapshot_count": calibration_count, "settled_event_count": settled_events,
                            "required_settled_events": 30, "evidence_complete": evidence_complete,
                            "average_brier_score": float(avg_brier) if avg_brier is not None else None,
                            "average_forecast_error_lower_bound_f": float(avg_error_floor) if avg_error_floor is not None else None,
                            "paper_realized_pnl_per_contract_sum": float(paper_pnl) if paper_pnl is not None else None,
                            "paper_profitable_snapshot_count": paper_wins},
            "verdict": verdict, "execution_enabled": False}


def triangular_scoreboard(*, hours: int = 72) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "error": "opportunity_database_not_configured"}
    import psycopg

    hours = max(1, min(24 * 30, int(hours)))
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.triangular_observations (
            observation_id uuid PRIMARY KEY, observed_at timestamptz NOT NULL, venue text NOT NULL,
            profitable_cycle_count integer NOT NULL, best_net_profit numeric NOT NULL,
            best_roi_pct numeric NOT NULL, payload jsonb NOT NULL
        )""")
        cursor.execute("""SELECT count(*), min(observed_at), max(observed_at),
            coalesce(sum(profitable_cycle_count),0), max(best_net_profit), max(best_roi_pct)
            FROM opportunity_lab.triangular_observations
            WHERE observed_at >= now() - (%s * interval '1 hour')""", (hours,))
        count, first_at, last_at, profitable, best_profit, best_roi = cursor.fetchone()
    return {"configured": True, "strategy": "kraken_triangular_crypto", "window_hours": hours,
            "observation_count": count, "first_observed_at": first_at.isoformat() if first_at else None,
            "last_observed_at": last_at.isoformat() if last_at else None, "profitable_cycle_hits": int(profitable),
            "best_net_profit": float(best_profit) if best_profit is not None else None,
            "best_roi_pct": float(best_roi) if best_roi is not None else None,
            "verdict": "investigate_execution_feasibility" if profitable else "collecting_evidence",
            "execution_enabled": False}


def cross_exchange_scoreboard(*, hours: int = 72) -> dict:
    url = (os.getenv("OPPORTUNITY_DATABASE_URL") or "").strip()
    if not url:
        return {"configured": False, "error": "opportunity_database_not_configured"}
    import psycopg

    hours = max(1, min(24 * 30, int(hours)))
    with psycopg.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.cross_exchange_observations (
            observation_id uuid PRIMARY KEY, observed_at timestamptz NOT NULL, symbol text NOT NULL,
            profitable_direction_count integer NOT NULL, best_net_profit numeric NOT NULL,
            best_roi_pct numeric NOT NULL, payload jsonb NOT NULL
        )""")
        cursor.execute("""SELECT count(*), min(observed_at), max(observed_at),
            coalesce(sum(profitable_direction_count),0), max(best_net_profit), max(best_roi_pct)
            FROM opportunity_lab.cross_exchange_observations
            WHERE observed_at >= now() - (%s * interval '1 hour')""", (hours,))
        count, first_at, last_at, profitable, best_profit, best_roi = cursor.fetchone()
        cursor.execute("""SELECT symbol, best_net_profit, best_roi_pct, observed_at, payload
            FROM opportunity_lab.cross_exchange_observations
            WHERE observed_at >= now() - (%s * interval '1 hour')
            ORDER BY best_net_profit DESC LIMIT 1""", (hours,))
        best = cursor.fetchone()
    return {"configured": True, "strategy": "coinbase_kraken_cross_exchange", "window_hours": hours,
            "observation_count": count, "first_observed_at": first_at.isoformat() if first_at else None,
            "last_observed_at": last_at.isoformat() if last_at else None, "profitable_direction_hits": int(profitable),
            "best_net_profit": float(best_profit) if best_profit is not None else None,
            "best_roi_pct": float(best_roi) if best_roi is not None else None,
            "best_observation": ({"symbol": best[0], "net_profit": float(best[1]), "roi_pct": float(best[2]),
                                  "observed_at": best[3].isoformat(), "direction": best[4]["scan"]["best_direction"]} if best else None),
            "verdict": "investigate_execution_feasibility" if profitable else "collecting_evidence",
            "execution_enabled": False}


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
    trades = list(scan.get("_public_trades") or [])
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
            cursor.execute("""CREATE TABLE IF NOT EXISTS opportunity_lab.market_making_replays (
                run_id uuid NOT NULL REFERENCES opportunity_lab.scan_runs(run_id) ON DELETE CASCADE,
                ticker text NOT NULL, net_marked_pnl numeric NOT NULL, roi_pct numeric NOT NULL,
                profitable boolean NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, ticker)
            )""")
            previous_rows = {}
            if maker_rows and trades:
                tickers = [row["ticker"] for row in maker_rows]
                cursor.execute("""SELECT DISTINCT ON (m.ticker) m.ticker, m.payload
                    FROM opportunity_lab.market_making_observations m
                    JOIN opportunity_lab.scan_runs r USING (run_id)
                    WHERE m.ticker = ANY(%s) ORDER BY m.ticker, r.observed_at DESC""", (tickers,))
                previous_rows = {row[0]: row[1] for row in cursor.fetchall()}
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
            replay_rows = []
            for current in maker_rows:
                previous = previous_rows.get(current.get("ticker"))
                if not previous:
                    continue
                replay = replay_quote(previous, current, trades)
                replay_rows.append(replay)
                cursor.execute(
                    "INSERT INTO opportunity_lab.market_making_replays VALUES (%s,%s,%s,%s,%s,%s::jsonb)",
                    (run_id, replay["ticker"], replay["net_marked_pnl"], replay["roi_on_quote_capital_pct"],
                     replay["profitable"], json.dumps(replay)),
                )
    return {"configured": True, "saved": True, "run_id": run_id, "observed_at": observed_at.isoformat(),
            "pair_rows": len(pairs), "near_miss_rows": len(closest_pairs), "market_making_rows": len(maker_rows),
            "market_making_replay_rows": len(replay_rows),
            "market_making_profitable_replays": sum(row["profitable"] for row in replay_rows)}


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
