"""Bounded retrospective weather scoring on a private in-memory copy.

Source evidence is read-only. Only official Demo settlement GETs are permitted;
no order, fill or publication path exists in this utility.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from .kalshi_demo_market_data import DemoMarkets
from .kalshi_external_sleeves_worker import resolve_one_weather, _weather_summary


class RequestBudgetExhausted(Exception):
    pass


class BoundedMarkets:
    def __init__(self, client, maximum):
        self.client = client; self.maximum = maximum; self.requests = 0

    def get(self, ticker):
        if self.requests >= self.maximum:
            raise RequestBudgetExhausted()
        self.requests += 1
        return self.client.get(ticker)


def audit(database, *, client=None, maximum_requests=20, now=None):
    if type(maximum_requests) is not int or not 0 <= maximum_requests <= 20:
        raise ValueError("maximum_20_settlement_requests")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("timezone_required")
    clone = sqlite3.connect(":memory:", isolation_level=None)
    with sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True) as source:
        source.execute("PRAGMA query_only=ON"); source.execute("BEGIN")
        tables = {r[0] for r in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        count = source.execute("SELECT count(*) FROM weather_observations").fetchone()[0]
        if count > 50000:
            raise ValueError("maximum_50000_weather_observations")
        for table in ("weather_observations", "weather_forecasts", "weather_settlement_markets"):
            if table not in tables:
                if table == "weather_settlement_markets":
                    clone.execute("CREATE TABLE weather_settlement_markets(ticker TEXT PRIMARY KEY,checked_at TEXT,detail TEXT)")
                    continue
                raise ValueError("missing_" + table)
            definition = source.execute("SELECT sql FROM sqlite_master WHERE name=?", (table,)).fetchone()[0]
            clone.execute(definition)
            rows = source.execute(f"SELECT * FROM {table}").fetchall()
            if rows:
                clone.executemany(f"INSERT INTO {table} VALUES({','.join('?' for _ in rows[0])})", rows)
    clone.execute("CREATE TABLE coverage_settings(name TEXT PRIMARY KEY,detail TEXT)")
    original_summary = _weather_summary(clone)
    bounded = BoundedMarkets(client or DemoMarkets(), maximum_requests)
    error = None; budget_exhausted = False; batches = 0
    # Three complete passes allow cached multi-contract outcomes to join while
    # keeping the run finite even if every market is unresolved or unavailable.
    for _ in range(3 * ((count + 19) // 20) + 1):
        try:
            resolve_one_weather(clone, bounded, now)
            batches += 1
        except RequestBudgetExhausted:
            budget_exhausted = True
            break
        except (ValueError, KeyError, TypeError, OSError) as exc:
            error = type(exc).__name__ + ":" + str(exc)
            break
    result = {"schema": "kalshi_weather_historical_audit_v1", "generated_at": now.isoformat(),
              "method": "same frozen weather scoring on private snapshot plus bounded official settlement reads",
              "source_observations": count, "maximum_requests": maximum_requests,
              "requests": bounded.requests, "completed_batches": batches,
              "request_budget_exhausted": budget_exhausted, "transport_or_data_error": error,
              "before": original_summary, "after": _weather_summary(clone),
              "settlement_evidence": [json.loads(row[0]) for row in clone.execute(
                  "SELECT detail FROM weather_settlement_markets ORDER BY ticker")],
              "original_database_modified": False, "fill_assumed": False,
              "execution_enabled": False, "profitability_proven": False,
              "live_promotion_ready": False,
              "limitations": ["Hypothetical historical settlement scores, not broker fills.",
                              "Selected retained forecasts, not a new untouched strategy test.",
                              "Unavailable and budget-limited outcomes remain incomplete."]}
    clone.close()
    return result


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--maximum-requests", type=int, default=20)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = audit(args.database, maximum_requests=args.maximum_requests)
    with Path(args.output).open("x", encoding="utf-8") as destination:
        destination.write(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")
    # Keep terminal output bounded; the full settlement archive stays on disk.
    print(json.dumps({k: v for k, v in result.items() if k != "settlement_evidence"}, sort_keys=True))


if __name__ == "__main__":
    main()
