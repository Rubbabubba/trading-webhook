"""Shadow-only collector for registered structural and calibration sleeves."""
from collections import defaultdict
from datetime import datetime, timezone
import gc
import json
from pathlib import Path
import sqlite3
import time

from .kalshi_demo_market_data import DemoMarkets
from .kalshi_external_sleeves import (
    confirm_structural_candidate, favorite_longshot_observations,
    favorite_maker_observations,
    structural_candidates,
)
from .kalshi_strategy_evaluation import cluster_lower_bound
from .kalshi_sleeve_comparison import comparison_packet
from .kalshi_demo_v5_maker_worker import eligible_market_candidates, market_family


STRUCTURAL_ID = "kalshi_structural_arb_v1_shadow"
FLB_ID = "kalshi_favorite_longshot_v1_shadow"
FAVORITE_MAKER_ID = "kalshi_favorite_maker_v13_shadow"
SPORTS_ID = "sports_persistent_passive_v1_shadow"


def utcnow():
    return datetime.now(timezone.utc)


def open_db(path):
    db = sqlite3.connect(path, isolation_level=None, timeout=30)
    db.executescript("""
      CREATE TABLE IF NOT EXISTS calibration_observations(
        observation_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,decision_bucket INTEGER NOT NULL,
        observed_at TEXT NOT NULL,detail TEXT NOT NULL,resolution TEXT);
      CREATE TABLE IF NOT EXISTS structural_signals(
        signal_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,decision_bucket INTEGER NOT NULL,
        observed_at TEXT NOT NULL,detail TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS favorite_maker_observations(
        observation_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,decision_bucket INTEGER NOT NULL,
        observed_at TEXT NOT NULL,detail TEXT NOT NULL,resolution TEXT);
      CREATE TABLE IF NOT EXISTS scans(
        generation INTEGER PRIMARY KEY,observed_at TEXT NOT NULL,market_count INTEGER NOT NULL,
        calibration_observations INTEGER NOT NULL,indicative_candidates INTEGER NOT NULL,
        confirmed_structural_signals INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS coverage_settings(
        name TEXT PRIMARY KEY,detail TEXT NOT NULL);
    """)
    return db


def _coverage_load(db, name, default=None):
    row = db.execute("SELECT detail FROM coverage_settings WHERE name=?", (name,)).fetchone()
    return json.loads(row[0]) if row else default


def _coverage_save(db, name, value):
    db.execute("INSERT OR REPLACE INTO coverage_settings VALUES(?,?)",
               (name, json.dumps(value, sort_keys=True)))


def advance_mve_coverage(db, client, now):
    """Inventory every currently listed combo market without producing signals."""
    scan = _coverage_load(db, "mve_discovery", {})
    if not scan.get("in_progress"):
        scan = {
            "generation": int(scan.get("generation", 0)) + 1,
            "in_progress": True, "cursor": None, "started_at": now.isoformat(),
            "pages": 0, "markets_scanned": 0, "active_binary_markets": 0,
            "maker_screen_candidates": 0, "market_families": {},
        }
    params = {"status": "open", "limit": 200, "mve_filter": "only"}
    if scan.get("cursor"):
        params["cursor"] = scan["cursor"]
    page, _started, _observed = client.get(params=params)
    rows = page.get("markets", [])
    candidates = {row[3]["ticker"] for row in eligible_market_candidates(
        rows, now=now.timestamp())}
    for market in rows:
        family = market_family(market)
        scan["market_families"][family] = scan["market_families"].get(family, 0) + 1
        if market.get("status") == "active" and market.get("market_type") == "binary":
            scan["active_binary_markets"] += 1
        scan["maker_screen_candidates"] += int(market.get("ticker") in candidates)
    scan["pages"] += 1
    scan["markets_scanned"] += len(rows)
    scan["cursor"] = page.get("cursor") or None
    if not scan["cursor"]:
        scan["in_progress"] = False
        scan["completed_at"] = now.isoformat()
    _coverage_save(db, "mve_discovery", scan)
    return scan


def maker_snapshot(path):
    try:
        db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=10)
        generation_row = db.execute(
            "SELECT max(generation) FROM research_market_universe"
        ).fetchone()
        generation = generation_row[0] if generation_row else None
        fields = (
            "ticker", "event_ticker", "status", "market_type", "exchange_index",
            "category", "title", "subtitle", "yes_sub_title", "no_sub_title",
            "yes_bid_dollars", "yes_ask_dollars", "no_bid_dollars", "no_ask_dollars",
            "yes_bid_size_fp", "no_bid_size_fp", "volume_24h_fp", "strike_type",
            "floor_strike", "rules_primary", "rules_secondary", "close_time",
            "expiration_time", "occurrence_datetime",
        )
        rows = []
        if generation is not None:
            expressions = ",".join(
                f"json_extract(detail,'$.{field}')" for field in fields)
            cursor = db.execute(
                f"SELECT {expressions} FROM research_market_universe "
                "WHERE generation=? ORDER BY event_id,ticker", (generation,)
            )
            # Keep only the fields needed by the three research sleeves. Full
            # market JSON averages several KB and tens of thousands of decoded
            # dictionaries can exceed a 512 MB worker during restart.
            rows = [dict(zip(fields, values)) for values in cursor]
        try:
            setting = db.execute(
                "SELECT detail FROM settings WHERE name='market_discovery'"
            ).fetchone()
            coverage = json.loads(setting[0]) if setting else {}
        except sqlite3.Error:
            coverage = {}
        db.close()
    except (sqlite3.Error, OSError):
        return None, [], {}
    if generation is None or not rows:
        return None, [], coverage
    return generation, rows, coverage


def collect_generation(db, client, generation, markets, now):
    inserted = 0
    for market in markets:
        for observation in favorite_longshot_observations(market, now.isoformat()):
            cursor = db.execute(
                "INSERT OR IGNORE INTO calibration_observations VALUES(?,?,?,?,?,NULL)",
                (observation["observation_id"], observation["event_id"],
                 observation["decision_bucket"], observation["observed_at"],
                 json.dumps(observation, sort_keys=True)),
            )
            inserted += cursor.rowcount
    for observation in favorite_maker_observations(markets, now.isoformat()):
        db.execute(
            "INSERT OR IGNORE INTO favorite_maker_observations VALUES(?,?,?,?,?,NULL)",
            (observation["observation_id"], observation["event_id"],
             observation["decision_bucket"], observation["observed_at"],
             json.dumps(observation, sort_keys=True)),
        )
    candidates = structural_candidates(markets)
    confirmed = 0
    # Confirm only the two best indicative candidates in a generation. The
    # public Demo client enforces slow reads, and no order or fill is attempted.
    prior = db.execute("SELECT observed_at,market_count FROM scans WHERE generation=?",
                       (generation,)).fetchone()
    should_confirm = (prior is None or len(markets) >= prior[1] + 1000 or
                      (now - datetime.fromisoformat(prior[0])).total_seconds() >= 1800)
    for candidate in candidates[:2] if should_confirm else []:
        try:
            low = client.quote({"ticker": candidate["low_ticker"]})
            high = client.quote({"ticker": candidate["high_ticker"]})
            signal = confirm_structural_candidate(candidate, low, high, now=time.time())
            signal_id = (candidate["relationship_id"] + ":" + candidate["low_ticker"] +
                         ":" + candidate["high_ticker"] + ":" + str(signal["decision_bucket"]))
            cursor = db.execute(
                "INSERT OR IGNORE INTO structural_signals VALUES(?,?,?,?,?)",
                (signal_id, signal["event_id"], signal["decision_bucket"],
                 datetime.fromtimestamp(signal["observed_at"], timezone.utc).isoformat(),
                 json.dumps(signal, sort_keys=True)),
            )
            confirmed += cursor.rowcount
        except (KeyError, TypeError, ValueError):
            continue
    db.execute(
        "INSERT OR REPLACE INTO scans VALUES(?,?,?,?,?,?)",
        (generation, now.isoformat(), len(markets), inserted, len(candidates), confirmed),
    )


def _resolve_one_table(db, client, now, table, price_key):
    for observation_id, detail in db.execute(
            f"SELECT observation_id,detail FROM {table} "
            "WHERE resolution IS NULL ORDER BY observed_at LIMIT 20"):
        row = json.loads(detail)
        expiry = row.get("expiration_time")
        if not expiry:
            continue
        try:
            expires = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        except ValueError:
            continue
        if expires > now:
            continue
        try:
            payload, _, _ = client.get(row["ticker"])
            market = payload["market"]
        except (KeyError, TypeError, ValueError):
            return
        result = str(market.get("result") or "").lower()
        if result not in ("yes", "no"):
            return
        payout = 100 if result == row["side"] else 0
        elapsed_days = max(0.0, (now - datetime.fromisoformat(
            row["observed_at"].replace("Z", "+00:00"))).total_seconds() / 86400)
        resolution = {
            "resolved_at": now.isoformat(), "result": result, "payout_cents": payout,
            "fee_stress_cents": 2, "capital_days": row[price_key] * elapsed_days / 100,
            "cost_stressed_net_cents": payout - row[price_key] - 2,
            "hypothetical_only": True,
        }
        db.execute(f"UPDATE {table} SET resolution=? WHERE observation_id=?",
                   (json.dumps(resolution, sort_keys=True), observation_id))
        return


def resolve_one(db, client, now):
    _resolve_one_table(db, client, now, "calibration_observations", "price_cents")
    _resolve_one_table(db, client, now, "favorite_maker_observations",
                       "passive_price_cents")


def _flb_summary(db):
    groups = defaultdict(list); net = capital_days = 0.0; complete = 0; equity = peak = drawdown = 0.0
    for event_id, resolution in db.execute(
            "SELECT event_id,resolution FROM calibration_observations WHERE resolution IS NOT NULL"):
        outcome = json.loads(resolution); value = float(outcome["cost_stressed_net_cents"])
        groups[event_id].append(value); net += value
        capital_days += float(outcome["capital_days"]); complete += 1
        equity += value; peak = max(peak, equity); drawdown = max(drawdown, peak - equity)
    return {"strategy_id": FLB_ID, "execution_enabled": False,
            "independent_events": len(groups), "complete_observations": complete,
            "cost_stressed_net_cents": net if complete else None,
            "event_clustered_95pct_lower_bound_cents": cluster_lower_bound(groups) if groups else None,
            "capital_days": capital_days if complete else None,
            "maximum_drawdown_cents": drawdown if complete else None}


def _favorite_maker_summary(db):
    groups = defaultdict(list); families = defaultdict(list)
    net = capital_days = 0.0; complete = 0; equity = peak = drawdown = 0.0
    for event_id, detail, resolution in db.execute(
            "SELECT event_id,detail,resolution FROM favorite_maker_observations "
            "WHERE resolution IS NOT NULL"):
        row = json.loads(detail); outcome = json.loads(resolution)
        value = float(outcome["cost_stressed_net_cents"])
        groups[event_id].append(value); families[row["stratum"]].append(value)
        net += value; capital_days += float(outcome["capital_days"]); complete += 1
        equity += value; peak = max(peak, equity); drawdown = max(drawdown, peak - equity)
    candidates = db.execute("SELECT count(*) FROM favorite_maker_observations").fetchone()[0]
    family_events = {}
    for family in ("crypto", "politics"):
        family_events[family] = db.execute(
            "SELECT count(DISTINCT event_id) FROM favorite_maker_observations "
            "WHERE json_extract(detail,'$.stratum')=? AND resolution IS NOT NULL",
            (family,),
        ).fetchone()[0]
    return {"strategy_id": FAVORITE_MAKER_ID, "execution_enabled": False,
            "candidate_observations": candidates,
            "independent_events": len(groups), "complete_observations": complete,
            "cost_stressed_net_cents": net if complete else None,
            "event_clustered_95pct_lower_bound_cents": cluster_lower_bound(groups) if groups else None,
            "family_resolved_events": family_events,
            "family_cost_stressed_net_cents": {
                family: sum(values) if values else None for family, values in families.items()},
            "capital_days": capital_days if complete else None,
            "maximum_drawdown_cents": drawdown if complete else None}


def _records(db, table):
    return [{"event_id": event_id, "decision_bucket": bucket}
            for event_id, bucket in db.execute(f"SELECT event_id,decision_bucket FROM {table}")]


def _sports(root):
    status_path = root / "sports-challenger" / "status.json"
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    summary = {"strategy_id": SPORTS_ID, "execution_enabled": False,
               "independent_events": None, "complete_observations": None,
               "cost_stressed_net_cents": None,
               "event_clustered_95pct_lower_bound_cents": None,
               "capital_days": None, "maximum_drawdown_cents": None}
    records = []
    path = root / "sports-challenger" / "sports_shadow.sqlite3"
    if path.exists():
        try:
            sports = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=10)
            games = {slug: json.loads(config).get("market_event")
                     for slug, config in sports.execute("SELECT slug,config FROM games")}
            for slug, at in sports.execute("SELECT slug,at FROM signals"):
                stamp = int(datetime.fromisoformat(at.replace("Z", "+00:00")).timestamp())
                records.append({"event_id": games.get(slug),
                                "decision_bucket": stamp - stamp % 3600})
            sports.close()
        except (sqlite3.Error, ValueError, TypeError):
            records = []
    summary["independent_events"] = len({row["event_id"] for row in records if row["event_id"]})
    summary["complete_observations"] = status.get("complete_signals")
    return summary, records


def write_status(root, db, generation, market_count, coverage=None, mve_coverage=None,
                 error=None):
    structural_records = _records(db, "structural_signals")
    flb_records = _records(db, "calibration_observations")
    structural = {"strategy_id": STRUCTURAL_ID, "execution_enabled": False,
                  "independent_events": len({row["event_id"] for row in structural_records}),
                  "complete_observations": len(structural_records),
                  "cost_stressed_net_cents": None,
                  "event_clustered_95pct_lower_bound_cents": None,
                  "capital_days": None, "maximum_drawdown_cents": None}
    flb = _flb_summary(db)
    favorite_maker = _favorite_maker_summary(db)
    sports, sports_records = _sports(root)
    favorite_maker_records = _records(db, "favorite_maker_observations")
    packet = comparison_packet([sports, structural, flb, favorite_maker], {
        SPORTS_ID: sports_records, STRUCTURAL_ID: structural_records, FLB_ID: flb_records,
        FAVORITE_MAKER_ID: favorite_maker_records,
    })
    packet["favorite_maker_gate"] = favorite_maker
    coverage = coverage or {}
    mve_coverage = mve_coverage or {}
    packet.update({"generated_at": utcnow().isoformat(), "execution_enabled": False,
                   "source_generation": generation, "source_markets": market_count,
                   "coverage": {
                       "catalog_scope": "all_open_standard_non_mve_markets",
                       "scan_generation": coverage.get("generation"),
                       "scan_in_progress": coverage.get("in_progress"),
                       "pages": coverage.get("pages"),
                       "markets_scanned": coverage.get("markets_scanned"),
                       "research_relevant_markets": coverage.get("research_relevant_markets"),
                       "research_relevant_events": coverage.get("research_relevant_events"),
                       "favorite_longshot_markets": coverage.get("favorite_longshot_markets"),
                       "favorite_maker_markets": coverage.get("favorite_maker_markets"),
                       "nested_threshold_markets": coverage.get("nested_threshold_markets"),
                       "market_families": coverage.get("market_families"),
                       "eligible_families": coverage.get("eligible_families"),
                       "admission_rejections": coverage.get("admission_rejections"),
                       "coverage_accounting_complete": coverage.get(
                           "coverage_accounting_complete"),
                       "coverage_complete": bool(
                           generation is not None and not coverage.get("in_progress", True)
                           and generation == coverage.get("generation")
                           and coverage.get("coverage_accounting_complete") is True
                       ),
                   },
                   "multivariate_coverage": {
                       "catalog_scope": "all_open_mve_combo_markets",
                       "strategy_route": "inventory_only_pending_registered_mve_challenger",
                       **{key: mve_coverage.get(key) for key in (
                           "generation", "in_progress", "pages", "markets_scanned",
                           "active_binary_markets", "maker_screen_candidates",
                           "market_families", "started_at", "completed_at",
                       )},
                   },
                   "error": error})
    destination = root / "sleeve_comparison.json"
    temporary = destination.with_suffix(".tmp")
    temporary.write_text(json.dumps(packet, indent=2) + "\n")
    temporary.replace(destination)
    return packet


def run(data_root, cycles=None, interval_seconds=60):
    root = Path(data_root).resolve(); root.mkdir(parents=True, exist_ok=True)
    db = open_db(root / "research_sleeves.sqlite3"); client = DemoMarkets(); cycle = 0
    try:
        while cycles is None or cycle < cycles:
            generation = None; markets = []; coverage = {}; error = None
            mve_coverage = _coverage_load(db, "mve_discovery", {})
            try:
                generation, markets, coverage = maker_snapshot(root / "worker.sqlite3")
                if generation is not None:
                    collect_generation(db, client, generation, markets, utcnow())
                    resolve_one(db, client, utcnow())
                mve_coverage = advance_mve_coverage(db, client, utcnow())
            except Exception as exc:
                error = type(exc).__name__
            packet = write_status(root, db, generation, len(markets), coverage,
                                  mve_coverage, error)
            if cycle == 0 or error or any(row["paired"] for row in packet["paired_comparisons"]):
                print(json.dumps({"at": packet["generated_at"],
                                  "event": "research_sleeves_status",
                                  "source_generation": generation,
                                  "source_markets": len(markets),
                                  "execution_enabled": False,
                                  "error": error}), flush=True)
            # Do not retain or overlap a full decoded research snapshot across
            # the sleep interval. CPython can otherwise keep hundreds of MB of
            # market dictionaries alive while the next snapshot is built.
            markets = []
            gc.collect()
            cycle += 1
            if cycles is None or cycle < cycles:
                time.sleep(interval_seconds)
    finally:
        db.close()


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="/var/data/kalshi-demo-v9")
    parser.add_argument("--cycles", type=int)
    args = parser.parse_args(argv)
    run(args.data_root, cycles=args.cycles)


if __name__ == "__main__":
    main()
