"""Shadow-only collector for registered structural and calibration sleeves."""
from collections import defaultdict
import ctypes
from datetime import datetime, timedelta, timezone
import gc
import hashlib
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
from .kalshi_strategy_factory import (
    cycle as strategy_factory_cycle, init as init_strategy_factory,
    status as strategy_factory_status,
)
from .kalshi_experiment_registry import register as register_experiments, status as experiment_status
from .kalshi_structural_execution_probe import init as init_structural_probe, capture as capture_structural_probe, status as structural_probe_status
from .kalshi_factory_fee_probe import (
    probe_next as factory_fee_probe_next, status as factory_fee_probe_status,
)
from .kalshi_official_release_probe import (
    cycle as official_release_cycle, init as init_official_release,
    status as official_release_status,
)
from .kalshi_demo_v5_maker_worker import eligible_market_candidates, market_family
from .kalshi_weather_ensemble import (
    fetch_ensemble, parse_target_date, score_resolution,
    weather_event_observation, weather_market_spec,
)


STRUCTURAL_ID = "kalshi_structural_arb_v1_shadow"
FLB_ID = "kalshi_favorite_longshot_v1_shadow"
FAVORITE_MAKER_ID = "kalshi_favorite_maker_v13_shadow"
WEATHER_ENSEMBLE_ID = "kalshi_weather_ensemble_v14_shadow"
SPORTS_ID = "sports_persistent_passive_v1_shadow"


def utcnow():
    return datetime.now(timezone.utc)


def release_snapshot_memory():
    """Return freed decoded-market arenas to the Linux worker cgroup.

    A complete research snapshot currently contains tens of thousands of
    dictionaries.  Dropping the list makes those objects unreachable, but
    CPython's allocator can retain the empty arenas indefinitely.  On the
    512 MB Render instance that retained high-water allocation is enough to
    trigger a restart during a later discovery pass.  Collect cycles first,
    then ask glibc to return fully free heap pages.  Other platforms simply
    keep the normal garbage-collection behavior.
    """
    gc.collect()
    try:
        trim = ctypes.CDLL(None).malloc_trim
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int
        trim(0)
    except (AttributeError, OSError):
        pass


def open_db(path):
    db = sqlite3.connect(path, isolation_level=None, timeout=30)
    db.executescript("""
      CREATE TABLE IF NOT EXISTS calibration_observations(
        observation_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,decision_bucket INTEGER NOT NULL,
        observed_at TEXT NOT NULL,detail TEXT NOT NULL,resolution TEXT);
      CREATE TABLE IF NOT EXISTS calibration_parent_observations(
        observation_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,decision_bucket INTEGER NOT NULL,
        observed_at TEXT NOT NULL,detail TEXT NOT NULL,resolution TEXT);
      CREATE TABLE IF NOT EXISTS structural_signals(
        signal_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,decision_bucket INTEGER NOT NULL,
        observed_at TEXT NOT NULL,detail TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS favorite_maker_observations(
        observation_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,decision_bucket INTEGER NOT NULL,
        observed_at TEXT NOT NULL,detail TEXT NOT NULL,resolution TEXT);
      CREATE TABLE IF NOT EXISTS weather_forecasts(
        snapshot_id TEXT PRIMARY KEY,observed_at TEXT NOT NULL,station TEXT NOT NULL,
        target_date TEXT NOT NULL,extreme TEXT NOT NULL,model TEXT NOT NULL,detail TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS weather_observations(
        observation_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,decision_bucket INTEGER NOT NULL,
        observed_at TEXT NOT NULL,detail TEXT NOT NULL,resolution TEXT);
      CREATE TABLE IF NOT EXISTS scans(
        generation INTEGER PRIMARY KEY,observed_at TEXT NOT NULL,market_count INTEGER NOT NULL,
        calibration_observations INTEGER NOT NULL,indicative_candidates INTEGER NOT NULL,
        confirmed_structural_signals INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS coverage_settings(
        name TEXT PRIMARY KEY,detail TEXT NOT NULL);
    """)
    migrated = db.execute(
        "SELECT 1 FROM coverage_settings WHERE name='parent_event_calibration_v1'"
    ).fetchone()
    if not migrated:
        db.execute("BEGIN")
        try:
            # Preserve the original contract-hour table as audit evidence, but
            # seed the corrected parent-event table only with canonical rows
            # produced by the bounded collector if a prior deploy wrote any.
            db.execute(
                "INSERT OR IGNORE INTO calibration_parent_observations "
                "SELECT * FROM calibration_observations "
                "WHERE observation_id=event_id||':'||"
                "json_extract(detail,'$.classification')||':'||"
                "json_extract(detail,'$.price_bin')"
            )
            db.execute(
                "INSERT INTO coverage_settings VALUES(?,?)",
                ("parent_event_calibration_v1", json.dumps({
                    "legacy_table_preserved": True,
                    "gate_table": "calibration_parent_observations",
                }, sort_keys=True)),
            )
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise
    init_strategy_factory(db)
    init_official_release(db)
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
        try:
            setting = db.execute(
                "SELECT detail FROM settings WHERE name='market_discovery'"
            ).fetchone()
            coverage = json.loads(setting[0]) if setting else {}
        except sqlite3.Error:
            coverage = {}
        generation_row = db.execute(
            "SELECT max(generation) FROM research_market_universe"
        ).fetchone()
        generation = generation_row[0] if generation_row else None
        source_generations = [generation] if generation is not None else []
        # Keep the last complete catalog stable while the maker scans a new
        # generation page by page. Otherwise a sleeve temporarily sees only
        # the first few pages and can miss weather events later in the scan.
        if coverage.get("in_progress") and coverage.get("generation") == generation:
            complete_row = db.execute(
                "SELECT max(generation) FROM research_market_universe "
                "WHERE generation<?", (generation,)
            ).fetchone()
            if complete_row and complete_row[0] is not None:
                # Keep the stable complete catalog while adding every market
                # already discovered in the new pass.  Current rows override
                # matching older tickers.  This prevents newly listed weather
                # city-days from waiting hours for the full 150k-market walk.
                source_generations = [complete_row[0], generation]
        fields = (
            "ticker", "event_ticker", "status", "market_type", "exchange_index",
            "category", "title", "subtitle", "yes_sub_title", "no_sub_title",
            "yes_bid_dollars", "yes_ask_dollars", "no_bid_dollars", "no_ask_dollars",
            "yes_bid_size_fp", "yes_ask_size_fp", "no_bid_size_fp", "no_ask_size_fp",
            "volume_24h_fp", "strike_type", "floor_strike", "cap_strike",
            "rules_primary", "rules_secondary", "close_time",
            "expiration_time", "occurrence_datetime",
        )
        rows = []
        if generation is not None:
            expressions = ",".join(
                f"json_extract(detail,'$.{field}')" for field in fields)
            placeholders = ",".join("?" for _ in source_generations)
            cursor = db.execute(
                f"SELECT {expressions} FROM research_market_universe "
                f"WHERE generation IN ({placeholders}) "
                "ORDER BY generation,event_id,ticker", source_generations,
            )
            # Keep only the fields needed by the three research sleeves. Full
            # market JSON averages several KB and tens of thousands of decoded
            # dictionaries can exceed a 512 MB worker during restart.
            merged = {}
            for values in cursor:
                row = dict(zip(fields, values))
                merged[row["ticker"]] = row
            rows = sorted(merged.values(), key=lambda row: (
                str(row.get("event_ticker") or ""), str(row.get("ticker") or "")))
        db.close()
    except (sqlite3.Error, OSError):
        return None, [], {}
    if generation is None or not rows:
        return None, [], coverage
    return generation, rows, coverage


def parent_event_calibration_observations(markets, observed_at):
    """Keep one lifetime observation per parent event, class, and price bin.

    The sleeve is preregistered with parent-event grouping.  The original
    collector instead keyed rows by contract and hour, which wrote thousands
    of correlated variants from the same event on every pass.  Prefer the
    highest-volume representative deterministically and give it a stable
    parent-event identity.  Existing rows remain untouched audit evidence.
    """
    selected = {}
    for market in markets:
        try:
            volume = float(market.get("volume_24h_fp") or 0)
        except (TypeError, ValueError):
            volume = 0.0
        ticker = str(market.get("ticker") or "")
        for observation in favorite_longshot_observations(market, observed_at):
            key = (observation["event_id"], observation["classification"],
                   observation["price_bin"])
            rank = (volume, ticker)
            previous = selected.get(key)
            if previous is None or rank > previous[0]:
                observation["observation_id"] = ":".join(key)
                selected[key] = (rank, observation)
    return [selected[key][1] for key in sorted(selected)]


def collect_generation(db, client, generation, markets, now):
    init_structural_probe(db,now)
    calibration = parent_event_calibration_observations(markets, now.isoformat())
    favorite_maker = favorite_maker_observations(markets, now.isoformat())
    before = db.total_changes
    db.execute("BEGIN")
    try:
        db.executemany(
            "INSERT OR IGNORE INTO calibration_parent_observations "
            "VALUES(?,?,?,?,?,NULL)",
            ((row["observation_id"], row["event_id"], row["decision_bucket"],
              row["observed_at"], json.dumps(row, sort_keys=True))
             for row in calibration),
        )
        inserted = db.total_changes - before
        db.executemany(
            "INSERT OR IGNORE INTO favorite_maker_observations VALUES(?,?,?,?,?,NULL)",
            ((row["observation_id"], row["event_id"], row["decision_bucket"],
              row["observed_at"], json.dumps(row, sort_keys=True))
             for row in favorite_maker),
        )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise
    candidates = structural_candidates(markets, limit=2)
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
            if cursor.rowcount:
                capture_structural_probe(db,client,signal_id,signal,clock=time.time,
                                         now=datetime.now(timezone.utc))
        except (KeyError, TypeError, ValueError):
            continue
    db.execute(
        "INSERT OR REPLACE INTO scans VALUES(?,?,?,?,?,?)",
        (generation, now.isoformat(), len(markets), inserted, len(candidates), confirmed),
    )


def _weather_groups(markets, now):
    groups = defaultdict(list)
    tomorrow = now.date() + timedelta(days=1)
    horizon = now.date() + timedelta(days=7)
    for market in markets:
        spec = weather_market_spec(market)
        target = parse_target_date(market.get("event_ticker"))
        if (spec is not None and target is not None and tomorrow <= target <= horizon
                and market.get("status") == "active"
                and market.get("market_type") == "binary"):
            groups[(spec.station, market["event_ticker"])].append(market)
    return groups


def collect_weather_one_station(db, markets, now, *, fetcher=fetch_ensemble):
    """Refresh the oldest due station and retain every forecast vintage.

    One station per minute keeps the worker and free public API bounded. With
    the registered 20-station universe, each active station is revisited in at
    most about 30 minutes without a second crawler.
    """
    groups = _weather_groups(markets, now)
    by_station = defaultdict(list)
    for (station, _event), rows in groups.items():
        by_station[station].extend(rows)
    fetched = _coverage_load(db, "weather_station_fetches", {})
    candidates = []
    for station in by_station:
        try:
            last = datetime.fromisoformat(str(fetched.get(station)).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            last = datetime.min.replace(tzinfo=timezone.utc)
        age = (now - last).total_seconds()
        if age >= 1800:
            candidates.append((last, station))
    if not candidates:
        return {"active_events": len(groups), "active_stations": len(by_station),
                "station_fetched": None, "error": None}
    _last, station = min(candidates)
    station_rows = by_station[station]
    spec = weather_market_spec(station_rows[0])
    targets = sorted({parse_target_date(row.get("event_ticker")) for row in station_rows})
    targets = [target for target in targets if target is not None]
    if not spec or not targets:
        return {"active_events": len(groups), "active_stations": len(by_station),
                "station_fetched": None, "error": "invalid_station_group"}
    forecasts, transport = fetcher(spec, targets[0], targets[-1])
    fetched[station] = now.isoformat()
    _coverage_save(db, "weather_station_fetches", fetched)
    _coverage_save(db, "weather_last_transport", transport)
    if transport.get("error"):
        return {"active_events": len(groups), "active_stations": len(by_station),
                "station_fetched": station, "error": transport["error"]}
    new_vintages = observations = signals = 0
    for target_text, models in forecasts.items():
        for model, row in models.items():
            for extreme in ("high", "low"):
                snapshot_id = ":".join((station, target_text, extreme, model,
                                        str(row.get("forecast_hash"))))
                cursor = db.execute(
                    "INSERT OR IGNORE INTO weather_forecasts VALUES(?,?,?,?,?,?,?)",
                    (snapshot_id, now.isoformat(), station, target_text, extreme, model,
                     json.dumps({"members": row.get(extreme) or [],
                                 "forecast_hash": row.get("forecast_hash"),
                                 "transport": transport}, sort_keys=True)),
                )
                new_vintages += cursor.rowcount
    for (event_station, event_id), event_markets in sorted(groups.items()):
        if event_station != station:
            continue
        target = parse_target_date(event_id)
        observation = weather_event_observation(
            event_id, event_markets, forecasts.get(target.isoformat(), {}) if target else {},
            now.isoformat())
        cursor = db.execute(
            "INSERT OR IGNORE INTO weather_observations VALUES(?,?,?,?,?,NULL)",
            (observation["observation_id"], event_id, observation["decision_bucket"],
             observation["observed_at"], json.dumps(observation, sort_keys=True)),
        )
        observations += cursor.rowcount
        signals += cursor.rowcount * int(observation.get("signal") is not None)
    return {"active_events": len(groups), "active_stations": len(by_station),
            "station_fetched": station, "new_forecast_vintages": new_vintages,
            "new_observations": observations, "new_signals": signals, "error": None}


def resolve_one_weather(db, client, now):
    for observation_id, detail in db.execute(
            "SELECT observation_id,detail FROM weather_observations "
            "WHERE resolution IS NULL ORDER BY observed_at LIMIT 20"):
        row = json.loads(detail)
        expiry = row.get("expiration_time")
        try:
            expires = datetime.fromisoformat(str(expiry).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if expires > now:
            continue
        contracts = row.get("contracts") or []
        if not contracts:
            db.execute("UPDATE weather_observations SET resolution=? WHERE observation_id=?",
                       (json.dumps({"resolved_at": now.isoformat(),
                                    "unscored_reason": row.get("classification")}, sort_keys=True),
                        observation_id))
            return
        try:
            payload, _, _ = client.get(contracts[0]["ticker"])
            market = payload["market"]
            expiration_value = float(market["expiration_value"])
        except (KeyError, TypeError, ValueError):
            return
        resolution = score_resolution(row, expiration_value)
        resolution["resolved_at"] = now.isoformat()
        db.execute("UPDATE weather_observations SET resolution=? WHERE observation_id=?",
                   (json.dumps(resolution, sort_keys=True), observation_id))
        return


def _resolve_one_table(db, client, now, table, price_key, *, parent_event_only=False):
    canonical = (" AND observation_id=event_id||':'||"
                 "json_extract(detail,'$.classification')||':'||"
                 "json_extract(detail,'$.price_bin')" if parent_event_only else "")
    for observation_id, detail in db.execute(
            f"SELECT observation_id,detail FROM {table} "
            "WHERE resolution IS NULL "
            "AND julianday(json_extract(detail,'$.expiration_time'))<=julianday(?)"
            f"{canonical} ORDER BY observed_at LIMIT 20", (now.isoformat(),)):
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
    _resolve_one_table(db, client, now, "calibration_parent_observations",
                       "price_cents", parent_event_only=True)
    _resolve_one_table(db, client, now, "favorite_maker_observations",
                       "passive_price_cents")


def _flb_summary(db):
    groups = defaultdict(list); net = capital_days = 0.0; complete = 0; equity = peak = drawdown = 0.0
    for event_id, resolution in db.execute(
            "SELECT event_id,resolution FROM calibration_parent_observations "
            "WHERE resolution IS NOT NULL"):
        outcome = json.loads(resolution); value = float(outcome["cost_stressed_net_cents"])
        groups[event_id].append(value); net += value
        capital_days += float(outcome["capital_days"]); complete += 1
        equity += value; peak = max(peak, equity); drawdown = max(drawdown, peak - equity)
    return {"strategy_id": FLB_ID, "execution_enabled": False,
            "evidence_scope": "parent_event_canonical_only",
            "legacy_contract_hour_table_preserved": True,
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


def _weather_summary(db):
    groups = defaultdict(list); stations = defaultdict(set)
    resolved_city_days = set(); seasons = set()
    net = capital_days = 0.0; complete = signals_complete = 0
    equity = peak = drawdown = 0.0; brier_deltas = []; rps_deltas = []
    for event_id, detail, resolution in db.execute(
            "SELECT event_id,detail,resolution FROM weather_observations "
            "WHERE resolution IS NOT NULL"):
        try:
            row = json.loads(detail); outcome = json.loads(resolution)
        except (json.JSONDecodeError, TypeError):
            # Preserve malformed historical rows for audit, but never let one
            # record stop current collection or enter a profitability gate.
            continue
        if "brier_delta_vs_market" not in outcome:
            continue
        complete += 1
        cluster = f"{row.get('station')}:{row.get('target_date')}"
        resolved_city_days.add(cluster)
        try:
            month = datetime.fromisoformat(row["target_date"] + "T00:00:00").month
            seasons.add("winter" if month in (12, 1, 2) else
                        "spring" if month in (3, 4, 5) else
                        "summer" if month in (6, 7, 8) else "fall")
        except (TypeError, ValueError):
            pass
        brier_deltas.append(float(outcome["brier_delta_vs_market"]))
        rps_deltas.append(float(outcome["rps_delta_vs_market"]))
        stations[str(row.get("station"))].add(str(row.get("target_date")))
        value = outcome.get("cost_stressed_net_dollars")
        if value is not None:
            cents = float(value) * 100
            groups[cluster].append(cents); net += cents; signals_complete += 1
            signal = row.get("signal") or {}
            try:
                observed = datetime.fromisoformat(row["observed_at"].replace("Z", "+00:00"))
                target = datetime.fromisoformat(row["target_date"] + "T23:59:59+00:00")
                capital_days += float(signal.get("ask") or 0) * max(
                    0, (target - observed).total_seconds() / 86400)
            except (TypeError, ValueError):
                pass
            equity += cents; peak = max(peak, equity); drawdown = max(drawdown, peak - equity)
    candidates = malformed = 0
    for (detail,) in db.execute("SELECT detail FROM weather_observations"):
        try:
            candidates += int(json.loads(detail).get("signal") is not None)
        except (json.JSONDecodeError, TypeError):
            malformed += 1
    observations = db.execute("SELECT count(*) FROM weather_observations").fetchone()[0]
    vintages = db.execute("SELECT count(*) FROM weather_forecasts").fetchone()[0]
    station_days = {station: len(days) for station, days in stations.items()}
    independent = len(resolved_city_days)
    lcb = cluster_lower_bound(groups) if groups else None
    average_brier = sum(brier_deltas) / len(brier_deltas) if brier_deltas else None
    average_rps = sum(rps_deltas) / len(rps_deltas) if rps_deltas else None
    gate = {
        "minimum_city_days": independent >= 300,
        "minimum_six_cities_with_50_days_each": sum(
            days >= 50 for days in station_days.values()) >= 6,
        "two_seasons": len(seasons) >= 2,
        "positive_cost_stressed_net": signals_complete > 0 and net > 0,
        "improved_brier_over_market": average_brier is not None and average_brier < 0,
        "improved_rps_over_market": average_rps is not None and average_rps < 0,
        "positive_city_day_clustered_95pct_lower_bound": lcb is not None and lcb > 0,
    }
    return {"strategy_id": WEATHER_ENSEMBLE_ID, "execution_enabled": False,
            "forecast_vintages": vintages, "observations": observations,
            "candidate_observations": candidates, "independent_events": independent,
            "malformed_observations_excluded": malformed,
            "complete_observations": complete, "complete_signal_observations": signals_complete,
            "cost_stressed_net_cents": net if signals_complete else None,
            "event_clustered_95pct_lower_bound_cents": lcb,
            "average_brier_delta_vs_market": average_brier,
            "average_rps_delta_vs_market": average_rps,
            "station_resolved_days": station_days, "gate": gate,
            "state": "eligible_for_separate_demo_trial" if gate and all(gate.values()) else "collecting",
            "capital_days": capital_days if signals_complete else None,
            "maximum_drawdown_cents": drawdown if signals_complete else None}


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
                 error=None, weather_cycle=None, fee_probe_error=None,
                 official_release_error=None, official_release_run=None):
    init_structural_probe(db,utcnow())
    structural_records = _records(db, "structural_signals")
    flb_records = _records(db, "calibration_parent_observations")
    structural = {"strategy_id": STRUCTURAL_ID, "execution_enabled": False,
                  "independent_events": len({row["event_id"] for row in structural_records}),
                  "complete_observations": len(structural_records),
                  "cost_stressed_net_cents": None,
                  "event_clustered_95pct_lower_bound_cents": None,
                  "capital_days": None, "maximum_drawdown_cents": None}
    flb = _flb_summary(db)
    favorite_maker = _favorite_maker_summary(db)
    weather = _weather_summary(db)
    sports, sports_records = _sports(root)
    favorite_maker_records = _records(db, "favorite_maker_observations")
    weather_records = _records(db, "weather_observations")
    packet = comparison_packet([sports, structural, flb, favorite_maker, weather], {
        SPORTS_ID: sports_records, STRUCTURAL_ID: structural_records, FLB_ID: flb_records,
        FAVORITE_MAKER_ID: favorite_maker_records,
        WEATHER_ENSEMBLE_ID: weather_records,
    })
    packet["favorite_longshot_gate"] = flb
    packet['structural_execution_probe']=structural_probe_status(db)
    packet["favorite_maker_gate"] = favorite_maker
    packet["weather_ensemble_gate"] = weather
    packet["weather_cycle"] = weather_cycle or {}
    packet["strategy_factory"] = strategy_factory_status(db)
    packet["factory_fee_probe"] = {**factory_fee_probe_status(db),
                                   "error": fee_probe_error}
    packet["official_release_probe"] = {**official_release_status(db, utcnow()),
                                        "error": official_release_error,
                                        "last_cycle": official_release_run or {}}
    frozen={'capability_id':'nested_threshold_spread_quote_v1','version':1,'spec':{}}
    register_experiments(db,[{'id':'6502b781-73a1-42f7-a6a1-16ac86eb86f0',**frozen,
        'spec_hash':hashlib.sha256(json.dumps(frozen,sort_keys=True,separators=(',',':')).encode()).hexdigest()}],now=utcnow())
    packet["experiment_registry"] = experiment_status(
        db, packet["strategy_factory"], packet["official_release_probe"])
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
                       "weather_ensemble_markets": coverage.get(
                           "weather_ensemble_markets"),
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
            weather_cycle = {}
            fee_probe_error = None
            official_release_error = None
            official_release_run = {}
            mve_coverage = _coverage_load(db, "mve_discovery", {})
            try:
                generation, markets, coverage = maker_snapshot(root / "worker.sqlite3")
                if generation is not None:
                    collect_generation(db, client, generation, markets, utcnow())
                    try:
                        # Acquire current books and fee metadata before slower
                        # research collectors. No settlement outcome is used.
                        for _ in range(4):
                            acquisition = factory_fee_probe_next(db, client, now=utcnow())
                            if not acquisition.get("probed") and not acquisition.get("attempted"):
                                break
                    except Exception as exc:
                        fee_probe_error = type(exc).__name__
                    resolve_one(db, client, utcnow())
                    weather_cycle = collect_weather_one_station(db, markets, utcnow())
                    resolve_one_weather(db, client, utcnow())
                    idea_file = root / "life_os_strategy_ideas.json"
                    ideas = []
                    try:
                        if idea_file.exists() and idea_file.stat().st_size <= 32768:
                            supplied = json.loads(idea_file.read_text(encoding="utf-8"))
                            if (isinstance(supplied, dict)
                                    and supplied.get("schema") == "kalshi_research_ideas_v1"
                                    and isinstance(supplied.get("ideas"), list)):
                                ideas = supplied["ideas"]
                    except (OSError, ValueError, TypeError):
                        pass
                    register_experiments(db, ideas, now=utcnow())
                    strategy_factory_cycle(db, now=utcnow(), ideas=ideas, parallel=True)
                    try:
                        official_release_run = official_release_cycle(
                            db, client, markets, utcnow())
                    except Exception as exc:
                        # Research acquisition cannot affect the Demo maker or
                        # another shadow sleeve. The status retains the error.
                        official_release_error = type(exc).__name__
                mve_coverage = advance_mve_coverage(db, client, utcnow())
            except Exception as exc:
                error = type(exc).__name__
            packet = write_status(root, db, generation, len(markets), coverage,
                                  mve_coverage, error, weather_cycle, fee_probe_error,
                                  official_release_error, official_release_run)
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
            release_snapshot_memory()
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
