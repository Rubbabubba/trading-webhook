"""Demo-only one-contract maker experiment with queue and markout evidence."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
import argparse
import json
import os
from pathlib import Path
import re
import sqlite3
import statistics
import time
import uuid

from .kalshi_binary_broker import BinaryDemoBroker
from .kalshi_binary_journal import BinaryJournal
from .kalshi_demo_broker import DemoClient, check_exchange
from .kalshi_demo_market_data import DemoMarkets
from .kalshi_demo_v4_worker import event_id, limit_price_cents, one_contract_frame
from .kalshi_depth_replenishment import init as init_depth_recorder, capture as capture_depth, status as depth_status
from .kalshi_public_trade_liquidity import init as init_trade_probe, poll as poll_trade_probe, status as trade_probe_status
from .kalshi_targeted_trade_liquidity import init as init_targeted_trade_probe, poll as poll_targeted_trade_probe, status as targeted_trade_probe_status
from .kalshi_targeted_trade_liquidity import candidate as targeted_trade_candidate
from .kalshi_depth_cohort_trades import init as init_depth_trade_probe, poll as poll_depth_trade_probe, status as depth_trade_probe_status
from .kalshi_trade_depth_overlap import report as trade_depth_overlap_report
from .kalshi_scan_trade_capture import init as init_scan_trade_capture, poll as poll_scan_trade_capture, status as scan_trade_capture_status
from .kalshi_deterministic_monitor import run_check as run_deterministic_monitor
from .life_os_reporter import schedule as schedule_life_os_report
from .kalshi_maker_v5 import maker_quote
from .kalshi_maker_v10 import (
    MARKOUT_HORIZONS as V10_MARKOUT_HORIZONS,
    SIGNAL_COOLDOWN_SECONDS as V10_SIGNAL_COOLDOWN_SECONDS,
    STRATEGY_ID as V10_STRATEGY_ID,
    shadow_decision as v10_shadow_decision,
    stressed_markout as v10_stressed_markout,
)
from .kalshi_maker_v11 import (
    MARKOUT_HORIZONS as V11_MARKOUT_HORIZONS,
    SIGNAL_EVENT_COOLDOWN_SECONDS as V11_SIGNAL_EVENT_COOLDOWN_SECONDS,
    STRATEGY_ID as V11_STRATEGY_ID,
    shadow_decision as v11_shadow_decision,
    stressed_markout as v11_stressed_markout,
)
from .kalshi_maker_v12 import (
    MARKOUT_HORIZONS as V12_MARKOUT_HORIZONS,
    MIN_ABS_IMBALANCE as V12_MIN_ABS_IMBALANCE,
    MIN_GROSS_EDGE_CENTS as V12_MIN_GROSS_EDGE_CENTS,
    SIGNAL_EVENT_COOLDOWN_SECONDS as V12_SIGNAL_EVENT_COOLDOWN_SECONDS,
    STRATEGY_ID as V12_STRATEGY_ID,
    shadow_decision as v12_shadow_decision,
    stressed_markout as v12_stressed_markout,
)
from .kalshi_v12_demo_trial import (
    CLIENT_ID_PREFIX as V12_TRIAL_CLIENT_ID_PREFIX,
    EXECUTION_ENABLED as V12_TRIAL_ENABLED,
    MAX_FILLS as V12_TRIAL_MAX_FILLS,
    MAX_FLAT_BALANCE_LOSS_CENTS as V12_TRIAL_LOSS_STOP_CENTS,
    MAX_ORDER_ATTEMPTS as V12_TRIAL_MAX_ORDER_ATTEMPTS,
    STRATEGY_ID as V12_TRIAL_STRATEGY_ID,
    trial_decision as v12_trial_decision,
)
from .kalshi_v12_fillability_trial import (
    CLIENT_ID_PREFIX as V12_FILLABILITY_CLIENT_ID_PREFIX,
    EXECUTION_ENABLED as V12_FILLABILITY_ENABLED,
    MAX_FILLS as V12_FILLABILITY_MAX_FILLS,
    MAX_FLAT_BALANCE_LOSS_CENTS as V12_FILLABILITY_LOSS_STOP_CENTS,
    MAX_ORDER_ATTEMPTS as V12_FILLABILITY_MAX_ORDER_ATTEMPTS,
    STRATEGY_ID as V12_FILLABILITY_STRATEGY_ID,
    improved_quote as v12_fillability_quote,
    trial_decision as v12_fillability_decision,
)
from .kalshi_process_lock import acquire
from .kalshi_shadow import cost, price_book
from .kalshi_external_sleeves import research_relevance
from .kalshi_factory_demo_trial import (
    CLIENT_ID_PREFIX as FACTORY_TRIAL_CLIENT_ID_PREFIX,
    MAX_FLAT_LOSS_CENTS as FACTORY_TRIAL_MAX_LOSS_CENTS,
    SCAN_INTERVAL_SECONDS as FACTORY_TRIAL_SCAN_INTERVAL_SECONDS,
    eligible_candidate as factory_eligible_candidate,
    recent_signal as factory_recent_signal,
    trial_allowed as factory_trial_allowed,
    trial_counts as factory_trial_counts,
)
from .kalshi_v12_quote_holdout import evaluate as evaluate_v12_holdout
from .kalshi_v12_execution_feasibility import crossing_diagnostic


STRATEGY_ID = "stable_balanced_maker_v9"
CLIENT_ID_PREFIX = "v9-maker-"
# V9 is retired from opening new positions after its first eight completed
# Demo trades all lost money (seven timed taker exits and one settlement).
# Keep the worker alive so it can reconcile existing state, scan the complete
# market universe, and collect the frozen V10 shadow evidence.  Re-enabling an
# executable strategy requires a separate, explicitly gated Demo trial.
EXECUTION_ENABLED = False
EXECUTION_POLICY_ID = "v9_retired_after_8_losses_20260924"
EXECUTION_DISABLED_REASON = "retired_negative_demo_evidence"
V12_TRIAL_POLICY_ID = "v12_one_contract_demo_trial_20261001"
V12_TRIAL_START_BALANCE_KEY = "v12_trial_start_balance_cents"
V12_TRIAL_LAST_FLAT_BALANCE_KEY = "v12_trial_last_flat_balance_cents"
V12_FILLABILITY_POLICY_ID = "v12_one_tick_fillability_trial_20261002"
V12_FILLABILITY_START_BALANCE_KEY = "v12_fillability_start_balance_cents"
V12_FILLABILITY_LAST_FLAT_BALANCE_KEY = "v12_fillability_last_flat_balance_cents"
CAPITAL_LIMIT_CENTS = 160
ORDER_LIMIT_CENTS = 110
DAILY_LOSS_CENTS = 100
FEE_RESERVE_CENTS = 5
# V7 proved that passive quotes can reach the front of the demo queue but still
# produced no fills during a fifteen-minute lifetime.  V8 quotes closer to the
# midpoint and rotates every three minutes so breadth and fillability are tested
# promptly without crossing the spread.
QUOTE_TTL_SECONDS = 180
ADVERSE_MOVE_CENTS = 2
IMMEDIATE_ADVERSE_MOVE_CENTS = 3
TOXIC_OBSERVATIONS_REQUIRED = 3
DEPTH_ADVERSE_MOVE_CENTS = 1
MAX_HOLD_SECONDS = 300
TAKE_PROFIT_CENTS = 2
MARKOUT_HORIZONS = (5, 30, 300)
COHORT_SELECTOR = "all_open_binary_events_v2"
COHORT_MINIMUM = 16
COHORT_ROTATION_SECONDS = 30 * 60
# A quote observation currently takes about ten seconds after exchange rate
# limiting.  Eight markets keep revisits below 90 seconds, so V10 can retain
# three prior 60--300 second anchors inside its frozen 300-second history.
SAMPLING_WINDOW_SIZE = 8
SAMPLING_WINDOW_SECONDS = 15 * 60
MARKET_PAGE_LIMIT = 200
ZERO_FILL_RECOVERY = "v5_all_terminal_zero_fill_recovery_20260918"
LEGACY_UNCERTAINTY_STOP_RECOVERY = "v5_legacy_uncertainty_stop_recovery_20260919"
FRESH_FLAT_RECOVERY = "v7_fresh_flat_startup_recovery_20260919"
TERMINAL_FLAT_STOP_RECOVERY = "v9_terminal_flat_stop_recovery_20260922"
RECONCILIATION_WAIT_SECONDS = 30
UNRESOLVED_QUARANTINE_SECONDS = 5 * 60
UNRESOLVED_REQUIRED_OBSERVATIONS = 2
UNRESOLVED_CONFIRMATION_SPAN_SECONDS = 60
RECONCILIATION_BLOCK_CODES = frozenset({
    "submission_unresolved",
    "fills_not_reconciled",
    "BrokerError",
    "TimeoutError",
    "ConnectionError",
})
def now_iso():
    return datetime.now(timezone.utc).isoformat()


def eligible_market_candidates(rows, *, now):
    """Rank eligible contracts from any Kalshi market-list page."""
    candidates = []
    for market in rows:
        try:
            if (market.get("status") != "active" or market.get("market_type") != "binary"
                    or market.get("exchange_index", 0) != 0):
                continue
            close = datetime.fromisoformat(
                market["close_time"].replace("Z", "+00:00")
            ).timestamp()
            bid = Decimal(str(market.get("yes_bid_dollars") or "0"))
            ask = Decimal(str(market.get("yes_ask_dollars") or "0"))
            bid_size = Decimal(str(market.get("yes_bid_size_fp") or "0"))
            ask_size = Decimal(str(market.get("yes_ask_size_fp") or "0"))
            volume = Decimal(str(
                market.get("volume_24h_fp") or market.get("volume_24h") or "0"
            ))
            if close <= now + 1800:
                continue
            midpoint_value = (bid + ask) / 2
            spread = ask - bid
            if not Decimal(".20") <= midpoint_value <= Decimal(".80"):
                continue
            # Use the union of the frozen V9 execution screen and V10 shadow
            # screen. V9 will still decline 9-10 cent spreads or books beyond
            # its 2:1 balance rule; including them here lets V10 assess every
            # market allowed by its registered 10-cent/4:1 hypothesis.
            if not Decimal(".03") <= spread <= Decimal(".10"):
                continue
            if min(bid_size, ask_size) < 3:
                continue
            if max(bid_size, ask_size) > min(bid_size, ask_size) * 4:
                continue
            candidates.append((volume, min(bid_size, ask_size), spread, market))
        except (KeyError, ValueError, ArithmeticError):
            continue
    return candidates


def market_family(market):
    """Stable audit label even when market-list responses omit category."""
    category = str(market.get("category") or "").strip()
    if category:
        return category
    identity = event_id(market)
    return identity.split("-", 1)[0] if identity else "unknown"


def market_admission_rejection(market, *, now):
    """Explain why an open standard contract did not enter the maker cohort."""
    try:
        if market.get("status") != "active":
            return "not_active"
        if market.get("market_type") != "binary":
            return "not_binary"
        if market.get("exchange_index", 0) != 0:
            return "unsupported_exchange_index"
        close = datetime.fromisoformat(
            market["close_time"].replace("Z", "+00:00")
        ).timestamp()
        if close <= now + 1800:
            return "closes_within_30_minutes"
        bid = Decimal(str(market.get("yes_bid_dollars") or "0"))
        ask = Decimal(str(market.get("yes_ask_dollars") or "0"))
        bid_size = Decimal(str(market.get("yes_bid_size_fp") or "0"))
        ask_size = Decimal(str(market.get("yes_ask_size_fp") or "0"))
        midpoint_value = (bid + ask) / 2
        spread = ask - bid
        if not Decimal(".20") <= midpoint_value <= Decimal(".80"):
            return "midpoint_out_of_range"
        if not Decimal(".03") <= spread <= Decimal(".10"):
            return "spread_out_of_range"
        if min(bid_size, ask_size) < 3:
            return "insufficient_depth"
        if max(bid_size, ask_size) > min(bid_size, ask_size) * 4:
            return "depth_imbalance"
        return None
    except (KeyError, ValueError, ArithmeticError):
        return "invalid_or_incomplete_market_data"


def select_maker_markets(rows, *, now, limit=None, offset=0):
    """Select one liquid contract per event from an already complete universe."""
    candidates = eligible_market_candidates(rows, now=now)

    candidates.sort(key=lambda row: (-row[0], -row[1], -row[2], row[3]["ticker"]))
    diverse = []
    selected_events = set()
    for _volume, _depth, _spread, market in candidates:
        identity = event_id(market)
        if identity in selected_events:
            continue
        diverse.append(market)
        selected_events.add(identity)
    if not diverse:
        return []
    offset %= len(diverse)
    selected_count = len(diverse) if limit is None else min(limit, len(diverse))
    return [diverse[(offset + index) % len(diverse)]
            for index in range(selected_count)]


def advance_market_discovery(state, markets, *, now, limit=None):
    """Scan one page of the complete open-market universe without blocking status.

    Every open non-combo market is examined. Only active binary contracts with
    executable midpoint, spread, depth, balance and time-to-close enter the
    rotating order-book cohort. One contract per event prevents correlated
    variants from inflating the independent-event evidence gate.
    """
    scan = state.load("market_discovery", {})
    if not scan.get("in_progress"):
        generation = int(scan.get("generation", 0)) + 1
        scan = {
            "in_progress": True, "generation": generation, "cursor": None,
            "started_at": now, "pages": 0, "markets_scanned": 0,
            "eligible_markets": 0,
            "research_relevant_markets": 0,
            "favorite_longshot_markets": 0,
            "favorite_maker_markets": 0,
            "weather_ensemble_markets": 0,
            "nested_threshold_markets": 0,
            "coverage_accounting_complete": True,
            "market_families": {}, "eligible_families": {},
            "admission_rejections": {},
        }
    else:
        # A deployment can resume a scan created before coverage accounting
        # existed. Preserve the generation and cursor while initializing the
        # additive counters instead of failing mid-scan.
        missing_coverage = any(key not in scan for key in (
            "research_relevant_markets", "favorite_longshot_markets",
            "favorite_maker_markets",
            "weather_ensemble_markets",
            "nested_threshold_markets", "market_families", "eligible_families",
            "admission_rejections"))
        for key in ("research_relevant_markets", "favorite_longshot_markets",
                    "favorite_maker_markets",
                    "weather_ensemble_markets",
                    "nested_threshold_markets"):
            scan.setdefault(key, 0)
        if missing_coverage:
            scan["coverage_accounting_complete"] = False
        for key in ("market_families", "eligible_families", "admission_rejections"):
            scan.setdefault(key, {})
    params = {"status": "open", "limit": MARKET_PAGE_LIMIT, "mve_filter": "exclude"}
    if scan.get("cursor"):
        params["cursor"] = scan["cursor"]
    page, _started, _observed = markets.get(params=params)
    rows = page.get("markets", [])
    generation = scan["generation"]
    for market in rows:
        family = market_family(market)
        scan["market_families"][family] = scan["market_families"].get(family, 0) + 1
        rejection = market_admission_rejection(market, now=now)
        if rejection is None:
            scan["eligible_families"][family] = scan["eligible_families"].get(family, 0) + 1
        else:
            scan["admission_rejections"][rejection] = (
                scan["admission_rejections"].get(rejection, 0) + 1)
        tags = research_relevance(market)
        if tags:
            scan["research_relevant_markets"] += 1
            scan["favorite_longshot_markets"] += int("favorite_longshot" in tags)
            scan["favorite_maker_markets"] += int("favorite_maker" in tags)
            scan["weather_ensemble_markets"] += int("weather_ensemble" in tags)
            scan["nested_threshold_markets"] += int("nested_threshold" in tags)
            state.db.execute(
                "INSERT OR REPLACE INTO research_market_universe VALUES(?,?,?,?,?)",
                (market["ticker"], event_id(market), generation,
                 json.dumps(tags, sort_keys=True), json.dumps(market, sort_keys=True)),
            )
    ranked = eligible_market_candidates(rows, now=now)
    for volume, depth, spread, market in ranked:
        state.db.execute(
            "INSERT OR REPLACE INTO market_universe VALUES(?,?,?,?,?,?,?)",
            (market["ticker"], event_id(market), generation,
             json.dumps(market, sort_keys=True), str(volume), str(depth), str(spread)),
        )
    scan["pages"] += 1
    scan["markets_scanned"] += len(rows)
    scan["eligible_markets"] += len(ranked)
    scan["cursor"] = page.get("cursor") or None
    if scan["cursor"]:
        state.save("market_discovery", scan)
        return None, scan

    universe = [json.loads(row[0]) for row in state.db.execute(
        "SELECT detail FROM market_universe WHERE generation=?", (generation,)
    )]
    rotation = int(state.load("cohort_rotation", 0))
    selected = select_maker_markets(
        universe, now=now, limit=limit,
        offset=rotation * limit if limit is not None else 0,
    )
    distinct_events = state.db.execute(
        "SELECT count(DISTINCT event_id) FROM market_universe WHERE generation=?",
        (generation,),
    ).fetchone()[0]
    research_events = state.db.execute(
        "SELECT count(DISTINCT event_id) FROM research_market_universe WHERE generation=?",
        (generation,),
    ).fetchone()[0]
    scan.update({
        "in_progress": False, "cursor": None, "completed_at": now,
        "eligible_events": distinct_events, "selected_markets": len(selected),
        "research_relevant_events": research_events,
    })
    state.db.execute("DELETE FROM market_universe WHERE generation!=?", (generation,))
    state.db.execute("DELETE FROM research_market_universe WHERE generation!=?", (generation,))
    state.save("market_discovery", scan)
    state.save("last_completed_market_discovery", {
        key: scan.get(key) for key in (
            "generation", "completed_at", "coverage_accounting_complete",
            "markets_scanned", "eligible_markets", "research_relevant_markets",
            "eligible_events", "research_relevant_events",
        )
    } | {"market_families": len(scan["market_families"])})
    state.save("cohort_rotation", rotation + 1)
    state.record(None, {"action": "all_market_discovery_complete", **{
        key: scan[key] for key in (
            "generation", "pages", "markets_scanned", "eligible_markets",
            "eligible_events", "selected_markets", "research_relevant_markets",
            "research_relevant_events", "favorite_longshot_markets",
            "favorite_maker_markets",
            "weather_ensemble_markets",
            "nested_threshold_markets",
        )
    }})
    return selected, scan


def refresh_cohort(state, markets, cohort, cohort_at, *, now):
    """Refresh the scan cohort without discarding a previously valid cohort.

    Demo market-list responses occasionally exceed the strict freshness bound.
    A failed refresh must not turn that transient read failure into a two-second
    error loop; fresh per-market books still validate every later signal.
    """
    discovery = state.load("market_discovery", {})
    if (cohort and now - cohort_at < COHORT_ROTATION_SECONDS
            and not discovery.get("in_progress")):
        return cohort, cohort_at
    try:
        refreshed, discovery = advance_market_discovery(state, markets, now=now)
    except ValueError:
        refreshed = None
    if refreshed:
        state.save("cohort", refreshed)
        state.save("cohort_at", now)
        return refreshed, now
    cohort_stale = bool(cohort and now - cohort_at >= COHORT_ROTATION_SECONDS)
    cohort_underfilled = bool(cohort and len(cohort) < COHORT_MINIMUM)
    if cohort and (cohort_stale or cohort_underfilled
                   or discovery.get("in_progress")):
        # The Demo universe can contain well over 100,000 contracts. Do not
        # make active evidence collection wait for an unbounded cursor walk.
        # Rotate through the eligible events accumulated in the current pass
        # while discovery continues one page at a time in the background.
        generation = discovery.get("generation")
        partial = [json.loads(row[0]) for row in state.db.execute(
            "SELECT detail FROM market_universe WHERE generation=?", (generation,)
        )] if generation else []
        source_generation = generation
        if not partial and hasattr(state, "db"):
            # Early pages in Kalshi's very large ordering may contain no
            # executable books. Keep rotating through the last completed,
            # still-live eligible universe until the new pass reaches useful
            # candidates. Each selected contract is revalidated from its live
            # order book before any quote can be submitted.
            previous = state.db.execute(
                "SELECT MAX(generation) FROM market_universe WHERE generation!=?",
                (generation,),
            ).fetchone()[0]
            if previous is not None:
                source_generation = previous
                partial = [json.loads(row[0]) for row in state.db.execute(
                    "SELECT detail FROM market_universe WHERE generation=?",
                    (previous,),
                )]
        # Preserve still-live events from the last complete pass while adding
        # every newly discovered event. Per-market book reads revalidate each
        # candidate, and the completed pass later removes stale events.
        partial = select_maker_markets(partial + cohort, now=now)
        # During a long universe walk, expand the active scan set whenever a
        # new eligible event appears. Never replace a broad serving set with a
        # smaller partial page while the full discovery pass is incomplete.
        old_tickers = {market["ticker"] for market in cohort}
        new_tickers = {market["ticker"] for market in partial}
        if partial and len(partial) >= len(cohort) and new_tickers != old_tickers:
            state.save("cohort", partial)
            state.save("cohort_at", now)
            state.record(None, {
                "action": "partial_market_discovery_cohort_rotated",
                "generation": generation,
                "source_generation": source_generation,
                "pages": discovery.get("pages", 0),
                "markets_scanned": discovery.get("markets_scanned", 0),
                "eligible_markets": discovery.get("eligible_markets", 0),
                "selected_markets": len(partial),
            })
            return partial, now
    if cohort:
        return cohort, cohort_at
    generation = discovery.get("generation")
    if generation:
        provisional = [json.loads(row[0]) for row in state.db.execute(
            "SELECT detail FROM market_universe WHERE generation=?", (generation,)
        )]
        provisional = select_maker_markets(provisional, now=now)
        if provisional:
            state.save("cohort", provisional)
            state.save("cohort_at", now)
            return provisional, now
    raise ValueError("no_eligible_demo_markets")


def sampling_window(state, cohort, *, now):
    """Serve every eligible event through cadence-safe rotating windows.

    A public Demo quote needs two rate-limited reads. Walking a large all-event
    cohort once therefore takes longer than V10's frozen 300-second history
    window and leaves every market permanently short of its three anchors.
    Keep the complete cohort for coverage, but observe a stable bounded window
    long enough to form valid history and future markouts before rotating to
    the next events.
    """
    if not cohort:
        raise ValueError("no_eligible_demo_markets")
    current = state.load("sampling_window", {})
    current_markets = current.get("markets", [])
    started_at = current.get("started_at")
    if (len(current_markets) == min(SAMPLING_WINDOW_SIZE, len(cohort))
            and type(started_at) in (int, float)
            and now - started_at < SAMPLING_WINDOW_SECONDS):
        # A partial universe pass may choose a different contract for an event
        # as new pages arrive.  Keep this observation window stable until its
        # history and markouts mature; every order book is still read live.
        return current_markets

    ordered = sorted(cohort, key=lambda market: market["ticker"])
    offset = int(state.load("sampling_window_offset", 0)) % len(ordered)
    count = min(SAMPLING_WINDOW_SIZE, len(ordered))
    selected = [ordered[(offset + index) % len(ordered)] for index in range(count)]
    next_offset = (offset + count) % len(ordered)
    window = {
        "started_at": now,
        "offset": offset,
        "next_offset": next_offset,
        "size": count,
        "cohort_size": len(ordered),
        "tickers": [market["ticker"] for market in selected],
        "markets": selected,
    }
    state.save("sampling_window", window)
    state.save("sampling_window_offset", next_offset)
    state.save("sampling_window_scan", 0)
    state.record(None, {"action": "sampling_window_rotated", **{
        key: window[key] for key in (
            "started_at", "offset", "next_offset", "size", "cohort_size",
        )
    }})
    return selected


def next_sampling_market(state, cohort, *, now):
    window = sampling_window(state, cohort, now=now)
    scan = int(state.load("sampling_window_scan", 0))
    market = window[scan % len(window)]
    state.save("sampling_window_scan", scan + 1)
    return market


class MakerState:
    def __init__(self, path):
        self.db = sqlite3.connect(path, isolation_level=None, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        init_depth_recorder(self.db)
        init_trade_probe(self.db,time.time())
        init_targeted_trade_probe(self.db,time.time())
        init_depth_trade_probe(self.db,time.time())
        init_scan_trade_capture(self.db,time.time())
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS settings(name TEXT PRIMARY KEY,detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS history(at REAL NOT NULL,ticker TEXT NOT NULL,mid TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS intent_meta(
            client_id TEXT PRIMARY KEY,kind TEXT NOT NULL,event_id TEXT NOT NULL,
            ticker TEXT NOT NULL,outcome TEXT NOT NULL,created_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS factory_trial_assignments(
            client_id TEXT PRIMARY KEY,strategy_id TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS entered_events(event_id TEXT PRIMARY KEY,entered_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS maker_fills(
            client_id TEXT PRIMARY KEY,ticker TEXT NOT NULL,outcome TEXT NOT NULL,
            filled_at REAL NOT NULL,entry_mid TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS queue_records(
            client_id TEXT NOT NULL,at REAL NOT NULL,detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS working_quote_observations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,client_id TEXT NOT NULL,
            at REAL NOT NULL,detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS flow_context(
            client_id TEXT PRIMARY KEY,detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS markouts(
            client_id TEXT NOT NULL,horizon_seconds INTEGER NOT NULL,at REAL NOT NULL,
            midpoint TEXT NOT NULL,PRIMARY KEY(client_id,horizon_seconds));
          CREATE TABLE IF NOT EXISTS uncertainty_checks(
            client_id TEXT NOT NULL,observed_at REAL NOT NULL,evidence TEXT NOT NULL,
            PRIMARY KEY(client_id,observed_at));
          CREATE TABLE IF NOT EXISTS actions(at REAL NOT NULL,ticker TEXT,detail TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS v10_shadow_signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,ticker TEXT NOT NULL,
            observed_at REAL NOT NULL,outcome TEXT NOT NULL,
            price_cents INTEGER NOT NULL,detail TEXT NOT NULL,event_id TEXT);
          CREATE TABLE IF NOT EXISTS v10_shadow_markouts(
            signal_id INTEGER NOT NULL,horizon_seconds INTEGER NOT NULL,
            observed_at REAL NOT NULL,yes_mid TEXT NOT NULL,
            gross_cents TEXT NOT NULL,stressed_cents TEXT NOT NULL,
            PRIMARY KEY(signal_id,horizon_seconds));
          CREATE TABLE IF NOT EXISTS v10_shadow_evaluations(
            reason TEXT PRIMARY KEY,count INTEGER NOT NULL,last_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS v11_shadow_signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,ticker TEXT NOT NULL,
            observed_at REAL NOT NULL,outcome TEXT NOT NULL,
            price_cents INTEGER NOT NULL,detail TEXT NOT NULL,event_id TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS v11_shadow_markouts(
            signal_id INTEGER NOT NULL,horizon_seconds INTEGER NOT NULL,
            observed_at REAL NOT NULL,yes_mid TEXT NOT NULL,
            gross_cents TEXT NOT NULL,stressed_cents TEXT NOT NULL,
            PRIMARY KEY(signal_id,horizon_seconds));
          CREATE TABLE IF NOT EXISTS v11_shadow_evaluations(
            reason TEXT PRIMARY KEY,count INTEGER NOT NULL,last_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS v12_shadow_signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,ticker TEXT NOT NULL,
            observed_at REAL NOT NULL,outcome TEXT NOT NULL,
            price_cents INTEGER NOT NULL,detail TEXT NOT NULL,event_id TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS v12_shadow_markouts(
            signal_id INTEGER NOT NULL,horizon_seconds INTEGER NOT NULL,
            observed_at REAL NOT NULL,yes_mid TEXT NOT NULL,
            gross_cents TEXT NOT NULL,stressed_cents TEXT NOT NULL,
            PRIMARY KEY(signal_id,horizon_seconds));
          CREATE TABLE IF NOT EXISTS v12_shadow_evaluations(
            reason TEXT PRIMARY KEY,count INTEGER NOT NULL,last_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS market_universe(
            ticker TEXT PRIMARY KEY,event_id TEXT NOT NULL,generation INTEGER NOT NULL,
            detail TEXT NOT NULL,volume TEXT NOT NULL,depth TEXT NOT NULL,spread TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS research_market_universe(
            ticker TEXT PRIMARY KEY,event_id TEXT NOT NULL,generation INTEGER NOT NULL,
            tags TEXT NOT NULL,detail TEXT NOT NULL);
        """)
        if "event_id" not in {
                row[1] for row in self.db.execute("PRAGMA table_info(v10_shadow_signals)")}:
            self.db.execute("ALTER TABLE v10_shadow_signals ADD COLUMN event_id TEXT")
        protocol = {
            "strategy_id": STRATEGY_ID, "environment": "demo", "one_contract": True,
            "capital_limit_cents": CAPITAL_LIMIT_CENTS, "order_limit_cents": ORDER_LIMIT_CENTS,
            "daily_loss_cents": DAILY_LOSS_CENTS, "quote_ttl_seconds": QUOTE_TTL_SECONDS,
            "max_hold_seconds": MAX_HOLD_SECONDS, "markout_horizons": list(MARKOUT_HORIZONS),
            "take_profit_cents": TAKE_PROFIT_CENTS,
            "adverse_move_cents": ADVERSE_MOVE_CENTS,
            "immediate_adverse_move_cents": IMMEDIATE_ADVERSE_MOVE_CENTS,
            "toxic_observations_required": TOXIC_OBSERVATIONS_REQUIRED,
            "depth_adverse_move_cents": DEPTH_ADVERSE_MOVE_CENTS,
        }
        saved = self.load("protocol")
        if saved is not None and saved != protocol:
            raise ValueError("maker_protocol_changed")
        self.save("protocol", protocol)
        self.save("execution_policy", {
            "policy_id": EXECUTION_POLICY_ID,
            "strategy_id": STRATEGY_ID,
            "execution_enabled": EXECUTION_ENABLED,
            "reason": EXECUTION_DISABLED_REASON,
            "replacement_candidate": V10_STRATEGY_ID,
            "replacement_execution_enabled": False,
        })
        shadow_protocol = {
            "strategy_id": V10_STRATEGY_ID,
            "execution_enabled": False,
            "signal_cooldown_seconds": V10_SIGNAL_COOLDOWN_SECONDS,
            "markout_horizons": list(V10_MARKOUT_HORIZONS),
        }
        saved_shadow = self.load("v10_shadow_protocol")
        if saved_shadow is not None and saved_shadow != shadow_protocol:
            raise ValueError("v10_shadow_protocol_changed")
        self.save("v10_shadow_protocol", shadow_protocol)
        v11_protocol = {
            "strategy_id": V11_STRATEGY_ID,
            "execution_enabled": False,
            "minimum_absolute_imbalance": "0.25",
            "event_signal_cooldown_seconds": V11_SIGNAL_EVENT_COOLDOWN_SECONDS,
            "markout_horizons": list(V11_MARKOUT_HORIZONS),
        }
        saved_v11 = self.load("v11_shadow_protocol")
        if saved_v11 is not None and saved_v11 != v11_protocol:
            raise ValueError("v11_shadow_protocol_changed")
        self.save("v11_shadow_protocol", v11_protocol)
        v12_protocol = {
            "strategy_id": V12_STRATEGY_ID,
            "execution_enabled": False,
            "minimum_absolute_imbalance": str(V12_MIN_ABS_IMBALANCE),
            "minimum_microprice_gross_edge_cents": V12_MIN_GROSS_EDGE_CENTS,
            "quote_location": "selected_outcome_best_bid",
            "event_signal_cooldown_seconds": V12_SIGNAL_EVENT_COOLDOWN_SECONDS,
            "markout_horizons": list(V12_MARKOUT_HORIZONS),
        }
        saved_v12 = self.load("v12_shadow_protocol")
        if saved_v12 is not None and saved_v12 != v12_protocol:
            raise ValueError("v12_shadow_protocol_changed")
        self.save("v12_shadow_protocol", v12_protocol)
        v12_trial_protocol = {
            "strategy_id": V12_TRIAL_STRATEGY_ID,
            "policy_id": V12_TRIAL_POLICY_ID,
            "environment": "demo",
            "execution_enabled": V12_TRIAL_ENABLED,
            "production_execution_enabled": False,
            "contracts_per_order": 1,
            "maximum_working_orders": 1,
            "maximum_open_positions": 1,
            "maximum_order_attempts": V12_TRIAL_MAX_ORDER_ATTEMPTS,
            "maximum_fills": V12_TRIAL_MAX_FILLS,
            "maximum_flat_balance_loss_cents": V12_TRIAL_LOSS_STOP_CENTS,
        }
        saved_v12_trial = self.load("v12_trial_protocol")
        if saved_v12_trial is not None and saved_v12_trial != v12_trial_protocol:
            raise ValueError("v12_trial_protocol_changed")
        self.save("v12_trial_protocol", v12_trial_protocol)
        v12_fillability_protocol = {
            "strategy_id": V12_FILLABILITY_STRATEGY_ID,
            "policy_id": V12_FILLABILITY_POLICY_ID,
            "environment": "demo",
            "execution_enabled": V12_FILLABILITY_ENABLED,
            "production_execution_enabled": False,
            "quote_location": "selected_outcome_best_bid_plus_one",
            "price_improvement_cents": 1,
            "minimum_gross_edge_cents": 4,
            "minimum_remaining_stressed_edge_cents": 1,
            "contracts_per_order": 1,
            "maximum_working_orders": 1,
            "maximum_open_positions": 1,
            "maximum_order_attempts": V12_FILLABILITY_MAX_ORDER_ATTEMPTS,
            "maximum_fills": V12_FILLABILITY_MAX_FILLS,
            "maximum_flat_balance_loss_cents": V12_FILLABILITY_LOSS_STOP_CENTS,
        }
        saved_fillability = self.load("v12_fillability_trial_protocol")
        if saved_fillability is not None and saved_fillability != v12_fillability_protocol:
            raise ValueError("v12_fillability_trial_protocol_changed")
        self.save("v12_fillability_trial_protocol", v12_fillability_protocol)

    def save(self, name, value):
        self.db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)",
                        (name, json.dumps(value, sort_keys=True)))

    def load(self, name, default=None):
        row = self.db.execute("SELECT detail FROM settings WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    def record(self, ticker, detail):
        self.db.execute("INSERT INTO actions VALUES(?,?,?)", (time.time(), ticker, json.dumps(detail)))

    def close(self):
        self.db.close()


def log_event(event, **values):
    print(json.dumps({"at": now_iso(), "event": event, "environment": "demo",
                      "strategy_id": STRATEGY_ID, **values}, sort_keys=True), flush=True)


def safe_cycle_error(error):
    """Expose only bounded internal error codes, never transport or credential text."""
    if isinstance(error, ValueError):
        code = str(error)
        if re.fullmatch(r"[a-z0-9_]{1,80}", code):
            return code
    return type(error).__name__


def recover(state, journal, broker):
    for row in journal.records():
        cid = row["payload"]["client_order_id"]
        if row["state"] == "reserved":
            journal.abandon_reserved(cid)
        elif row["state"] in ("uncertain", "working"):
            try:
                row = broker.refresh(cid)
            except ValueError as error:
                if str(error) != "submission_unresolved" or not quarantine_stale_unresolved(
                        state, journal, broker, cid):
                    raise
                continue
        if row["state"] == "working":
            broker.cancel(cid)
    # A filled market can settle while the service is restarting.  Record this
    # ledger's settlement before comparing active positions, since settled
    # contracts correctly disappear from the exchange position endpoint.
    broker.reconcile_settlements()
    broker.reconcile_positions(allow_reserved=True)
    state.save("factory_restart_reconciliation", {"reconciled_at": now_iso(),
                                                  "environment": "demo", "positions_verified": True})
    stopped = journal.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0]
    records = journal.records()
    if stopped and not records and state.load(FRESH_FLAT_RECOVERY) is None:
        # reconcile_positions above has already proved the demo account has no
        # position or resting order. This recovers only a never-used ledger that
        # stopped during a guarded version cutover.
        journal.db.execute("UPDATE controls SET stopped=0 WHERE id=1")
        state.save(FRESH_FLAT_RECOVERY, True)
        state.record(None, {"action": "fresh_flat_startup_recovery",
                            "environment": "demo"})
        stopped = 0
    if stopped and state.load(ZERO_FILL_RECOVERY) is None:
        maker = [row for row in records if row["intent"].get("order_mode") == "post_only_gtc"]
        accounting = journal.accounting()
        if (len(records) == len(maker) >= 3 and all(row["state"] == "terminal" for row in maker)
                and all(row["filled"] == 0 for row in maker) and not accounting["positions"]):
            journal.db.execute("UPDATE controls SET stopped=0 WHERE id=1")
            state.save(ZERO_FILL_RECOVERY, True)
            state.record(None, {"action": "verified_zero_fill_stop_recovery",
                                "attempts": len(maker), "environment": "demo"})
            stopped = 0
    if stopped and state.load(TERMINAL_FLAT_STOP_RECOVERY) is None:
        accounting = journal.accounting()
        # Reconciliation above has independently established that the Demo
        # exchange has no position or resting order. Release only a safety stop
        # whose complete local ledger is also terminal and flat. This covers a
        # transient settlement/position visibility mismatch across a restart;
        # it cannot release uncertainty, working orders, or open exposure.
        if (records and all(row["state"] == "terminal" for row in records)
                and any(row["filled"] for row in records)
                and not accounting["positions"] and accounting["open_basis"] == 0
                and accounting["pending_reserves"] == 0):
            journal.db.execute("UPDATE controls SET stopped=0 WHERE id=1")
            state.save(TERMINAL_FLAT_STOP_RECOVERY, True)
            state.record(None, {
                "action": "verified_terminal_flat_stop_recovery",
                "orders": len(records), "environment": "demo",
            })


def quarantine_stale_unresolved(state, journal, broker, client_id):
    """Archive only an old, zero-exposure demo submission with complete proof."""
    now = journal.clock().timestamp()
    record = journal.get(client_id)
    if (record["state"] != "uncertain" or record["broker_id"] is not None
            or record["filled"] != 0 or record["intent"].get("order_mode") != "post_only_gtc"):
        return False
    meta = state.db.execute(
        "SELECT kind,ticker FROM intent_meta WHERE client_id=?", (client_id,)
    ).fetchone()
    quote = journal.db.execute(
        "SELECT detail FROM submission_quotes WHERE client_id=?", (client_id,)
    ).fetchone()
    submitted_at = json.loads(quote[0]).get("observed_at") if quote else None
    if (meta is None or meta[0] != "maker_entry" or type(submitted_at) not in (int, float)
            or now - submitted_at < UNRESOLVED_QUARANTINE_SECONDS):
        return False
    ticker = meta[1]
    current = broker.client.pages("/portfolio/orders", "orders", ticker=ticker, subaccount=0)
    historical = broker.client.pages("/historical/orders", "orders", ticker=ticker)
    current_fills = broker.client.pages("/portfolio/fills", "fills", ticker=ticker, subaccount=0)
    historical_fills = broker.client.pages("/historical/fills", "fills", ticker=ticker)
    positions = broker.client.pages(
        "/portfolio/positions", "market_positions", subaccount=0, count_filter="position"
    )
    resting = broker.client.pages("/portfolio/orders", "orders", subaccount=0, status="resting")
    # Kalshi fill rows identify the exchange order, not the client order.  A
    # ticker may therefore contain fills from earlier, fully journaled orders.
    # Those known fills are unrelated to this uncertain submission and must not
    # prevent zero-exposure quarantine forever.  Any fill whose order ID is not
    # already durable in the journal remains blocking evidence.
    known_broker_ids = {
        row["broker_id"] for row in journal.records() if row["broker_id"] is not None
    }
    proof = {
        "environment": "demo", "observed_at": now,
        "current_exact_orders": sum(row.get("client_order_id") == client_id for row in current),
        "historical_exact_orders": sum(row.get("client_order_id") == client_id for row in historical),
        "ticker_current_fills": len(current_fills),
        "ticker_historical_fills": len(historical_fills),
        "ticker_unattributed_current_fills": sum(
            row.get("order_id") not in known_broker_ids for row in current_fills
        ),
        "ticker_unattributed_historical_fills": sum(
            row.get("order_id") not in known_broker_ids for row in historical_fills
        ),
        "ticker_positions": sum(row.get("ticker") == ticker for row in positions),
        "all_positions": len(positions), "all_resting_orders": len(resting),
    }
    blocking_keys = (
        "current_exact_orders", "historical_exact_orders",
        "ticker_unattributed_current_fills", "ticker_unattributed_historical_fills",
        "ticker_positions", "all_positions", "all_resting_orders",
    )
    if any(proof[key] for key in blocking_keys):
        state.db.execute("DELETE FROM uncertainty_checks WHERE client_id=?", (client_id,))
        return False
    state.db.execute(
        "INSERT OR IGNORE INTO uncertainty_checks VALUES(?,?,?)",
        (client_id, now, json.dumps(proof, sort_keys=True)),
    )
    observations = [json.loads(row[0]) for row in state.db.execute(
        "SELECT evidence FROM uncertainty_checks WHERE client_id=? ORDER BY observed_at",
        (client_id,),
    )]
    if (len(observations) < UNRESOLVED_REQUIRED_OBSERVATIONS
            or observations[-1]["observed_at"] - observations[0]["observed_at"]
            < UNRESOLVED_CONFIRMATION_SPAN_SECONDS):
        return False
    proof["negative_observations"] = observations
    journal.quarantine_uncertain_zero_fill(
        client_id, proof, minimum_age_seconds=UNRESOLVED_QUARANTINE_SECONDS,
        minimum_observations=UNRESOLVED_REQUIRED_OBSERVATIONS,
        minimum_observation_span_seconds=UNRESOLVED_CONFIRMATION_SPAN_SECONDS,
    )
    state.save("last_uncertain_quarantine", {
        "client_order_id": client_id, "ticker": ticker, "at": proof["observed_at"]
    })
    state.record(ticker, {"action": "uncertain_zero_fill_quarantined",
                          "client_order_id": client_id, "environment": "demo"})
    return True


def release_legacy_uncertainty_stop(state, journal):
    """Undo only the V5 stop created by the old crash-on-uncertainty path.

    Submission uncertainty remains authoritative and continues to block every new
    reservation. Removing this redundant hard stop lets a later confirmed fill be
    managed and exited after read-only reconciliation succeeds.
    """
    stopped = journal.db.execute("SELECT stopped FROM controls WHERE id=1").fetchone()[0]
    uncertain = [row for row in journal.records() if row["state"] == "uncertain"]
    if not stopped or not uncertain or state.load(LEGACY_UNCERTAINTY_STOP_RECOVERY) is not None:
        return False
    latest = state.db.execute(
        "SELECT detail FROM actions WHERE ticker IS NULL ORDER BY at DESC LIMIT 1"
    ).fetchone()
    detail = json.loads(latest[0]) if latest else {}
    if detail.get("action") != "cycle_error" or detail.get("error_code") not in RECONCILIATION_BLOCK_CODES:
        return False
    journal.db.execute("UPDATE controls SET stopped=0 WHERE id=1")
    state.save(LEGACY_UNCERTAINTY_STOP_RECOVERY, True)
    state.record(None, {"action": "legacy_uncertainty_stop_released",
                        "unresolved_orders": len(uncertain), "environment": "demo"})
    return True


def submit(state, journal, broker, markets, market, outcome, action, price_cents, *,
           maker=False, context=None, client_id_prefix=CLIENT_ID_PREFIX,
           entry_kind=None, strategy_id=None):
    client_id = client_id_prefix + uuid.uuid4().hex
    if entry_kind not in (None, "factory_trial_entry") or (entry_kind and (maker or action != "buy")):
        raise ValueError("invalid_demo_entry_kind")
    if (entry_kind == "factory_trial_entry") != (isinstance(strategy_id, str) and bool(strategy_id)):
        raise ValueError("factory_trial_version_required")
    kind = entry_kind or ("maker_entry" if maker else "exit")
    at = time.time()
    state.db.execute("INSERT INTO intent_meta VALUES(?,?,?,?,?,?)",
                     (client_id, kind, event_id(market), market["ticker"], outcome, at))
    if entry_kind == "factory_trial_entry":
        state.db.execute("INSERT INTO factory_trial_assignments VALUES(?,?)", (client_id, strategy_id))
    if maker:
        if not isinstance(context, dict):
            raise ValueError("maker_flow_context_required")
        state.db.execute("INSERT INTO flow_context VALUES(?,?)", (client_id, json.dumps(context, sort_keys=True)))
    def discard_unsent_metadata():
        state.db.execute("DELETE FROM factory_trial_assignments WHERE client_id=?", (client_id,))
        state.db.execute("DELETE FROM flow_context WHERE client_id=?", (client_id,))
        state.db.execute("DELETE FROM intent_meta WHERE client_id=?", (client_id,))

    try:
        snapshot = broker.snapshot()
        journal.reserve(client_id, market["ticker"], 1, price_cents, FEE_RESERVE_CENTS,
                        outcome=outcome, action=action, account_snapshot=snapshot,
                        order_mode="post_only_gtc" if maker else "ioc")
    except Exception:
        discard_unsent_metadata()
        raise
    try:
        if entry_kind == "factory_trial_entry":
            from .kalshi_factory_demo_trial import attest_attempt
            attest_attempt(state, journal, client_id, strategy_id)
        result = broker.submit(client_id, quote_provider=markets.quote)
    except Exception:
        if journal.get(client_id)["state"] == "reserved":
            journal.abandon_reserved(client_id)
            discard_unsent_metadata()
        raise
    if not maker and result["state"] == "working":
        result = broker.cancel(client_id)
    state.record(market["ticker"], {"action": kind, "outcome": outcome,
                                    "client_order_id": client_id, "state": result["state"],
                                    "filled": result["filled"], "environment": "demo"})
    return result


def working_order(journal):
    rows = [row for row in journal.records() if row["state"] == "working"]
    if len(rows) > 1:
        raise ValueError("multiple_working_orders")
    return rows[0] if rows else None


def midpoint(frame):
    bid, ask, _bid_size, _ask_size = price_book(frame, "yes")
    return (bid + ask) / 2


def preferred_outcome(state, journal, ticker):
    """Keep a ticker in one economic outcome for the lifetime of its ledger."""
    maker_ids = {
        row["payload"]["client_order_id"]
        for row in journal.records()
        if row["intent"].get("order_mode") == "post_only_gtc"
    }
    rows = state.db.execute(
        "SELECT client_id,outcome FROM intent_meta WHERE ticker=? AND kind='maker_entry'",
        (ticker,),
    ).fetchall()
    outcomes = {outcome for client_id, outcome in rows if client_id in maker_ids}
    if len(outcomes) > 1:
        raise ValueError("opposing_outcomes_in_maker_state")
    if outcomes:
        return next(iter(outcomes))
    counts = {side: 0 for side in ("yes", "no")}
    for client_id, outcome in state.db.execute(
            "SELECT client_id,outcome FROM intent_meta WHERE kind='maker_entry'"):
        if client_id in maker_ids:
            counts[outcome] += 1
    return "yes" if counts["yes"] <= counts["no"] else "no"


def v9_submission_allowed(signal, *, event_locked):
    """Fail closed after V9's negative live-Demo evidence review."""
    return EXECUTION_ENABLED and signal is not None and not event_locked


def v12_trial_counts(journal):
    records = [
        row for row in journal.records()
        if row["payload"]["client_order_id"].startswith(V12_TRIAL_CLIENT_ID_PREFIX)
        and row["intent"].get("order_mode") == "post_only_gtc"
    ]
    return len(records), sum(bool(row["filled"]) for row in records)


def v12_trial_event_recent(state, independent_event, *, now=None):
    """Apply V12's frozen 30-minute event cooldown to real Demo attempts."""
    now = time.time() if now is None else now
    row = state.db.execute(
        "SELECT max(created_at) FROM intent_meta WHERE event_id=? AND client_id LIKE ?",
        (independent_event, V12_TRIAL_CLIENT_ID_PREFIX + "%"),
    ).fetchone()
    return row is not None and row[0] is not None and (
        now - row[0] < V12_SIGNAL_EVENT_COOLDOWN_SECONDS
    )


def v12_trial_status(state, journal, shadow, *, event_locked=False):
    attempts, fills = v12_trial_counts(journal)
    return v12_trial_decision(
        shadow=shadow,
        attempts=attempts,
        fills=fills,
        start_balance_cents=state.load(V12_TRIAL_START_BALANCE_KEY),
        current_flat_balance_cents=state.load(V12_TRIAL_LAST_FLAT_BALANCE_KEY),
        event_locked=event_locked,
    )


def v12_fillability_trial_counts(journal):
    records = [
        row for row in journal.records()
        if row["payload"]["client_order_id"].startswith(V12_FILLABILITY_CLIENT_ID_PREFIX)
        and row["intent"].get("order_mode") == "post_only_gtc"
    ]
    return records, len(records), sum(bool(row["filled"]) for row in records)


def v12_fillability_event_recent(state, independent_event, *, now=None):
    """Prevent repeated live-Demo attempts on the same event for 30 minutes."""
    now = time.time() if now is None else now
    row = state.db.execute(
        "SELECT max(created_at) FROM intent_meta WHERE event_id=? "
        "AND (client_id LIKE ? OR client_id LIKE ?)",
        (independent_event, V12_FILLABILITY_CLIENT_ID_PREFIX + "%",
         V12_TRIAL_CLIENT_ID_PREFIX + "%"),
    ).fetchone()
    return row is not None and row[0] is not None and (
        now - row[0] < V12_SIGNAL_EVENT_COOLDOWN_SECONDS
    )


def v12_fillability_status(state, journal, shadow, *, event_locked=False):
    records, attempts, fills = v12_fillability_trial_counts(journal)
    result = v12_fillability_decision(
        shadow=shadow,
        attempts=attempts,
        fills=fills,
        start_balance_cents=state.load(V12_FILLABILITY_START_BALANCE_KEY),
        current_flat_balance_cents=state.load(V12_FILLABILITY_LAST_FLAT_BALANCE_KEY),
        event_locked=event_locked,
    )
    record_ids = {row["payload"]["client_order_id"] for row in records}
    filled_ids = {
        row["payload"]["client_order_id"] for row in records if row["filled"]
    }
    metadata = [row for row in state.db.execute(
        "SELECT client_id,event_id,ticker FROM intent_meta WHERE kind='maker_entry'"
    ) if row[0] in record_ids]
    fee_cents = Decimal("0")
    for client_id in filled_ids:
        evidence_row = journal.db.execute(
            "SELECT detail FROM broker_evidence WHERE client_id=?", (client_id,)
        ).fetchone()
        if evidence_row is not None:
            fee_cents += Decimal(str(json.loads(evidence_row[0]).get("fees_dollars") or "0")) * 100
    filled_metadata = [row for row in metadata if row[0] in filled_ids]
    result.update({
        "terminal_orders": sum(row["state"] == "terminal" for row in records),
        "unresolved_orders": sum(row["state"] in ("working", "uncertain") for row in records),
        "attempted_independent_events": len({row[1] for row in metadata}),
        "attempted_market_families": len({row[2].split("-")[0] for row in metadata}),
        "filled_independent_events": len({row[1] for row in filled_metadata}),
        "filled_market_families": len({row[2].split("-")[0] for row in filled_metadata}),
        "actual_fee_cents": float(fee_cents),
    })
    return result


def observe_working_quote(state, record, frame):
    """Record live quote health and return a bounded early-cancel reason.

    A single two-cent move or one imbalanced book is only noise. Three
    consecutive observations must agree before a sustained move cancels the
    order. Depth imbalance is actionable only when the midpoint also moves at
    least one cent against the quote. A three-cent adverse move cancels
    immediately.
    """
    cid = record["payload"]["client_order_id"]
    meta = state.db.execute(
        "SELECT outcome FROM intent_meta WHERE client_id=? AND kind='maker_entry'",
        (cid,),
    ).fetchone()
    context = state.db.execute(
        "SELECT detail FROM flow_context WHERE client_id=?", (cid,)
    ).fetchone()
    if meta is None or context is None:
        raise ValueError("working_quote_metadata_missing")
    outcome = meta[0]
    initial_yes_mid = Fraction(json.loads(context[0])["yes_mid"])
    current_yes_mid = midpoint(frame)
    bid, ask, bid_depth, ask_depth = price_book(frame, outcome)
    adverse = (initial_yes_mid - current_yes_mid if outcome == "yes"
               else current_yes_mid - initial_yes_mid)
    adverse_cents = adverse * 100
    against_depth = ask_depth > bid_depth * 3
    detail = {
        "yes_mid": str(current_yes_mid),
        "side_bid": str(bid),
        "side_ask": str(ask),
        "bid_depth": str(bid_depth),
        "ask_depth": str(ask_depth),
        "adverse_move_cents": str(adverse_cents),
        "against_side_depth": against_depth,
    }
    at = time.time()
    state.db.execute(
        "INSERT INTO working_quote_observations(client_id,at,detail) VALUES(?,?,?)",
        (cid, at, json.dumps(detail, sort_keys=True)),
    )
    if adverse_cents >= IMMEDIATE_ADVERSE_MOVE_CENTS:
        return "immediate_adverse_midpoint"
    recent = [json.loads(row[0]) for row in state.db.execute(
        "SELECT detail FROM working_quote_observations WHERE client_id=? "
        "ORDER BY at DESC,id DESC LIMIT ?", (cid, TOXIC_OBSERVATIONS_REQUIRED)
    )]
    if len(recent) < TOXIC_OBSERVATIONS_REQUIRED:
        return None
    if all(Fraction(row["adverse_move_cents"]) >= ADVERSE_MOVE_CENTS for row in recent):
        return "sustained_adverse_midpoint"
    if (all(row["against_side_depth"] is True for row in recent)
            and all(Fraction(row["adverse_move_cents"]) >= DEPTH_ADVERSE_MOVE_CENTS
                    for row in recent)):
        return "sustained_depth_and_adverse_midpoint"
    return None


def register_fill(state, record, frame):
    if record["filled"] != 1:
        return
    cid = record["payload"]["client_order_id"]
    meta = state.db.execute("SELECT event_id,ticker,outcome,created_at,kind FROM intent_meta WHERE client_id=?",
                            (cid,)).fetchone()
    if meta is None:
        raise ValueError("maker_fill_metadata_missing")
    state.db.execute("INSERT OR IGNORE INTO entered_events VALUES(?,?)", (meta[0], meta[3]))
    if meta[4] == "factory_trial_entry":
        state.record(meta[1], {"action": "factory_trial_fill", "client_order_id": cid,
                               "event_id": meta[0], "environment": "demo"})
        return
    if meta[4] != "maker_entry":
        raise ValueError("entry_fill_kind_invalid")
    state.db.execute("INSERT OR IGNORE INTO maker_fills VALUES(?,?,?,?,?)",
                     (cid, meta[1], meta[2], time.time(), str(midpoint(frame))))


def current_position(journal, state):
    accounting = journal.accounting()
    if len(accounting["positions"]) > 1 or any(abs(value) != 1 for value in accounting["positions"].values()):
        raise ValueError("demo_inventory_limit_breached")
    if not accounting["positions"]:
        return None, accounting
    ticker, signed = next(iter(accounting["positions"].items()))
    outcome = "yes" if signed > 0 else "no"
    row = state.db.execute(
        "SELECT event_id,created_at,kind FROM intent_meta WHERE ticker=? AND outcome=? "
        "AND kind IN ('maker_entry','factory_trial_entry') "
        "ORDER BY created_at DESC LIMIT 1", (ticker, outcome)).fetchone()
    if row is None:
        raise ValueError("position_metadata_missing")
    return {"ticker": ticker, "outcome": outcome, "event_id": row[0], "opened_at": row[1],
            "entry_kind": row[2],
            "basis_cents": float(accounting["open_basis"] * 100)}, accounting


def record_due_markouts(state, ticker, frame):
    now = time.time(); mid = str(midpoint(frame))
    for cid, filled_at in state.db.execute(
            "SELECT client_id,filled_at FROM maker_fills WHERE ticker=?", (ticker,)).fetchall():
        for horizon in MARKOUT_HORIZONS:
            if now - filled_at >= horizon:
                state.db.execute("INSERT OR IGNORE INTO markouts VALUES(?,?,?,?)",
                                 (cid, horizon, now, mid))


def pending_markout_ticker(state):
    for cid, ticker, filled_at in state.db.execute(
            "SELECT client_id,ticker,filled_at FROM maker_fills ORDER BY filled_at"):
        count = state.db.execute("SELECT count(*) FROM markouts WHERE client_id=?", (cid,)).fetchone()[0]
        if count < len(MARKOUT_HORIZONS):
            return ticker
    return None


def observe_v10_shadow(state, ticker, history, frame, independent_event=None):
    """Record prospective V10 signals and cost-stressed future markouts."""
    observed_at = frame["received_at"]
    yes_mid = midpoint(frame)
    for signal_id, signal_at, outcome, price_cents in state.db.execute(
            "SELECT id,observed_at,outcome,price_cents FROM v10_shadow_signals WHERE ticker=?",
            (ticker,)).fetchall():
        signal = {"outcome": outcome, "price_cents": price_cents}
        markout = v10_stressed_markout(signal, yes_mid)
        for horizon in V10_MARKOUT_HORIZONS:
            if observed_at - signal_at >= horizon:
                state.db.execute(
                    "INSERT OR IGNORE INTO v10_shadow_markouts VALUES(?,?,?,?,?,?)",
                    (signal_id, horizon, observed_at, str(yes_mid),
                     markout["gross_cents"], markout["stressed_cents"]),
                )
    latest = state.db.execute(
        "SELECT observed_at FROM v10_shadow_signals WHERE ticker=? ORDER BY observed_at DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    if latest is not None and observed_at - latest[0] < V10_SIGNAL_COOLDOWN_SECONDS:
        return None
    signal, reason = v10_shadow_decision(history, frame)
    state.db.execute(
        "INSERT INTO v10_shadow_evaluations(reason,count,last_at) VALUES(?,1,?) "
        "ON CONFLICT(reason) DO UPDATE SET count=count+1,last_at=excluded.last_at",
        (reason, observed_at),
    )
    if signal is None:
        return None
    state.db.execute(
        "INSERT INTO v10_shadow_signals(ticker,observed_at,outcome,price_cents,detail,event_id) "
        "VALUES(?,?,?,?,?,?)",
        (ticker, observed_at, signal["outcome"], signal["price_cents"],
         json.dumps(signal, sort_keys=True), independent_event or ticker),
    )
    state.record(ticker, {"action": "v10_shadow_signal", **signal,
                          "execution_enabled": False, "environment": "demo"})
    return signal


def observe_v11_shadow(state, ticker, history, frame, independent_event=None):
    """Record prospective V11 signals with event-level deduplication."""
    observed_at = frame["received_at"]
    yes_mid = midpoint(frame)
    for signal_id, signal_at, outcome, price_cents in state.db.execute(
            "SELECT id,observed_at,outcome,price_cents FROM v11_shadow_signals WHERE ticker=?",
            (ticker,)).fetchall():
        signal = {"outcome": outcome, "price_cents": price_cents}
        markout = v11_stressed_markout(signal, yes_mid)
        for horizon in V11_MARKOUT_HORIZONS:
            if observed_at - signal_at >= horizon:
                state.db.execute(
                    "INSERT OR IGNORE INTO v11_shadow_markouts VALUES(?,?,?,?,?,?)",
                    (signal_id, horizon, observed_at, str(yes_mid),
                     markout["gross_cents"], markout["stressed_cents"]),
                )
    independent_event = independent_event or ticker
    latest = state.db.execute(
        "SELECT observed_at FROM v11_shadow_signals WHERE event_id=? "
        "ORDER BY observed_at DESC LIMIT 1", (independent_event,),
    ).fetchone()
    if latest is not None and observed_at - latest[0] < V11_SIGNAL_EVENT_COOLDOWN_SECONDS:
        return None
    signal, reason = v11_shadow_decision(history, frame)
    state.db.execute(
        "INSERT INTO v11_shadow_evaluations(reason,count,last_at) VALUES(?,1,?) "
        "ON CONFLICT(reason) DO UPDATE SET count=count+1,last_at=excluded.last_at",
        (reason, observed_at),
    )
    if signal is None:
        return None
    state.db.execute(
        "INSERT INTO v11_shadow_signals(ticker,observed_at,outcome,price_cents,detail,event_id) "
        "VALUES(?,?,?,?,?,?)",
        (ticker, observed_at, signal["outcome"], signal["price_cents"],
         json.dumps(signal, sort_keys=True), independent_event),
    )
    state.record(ticker, {"action": "v11_shadow_signal", **signal,
                          "execution_enabled": False, "environment": "demo"})
    return signal


def observe_v12_shadow(state, ticker, history, frame, independent_event=None):
    """Record prospective V12 signals and markouts without placing orders."""
    observed_at = frame["received_at"]
    yes_mid = midpoint(frame)
    for signal_id, signal_at, outcome, price_cents in state.db.execute(
            "SELECT id,observed_at,outcome,price_cents FROM v12_shadow_signals WHERE ticker=?",
            (ticker,)).fetchall():
        signal = {"outcome": outcome, "price_cents": price_cents}
        markout = v12_stressed_markout(signal, yes_mid)
        for horizon in V12_MARKOUT_HORIZONS:
            if observed_at - signal_at >= horizon:
                state.db.execute(
                    "INSERT OR IGNORE INTO v12_shadow_markouts VALUES(?,?,?,?,?,?)",
                    (signal_id, horizon, observed_at, str(yes_mid),
                     markout["gross_cents"], markout["stressed_cents"]),
                )
    independent_event = independent_event or ticker
    latest = state.db.execute(
        "SELECT observed_at FROM v12_shadow_signals WHERE event_id=? "
        "ORDER BY observed_at DESC LIMIT 1", (independent_event,),
    ).fetchone()
    if latest is not None and observed_at - latest[0] < V12_SIGNAL_EVENT_COOLDOWN_SECONDS:
        return None
    signal, reason = v12_shadow_decision(history, frame)
    state.db.execute(
        "INSERT INTO v12_shadow_evaluations(reason,count,last_at) VALUES(?,1,?) "
        "ON CONFLICT(reason) DO UPDATE SET count=count+1,last_at=excluded.last_at",
        (reason, observed_at),
    )
    if signal is None:
        return None
    state.db.execute(
        "INSERT INTO v12_shadow_signals(ticker,observed_at,outcome,price_cents,detail,event_id) "
        "VALUES(?,?,?,?,?,?)",
        (ticker, observed_at, signal["outcome"], signal["price_cents"],
         json.dumps(signal, sort_keys=True), independent_event),
    )
    state.record(ticker, {"action": "v12_shadow_signal", **signal,
                          "execution_enabled": False, "environment": "demo"})
    return signal


def observe_frame(state, ticker, frame, independent_event=None):
    """Feed one decision-time frame to frozen V10 and shadow challengers."""
    try:
        capture_depth(state.db, ticker, independent_event or ticker, frame)
    except (ValueError, TypeError, ArithmeticError) as error:
        # Missing research evidence cannot change order authority or frozen trials.
        state.save('depth_recorder_error', type(error).__name__)
    rows = state.db.execute(
        "SELECT at,mid FROM history WHERE ticker=? AND at>=? ORDER BY at",
        (ticker, frame["received_at"] - 300),
    ).fetchall()
    history = [(at, Fraction(mid)) for at, mid in rows]
    observe_v10_shadow(state, ticker, history, frame, independent_event)
    observe_v11_shadow(state, ticker, history, frame, independent_event)
    observe_v12_shadow(state, ticker, history, frame, independent_event)
    state.db.execute("INSERT INTO history VALUES(?,?,?)",
                     (frame["received_at"], ticker, str(midpoint(frame))))
    state.db.execute("DELETE FROM history WHERE at<?", (frame["received_at"] - 300,))
    return history


def scan_market_candidate(state, journal, markets, market):
    """Return a validated frame, skipping normal market-lifecycle races safely."""
    try:
        quote = markets.quote({"ticker": market["ticker"]})
        frame = one_contract_frame(quote, book_id=str(quote["observed_at"]))
        rows = observe_frame(state, market["ticker"], frame, event_id(market))
        preferred = preferred_outcome(state, journal, market["ticker"])
        signal = maker_quote(rows, frame, preferred)
        v12_signal, _reason = v12_shadow_decision(rows, frame)
        frame = dict(frame)
        frame["v12_trial_signal"] = v12_signal
        return frame, signal
    except ValueError as error:
        reason = str(error)
        if reason not in {"missing_book", "demo_market_not_active_binary"}:
            raise
        state.record(market["ticker"], {
            "action": "scan_skip", "reason": reason
        })
        return None, None


def evidence(state, journal):
    maker = [row for row in journal.records() if row["intent"].get("order_mode") == "post_only_gtc"]
    maker_ids = {row["payload"]["client_order_id"] for row in maker}
    filled_maker_ids = {
        row["payload"]["client_order_id"] for row in maker if row["filled"]
    }
    metadata = [row for row in state.db.execute(
        "SELECT client_id,ticker,outcome FROM intent_meta WHERE kind='maker_entry'"
    ) if row[0] in maker_ids]
    sides = {side: sum(row[2] == side for row in metadata) for side in ("yes", "no")}
    # The journal is the durable source of truth for executions.  A process can
    # restart after exchange reconciliation but before the strategy database
    # records the fill observation, especially when the market settles during
    # that restart.  Counting only maker_fills would silently erase that valid
    # execution from the proof sample.
    fills = len(filled_maker_ids)
    fee_records = 0
    for row in maker:
        if not row["filled"]:
            continue
        found = journal.db.execute("SELECT detail FROM broker_evidence WHERE client_id=?",
                                   (row["payload"]["client_order_id"],)).fetchone()
        if found is not None and "fees_dollars" in json.loads(found[0]):
            fee_records += 1
    marks = {str(h): state.db.execute(
        "SELECT count(*) FROM markouts WHERE horizon_seconds=?", (h,)).fetchone()[0]
        for h in MARKOUT_HORIZONS}
    v10_marks = {str(h): state.db.execute(
        "SELECT count(*) FROM v10_shadow_markouts WHERE horizon_seconds=?", (h,)
    ).fetchone()[0] for h in V10_MARKOUT_HORIZONS}
    v10_pnl = {str(h): state.db.execute(
        "SELECT coalesce(sum(CAST(stressed_cents AS REAL)),0) FROM v10_shadow_markouts "
        "WHERE horizon_seconds=?", (h,)
    ).fetchone()[0] for h in V10_MARKOUT_HORIZONS}
    evaluation_rows = state.db.execute(
        "SELECT reason,count,last_at FROM v10_shadow_evaluations"
    ).fetchall()
    v10_evaluations = sum(row[1] for row in evaluation_rows)
    v10_last_evaluation = max((row[2] for row in evaluation_rows), default=None)
    complete_signals = state.db.execute(
        "SELECT count(*) FROM (SELECT signal_id FROM v10_shadow_markouts "
        "GROUP BY signal_id HAVING count(DISTINCT horizon_seconds)=?)",
        (len(V10_MARKOUT_HORIZONS),),
    ).fetchone()[0]
    independent_events = state.db.execute(
        "SELECT count(DISTINCT coalesce(s.event_id,s.ticker)) "
        "FROM v10_shadow_signals s JOIN (SELECT signal_id FROM v10_shadow_markouts "
        "GROUP BY signal_id HAVING count(DISTINCT horizon_seconds)=?) c ON c.signal_id=s.id",
        (len(V10_MARKOUT_HORIZONS),),
    ).fetchone()[0]
    v10_lcb = {}
    for horizon in V10_MARKOUT_HORIZONS:
        clusters = [float(row[0]) for row in state.db.execute(
            "SELECT avg(CAST(m.stressed_cents AS REAL)) FROM v10_shadow_markouts m "
            "JOIN v10_shadow_signals s ON s.id=m.signal_id "
            "WHERE m.horizon_seconds=? GROUP BY coalesce(s.event_id,s.ticker)",
            (horizon,),
        ).fetchall()]
        v10_lcb[str(horizon)] = None if len(clusters) < 2 else (
            statistics.mean(clusters) - 1.96 * statistics.stdev(clusters) / len(clusters) ** .5
        )
    v11_marks = {str(h): state.db.execute(
        "SELECT count(*) FROM v11_shadow_markouts WHERE horizon_seconds=?", (h,)
    ).fetchone()[0] for h in V11_MARKOUT_HORIZONS}
    v11_pnl = {str(h): state.db.execute(
        "SELECT coalesce(sum(CAST(stressed_cents AS REAL)),0) FROM v11_shadow_markouts "
        "WHERE horizon_seconds=?", (h,)
    ).fetchone()[0] for h in V11_MARKOUT_HORIZONS}
    v11_evaluation_rows = state.db.execute(
        "SELECT reason,count,last_at FROM v11_shadow_evaluations"
    ).fetchall()
    v11_complete_signals = state.db.execute(
        "SELECT count(*) FROM (SELECT signal_id FROM v11_shadow_markouts "
        "GROUP BY signal_id HAVING count(DISTINCT horizon_seconds)=?)",
        (len(V11_MARKOUT_HORIZONS),),
    ).fetchone()[0]
    v11_independent_events = state.db.execute(
        "SELECT count(DISTINCT s.event_id) FROM v11_shadow_signals s JOIN "
        "(SELECT signal_id FROM v11_shadow_markouts GROUP BY signal_id "
        "HAVING count(DISTINCT horizon_seconds)=?) c ON c.signal_id=s.id",
        (len(V11_MARKOUT_HORIZONS),),
    ).fetchone()[0]
    v11_lcb = {}
    v11_cluster_means = {}
    for horizon in V11_MARKOUT_HORIZONS:
        clusters = [float(row[0]) for row in state.db.execute(
            "SELECT avg(CAST(m.stressed_cents AS REAL)) FROM v11_shadow_markouts m "
            "JOIN v11_shadow_signals s ON s.id=m.signal_id "
            "WHERE m.horizon_seconds=? GROUP BY s.event_id", (horizon,),
        ).fetchall()]
        v11_cluster_means[str(horizon)] = None if not clusters else statistics.mean(clusters)
        v11_lcb[str(horizon)] = None if len(clusters) < 2 else (
            statistics.mean(clusters) - 1.96 * statistics.stdev(clusters) / len(clusters) ** .5
        )
    v11_early_stop = v11_independent_events >= 15 and any(
        value is not None and value <= -1 for value in v11_cluster_means.values()
    )
    v12_marks = {str(h): state.db.execute(
        "SELECT count(*) FROM v12_shadow_markouts WHERE horizon_seconds=?", (h,)
    ).fetchone()[0] for h in V12_MARKOUT_HORIZONS}
    v12_pnl = {str(h): state.db.execute(
        "SELECT coalesce(sum(CAST(stressed_cents AS REAL)),0) FROM v12_shadow_markouts "
        "WHERE horizon_seconds=?", (h,)
    ).fetchone()[0] for h in V12_MARKOUT_HORIZONS}
    v12_evaluation_rows = state.db.execute(
        "SELECT reason,count,last_at FROM v12_shadow_evaluations"
    ).fetchall()
    v12_evaluations = sum(row[1] for row in v12_evaluation_rows)
    v12_complete_signals = state.db.execute(
        "SELECT count(*) FROM (SELECT signal_id FROM v12_shadow_markouts "
        "GROUP BY signal_id HAVING count(DISTINCT horizon_seconds)=?)",
        (len(V12_MARKOUT_HORIZONS),),
    ).fetchone()[0]
    v12_independent_events = state.db.execute(
        "SELECT count(DISTINCT s.event_id) FROM v12_shadow_signals s JOIN "
        "(SELECT signal_id FROM v12_shadow_markouts GROUP BY signal_id "
        "HAVING count(DISTINCT horizon_seconds)=?) c ON c.signal_id=s.id",
        (len(V12_MARKOUT_HORIZONS),),
    ).fetchone()[0]
    v12_lcb = {}
    v12_cluster_means = {}
    for horizon in V12_MARKOUT_HORIZONS:
        clusters = [float(row[0]) for row in state.db.execute(
            "SELECT avg(CAST(m.stressed_cents AS REAL)) FROM v12_shadow_markouts m "
            "JOIN v12_shadow_signals s ON s.id=m.signal_id "
            "WHERE m.horizon_seconds=? GROUP BY s.event_id", (horizon,),
        ).fetchall()]
        v12_cluster_means[str(horizon)] = None if not clusters else statistics.mean(clusters)
        v12_lcb[str(horizon)] = None if len(clusters) < 2 else (
            statistics.mean(clusters) - 1.96 * statistics.stdev(clusters) / len(clusters) ** .5
        )
    v12_profitability_stop = v12_independent_events >= 15 and any(
        value is not None and value <= -1 for value in v12_cluster_means.values()
    )
    v12_productivity_stop = v12_evaluations >= 25_000 and v12_independent_events < 10
    discovery = state.load("market_discovery", {})
    last_completed_discovery = state.load("last_completed_market_discovery", {})
    sample = state.load("sampling_window", {})
    return {
        "environment": "demo", "post_only": True,
        "market_discovery": {
            key: discovery.get(key) for key in (
                "in_progress", "generation", "pages", "markets_scanned",
                "eligible_markets", "eligible_events", "selected_markets",
                "research_relevant_markets", "research_relevant_events",
                "favorite_longshot_markets", "favorite_maker_markets",
                "weather_ensemble_markets", "nested_threshold_markets",
                "coverage_accounting_complete",
                "market_families", "eligible_families", "admission_rejections",
                "started_at", "completed_at",
            )
        } | {"last_completed": last_completed_discovery},
        "sampling_window": {
            key: sample.get(key) for key in (
                "started_at", "offset", "next_offset", "size", "cohort_size",
            )
        },
        "markets": len({row[1] for row in metadata}),
        "post_only_attempts": len(maker), "terminal_orders": sum(r["state"] == "terminal" for r in maker),
        "queue_position_records": state.db.execute("SELECT count(DISTINCT client_id) FROM queue_records").fetchone()[0],
        "working_quote_records": state.db.execute(
            "SELECT count(*) FROM working_quote_observations").fetchone()[0],
        "maker_fills": fills, "actual_fee_records": fee_records,
        "flow_context_records": sum(
            row[0] in filled_maker_ids
            for row in state.db.execute("SELECT client_id FROM flow_context")
        ),
        "markout_records": marks, "side_attempts": sides,
        "unresolved_orders": sum(r["state"] in ("uncertain", "working") for r in maker),
        # BinaryJournal accepts only whole-contract intents. Split exchange
        # executions are aggregated back to that integral fill count, but the
        # accounting engine represents positions as Fraction. Convert at this
        # JSON boundary so a valid one-contract position cannot crash status
        # reporting and the worker process.
        "ending_position_contracts": int(sum(
            abs(v) for v in journal.accounting()["positions"].values()
        )),
        "v10_shadow": {
            "strategy_id": V10_STRATEGY_ID,
            "execution_enabled": False,
            "signals": state.db.execute("SELECT count(*) FROM v10_shadow_signals").fetchone()[0],
            "evaluations": v10_evaluations,
            "last_evaluation_at": v10_last_evaluation,
            "rejection_reasons": {
                reason: count for reason, count, _at in evaluation_rows if reason != "signal"
            },
            "complete_signals": complete_signals,
            "independent_events": independent_events,
            "markout_records": v10_marks,
            "stressed_markout_pnl_cents": v10_pnl,
            "event_cluster_lcb_cents": v10_lcb,
        },
        "v11_shadow": {
            "strategy_id": V11_STRATEGY_ID,
            "execution_enabled": False,
            "signals": state.db.execute("SELECT count(*) FROM v11_shadow_signals").fetchone()[0],
            "evaluations": sum(row[1] for row in v11_evaluation_rows),
            "last_evaluation_at": max((row[2] for row in v11_evaluation_rows), default=None),
            "rejection_reasons": {
                reason: count for reason, count, _at in v11_evaluation_rows if reason != "signal"
            },
            "complete_signals": v11_complete_signals,
            "independent_events": v11_independent_events,
            "markout_records": v11_marks,
            "stressed_markout_pnl_cents": v11_pnl,
            "event_cluster_mean_cents": v11_cluster_means,
            "event_cluster_lcb_cents": v11_lcb,
            "automatic_rejection_triggered": v11_early_stop,
        },
        "v12_shadow": {
            "strategy_id": V12_STRATEGY_ID,
            "execution_enabled": False,
            "signals": state.db.execute("SELECT count(*) FROM v12_shadow_signals").fetchone()[0],
            "evaluations": v12_evaluations,
            "last_evaluation_at": max((row[2] for row in v12_evaluation_rows), default=None),
            "rejection_reasons": {
                reason: count for reason, count, _at in v12_evaluation_rows if reason != "signal"
            },
            "complete_signals": v12_complete_signals,
            "independent_events": v12_independent_events,
            "markout_records": v12_marks,
            "stressed_markout_pnl_cents": v12_pnl,
            "event_cluster_mean_cents": v12_cluster_means,
            "event_cluster_lcb_cents": v12_lcb,
            "profitability_early_stop_triggered": v12_profitability_stop,
            "productivity_stop_triggered": v12_productivity_stop,
            "automatic_rejection_triggered": (
                v12_profitability_stop or v12_productivity_stop
            ),
        },
    }


def write_status(path, state, journal, **values):
    current_evidence = evidence(state, journal)
    current_evidence['depth_replenishment'] = depth_status(state.db)
    current_evidence['public_trade_liquidity'] = trade_probe_status(state.db)
    current_evidence['targeted_trade_liquidity'] = targeted_trade_probe_status(state.db)
    current_evidence['depth_cohort_trades'] = depth_trade_probe_status(state.db)
    current_evidence['trade_depth_overlap'] = trade_depth_overlap_report(state.db)
    current_evidence['scan_trade_capture'] = scan_trade_capture_status(state.db)
    current_evidence["v12_quote_holdout"] = evaluate_v12_holdout(state.db)
    latest_v12 = state.db.execute(
        "SELECT detail,observed_at,ticker FROM v12_shadow_signals ORDER BY id DESC LIMIT 1"
    ).fetchone()
    current_evidence["v12_crossing_feasibility"] = (
        {"available": False, "reason": "no_v12_signal", "execution_enabled": False}
        if latest_v12 is None else {
            "available": True,
            "observed_at": latest_v12[1],
            "ticker": latest_v12[2],
            **crossing_diagnostic(json.loads(latest_v12[0])),
        }
    )
    trial = v12_trial_status(state, journal, current_evidence["v12_shadow"])
    fillability = v12_fillability_status(
        state, journal, current_evidence["v12_shadow"]
    )
    payload = {"at": now_iso(), "environment": "demo", "production_execution_enabled": False,
               "strategy_id": STRATEGY_ID,
               "strategy_execution_enabled": EXECUTION_ENABLED,
               "execution_policy_id": EXECUTION_POLICY_ID,
               "execution_disabled_reason": EXECUTION_DISABLED_REASON,
               "v12_demo_trial_enabled": V12_TRIAL_ENABLED,
               "v12_demo_trial_policy_id": V12_TRIAL_POLICY_ID,
               "v12_demo_trial": trial,
               "v12_fillability_trial_enabled": V12_FILLABILITY_ENABLED,
               "v12_fillability_trial_policy_id": V12_FILLABILITY_POLICY_ID,
               "v12_fillability_trial": fillability,
               "factory_demo_trial": {
                   "protocol": (factory_protocol := state.load("factory_trial_protocol")),
                   **factory_trial_counts(state, journal,
                                          strategy_id=(factory_protocol or {}).get("strategy_id")),
                   "execution_environment": "demo",
                   "live_execution_enabled": False,
                   "restart_reconciliation": state.load("factory_restart_reconciliation"),
               },
               "evidence": current_evidence, **values}
    temp = path.with_suffix(".tmp"); temp.write_text(json.dumps(payload, indent=2) + "\n")
    temp.replace(path)
    # Monitoring is deterministic and isolated from execution.  It is bounded
    # to one check per minute and must never stop trading if its own artifact
    # write fails.
    try:
        decision = run_deterministic_monitor(path.parent, payload)
        if decision.get("checked"):
            schedule_life_os_report(path.parent / "monitor" / "review_packet.json",
                                    urgent=bool(decision.get("investigation_needed")))
        if decision.get("investigation_needed"):
            log_event("deterministic_monitor_escalation",
                      triggers=decision.get("triggers", []))
    except Exception as exc:
        log_event("deterministic_monitor_error", error_code=safe_cycle_error(exc))


def recover_or_report(root, state, journal, broker):
    """Reconcile once, remaining alive and read-only while evidence is incomplete."""
    try:
        recover(state, journal, broker)
        return True
    except Exception as exc:
        code = safe_cycle_error(exc)
        if code not in RECONCILIATION_BLOCK_CODES:
            raise
        state.record(None, {"action": "reconciliation_wait", "error_code": code})
        write_status(root / "status.json", state, journal, phase="reconciling",
                     errors=[code], new_submissions_enabled=False,
                     next_read_seconds=RECONCILIATION_WAIT_SECONDS)
        log_event("worker_reconciling", errors=[code], new_submissions_enabled=False,
                  next_read_seconds=RECONCILIATION_WAIT_SECONDS)
        return False


def run(data_root, *, cycles=None):
    root = Path(data_root).resolve(); root.mkdir(parents=True, exist_ok=True)
    lock = acquire(root / "worker.lock"); state = MakerState(root / "worker.sqlite3")
    journal = BinaryJournal(root / "journal.sqlite3", order_limit_cents=ORDER_LIMIT_CENTS,
                            capital_limit_cents=CAPITAL_LIMIT_CENTS, daily_loss_cents=DAILY_LOSS_CENTS)
    client = DemoClient(os.environ["KALSHI_DEMO_API_KEY_ID"], os.environ["KALSHI_DEMO_PRIVATE_KEY_PATH"])
    markets = DemoMarkets(); broker = BinaryDemoBroker(journal, client)
    cohort = state.load("cohort", []); cohort_at = state.load("cohort_at", 0)
    if state.load("cohort_selector") != COHORT_SELECTOR:
        # Keep the last validated cohort serving while the first full-universe
        # paginated scan is assembled incrementally.
        cohort_at = 0
        state.save("cohort_selector", COHORT_SELECTOR)
    scan = int(state.load("scan", 0)); cycle = 0
    try:
        release_legacy_uncertainty_stop(state, journal)
        check_exchange(client)
        while not recover_or_report(root, state, journal, broker):
            cycle += 1
            if cycles is not None and cycle >= cycles:
                return
            time.sleep(RECONCILIATION_WAIT_SECONDS)
            check_exchange(client)
        log_event("worker_started", production_execution_enabled=False)
        while cycles is None or cycle < cycles:
            errors = []
            try:
                research_frame = None
                if any(row["state"] == "uncertain" for row in journal.records()):
                    if not recover_or_report(root, state, journal, broker):
                        cycle += 1
                        if cycles is None or cycle < cycles:
                            time.sleep(RECONCILIATION_WAIT_SECONDS)
                        continue
                position, accounting = current_position(journal, state)
                if (position and position.get("entry_kind") == "factory_trial_entry"
                        and time.time() - float(state.load("factory_trial_last_settlement_check", 0)) >= 60):
                    broker.reconcile_settlements()
                    state.save("factory_trial_last_settlement_check", time.time())
                    position, accounting = current_position(journal, state)
                active = working_order(journal)
                if not position and not active and state.load(V12_TRIAL_START_BALANCE_KEY) is None:
                    activation = broker.snapshot()["balance"]["balance"]
                    state.save(V12_TRIAL_START_BALANCE_KEY, activation)
                    state.save(V12_TRIAL_LAST_FLAT_BALANCE_KEY, activation)
                    state.record(None, {
                        "action": "v12_demo_trial_activated",
                        "strategy_id": V12_TRIAL_STRATEGY_ID,
                        "policy_id": V12_TRIAL_POLICY_ID,
                        "start_balance_cents": activation,
                        "production_execution_enabled": False,
                    })
                if (not position and not active
                        and state.load(V12_FILLABILITY_START_BALANCE_KEY) is None):
                    activation = broker.snapshot()["balance"]["balance"]
                    state.save(V12_FILLABILITY_START_BALANCE_KEY, activation)
                    state.save(V12_FILLABILITY_LAST_FLAT_BALANCE_KEY, activation)
                    state.record(None, {
                        "action": "v12_fillability_trial_activated",
                        "strategy_id": V12_FILLABILITY_STRATEGY_ID,
                        "policy_id": V12_FILLABILITY_POLICY_ID,
                        "start_balance_cents": activation,
                        "production_execution_enabled": False,
                    })
                cohort, cohort_at = refresh_cohort(
                    state, markets, cohort, cohort_at, now=time.time()
                )
                if active:
                    cid = active["payload"]["client_order_id"]
                    queue = client.request("GET", "/portfolio/orders/" + active["broker_id"] + "/queue_position")
                    state.db.execute("INSERT INTO queue_records VALUES(?,?,?)", (cid, time.time(), json.dumps(queue)))
                    refreshed = broker.refresh(cid)
                    ticker = active["payload"]["ticker"]
                    if refreshed["state"] == "working":
                        cancel_reason = None
                        try:
                            quote = markets.quote({"ticker": ticker})
                            frame = one_contract_frame(
                                quote, book_id=str(quote["observed_at"])
                            )
                            active_event = state.db.execute(
                                "SELECT event_id FROM intent_meta WHERE client_id=?", (cid,)
                            ).fetchone()
                            observe_frame(state, ticker, frame,
                                          active_event[0] if active_event else ticker)
                            cancel_reason = observe_working_quote(state, refreshed, frame)
                        except ValueError as error:
                            if str(error) != "missing_book":
                                raise
                        age = time.time() - state.db.execute(
                            "SELECT created_at FROM intent_meta WHERE client_id=?", (cid,)
                        ).fetchone()[0]
                        if cancel_reason or age >= QUOTE_TTL_SECONDS:
                            reason = cancel_reason or "quote_ttl"
                            refreshed = broker.cancel(cid)
                            state.record(ticker, {
                                "action": "maker_cancel", "reason": reason,
                                "client_order_id": cid, "age_seconds": age,
                                "environment": "demo",
                            })
                    if refreshed["filled"]:
                        quote = markets.quote({"ticker": ticker}); frame = one_contract_frame(quote, book_id=str(quote["observed_at"]))
                        register_fill(state, refreshed, frame); record_due_markouts(state, ticker, frame)
                elif position:
                    if position.get("entry_kind") == "factory_trial_entry":
                        # The registered hypothesis is buy-at-ask to settlement.
                        # Do not apply the older maker strategy's timed exit.
                        if (time.time() - position["opened_at"] > 48 * 3600
                                and state.load("factory_trial_overdue_ticker") != position["ticker"]):
                            journal.stop()
                            state.save("factory_trial_overdue_ticker", position["ticker"])
                            state.record(position["ticker"], {
                                "action": "factory_trial_settlement_overdue",
                                "event_id": position["event_id"], "environment": "demo",
                            })
                        write_status(root / "status.json", state, journal,
                                     phase="running", errors=[],
                                     open_positions=len(accounting["positions"]),
                                     cohort_size=len(cohort))
                        cycle += 1
                        if cycles is None or cycle < cycles:
                            time.sleep(2)
                        continue
                    raw, _a, _b = markets.get(position["ticker"]); market = raw["market"]
                    quote = markets.quote({"ticker": position["ticker"]}); frame = one_contract_frame(quote, book_id=str(quote["observed_at"]))
                    observe_frame(state, position["ticker"], frame, position["event_id"])
                    record_due_markouts(state, position["ticker"], frame)
                    bid, _ask, _bs, _as = price_book(frame, position["outcome"])
                    proceeds = cost(bid, Decimal(".07"), 1, False); net = proceeds - position["basis_cents"]
                    if (net >= TAKE_PROFIT_CENTS or net <= -12
                            or time.time() - position["opened_at"] >= MAX_HOLD_SECONDS):
                        price = limit_price_cents(frame, position["outcome"], "sell")
                        submit(state, journal, broker, markets, market, position["outcome"], "sell", price)
                else:
                    pending = pending_markout_ticker(state)
                    if pending:
                        quote = markets.quote({"ticker": pending}); frame = one_contract_frame(quote, book_id=str(quote["observed_at"]))
                        pending_event = state.db.execute(
                            "SELECT event_id FROM intent_meta WHERE ticker=? "
                            "ORDER BY created_at DESC LIMIT 1", (pending,)
                        ).fetchone()
                        observe_frame(state, pending, frame,
                                      pending_event[0] if pending_event else pending)
                        record_due_markouts(state, pending, frame)
                    else:
                        if (time.time() - float(state.load("factory_trial_last_scan", 0))
                                >= FACTORY_TRIAL_SCAN_INTERVAL_SECONDS):
                            state.save("factory_trial_last_scan", time.time())
                            completed_trials = state.load("factory_trial_completed_ids", [])
                            candidate = factory_eligible_candidate(root, excluded=completed_trials)
                            if candidate is not None:
                                flat_balance = broker.snapshot()["balance"]["balance"]
                                allowed, reason = factory_trial_allowed(
                                    state, journal, candidate, flat_balance_cents=flat_balance)
                                if allowed:
                                    prior_net = factory_trial_counts(state, journal)["realized_net_cents"]
                                    plan = factory_recent_signal(
                                        root, candidate, state, markets,
                                        max_total_risk_cents=FACTORY_TRIAL_MAX_LOSS_CENTS + prior_net)
                                    if plan is not None:
                                        result = submit(
                                            state, journal, broker, markets, plan["market"],
                                            plan["outcome"], "buy", plan["price_cents"],
                                            client_id_prefix=FACTORY_TRIAL_CLIENT_ID_PREFIX,
                                            entry_kind="factory_trial_entry",
                                            strategy_id=plan["strategy_id"],
                                        )
                                        state.record(plan["market"]["ticker"], {
                                            "action": "factory_trial_entry", "strategy_id": plan["strategy_id"],
                                            "event_id": plan["event_id"], "state": result["state"],
                                            "filled": result["filled"], "environment": "demo",
                                        })
                                        if result["filled"]:
                                            register_fill(state, result, plan["frame"])
                                        write_status(root / "status.json", state, journal,
                                                     phase="running", errors=[],
                                                     open_positions=int(bool(result["filled"])),
                                                     cohort_size=len(cohort))
                                        cycle += 1
                                        if cycles is None or cycle < cycles:
                                            time.sleep(2)
                                        continue
                                elif reason != state.load("factory_trial_last_skip_reason"):
                                    state.save("factory_trial_last_skip_reason", reason)
                                    state.record(None, {"action": "factory_trial_skip", "reason": reason})
                                if reason in ("attempt_cap", "fill_cap", "negative_demo_net"):
                                    state.save("factory_trial_completed_ids",
                                               completed_trials + [candidate["strategy_id"]])
                        market = next_sampling_market(state, cohort, now=time.time())
                        scan += 1; state.save("scan", scan)
                        frame, signal = scan_market_candidate(
                            state, journal, markets, market
                        )
                        research_frame = frame
                        if frame is not None:
                            v12_signal = frame.get("v12_trial_signal")
                            independent_event = event_id(market)
                            locked = state.db.execute(
                                "SELECT 1 FROM entered_events WHERE event_id=?",
                                (independent_event,),
                            ).fetchone()
                            if v9_submission_allowed(signal, event_locked=bool(locked)):
                                result = submit(state, journal, broker, markets, market, signal["side"], "buy",
                                                signal["price_cents"], maker=True, context=signal)
                                if result["filled"]:
                                    register_fill(state, result, frame)
                            elif v12_signal is not None:
                                improved, improvement_reason = v12_fillability_quote(v12_signal)
                                if improved is None:
                                    state.record(market["ticker"], {
                                        "action": "v12_fillability_skip",
                                        "reason": improvement_reason,
                                        "source_strategy_id": V12_STRATEGY_ID,
                                    })
                                else:
                                    current_evidence = evidence(state, journal)
                                    trial = v12_fillability_status(
                                        state, journal, current_evidence["v12_shadow"],
                                        event_locked=(
                                            bool(locked)
                                            or v12_fillability_event_recent(
                                                state, independent_event
                                            )
                                        ),
                                    )
                                if improved is not None and trial["allowed"]:
                                    context = {
                                        **improved,
                                        "execution_policy_id": V12_FILLABILITY_POLICY_ID,
                                    }
                                    result = submit(
                                        state, journal, broker, markets, market,
                                        improved["outcome"], "buy",
                                        improved["price_cents"], maker=True,
                                        context=context,
                                        client_id_prefix=V12_FILLABILITY_CLIENT_ID_PREFIX,
                                    )
                                    state.record(market["ticker"], {
                                        "action": "v12_fillability_trial_entry",
                                        "state": result["state"],
                                        "filled": result["filled"],
                                        "policy_id": V12_FILLABILITY_POLICY_ID,
                                        "source_price_cents": improved[
                                            "source_signal_price_cents"
                                        ],
                                        "submitted_price_cents": improved["price_cents"],
                                    })
                                    if result["filled"]:
                                        register_fill(state, result, frame)
                snapshot = broker.snapshot() if not working_order(journal) else None
                end_accounting = journal.accounting()
                if snapshot is not None and not end_accounting["positions"]:
                    state.save(
                        V12_FILLABILITY_LAST_FLAT_BALANCE_KEY,
                        snapshot["balance"]["balance"],
                    )
                if snapshot is not None and not end_accounting['positions']:
                    if research_frame is not None:
                        try:
                            poll_scan_trade_capture(state.db, markets, research_frame,
                                                    event_id(market), time.time())
                        except (ValueError, TypeError, ArithmeticError):
                            state.save('scan_trade_capture_error', 'invalid_or_stale_scan_frame')
                    poll_trade_probe(state.db,markets,time.time())
                    poll_targeted_trade_probe(state.db,markets,time.time())
                    depth_now = time.time()
                    poll_depth_trade_probe(state.db,markets,depth_now,
                                           signal_candidate=targeted_trade_candidate(state.db,depth_now))
                write_status(root / "status.json", state, journal, phase="running", errors=[],
                             demo_balance_cents=(snapshot or {}).get("balance", {}).get("balance"),
                             open_positions=len(end_accounting["positions"]), cohort_size=len(cohort))
                if cycle % 30 == 0: log_event("worker_heartbeat", **evidence(state, journal))
            except Exception as exc:
                code = safe_cycle_error(exc)
                errors.append(code); state.record(None, {"action": "cycle_error", "error_code": code})
                write_status(root / "status.json", state, journal, phase="blocked", errors=errors)
                log_event("worker_blocked", errors=errors)
            cycle += 1
            if cycles is None or cycle < cycles: time.sleep(2)
    finally:
        journal.close(); state.close(); lock.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="/var/data/kalshi-demo-v9")
    parser.add_argument("--cycles", type=int)
    args = parser.parse_args(argv); run(args.data_root, cycles=args.cycles)


if __name__ == "__main__": main()
