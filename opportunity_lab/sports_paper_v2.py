"""Frozen multi-sport paper experiment; public GETs only, never submits orders."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import json
import math
from pathlib import Path
import sqlite3
import time
from urllib.request import Request, urlopen

from .college_football_paper import (account, fee_cents, lock_process, quotes, timestamp,
                                    utcnow, value_cents, write_json)
from .kalshi_bot import number
from .kalshi_market_data import BASE_URL

VERSION = "sports_value_v2.0"
SPORTS = {"mlb": ("baseball/mlb", "KXMLBGAME"), "nfl": ("football/nfl", "KXNFLGAME")}
DEFAULTS = {"initial_cash_cents": 100000, "max_trade_cents": 1000, "max_contracts": 10,
            "max_loss_cents": 2000, "max_entries": 5, "entry_edge": .08, "exit_hold_margin": .02,
            "stop_loss_fraction": .25, "max_entry_loss_fraction": .12, "max_spread": .03,
            "slippage_per_contract": .01, "fee_coefficient": .07, "max_price_move": .02,
            "max_signal_age_seconds": 90, "feed_grace_seconds": 300, "break_grace_seconds": 1800,
            "poll_seconds": 20, "confirmation_seconds": 10, "pending_expiry_seconds": 65,
            "cooldown_seconds": 300, "markout_horizons": [60, 300, 900]}


def fetch(url):
    start = time.monotonic()
    try:
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=8) as response:
            data = json.load(response)
            age = float(response.headers.get("Age", "0"))
            if not math.isfinite(age) or not 0 <= age <= 20:
                raise ValueError("stale_http_cache")
        return {"ok": True, "data": data, "received_at": utcnow().isoformat(),
                "duration": time.monotonic() - start}
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__ + ": " + str(exc)[:160],
                "received_at": utcnow().isoformat()}


def parse_game(data, config, now):
    header = data.get("header", {})
    competition = (header.get("competitions") or [{}])[0]
    status = competition.get("status", {})
    kind = status.get("type", {})
    teams = {t.get("homeAway"): t for t in competition.get("competitors", [])}
    identity = (str(header.get("id")) == config["event_id"] and all(
        str(teams.get(side, {}).get("id")) == config[side + "_team_id"] for side in ("home", "away")))
    blockers = [] if identity else ["identity_mismatch"]
    plays = list(data.get("plays") or []) if config["league"] == "mlb" else []
    drives = data.get("drives") or {}
    for drive in list(drives.get("previous") or []) + ([drives["current"]] if drives.get("current") else []):
        plays.extend(drive.get("plays") or [])
    plays = sorted({str(p["id"]): p for p in plays if str(p.get("id", "")).isdigit()}.values(), key=lambda p: int(p["id"]))
    probabilities = data.get("winprobability") or []
    probability = probabilities[-1] if probabilities else {}
    linked = next((p for p in plays if str(p["id"]) == str(probability.get("playId"))), {})
    # Baseball emits an End Batter/Pitcher marker after the probability-linked
    # play. Permit only that marker within the same at-bat, with equal scores.
    later = [p for p in plays if linked and int(p["id"]) > int(linked["id"])]
    compatible = bool(linked) and all(config["league"] == "mlb"
        and p.get("type", {}).get("type") == "end-batterpitcher"
        and p.get("atBatId") == linked.get("atBatId") and linked.get("atBatId")
        and all(p.get(s + "Score") == linked.get(s + "Score") for s in ("home", "away")) for p in later)
    p, tie = number(probability.get("homeWinPercentage")), number(probability.get("tiePercentage", 0))
    if p is None or not 0 <= p <= 1 or tie is None or tie != 0:
        blockers.append("probability_missing_or_tie_unsupported")
        p = None
    if not compatible:
        blockers.append("probability_play_mismatch")
    scores = {s: teams.get(s, {}).get("score") for s in ("home", "away")}
    if not linked or any(str(linked.get(s + "Score")) != str(scores[s]) for s in scores):
        blockers.append("score_mismatch")
    age = None
    try:
        age = (now - timestamp(linked["wallclock"])).total_seconds()
        if age < -5 or age > config["max_signal_age_seconds"]:
            blockers.append("old_or_inconsistent_play_timestamp")
    except (KeyError, TypeError, ValueError, AttributeError):
        blockers.append("play_timestamp_missing")
    phase = kind.get("name")
    scheduled_break = phase in {"STATUS_HALFTIME", "STATUS_END_PERIOD"}
    if config["league"] == "mlb" and plays and plays[-1].get("outs") == 3:
        scheduled_break = True
    active = kind.get("state") == "in" and phase == "STATUS_IN_PROGRESS" and not kind.get("completed")
    if not active or scheduled_break:
        blockers.append("not_active_play")
    return {"identity_ok": identity, "state": kind.get("state"), "phase": phase,
            "completed": bool(identity and kind.get("completed")), "scheduled_break": scheduled_break,
            "period": status.get("period"), "clock": status.get("displayClock"), "scores": scores,
            "home_probability": float(p) if p is not None else None, "play_id": probability.get("playId"),
            "play_age_seconds": age, "play_wallclock": linked.get("wallclock"), "play_modified": linked.get("modified"),
            "last_play_id": plays[-1].get("id") if plays else None,
            "situation": {k: linked.get(k) for k in ("period", "clock", "end", "outs", "pitchCount", "resultCount", "atBatId")},
            "blockers": list(dict.fromkeys(blockers))}


def fee_supported(series, ticker, config):
    multiplier = number(series.get("fee_multiplier"))
    return (series.get("ticker") == ticker
            and series.get("fee_type") in {"quadratic", "quadratic_with_maker_fees"}
            and multiplier is not None and 0 <= multiplier <= config["fee_coefficient"] / .07)


def collect(config):
    path, series_ticker = SPORTS[config["league"]]
    urls = {"game": f"https://site.api.espn.com/apis/site/v2/sports/{path}/summary?event={config['event_id']}",
            "fees": BASE_URL + "/series/" + series_ticker}
    for side, ticker in config["markets"].items():
        urls[side] = BASE_URL + "/markets/" + ticker
        urls[side + "_book"] = urls[side] + "/orderbook?depth=1"
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = dict(zip(urls, pool.map(fetch, urls.values())))
    now = utcnow()
    def fresh(key):
        r = results[key]
        return r["ok"] and r.get("duration", 999) <= 5 and (now - timestamp(r["received_at"])).total_seconds() <= 10
    series = results["fees"].get("data", {}).get("series", {})
    obs = {"at": now.isoformat(), "game": parse_game(results["game"].get("data", {}), config, now),
           "game_transport_ok": fresh("game"), "markets": {},
           "fee_ok": fresh("fees") and fee_supported(series, series_ticker, config),
           "fee_metadata": {"type": series.get("fee_type"), "multiplier": series.get("fee_multiplier"),
                            "modeled_coefficient": config["fee_coefficient"], "rounding": "whole_cent_conservative"},
           "transport": {k: {a: b for a, b in v.items() if a != "data"} for k, v in results.items()}}
    for side, ticker in config["markets"].items():
        market = results[side].get("data", {}).get("market", {})
        q = quotes(results[side + "_book"].get("data", {}))
        identity = market.get("ticker") == ticker and market.get("event_ticker") == config["market_event"] and market.get("market_type") == "binary"
        obs["markets"][side] = {"ticker": ticker, "quote": q, "valid": identity and fresh(side) and fresh(side + "_book") and market.get("status") == "active" and q is not None,
                               "settlement_ok": identity and fresh(side) and market.get("status") == "finalized",
                               "result": market.get("result"), "rules": market.get("rules_primary"),
                               "secondary_rules": market.get("rules_secondary")}
    return obs


def entry_check(market, probability, count, config, cash, staged_price=None):
    if not market["valid"] or probability is None:
        return None
    q = market["quote"]
    price = max(q["ask"], staged_price) if staged_price is not None else q["ask"]
    if (min(q["ask_size"], q["bid_size"]) < count or q["ask"] - q["bid"] > config["max_spread"] + 1e-9
            or (staged_price is not None and q["ask"] > staged_price + config["max_price_move"] + 1e-9)):
        return None
    cost = value_cents(price, count, config, True)
    liquidation = value_cents(q["bid"], count, config, False)
    loss_fraction = (cost - liquidation) / cost
    reserve = fee_cents(probability, count, config) + math.ceil(count * config["slippage_per_contract"] * 100)
    edge = probability - (cost + reserve) / (100 * count)
    if (cost > min(config["max_trade_cents"], cash) or loss_fraction > config["max_entry_loss_fraction"]
            or liquidation <= cost * (1 - config["stop_loss_fraction"]) or edge < config["entry_edge"]):
        return None
    return {"price": price, "cost_cents": cost, "entry_liquidation_cents": liquidation, "entry_loss_fraction": loss_fraction, "edge": edge}


def initial_state(config):
    return {"cash_cents": config["initial_cash_cents"], "realized_cents": 0, "position": None, "pending": None,
            "entries": 0, "exits": 0, "peak_equity_cents": config["initial_cash_cents"], "max_drawdown_cents": 0,
            "halted": False, "last_entry_play": None, "last_exit_at": None, "bad_since": None, "break_since": None}


def mark(state, obs, config):
    equity = state["cash_cents"]
    p = state["position"]
    if p:
        market = obs["markets"][p["side"]]
        if not market["valid"] or market["quote"]["bid_size"] < p["count"]:
            state.update(mark_available=False, mark_equity_cents=None)
            return
        equity += value_cents(market["quote"]["bid"], p["count"], config, False)
    state.update(mark_available=True, mark_equity_cents=equity)
    state["peak_equity_cents"] = max(state["peak_equity_cents"], equity)
    state["max_drawdown_cents"] = max(state["max_drawdown_cents"], state["peak_equity_cents"] - equity)
    if config["initial_cash_cents"] - equity >= config["max_loss_cents"]:
        state["halted"] = True


def decide(state, obs, config, now, paused=False):
    game = obs["game"]
    signal_ok = obs["game_transport_ok"] and not game["blockers"]
    quiet = obs["game_transport_ok"] and game["identity_ok"] and game["scheduled_break"]
    if quiet:
        state["break_since"] = state["break_since"] or now.isoformat()
        quiet = (now - timestamp(state["break_since"])).total_seconds() <= config["break_grace_seconds"]
    else:
        state["break_since"] = None
    if signal_ok or quiet:
        state["bad_since"] = None
    else:
        state["bad_since"] = state["bad_since"] or now.isoformat()
    feed_failed = state["bad_since"] and (now - timestamp(state["bad_since"])).total_seconds() >= config["feed_grace_seconds"]
    if paused:
        state["pending"] = None
    position = state["position"]
    if position:
        m = obs["markets"][position["side"]]
        if m["settlement_ok"] and m["result"] in {"yes", "no"}:
            payout = 100 * position["count"] if m["result"] == "yes" else 0
            state["cash_cents"] += payout
            state["realized_cents"] += payout - position["cost_cents"]
            state["exits"] += 1
            state["position"] = state["pending"] = None
            state["last_exit_at"] = now.isoformat()
            return [{"action": "settle", "position": position, "payout_cents": payout}]
    mark(state, obs, config)
    if paused:
        return [{"action": "wait", "reason": "operator_pause"}]
    pending = state["pending"]
    if pending:
        elapsed = (now - timestamp(pending["at"])).total_seconds()
        if elapsed < config["confirmation_seconds"]:
            return [{"action": "wait", "reason": "confirming"}]
        state["pending"] = None
        market = obs["markets"][pending["side"]]
        valid = market["valid"] and elapsed <= config["pending_expiry_seconds"]
        probability = game["home_probability"]
        if probability is not None and pending["side"] == "away":
            probability = 1 - probability
        if pending["action"] == "buy":
            entry = entry_check(market, probability, pending["count"], config, state["cash_cents"], pending["price"]) if valid and signal_ok and obs["fee_ok"] and not state["halted"] and not position else None
            if entry:
                position = {"side": pending["side"], "ticker": market["ticker"], "count": pending["count"],
                            "opened_at": now.isoformat(), "play_id": game["play_id"], "entry_probability": probability, **entry}
                state["position"] = position
                state["cash_cents"] -= entry["cost_cents"]
                state["entries"] += 1
                state["last_entry_play"] = game["play_id"]
                return [{"action": "buy", "position": position}]
        elif valid and position and market["quote"]["bid_size"] >= position["count"]:
            price = min(pending["price"], market["quote"]["bid"])
            proceeds = value_cents(price, position["count"], config, False)
            if pending["reason"] == "fair_value" and (not signal_ok or proceeds / (100 * position["count"]) < probability - config["exit_hold_margin"]):
                return [{"action": "cancel", "reason": "exit_value_changed"}]
            state["cash_cents"] += proceeds
            state["realized_cents"] += proceeds - position["cost_cents"]
            state["exits"] += 1
            state["position"] = None
            state["last_exit_at"] = now.isoformat()
            return [{"action": "sell", "position": position, "price": price, "proceeds_cents": proceeds,
                     "profit_cents": proceeds - position["cost_cents"], "reason": pending["reason"]}]
        return [{"action": "cancel", "reason": "confirmation_failed"}]
    if position:
        m = obs["markets"][position["side"]]
        if not m["valid"] or m["quote"]["bid_size"] < position["count"]:
            return [{"action": "hold", "reason": "exit_unavailable"}]
        proceeds = value_cents(m["quote"]["bid"], position["count"], config, False)
        reason = None
        if state["halted"]:
            reason = "account_loss_limit"
        elif proceeds <= position["cost_cents"] * (1 - config["stop_loss_fraction"]):
            reason = "position_loss_limit"
        elif game["completed"]:
            reason = "game_finished"
        elif feed_failed:
            reason = "persistent_feed_failure"
        elif signal_ok:
            probability = game["home_probability"] if position["side"] == "home" else 1 - game["home_probability"]
            if proceeds / (100 * position["count"]) >= probability - config["exit_hold_margin"]:
                reason = "fair_value"
        if reason:
            state["pending"] = {"action": "sell", "side": position["side"], "price": m["quote"]["bid"], "at": now.isoformat(), "reason": reason}
            return [{"action": "stage_sell", "reason": reason}]
        return [{"action": "hold", "reason": "scheduled_break" if quiet else "feed_grace" if not signal_ok else "remaining_value"}]
    if not signal_ok or not obs["fee_ok"] or state["halted"] or state["entries"] >= config["max_entries"]:
        return [{"action": "wait", "reason": "entry_gate", "blockers": game["blockers"]}]
    if state["last_entry_play"] == game["play_id"] or (state["last_exit_at"] and (now - timestamp(state["last_exit_at"])).total_seconds() < config["cooldown_seconds"]):
        return [{"action": "wait", "reason": "cooldown"}]
    choices = []
    for side, market in obs["markets"].items():
        probability = game["home_probability"] if side == "home" else 1 - game["home_probability"]
        for count in range(config["max_contracts"], 0, -1):
            entry = entry_check(market, probability, count, config, state["cash_cents"])
            if entry:
                choices.append({"action": "buy", "side": side, "count": count, "at": now.isoformat(), **entry})
                break
    if choices:
        state["pending"] = max(choices, key=lambda x: x["edge"])
        return [{"action": "stage_buy", "intent": state["pending"]}]
    return [{"action": "wait", "reason": "edge_or_entry_cost_gate"}]


def open_db(path, config):
    db = sqlite3.connect(path, timeout=30)
    db.executescript("""
    CREATE TABLE IF NOT EXISTS experiment(id INTEGER PRIMARY KEY CHECK(id=1), config TEXT NOT NULL, state TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY, at TEXT NOT NULL, observation TEXT NOT NULL, actions TEXT NOT NULL, account TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS signals(play TEXT PRIMARY KEY, at TEXT NOT NULL, detail TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS markouts(play TEXT, side TEXT, horizon INTEGER, at TEXT, detail TEXT, PRIMARY KEY(play,side,horizon));
    """)
    saved = json.dumps({**config, "version": VERSION}, sort_keys=True)
    with db:
        db.execute("INSERT OR IGNORE INTO experiment VALUES(1,?,?)", (saved, json.dumps(initial_state(config))))
    if db.execute("SELECT config FROM experiment").fetchone()[0] != saved:
        db.close()
        raise ValueError("Frozen configuration mismatch; use a new output directory")
    return db


def measurements(db, obs, config, now):
    # One observation per play, whether traded or skipped. Future observations
    # are recorded separately, never fed back into contemporaneous decisions.
    g = obs["game"]
    if not g["blockers"] and obs["game_transport_ok"]:
        detail = {"home_probability": g["home_probability"], "markets": obs["markets"], "count": config["max_contracts"]}
        db.execute("INSERT OR IGNORE INTO signals VALUES(?,?,?)", (str(g["play_id"]), now.isoformat(), json.dumps(detail)))
    start = (now - timedelta(seconds=max(config["markout_horizons"]) + 60)).isoformat()
    for play, at, raw in db.execute("SELECT play,at,detail FROM signals WHERE at>=?", (start,)).fetchall():
        age = (now - timestamp(at)).total_seconds()
        original = json.loads(raw)
        for horizon in config["markout_horizons"]:
            if not horizon <= age <= horizon + 45:
                continue
            for side, market in obs["markets"].items():
                old = original["markets"][side]
                count = original["count"]
                if not old["valid"] or not market["valid"] or old["quote"]["ask_size"] < count or market["quote"]["bid_size"] < count:
                    continue
                cost = value_cents(old["quote"]["ask"], count, config, True)
                proceeds = value_cents(market["quote"]["bid"], count, config, False)
                db.execute("INSERT OR IGNORE INTO markouts VALUES(?,?,?,?,?)", (play, side, horizon, now.isoformat(),
                    json.dumps({"elapsed_seconds": age, "count": count, "cost_cents": cost, "proceeds_cents": proceeds, "net_cents": proceeds - cost})))


def step(db, obs, config, now, paused=False):
    with db:
        db.execute("BEGIN IMMEDIATE")
        state = json.loads(db.execute("SELECT state FROM experiment").fetchone()[0])
        actions = decide(state, obs, config, now, paused)
        mark(state, obs, config)
        measurements(db, obs, config, now)
        db.execute("UPDATE experiment SET state=?", (json.dumps(state),))
        db.execute("INSERT INTO samples(at,observation,actions,account) VALUES(?,?,?,?)", (now.isoformat(), json.dumps(obs), json.dumps(actions), json.dumps(account(state))))
    return {"at": now.isoformat(), "mode": "paper", "version": VERSION, "game": config["game"], "league": config["league"],
            "execution_enabled": False, "game_state": obs["game"], "account": account(state), "actions": actions,
            "transport_errors": {k: v for k, v in obs["transport"].items() if not v["ok"]}}


def report(db):
    state = json.loads(db.execute("SELECT state FROM experiment").fetchone()[0])
    count, last = db.execute("SELECT count(*),max(at) FROM samples").fetchone()
    return {"mode": "paper", "version": VERSION, "samples": count, "last_sample": last, "account": account(state),
            "signals": db.execute("SELECT count(*) FROM signals").fetchone()[0],
            "markouts": db.execute("SELECT count(*) FROM markouts").fetchone()[0]}


def validate(config):
    if config["league"] not in SPORTS or set(config["markets"]) != {"home", "away"}:
        raise ValueError("Unsupported sport or team mapping")
    if timestamp(config["stop_at"]) <= timestamp(config["kickoff"]):
        raise ValueError("Invalid deadline")
    for key, default in DEFAULTS.items():
        v = config[key]
        if isinstance(default, int) and (not isinstance(v, int) or isinstance(v, bool) or v <= 0):
            raise ValueError(key)
        if isinstance(default, float) and (number(v) is None or not 0 < number(v) < 1):
            raise ValueError(key)
    if not 0 < config["max_entry_loss_fraction"] < config["stop_loss_fraction"] or config["poll_seconds"] < 10:
        raise ValueError("Entry risk must leave room before the stop")
    if config["confirmation_seconds"] >= config["pending_expiry_seconds"]:
        raise ValueError("Invalid confirmation window")


def run(config_path, output, once=False, status=False):
    config = json.loads(Path(config_path).read_text())
    validate(config)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    handle = None if status else lock_process(output / "collector.lock")
    db = open_db(output / "paper.sqlite3", config)
    try:
        if status:
            print(json.dumps(report(db), indent=2))
            return
        while utcnow() < timestamp(config["stop_at"]):
            try:
                obs = collect(config)
                result = step(db, obs, config, utcnow(), (output / "PAUSE").exists() or (output.parent / "PAUSE").exists())
                completed = obs["game"]["completed"] and result["account"]["position"] is None
                write_json(output / "status.json", {"worker_state": "completed" if completed else "running", **result})
                write_json(output / "report.json", report(db))
                print(json.dumps({"at": result["at"], "actions": result["actions"], "errors": result["transport_errors"]}), flush=True)
                if completed or once:
                    return
            except Exception as exc:
                write_json(output / "status.json", {"at": utcnow().isoformat(), "worker_state": "error", "error": str(exc)[:200]})
                if once:
                    raise
            time.sleep(60 if utcnow() < timestamp(config["kickoff"]) else config["poll_seconds"])
        write_json(output / "status.json", {"at": utcnow().isoformat(), "worker_state": "deadline_reached", **report(db)})
    finally:
        write_json(output / "report.json", report(db))
        db.close()
        if handle:
            handle.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    run(args.config, args.output, args.once, args.status)


if __name__ == "__main__":
    main()
