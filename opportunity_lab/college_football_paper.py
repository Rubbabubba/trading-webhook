"""One-game, public-data paper experiment. No credentials or live order API."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import json
import math
from pathlib import Path
import sqlite3
import time
from urllib.parse import quote
from urllib.request import Request, urlopen

from .kalshi_bot import best_ask, number
from .kalshi_market_data import BASE_URL


ESPN = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event="
VERSION = "espn_probability_gap_v1"


def utcnow():
    return datetime.now(timezone.utc)


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timezone is required")
    return parsed


def fetch(url):
    start = time.monotonic()
    try:
        # ESPN's public endpoint accepts urllib's default request; the custom
        # research-agent header used for Kalshi receives a 403 there.
        request = Request(url) if url.startswith(ESPN) else Request(
            url, headers={"Accept": "application/json", "User-Agent": "CollegeFootballPaperResearch/1.0"})
        with urlopen(request, timeout=8) as response:
            payload = json.load(response)
            age = float(response.headers.get("Age", "0"))
            if not math.isfinite(age) or age > 20:
                raise ValueError("HTTP response cache age exceeds 20 seconds")
        return {"ok": True, "data": payload, "received_at": utcnow().isoformat(),
                "duration_seconds": round(time.monotonic() - start, 3), "http_age_seconds": age}
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__, "detail": str(exc)[:250],
                "received_at": utcnow().isoformat()}


def game_state(data, config, now):
    header = data.get("header", {})
    competition = (header.get("competitions") or [{}])[0]
    status = competition.get("status", {})
    teams = {c.get("homeAway"): c for c in competition.get("competitors", [])}
    blockers = []
    if (str(header.get("id")) != config["event_id"]
            or str(teams.get("home", {}).get("id")) != config["home_team_id"]
            or str(teams.get("away", {}).get("id")) != config["away_team_id"]):
        blockers.append("game_or_team_identity_mismatch")
    state = {"event_id": header.get("id"), "state": status.get("type", {}).get("state"),
             "status_name": status.get("type", {}).get("name"), "period": status.get("period"),
             "clock": status.get("displayClock"), "completed": status.get("type", {}).get("completed", False),
             "scores": {side: team.get("score") for side, team in teams.items()}, "blockers": blockers,
             "home_probability": None, "probability_play_id": None}
    if state["state"] != "in" or state["status_name"] != "STATUS_IN_PROGRESS":
        blockers.append("game_not_in_active_play")
    drives = data.get("drives", {})
    all_drives = list(drives.get("previous") or [])
    if drives.get("current"):
        all_drives.append(drives["current"])
    plays = {str(p["id"]): p for drive in all_drives for p in drive.get("plays", []) if p.get("id")}
    # ESPN play ids increase within a game; sequenceNumber can repeat for timeouts.
    latest = max(plays.values(), key=lambda p: int(p["id"]) if str(p["id"]).isdigit() else -1, default={})
    probabilities = data.get("winprobability") or []
    probability = probabilities[-1] if probabilities else {}
    state["probability_play_id"] = probability.get("playId")
    state["last_play"] = {k: latest.get(k) for k in ("id", "wallclock", "modified", "homeScore", "awayScore", "period", "clock", "end")}
    p, tie = number(probability.get("homeWinPercentage")), number(probability.get("tiePercentage", 0))
    if p is None or not 0 <= p <= 1 or tie is None or tie != 0:
        blockers.append("live_probability_missing_or_invalid")
    elif str(probability.get("playId")) != str(latest.get("id")) or not latest:
        blockers.append("probability_not_for_latest_play")
    else:
        state["home_probability"] = float(p)
    try:
        age = (now - timestamp(latest["wallclock"])).total_seconds()
        state["play_age_seconds"] = age
        if age < -5 or age > config["max_signal_age_seconds"]:
            blockers.append("play_timestamp_stale")
    except (KeyError, TypeError, ValueError, AttributeError):
        blockers.append("play_timestamp_missing")
    for side in ("home", "away"):
        if str(latest.get(side + "Score")) != str(state["scores"].get(side)):
            blockers.append("score_and_probability_out_of_sync")
            break
    return state


def quotes(book):
    ask = best_ask(book, "yes")
    opposite_ask = best_ask(book, "no")
    if ask is None or opposite_ask is None:
        return None
    bid = Decimal(1) - opposite_ask[0]
    if not 0 < bid < ask[0] < 1:
        return None
    return {"ask": float(ask[0]), "ask_size": float(ask[1]),
            "bid": float(bid), "bid_size": float(opposite_ask[1])}


def collect(config):
    urls = {"game": ESPN + quote(config["event_id"], safe=""),
            "fees": BASE_URL + "/series/KXNCAAFGAME"}
    for side, ticker in config["markets"].items():
        urls[side] = BASE_URL + "/markets/" + quote(ticker, safe="")
        urls[side + "_book"] = urls[side] + "/orderbook?depth=1"
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = dict(zip(urls, pool.map(fetch, urls.values())))
    now = utcnow()
    state = game_state(results["game"].get("data", {}), config, now)
    series = results["fees"].get("data", {}).get("series", {})
    fee_ok = (results["fees"]["ok"] and series.get("fee_type") in {"quadratic", "quadratic_with_maker_fees"}
              and number(series.get("fee_multiplier")) == 1)
    observations = {"at": now.isoformat(), "game": state, "markets": {}, "fee_metadata_matches": fee_ok,
                    "transport": {key: {k: v for k, v in result.items() if k != "data"} for key, result in results.items()}}
    for side, ticker in config["markets"].items():
        market = results[side].get("data", {}).get("market", {})
        quote_data = quotes(results[side + "_book"].get("data", {}))
        observations["markets"][side] = {"ticker": ticker, "status": market.get("status"),
             "settlement_confirmed": (results[side]["ok"] and market.get("ticker") == ticker
                                      and market.get("market_type") == "binary"),
             "result": market.get("result"), "settlement_value_dollars": market.get("settlement_value_dollars"),
             "title": market.get("title"), "rules_primary": market.get("rules_primary"),
             "rules_secondary": market.get("rules_secondary"), "quote": quote_data,
             "valid": (results[side]["ok"] and results[side + "_book"]["ok"]
                       and market.get("ticker") == ticker and market.get("market_type") == "binary"
                       and market.get("status") == "active" and quote_data is not None
                       and all(results[k].get("duration_seconds", 999) <= 5 for k in (side, side + "_book"))
                       and all((now - timestamp(results[k]["received_at"])).total_seconds() <= 10 for k in (side, side + "_book")))}
    return observations


def fee_cents(price, count, config):
    p = Decimal(str(price))
    return int((Decimal(str(config["fee_coefficient"])) * count * p * (1 - p) * 100).to_integral_value(rounding=ROUND_CEILING))


def value_cents(price, count, config, buying):
    rounding = ROUND_CEILING if buying else ROUND_FLOOR
    gross = int((Decimal(str(price)) * count * 100).to_integral_value(rounding=rounding))
    cost = fee_cents(price, count, config) + math.ceil(config["slippage_per_contract"] * count * 100 - 1e-9)
    return gross + cost if buying else max(0, gross - cost)


def account(state):
    return {"cash_cents": state["cash_cents"], "realized_paper_profit_cents": state["realized_cents"],
            "position": state["position"], "pending": state["pending"], "entries": state["entries"],
            "exits": state["exits"], "max_drawdown_cents": state["max_drawdown_cents"],
            "halted": state["halted"], "mark_equity_cents": state.get("mark_equity_cents"),
            "mark_available": state.get("mark_available", False)}


def decide(state, obs, config, now, paused=False):
    """Mutate a paper account using only this observation and saved past state."""
    actions = []
    position = state["position"]
    game = obs["game"]
    signal_ok = not game["blockers"] and obs["transport"]["game"]["ok"]
    if obs["transport"]["game"].get("duration_seconds", 999) > 5:
        signal_ok = False
    if paused:
        state["pending"] = None
    # Settle only from Kalshi's finalized result, never from ESPN's final score.
    if position:
        market = obs["markets"][position["side"]]
        if market.get("settlement_confirmed") and market["status"] == "finalized" and market["result"] in {"yes", "no"}:
            payout = position["count"] * 100 if market["result"] == "yes" else 0
            state["cash_cents"] += payout
            state["realized_cents"] += payout - position["cost_cents"]
            state["exits"] += 1
            actions.append({"action": "settle", "payout_cents": payout, "position": position})
            state["position"] = state["pending"] = None
            state["last_exit_at"] = now.isoformat()
            position = None
    equity = state["cash_cents"]
    mark_available = True
    if position:
        market = obs["markets"][position["side"]]
        q = market["quote"]
        mark_available = bool(market["valid"] and q["bid_size"] >= position["count"])
        equity += value_cents(q["bid"], position["count"], config, False) if mark_available else 0
    state["mark_available"] = mark_available
    state["mark_equity_cents"] = equity if mark_available else None
    if mark_available:
        state["peak_equity_cents"] = max(state["peak_equity_cents"], equity)
        state["max_drawdown_cents"] = max(state["max_drawdown_cents"], state["peak_equity_cents"] - equity)
        if config["initial_cash_cents"] - equity >= config["max_loss_cents"]:
            state["halted"] = True

    pending = state["pending"]
    if pending:
        elapsed = (now - timestamp(pending["at"])).total_seconds()
        if elapsed < config["confirmation_seconds"]:
            return actions + [{"action": "wait", "reason": "confirmation_delay"}]
        market = obs["markets"][pending["side"]]
        q = market["quote"]
        valid = market["valid"] and elapsed <= config["pending_expiry_seconds"] and not paused
        if pending["action"] == "buy":
            valid = valid and signal_ok and obs["fee_metadata_matches"] and not state["halted"] and position is None
            if valid:
                p = game["home_probability"] if pending["side"] == "home" else 1 - game["home_probability"]
                price = max(q["ask"], pending["price"])
                cost = value_cents(price, pending["count"], config, True)
                # Include a reserve for an eventual exit fee and exit slippage.
                exit_reserve = fee_cents(p, pending["count"], config) + math.ceil(config["slippage_per_contract"] * pending["count"] * 100)
                edge = p - (cost + exit_reserve) / (100 * pending["count"])
                valid = (q["ask"] <= pending["price"] + config["max_price_move"]
                         and q["ask_size"] >= pending["count"] and q["ask"] - q["bid"] <= config["max_spread"]
                         and cost <= min(config["max_trade_cents"], state["cash_cents"])
                         and edge >= config["entry_edge"])
            if valid:
                state["cash_cents"] -= cost
                state["position"] = {"side": pending["side"], "ticker": market["ticker"], "count": pending["count"],
                                     "cost_cents": cost, "price": price, "opened_at": now.isoformat(),
                                     "entry_probability": p, "play_id": game["probability_play_id"]}
                state["entries"] += 1
                state["last_entry_play"] = game["probability_play_id"]
                actions.append({"action": "buy", "position": state["position"], "edge": edge})
        else:
            valid = valid and position is not None
            if valid:
                valid = q["bid_size"] >= position["count"]
            if valid and pending["reason"] == "remaining_edge_below_hold_margin" and signal_ok:
                p = game["home_probability"] if position["side"] == "home" else 1 - game["home_probability"]
                valid = value_cents(q["bid"], position["count"], config, False) / (100 * position["count"]) >= p - config["exit_hold_margin"]
            if valid:
                price = min(q["bid"], pending["price"])
                proceeds = value_cents(price, position["count"], config, False)
                pnl = proceeds - position["cost_cents"]
                state["cash_cents"] += proceeds
                state["realized_cents"] += pnl
                state["exits"] += 1
                state["position"] = None
                state["last_exit_at"] = now.isoformat()
                actions.append({"action": "sell", "position": position, "price": price, "proceeds_cents": proceeds,
                                "profit_cents": pnl, "reason": pending["reason"]})
        state["pending"] = None
        if not valid:
            actions.append({"action": "cancel_paper_intent", "reason": "confirmation_failed", "intent": pending})
        return actions

    if paused:
        return actions + [{"action": "wait", "reason": "operator_pause"}]
    if position:
        market = obs["markets"][position["side"]]
        q = market["quote"]
        if not market["valid"] or q["bid_size"] < position["count"]:
            return actions + [{"action": "wait", "reason": "exit_liquidity_or_market_unavailable"}]
        proceeds = value_cents(q["bid"], position["count"], config, False)
        held = (now - timestamp(position["opened_at"])).total_seconds()
        reason = None
        if state["halted"]:
            reason = "account_loss_limit"
        elif proceeds <= position["cost_cents"] * (1 - config["stop_loss_fraction"]):
            reason = "position_loss_limit"
        elif held >= config["max_hold_seconds"]:
            reason = "maximum_hold_time"
        elif game["completed"]:
            reason = "game_finished"
        elif not signal_ok:
            reason = "signal_unavailable"
        else:
            p = game["home_probability"] if position["side"] == "home" else 1 - game["home_probability"]
            if proceeds / (100 * position["count"]) >= p - config["exit_hold_margin"]:
                reason = "remaining_edge_below_hold_margin"
        if reason:
            state["pending"] = {"action": "sell", "side": position["side"], "price": q["bid"],
                                "at": now.isoformat(), "reason": reason}
            return actions + [{"action": "stage_sell", "reason": reason}]
        return actions + [{"action": "hold", "reason": "remaining_edge"}]

    if (state["halted"] or state["entries"] >= config["max_entries"] or not signal_ok
            or not obs["fee_metadata_matches"]):
        return actions + [{"action": "wait", "reason": "entry_gate", "signal_blockers": game["blockers"]}]
    if (state["last_entry_play"] == game["probability_play_id"]
            or (state["last_exit_at"] and (now - timestamp(state["last_exit_at"])).total_seconds() < config["cooldown_seconds"])):
        return actions + [{"action": "wait", "reason": "cooldown_or_same_play"}]
    choices = []
    for side, market in obs["markets"].items():
        q = market["quote"]
        if not market["valid"] or q["ask"] - q["bid"] > config["max_spread"]:
            continue
        p = game["home_probability"] if side == "home" else 1 - game["home_probability"]
        for count in range(min(config["max_contracts"], math.floor(q["ask_size"])), 0, -1):
            cost = value_cents(q["ask"], count, config, True)
            reserve = fee_cents(p, count, config) + math.ceil(config["slippage_per_contract"] * count * 100)
            edge = p - (cost + reserve) / (100 * count)
            if cost <= min(config["max_trade_cents"], state["cash_cents"]) and edge >= config["entry_edge"]:
                choices.append({"action": "buy", "side": side, "count": count, "price": q["ask"],
                                "at": now.isoformat(), "edge": edge})
                break
    if choices:
        state["pending"] = max(choices, key=lambda row: row["edge"])
        return actions + [{"action": "stage_buy", "intent": state["pending"]}]
    return actions + [{"action": "wait", "reason": "no_edge_after_costs"}]


def open_db(path, config):
    db = sqlite3.connect(path, timeout=30)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS experiment(id INTEGER PRIMARY KEY CHECK(id=1), config TEXT NOT NULL, state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY, at TEXT NOT NULL, observation TEXT NOT NULL, actions TEXT NOT NULL, account TEXT NOT NULL);
    """)
    saved_config = json.dumps({"version": VERSION, **config}, sort_keys=True)
    state = {"cash_cents": config["initial_cash_cents"], "realized_cents": 0, "position": None, "pending": None,
             "entries": 0, "exits": 0, "peak_equity_cents": config["initial_cash_cents"], "max_drawdown_cents": 0,
             "halted": False, "last_entry_play": None, "last_exit_at": None}
    with db:
        db.execute("INSERT OR IGNORE INTO experiment VALUES(1,?,?)", (saved_config, json.dumps(state)))
    if db.execute("SELECT config FROM experiment WHERE id=1").fetchone()[0] != saved_config:
        db.close()
        raise ValueError("This ledger uses a different config/version; use a separate output directory")
    return db


def step(db, observation, config, now, paused=False):
    with db:
        db.execute("BEGIN IMMEDIATE")
        state = json.loads(db.execute("SELECT state FROM experiment WHERE id=1").fetchone()[0])
        actions = decide(state, observation, config, now, paused)
        # Re-mark the resulting account after a fill or settlement in this sample.
        position = state["position"]
        equity, mark_available = state["cash_cents"], True
        if position:
            market = observation["markets"][position["side"]]
            q = market["quote"]
            mark_available = bool(market["valid"] and q["bid_size"] >= position["count"])
            if mark_available:
                equity += value_cents(q["bid"], position["count"], config, False)
        state["mark_available"] = mark_available
        state["mark_equity_cents"] = equity if mark_available else None
        if mark_available:
            state["peak_equity_cents"] = max(state["peak_equity_cents"], equity)
            state["max_drawdown_cents"] = max(state["max_drawdown_cents"], state["peak_equity_cents"] - equity)
            if config["initial_cash_cents"] - equity >= config["max_loss_cents"]:
                state["halted"] = True
        db.execute("UPDATE experiment SET state=? WHERE id=1", (json.dumps(state),))
        result = {"at": now.isoformat(), "game": config["game"], "mode": "paper", "version": VERSION,
                  "execution_enabled": False, "game_state": observation["game"], "account": account(state),
                  "actions": actions, "transport_errors": {k: v for k, v in observation["transport"].items() if not v["ok"]}}
        db.execute("INSERT INTO samples(at,observation,actions,account) VALUES(?,?,?,?)",
                   (now.isoformat(), json.dumps(observation), json.dumps(actions), json.dumps(account(state))))
    return result


def report(db):
    state = json.loads(db.execute("SELECT state FROM experiment WHERE id=1").fetchone()[0])
    count, first, last = db.execute("SELECT count(*),min(at),max(at) FROM samples").fetchone()
    recent = db.execute("SELECT actions FROM samples ORDER BY id DESC LIMIT 1").fetchone()
    return {"mode": "paper", "version": VERSION, "samples": count, "first_sample": first, "last_sample": last,
            "account": account(state), "last_actions": json.loads(recent[0]) if recent else [],
            "limitations": "ESPN probability proxy; delayed public feeds; hypothetical fills; one game cannot establish profitability"}


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temporary.replace(path)


def lock_process(path):
    handle = path.open("a+b")
    handle.seek(0)
    if not handle.read(1):
        handle.write(b"0")
        handle.flush()
    handle.seek(0)
    try:
        import msvcrt
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    except ImportError:
        import fcntl
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise RuntimeError("A collector already holds this output directory")
    return handle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", default="ncaaf_paper/smu_fsu_20260907")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    # Lock down the experiment's money, timing, and identity parameters at startup.
    for key in ("initial_cash_cents", "max_trade_cents", "max_contracts", "max_loss_cents", "max_entries",
                "max_hold_seconds", "max_signal_age_seconds", "poll_seconds", "pregame_poll_seconds",
                "confirmation_seconds", "pending_expiry_seconds", "cooldown_seconds"):
        if not isinstance(config[key], int) or isinstance(config[key], bool) or config[key] <= 0:
            parser.error(f"{key} must be a positive integer")
    for key in ("entry_edge", "exit_hold_margin", "stop_loss_fraction", "max_spread", "slippage_per_contract", "fee_coefficient", "max_price_move"):
        if number(config[key]) is None or not 0 < number(config[key]) < 1:
            parser.error(f"{key} must be between 0 and 1")
    if config["poll_seconds"] < 10 or config["confirmation_seconds"] >= config["pending_expiry_seconds"]:
        parser.error("Invalid polling or confirmation interval")
    if set(config["markets"]) != {"home", "away"} or timestamp(config["stop_at"]) <= timestamp(config["kickoff"]):
        parser.error("Invalid team mapping or stop time")
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    lock = None if args.status else lock_process(output / "collector.lock")
    db = open_db(output / "paper.sqlite3", config)
    try:
        if args.status:
            print(json.dumps(report(db), indent=2))
            return 0
        while True:
            now = utcnow()
            if now >= timestamp(config["stop_at"]):
                write_json(output / "status.json", {"at": now.isoformat(), "worker_state": "deadline_reached", **report(db)})
                break
            try:
                observation = collect(config)
                result = step(db, observation, config, utcnow(), (output / "PAUSE").exists())
                write_json(output / "status.json", {"worker_state": "running", **result})
                write_json(output / "report.json", report(db))
                print(json.dumps({"at": result["at"], "state": observation["game"]["state"],
                                  "actions": result["actions"], "errors": result["transport_errors"]}), flush=True)
                if (observation["game"]["completed"] and "game_or_team_identity_mismatch" not in observation["game"]["blockers"]
                        and result["account"]["position"] is None):
                    write_json(output / "status.json", {"worker_state": "completed", **result})
                    break
            except Exception as exc:
                write_json(output / "status.json", {"at": utcnow().isoformat(), "worker_state": "error", "error": type(exc).__name__})
                print(json.dumps({"at": utcnow().isoformat(), "error": type(exc).__name__}), flush=True)
                if args.once:
                    raise
            if args.once:
                break
            delay = config["pregame_poll_seconds"] if utcnow() < timestamp(config["kickoff"]) else config["poll_seconds"]
            time.sleep(delay)
    except KeyboardInterrupt:
        write_json(output / "status.json", {"at": utcnow().isoformat(), "worker_state": "stopped", **report(db)})
    finally:
        write_json(output / "report.json", report(db))
        db.close()
        if lock:
            lock.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
